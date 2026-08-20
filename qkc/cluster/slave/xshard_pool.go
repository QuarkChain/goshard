// Copyright 2026-2027, QuarkChain.

package slave

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

const defaultDialTimeout = 10 * time.Second

// XshardPool manages slave-to-slave xshard connections, indexed by full shard
// ID. It owns dial, handshake, identity bookkeeping, indexing, and cleanup;
// connections never escape the pool. Closed connections are never evicted.
//
// The pool is add-only (matching Python's SlaveConnectionManager): conns and
// slaveIDs only grow, and a closed or disconnected peer is never removed. The
// only path that clears them is Close. As a result a peer is dialed at most
// once, but also cannot be re-dialed after it drops.
type XshardPool struct {
	mu                   sync.RWMutex
	conns                map[uint32][]*xshardConn
	inbound              []*xshardConn
	slaveIDs             map[string]bool // Known peer identities; add-only, used for outbound dedup.
	selfID               []byte          // This slave's identity.
	localFullShardIDList []uint32
	maxPayloadSize       uint32 // 0 disables the payload limit.
	closed               bool
	log                  log.Logger
}

// Public API

// NewXshardPool creates a pool. selfID is this slave's identity (also the local
// identity sent in PING/PONG). maxPayloadSize 0 disables the payload limit.
func NewXshardPool(selfID []byte, localFullShardIDList []uint32, maxPayloadSize uint32, logger log.Logger) *XshardPool {
	if logger == nil {
		logger = log.Root()
	}
	return &XshardPool{
		conns:                make(map[uint32][]*xshardConn),
		slaveIDs:             make(map[string]bool),
		selfID:               append([]byte(nil), selfID...),
		localFullShardIDList: append([]uint32(nil), localFullShardIDList...),
		maxPayloadSize:       maxPayloadSize,
		log:                  logger,
	}
}

// DialToSlave establishes an outbound xshard connection to the given slave.
// It matches Python's SlaveConnectionManager.connect_to_slave(slave_info).
func (p *XshardPool) DialToSlave(ctx context.Context, slaveInfo wire.SlaveInfo) error {
	if p.knownRemote(slaveInfo.ID) {
		p.log.Info("outbound xshard connection skipped: remote already known", "remote_id", string(slaveInfo.ID))
		return nil
	}

	addr := net.JoinHostPort(string(slaveInfo.Host), strconv.Itoa(int(slaveInfo.Port)))
	nc, err := net.DialTimeout("tcp", addr, defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("dial xshard slave %s: %w", addr, err)
	}
	conn := newXshardConn(nc, p.maxPayloadSize, p.selfID, p.localFullShardIDList, p.log)
	conn.Start()

	id, shardList, err := conn.sendPing(ctx)
	if err != nil {
		conn.Close()
		return fmt.Errorf("ping failed for %s: %w", conn.RemoteAddr(), err)
	}
	// Close on mismatch instead of reproducing Python's leaked connection.
	if !bytes.Equal(id, slaveInfo.ID) {
		conn.Close()
		return fmt.Errorf("slave id mismatch for %s: expected %x, got %x", conn.RemoteAddr(), slaveInfo.ID, id)
	}
	if len(shardList) != len(slaveInfo.FullShardIDList) {
		conn.Close()
		return fmt.Errorf("shard list length mismatch for %s: expected %d, got %d", conn.RemoteAddr(), len(slaveInfo.FullShardIDList), len(shardList))
	}
	for i := range shardList {
		if shardList[i] != slaveInfo.FullShardIDList[i] {
			conn.Close()
			return fmt.Errorf("shard list mismatch for %s: expected %v, got %v", conn.RemoteAddr(), slaveInfo.FullShardIDList, shardList)
		}
	}

	// Outbound connections never receive a PING; set the identity explicitly.
	conn.setRemoteIdentity(id, shardList)

	// Index under the pool lock. The dedup re-check here (not just the
	// pre-check above) is what makes concurrent dials to the same remote safe:
	// two goroutines may both pass the pre-check, but only one wins the locked
	// section and registers; the loser is closed. This matches Python's
	// connect_to_slave, which both pre-checks and is naturally serialized by
	// the event loop.
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		return fmt.Errorf("xshard pool closed")
	}
	if len(id) > 0 && p.slaveIDs[string(id)] {
		p.mu.Unlock()
		conn.Close()
		p.log.Info("xshard connection skipped: duplicate slave id", "remote_id", string(id), "remote", conn.RemoteAddr())
		return nil
	}
	if len(id) > 0 {
		p.slaveIDs[string(id)] = true
	}
	for _, shardID := range shardList {
		p.conns[shardID] = append(p.conns[shardID], conn)
	}
	p.mu.Unlock()
	p.log.Info("indexed xshard connection", "remote_id", string(id), "shards", shardList)
	return nil
}

// HandleInbound takes ownership of an accepted xshard connection.
func (p *XshardPool) HandleInbound(nc net.Conn) {
	conn := newXshardConn(nc, p.maxPayloadSize, p.selfID, p.localFullShardIDList, p.log)

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		p.log.Warn("xshard pool closed, closing inbound conn immediately", "remote", conn.RemoteAddr())
		return
	}
	p.inbound = append(p.inbound, conn)
	p.mu.Unlock()
	p.log.Info("tracked inbound xshard connection", "remote", conn.RemoteAddr())

	conn.Start()

	if !conn.waitUntilPingReceived() {
		p.log.Warn("inbound xshard connection closed before ping", "remote", conn.RemoteAddr())
		// Evict the dead conn from the staging list — it will never be indexed.
		// Safe: the conn is already closed and was never routed. Python has no
		// equivalent cleanup because it never tracks pending conns at all.
		p.mu.Lock()
		p.removeInbound(conn)
		p.mu.Unlock()
		return
	}

	remoteID := conn.remoteID()
	shardList := conn.remoteFullShardIDList()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		return
	}

	// Inbound is not deduplicated — a remote may have multiple connections.
	if len(remoteID) > 0 {
		p.slaveIDs[string(remoteID)] = true
	}

	for _, shardID := range shardList {
		found := false
		for _, c := range p.conns[shardID] {
			if c == conn {
				found = true
				break
			}
		}
		if !found {
			p.conns[shardID] = append(p.conns[shardID], conn)
		}
	}

	p.removeInbound(conn)
	p.mu.Unlock()

	p.log.Info("indexed inbound xshard connection", "remote_id", string(remoteID), "shards", shardList)
}

// SendXshardTx broadcasts an xshard transaction to all connections for a shard.
func (p *XshardPool) SendXshardTx(ctx context.Context, fullShardID uint32, req *wire.AddXshardTxListRequest) error {
	return p.broadcast(fullShardID, func(c *xshardConn) error { return c.sendXshardTxList(ctx, req) })
}

// SendBatchXshardTx broadcasts a batch xshard transaction to all connections for a shard.
func (p *XshardPool) SendBatchXshardTx(ctx context.Context, fullShardID uint32, req *wire.BatchAddXshardTxListRequest) error {
	return p.broadcast(fullShardID, func(c *xshardConn) error { return c.sendBatchXshardTxList(ctx, req) })
}

// Close closes all pool connections.
func (p *XshardPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true

	var allConns []*xshardConn
	for _, conns := range p.conns {
		allConns = append(allConns, conns...)
	}
	allConns = append(allConns, p.inbound...)

	p.conns = nil
	p.inbound = nil
	p.slaveIDs = nil
	p.mu.Unlock()

	seen := make(map[*xshardConn]struct{}, len(allConns))
	for _, conn := range allConns {
		if _, ok := seen[conn]; ok {
			continue
		}
		seen[conn] = struct{}{}
		conn.Close()
	}
	p.log.Info("xshard pool closed", "connections", len(seen))
}

// Internal implementation

// removeInbound evicts conn from the inbound staging list. The caller must hold
// p.mu. Eviction keeps dead (never-PINGed) connections from accumulating in the
// staging list; Close is the only other path that clears it.
func (p *XshardPool) removeInbound(conn *xshardConn) {
	for i, c := range p.inbound {
		if c == conn {
			copy(p.inbound[i:], p.inbound[i+1:])
			p.inbound[len(p.inbound)-1] = nil
			p.inbound = p.inbound[:len(p.inbound)-1]
			break
		}
	}
}

// knownRemote reports whether expectedID is self or already known.
func (p *XshardPool) knownRemote(expectedID []byte) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.selfID) > 0 && bytes.Equal(p.selfID, expectedID) {
		return true
	}
	return p.slaveIDs[string(expectedID)]
}

// get returns a snapshot of connections for a shard.
func (p *XshardPool) get(fullShardID uint32) []*xshardConn {
	p.mu.RLock()
	conns := p.conns[fullShardID]
	result := make([]*xshardConn, len(conns))
	copy(result, conns)
	p.mu.RUnlock()
	return result
}

// broadcast sends a request to every indexed connection and requires all to succeed.
func (p *XshardPool) broadcast(fullShardID uint32, send func(*xshardConn) error) error {
	conns := p.get(fullShardID)
	if len(conns) == 0 {
		return nil
	}

	errs := make([]error, len(conns))
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Add(1)
		go func(idx int, c *xshardConn) {
			defer wg.Done()
			errs[idx] = send(c)
		}(i, conn)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
