// Copyright 2026-2027, QuarkChain.

package slave

import (
	"bytes"
	"context"
	"errors"
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
// ID. It is add-only (Python SlaveConnectionManager parity): connections and
// slave IDs are never evicted; a peer is dialed at most once.
type XshardPool struct {
	mu                   sync.RWMutex
	conns                map[uint32][]*xshardConn
	inbound              []*xshardConn
	slaveIDs             map[string]bool // Known peer identities; add-only, used for outbound dedup.
	selfID               []byte          // This slave's identity.
	handler              XshardHandler   // Serves inbound xshard requests.
	localFullShardIDList []uint32
	maxPayloadSize       uint32 // 0 disables the payload limit.
	closed               bool
	log                  log.Logger
}

// Public API

// NewXshardPool creates a pool. selfID is this slave's identity (also the local
// identity sent in PING/PONG). handler serves inbound xshard requests and must
// not be nil. maxPayloadSize 0 disables the payload limit.
func NewXshardPool(selfID []byte, localFullShardIDList []uint32, maxPayloadSize uint32, handler XshardHandler, logger log.Logger) (*XshardPool, error) {
	if handler == nil {
		return nil, errors.New("xshard handler must not be nil")
	}
	if logger == nil {
		logger = log.Root()
	}
	return &XshardPool{
		conns:                make(map[uint32][]*xshardConn),
		slaveIDs:             make(map[string]bool),
		selfID:               append([]byte(nil), selfID...),
		handler:              handler,
		localFullShardIDList: append([]uint32(nil), localFullShardIDList...),
		maxPayloadSize:       maxPayloadSize,
		log:                  logger,
	}, nil
}

// DialToSlave establishes an outbound xshard connection to the given slave.
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
	conn, err := newXshardConn(nc, p.maxPayloadSize, p.selfID, p.localFullShardIDList, p.handler, p.log)
	if err != nil {
		nc.Close()
		return fmt.Errorf("create xshard conn to %s: %w", addr, err)
	}
	conn.Start()

	id, shardList, err := conn.sendPing(ctx)
	if err != nil {
		conn.Close()
		return fmt.Errorf("ping failed for %s: %w", conn.RemoteAddr(), err)
	}
	// Python leaks the connection on mismatch; close it instead.
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

	// Dedup is re-checked under the lock: concurrent dials to the same remote
	// register at most one connection; the loser is closed.
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
	conn, err := newXshardConn(nc, p.maxPayloadSize, p.selfID, p.localFullShardIDList, p.handler, p.log)
	if err != nil {
		nc.Close()
		p.log.Error("inbound xshard conn rejected", "err", err)
		return
	}

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
		// Evict the dead conn; it will never be indexed.
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

// removeInbound evicts conn from the inbound staging list. The caller must
// hold p.mu.
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
