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
	"github.com/ethereum/go-ethereum/qkc/serialize"
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

	return p.verifyAndAddToShards(ctx, conn, slaveInfo.ID, slaveInfo.FullShardIDList)
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

	for i, c := range p.inbound {
		if c == conn {
			copy(p.inbound[i:], p.inbound[i+1:])
			p.inbound[len(p.inbound)-1] = nil
			p.inbound = p.inbound[:len(p.inbound)-1]
			break
		}
	}
	p.mu.Unlock()

	p.log.Info("indexed inbound xshard connection", "remote_id", string(remoteID), "shards", shardList)
}

// SendXshardTx broadcasts an xshard transaction to all connections for a shard.
func (p *XshardPool) SendXshardTx(ctx context.Context, fullShardID uint32, req *wire.AddXshardTxListRequest) error {
	payload, err := serialize.SerializeToBytes(req)
	if err != nil {
		return fmt.Errorf("serialize AddXshardTxListRequest: %w", err)
	}
	return p.broadcast(fullShardID,
		func(c *xshardConn) (*wire.Frame, error) { return c.sendXshardTxList(ctx, payload) },
		func(f *wire.Frame) error { _, err := parseAddXshardTxListResponse(f); return err },
	)
}

// SendBatchXshardTx broadcasts a batch xshard transaction to all connections for a shard.
func (p *XshardPool) SendBatchXshardTx(ctx context.Context, fullShardID uint32, req *wire.BatchAddXshardTxListRequest) error {
	payload, err := serialize.SerializeToBytes(req)
	if err != nil {
		return fmt.Errorf("serialize BatchAddXshardTxListRequest: %w", err)
	}
	return p.broadcast(fullShardID,
		func(c *xshardConn) (*wire.Frame, error) { return c.sendBatchXshardTxList(ctx, payload) },
		func(f *wire.Frame) error { _, err := parseBatchAddXshardTxListResponse(f); return err },
	)
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

// knownRemote reports whether expectedID is self or already known.
func (p *XshardPool) knownRemote(expectedID []byte) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.selfID) > 0 && bytes.Equal(p.selfID, expectedID) {
		return true
	}
	return p.slaveIDs[string(expectedID)]
}

// verifyAndAddToShards verifies the peer and registers the connection.
func (p *XshardPool) verifyAndAddToShards(ctx context.Context, conn *xshardConn, expectedID []byte, expectedShardList []uint32) error {
	// Self connection — already dialed, so close and treat as success.
	p.mu.RLock()
	selfID := p.selfID
	p.mu.RUnlock()
	if len(selfID) > 0 && bytes.Equal(selfID, expectedID) {
		conn.Close()
		p.log.Info("outbound xshard connection skipped: self connection", "remote", conn.RemoteAddr())
		return nil
	}

	id, shardList, err := conn.sendPing(ctx)
	if err != nil {
		conn.Close()
		return fmt.Errorf("ping failed for %s: %w", conn.RemoteAddr(), err)
	}
	// Close on mismatch instead of reproducing Python's leaked connection.
	if !bytes.Equal(id, expectedID) {
		conn.Close()
		return fmt.Errorf("slave id mismatch for %s: expected %x, got %x", conn.RemoteAddr(), expectedID, id)
	}
	if len(shardList) != len(expectedShardList) {
		conn.Close()
		return fmt.Errorf("shard list length mismatch for %s: expected %d, got %d", conn.RemoteAddr(), len(expectedShardList), len(shardList))
	}
	for i := range shardList {
		if shardList[i] != expectedShardList[i] {
			conn.Close()
			return fmt.Errorf("shard list mismatch for %s: expected %v, got %v", conn.RemoteAddr(), expectedShardList, shardList)
		}
	}

	// Outbound connections never receive a PING; set the identity explicitly.
	conn.setRemoteIdentity(id, shardList)

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		return fmt.Errorf("xshard pool closed")
	}

	remoteID := string(id)
	if remoteID != "" && p.slaveIDs[remoteID] {
		p.mu.Unlock()
		conn.Close()
		p.log.Info("outbound xshard connection skipped: duplicate slave id", "remote_id", remoteID, "remote", conn.RemoteAddr())
		return nil
	}
	if remoteID != "" {
		p.slaveIDs[remoteID] = true
	}

	for _, shardID := range shardList {
		p.conns[shardID] = append(p.conns[shardID], conn)
	}
	p.mu.Unlock()

	p.log.Info("verified and added xshard connection", "remote_id", remoteID, "remote", conn.RemoteAddr())
	return nil
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
func (p *XshardPool) broadcast(
	fullShardID uint32,
	send func(*xshardConn) (*wire.Frame, error),
	parse func(*wire.Frame) error,
) error {
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
			resp, err := send(c)
			if err != nil {
				errs[idx] = err
				return
			}
			errs[idx] = parse(resp)
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

// Test helpers

// add indexes conn under a single shard ID, bypassing verification.
func (p *XshardPool) add(fullShardID uint32, conn *xshardConn) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		p.log.Warn("xshard pool closed, closing outbound conn immediately", "remote", conn.RemoteAddr())
		return
	}

	remoteID := string(conn.remoteID())
	if remoteID != "" {
		p.slaveIDs[remoteID] = true
	}

	p.conns[fullShardID] = append(p.conns[fullShardID], conn)
	p.mu.Unlock()
	p.log.Info("added xshard connection", "full_shard_id", fullShardID, "remote", conn.RemoteAddr())
}

// hasSlaveID reports whether the pool tracks the given peer identity.
func (p *XshardPool) hasSlaveID(id []byte) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.slaveIDs[string(id)]
}

// outboundSize returns the number of unique outbound connections.
func (p *XshardPool) outboundSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	seen := make(map[*xshardConn]struct{})
	for _, conns := range p.conns {
		for _, conn := range conns {
			seen[conn] = struct{}{}
		}
	}
	return len(seen)
}

// inboundSize returns the number of tracked inbound connections.
func (p *XshardPool) inboundSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.inbound)
}

// targets returns all full shard IDs that have connections.
func (p *XshardPool) targets() []uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	targets := make([]uint32, 0, len(p.conns))
	for id := range p.conns {
		targets = append(targets, id)
	}
	return targets
}
