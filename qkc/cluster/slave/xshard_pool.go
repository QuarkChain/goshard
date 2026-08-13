// Copyright 2026-2027, QuarkChain.

package slave

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// XshardPool manages slave-to-slave xshard connections, indexed by full shard
// ID. Corresponds to Python's SlaveConnectionManager. It is add-only: closed
// connections are never evicted, matching Python.
type XshardPool struct {
	mu       sync.RWMutex
	conns    map[uint32][]*XshardConn
	inbound  []*XshardConn
	slaveIDs map[string]bool // Known remote identities (Python's slave_ids); an identity set, not a connection count. Never removed.
	selfID   []byte          // This slave's identity; connections to it are skipped.
	closed   bool
	log      log.Logger
}

// NewXshardPool creates a new pool. selfID is this slave's identity; connections
// to selfID are skipped.
func NewXshardPool(selfID []byte, logger log.Logger) *XshardPool {
	if logger == nil {
		logger = log.Root()
	}
	return &XshardPool{
		conns:    make(map[uint32][]*XshardConn),
		slaveIDs: make(map[string]bool),
		selfID:   append([]byte(nil), selfID...),
		log:      logger,
	}
}

// add indexes conn under a single shard ID (test helper, bypasses verification).
func (p *XshardPool) add(fullShardID uint32, conn *XshardConn) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		p.log.Warn("xshard pool closed, closing outbound conn immediately", "remote", conn.RemoteAddr())
		return
	}

	remoteID := string(conn.RemoteID())
	if remoteID != "" {
		p.slaveIDs[remoteID] = true
	}

	p.conns[fullShardID] = append(p.conns[fullShardID], conn)
	p.mu.Unlock()
	p.log.Info("added xshard connection", "full_shard_id", fullShardID, "remote", conn.RemoteAddr())
}

// HasSlaveID reports whether the pool already tracks the given slave ID.
func (p *XshardPool) HasSlaveID(id []byte) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.slaveIDs[string(id)]
}

// knownRemote matches Python's pre-dial self/duplicate check (slave.py:857).
func (p *XshardPool) knownRemote(expectedID []byte) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.selfID) > 0 && bytes.Equal(p.selfID, expectedID) {
		return true
	}
	return p.slaveIDs[string(expectedID)]
}

// verifyAndAddToShards verifies the peer and registers the connection, keeping
// the final duplicate check as the pre-dial TOCTOU safety net.
func (p *XshardPool) verifyAndAddToShards(ctx context.Context, conn *XshardConn, expectedID []byte, expectedShardList []uint32) error {
	// Self connection — already dialed, so close and treat as success.
	p.mu.RLock()
	selfID := p.selfID
	p.mu.RUnlock()
	if len(selfID) > 0 && bytes.Equal(selfID, expectedID) {
		conn.Close()
		p.log.Info("outbound xshard connection skipped: self connection", "remote", conn.RemoteAddr())
		return nil
	}

	id, shardList, err := conn.SendPing(ctx)
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
	conn.SetRemoteIdentity(id, shardList)

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

// DialToSlave is the high-level outbound entry point: pre-dial dedup, then dial,
// verify and register (Python's connect_to_slave). The final duplicate check in
// verifyAndAddToShards backs the pre-dial TOCTOU window.
func (p *XshardPool) DialToSlave(
	ctx context.Context,
	addr string,
	maxPayloadSize uint32,
	localID []byte,
	localFullShardIDList []uint32,
	expectedID []byte,
	expectedShardList []uint32,
	logger log.Logger,
) error {
	if p.knownRemote(expectedID) {
		p.log.Info("outbound xshard connection skipped: remote already known", "remote_id", string(expectedID))
		return nil
	}

	conn, err := NewXshardConn(addr, maxPayloadSize, localID, localFullShardIDList, logger)
	if err != nil {
		return err
	}
	conn.Start()

	return p.verifyAndAddToShards(ctx, conn, expectedID, expectedShardList)
}

// Get returns a snapshot of connections for the given full shard ID.
func (p *XshardPool) Get(fullShardID uint32) []*XshardConn {
	p.mu.RLock()
	conns := p.conns[fullShardID]
	result := make([]*XshardConn, len(conns))
	copy(result, conns)
	p.mu.RUnlock()
	return result
}

// SendXshardTx broadcasts to every connection indexed for the shard (CLOSED
// connections included, for Python parity). An empty target set is a silent
// no-op, matching Python's broadcast on an empty future list.
func (p *XshardPool) SendXshardTx(ctx context.Context, fullShardID uint32, payload []byte) (*wire.Frame, error) {
	conns := p.Get(fullShardID)
	if len(conns) == 0 {
		return nil, nil
	}

	// Do not filter CLOSED connections — their failure must propagate (Python parity).
	type result struct {
		resp *wire.Frame
		err  error
	}
	results := make([]result, len(conns))
	var wg sync.WaitGroup

	for i, conn := range conns {
		wg.Add(1)
		go func(idx int, c *XshardConn) {
			defer wg.Done()
			resp, err := c.SendXshardTxList(ctx, payload)
			results[idx] = result{resp: resp, err: err}
		}(i, conn)
	}
	wg.Wait()

	// Every response must decode with error_code == 0 (Python's check(all(...))).
	var firstErr error
	var firstResp *wire.Frame
	for _, r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		if _, err := ParseAddXshardTxListResponse(r.resp); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if firstResp == nil {
			firstResp = r.resp
		}
	}

	if firstErr != nil {
		return nil, firstErr
	}
	return firstResp, nil
}

// TrackInbound registers an inbound connection pending its PING handshake.
func (p *XshardPool) TrackInbound(conn *XshardConn) {
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
}

// WatchAndIndex waits for the inbound PING and indexes the connection.
func (p *XshardPool) WatchAndIndex(conn *XshardConn) bool {
	if !conn.WaitUntilPingReceived() {
		p.log.Warn("inbound xshard connection closed before ping", "remote", conn.RemoteAddr())
		return false
	}

	remoteID := conn.RemoteID()
	shardList := conn.RemoteFullShardIDList()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		return false
	}

	// Inbound is not deduplicated — a remote may have multiple connections.
	if len(remoteID) > 0 {
		p.slaveIDs[string(remoteID)] = true
	}

	// Idempotent: skip if already indexed for this shard.
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
			p.inbound[len(p.inbound)-1] = nil // clear reference to prevent memory leak
			p.inbound = p.inbound[:len(p.inbound)-1]
			break
		}
	}
	p.mu.Unlock()

	p.log.Info("indexed inbound xshard connection", "remote_id", string(remoteID), "shards", shardList)
	return true
}

// Close closes all connections in the pool and prevents new additions.
func (p *XshardPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true

	var allConns []*XshardConn
	for _, conns := range p.conns {
		allConns = append(allConns, conns...)
	}
	allConns = append(allConns, p.inbound...)

	p.conns = nil
	p.inbound = nil
	p.slaveIDs = nil
	p.mu.Unlock()

	seen := make(map[*XshardConn]struct{}, len(allConns))
	for _, conn := range allConns {
		if _, ok := seen[conn]; ok {
			continue
		}
		seen[conn] = struct{}{}
		conn.Close()
	}
	p.log.Info("xshard pool closed", "connections", len(seen))
}

// OutboundSize returns the number of unique outbound connections.
func (p *XshardPool) OutboundSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	seen := make(map[*XshardConn]struct{})
	for _, conns := range p.conns {
		for _, conn := range conns {
			seen[conn] = struct{}{}
		}
	}
	return len(seen)
}

// InboundSize returns the number of tracked inbound connections.
func (p *XshardPool) InboundSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.inbound)
}

// Targets returns all full shard IDs that have outbound connections.
func (p *XshardPool) Targets() []uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	targets := make([]uint32, 0, len(p.conns))
	for id := range p.conns {
		targets = append(targets, id)
	}
	return targets
}
