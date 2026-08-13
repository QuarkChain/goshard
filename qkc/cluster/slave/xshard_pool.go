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

// XshardPool manages direct slave-to-slave xshard connections, indexed by full
// shard ID. It corresponds to Python's SlaveConnectionManager.
//
// Consistent with Python's SlaveConnectionManager, the pool is add-only: a
// connection is registered in conns / inbound / slaveIDs and is never evicted
// when it closes. A CLOSED connection remains in the routing index and in the
// slave ID registry, exactly as Python keeps closed SlaveConnection objects in
// slave_connections / slave_ids / full_shard_id_to_slaves.
type XshardPool struct {
	mu       sync.RWMutex
	conns    map[uint32][]*XshardConn
	inbound  []*XshardConn
	slaveIDs map[string]bool // Known remote slave identities (Python's slave_ids set). Used for outbound duplicate dialing prevention (VerifyAndAddToShards) and HasSlaveID queries. A single remote slave may have multiple XshardConn objects; this is an identity registry, not a connection count. Like Python, entries are never removed.
	closed   bool
	log      log.Logger
}

// NewXshardPool creates a new, empty connection pool.
func NewXshardPool(logger log.Logger) *XshardPool {
	if logger == nil {
		logger = log.Root()
	}
	return &XshardPool{
		conns:    make(map[uint32][]*XshardConn),
		slaveIDs: make(map[string]bool),
		log:      logger,
	}
}

// VerifyAndAddToShards verifies the remote identity and indexes the connection
// by all advertised shard IDs.
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

// HasSlaveID reports whether the pool already tracks a connection to the given
// slave ID. This matches Python's slave_ids deduplication check before dialing.
func (p *XshardPool) HasSlaveID(id []byte) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.slaveIDs[string(id)]
}

// VerifyAndAddToShards verifies the remote identity and indexes the connection
// by all advertised shard IDs.
func (p *XshardPool) VerifyAndAddToShards(ctx context.Context, conn *XshardConn, expectedID []byte, expectedShardList []uint32) error {
	id, shardList, err := conn.SendPing(ctx)
	if err != nil {
		conn.Close()
		return fmt.Errorf("ping failed for %s: %w", conn.RemoteAddr(), err)
	}
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

	// Outbound connections do not receive a PING from the peer, so the identity
	// stored on the connection object must be set explicitly for indexing.
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

	// Index by all remote shard IDs for routing
	for _, shardID := range shardList {
		p.conns[shardID] = append(p.conns[shardID], conn)
	}
	p.mu.Unlock()

	p.log.Info("verified and added xshard connection", "remote_id", remoteID, "remote", conn.RemoteAddr())
	return nil
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

// SendXshardTx broadcasts to every connection indexed for the shard.
// CLOSED connections are intentionally included for Python parity.
func (p *XshardPool) SendXshardTx(ctx context.Context, fullShardID uint32, payload []byte) (*wire.Frame, error) {
	conns := p.Get(fullShardID)
	if len(conns) == 0 {
		return nil, fmt.Errorf("no xshard connection to full shard %d", fullShardID)
	}

	// Broadcast to all connections concurrently (matches Python's asyncio.gather).
	// Do NOT filter by IsActive/IsClosed: a CLOSED connection must be attempted
	// so that its failure propagates (Python's write_rpc_request returns an
	// exception future for non-ACTIVE connections, which asyncio.gather raises).
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

	// Validate every response: decode as AddXshardTxListResponse, check opcode
	// and error_code == 0 (matches Python's check(all([response.error_code == 0 for _, response, _ in responses]))).
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

// TrackInbound registers an inbound connection until its PING handshake
// completes. The pool closes tracked connections on pool shutdown.
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

// WatchAndIndex waits for the inbound PING and indexes the connection by
// the remote shard IDs.
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

	// Record slave identity (Python's slave_ids set).
	// Inbound connections are not deduplicated: a single remote slave may
	// have multiple connections (e.g., bidirectional S1↔S2 where both sides
	// initiate). Outbound deduplication is handled by VerifyAndAddToShards.
	if len(remoteID) > 0 {
		p.slaveIDs[string(remoteID)] = true
	}

	// Index by remote shard IDs for routing.
	// Skip if already indexed for this shard — WatchAndIndex is idempotent.
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

	// Remove from inbound tracking now that the connection is indexed.
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
