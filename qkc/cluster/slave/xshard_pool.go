// Copyright 2026-2027, QuarkChain.

package slave

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

const defaultDialTimeout = 10 * time.Second

// XshardPool manages slave-to-slave xshard connections, indexed by full shard
// ID. Corresponds to Python's SlaveConnectionManager. It is add-only: closed
// connections are never evicted, matching Python.
//
// XshardPool is the only public surface for xshard connectivity: it owns dial,
// handshake, identity bookkeeping, indexing, and cleanup. Connections never
// escape the pool.
type XshardPool struct {
	mu                   sync.RWMutex
	conns                map[uint32][]*xshardConn
	inbound              []*xshardConn
	slaveIDs             map[string]bool // Known remote identities (Python's slave_ids); an identity set, not a connection count. Never removed.
	selfID               []byte          // This slave's identity; used for self-skip and PING/PONG local identity.
	localFullShardIDList []uint32
	maxPayloadSize       uint32 // Frame payload limit (0 = no limit).
	closed               bool
	log                  log.Logger
}

// NewXshardPool creates a new pool. selfID is this slave's identity (also used
// as the local identity in PING/PONG); connections to selfID are skipped.
// maxPayloadSize 0 disables the frame payload limit.
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

// add indexes conn under a single shard ID (test helper, bypasses verification).
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

// hasSlaveID reports whether the pool already tracks the given slave ID.
func (p *XshardPool) hasSlaveID(id []byte) bool {
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

// DialToSlave establishes and registers an outbound xshard connection. It owns
// the outbound net.Conn creation and the full handshake: pre-dial dedup, dial,
// wrap, start, PING/PONG verification, and registration (Python's
// connect_to_slave). A nil error means the remote is fully established and
// registered, or was skipped (self/duplicate).
func (p *XshardPool) DialToSlave(ctx context.Context, addr string, expectedID []byte, expectedShardList []uint32) error {
	if p.knownRemote(expectedID) {
		p.log.Info("outbound xshard connection skipped: remote already known", "remote_id", string(expectedID))
		return nil
	}

	nc, err := net.DialTimeout("tcp", addr, defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("dial xshard slave %s: %w", addr, err)
	}
	conn := newXshardConn(nc, p.maxPayloadSize, p.selfID, p.localFullShardIDList, p.log)
	conn.Start()

	return p.verifyAndAddToShards(ctx, conn, expectedID, expectedShardList)
}

// HandleInbound takes over an already-accepted inbound net.Conn: it wraps,
// registers it as pending, starts the read loop, waits for the peer PING, then
// indexes the connection by its advertised shards (Python's
// handle_new_connection). It blocks until the handshake completes or the
// connection (or pool) closes; the accept loop should call it in a goroutine.
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
}

// get returns a snapshot of connections for the given full shard ID.
func (p *XshardPool) get(fullShardID uint32) []*xshardConn {
	p.mu.RLock()
	conns := p.conns[fullShardID]
	result := make([]*xshardConn, len(conns))
	copy(result, conns)
	p.mu.RUnlock()
	return result
}

// SendXshardTx broadcasts a serialized AddXshardTxList request to every
// connection indexed for the shard (CLOSED connections included, for Python
// parity). It succeeds only if every response has error_code == 0 (Python's
// check(all(...))). An empty target set is a silent no-op.
func (p *XshardPool) SendXshardTx(ctx context.Context, fullShardID uint32, payload []byte) error {
	return p.broadcast(ctx, fullShardID, payload,
		func(c *xshardConn) (*wire.Frame, error) { return c.sendXshardTxList(ctx, payload) },
		func(f *wire.Frame) error { _, err := parseAddXshardTxListResponse(f); return err },
	)
}

// SendBatchXshardTx broadcasts a serialized BatchAddXshardTxList request to
// every connection indexed for the shard, with the same all-or-nothing
// semantics as SendXshardTx (Python's batch_broadcast_xshard_tx_list).
func (p *XshardPool) SendBatchXshardTx(ctx context.Context, fullShardID uint32, payload []byte) error {
	return p.broadcast(ctx, fullShardID, payload,
		func(c *xshardConn) (*wire.Frame, error) { return c.sendBatchXshardTxList(ctx, payload) },
		func(f *wire.Frame) error { _, err := parseBatchAddXshardTxListResponse(f); return err },
	)
}

// broadcast sends a request to every indexed connection and requires every
// response to decode with error_code == 0, matching Python's gather + check.
func (p *XshardPool) broadcast(
	ctx context.Context,
	fullShardID uint32,
	payload []byte,
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

// Close closes all connections in the pool and prevents new additions.
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

// outboundSize returns the number of unique outbound connections (test helper).
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

// inboundSize returns the number of tracked inbound connections (test helper).
func (p *XshardPool) inboundSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.inbound)
}

// targets returns all full shard IDs that have outbound connections (test helper).
func (p *XshardPool) targets() []uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	targets := make([]uint32, 0, len(p.conns))
	for id := range p.conns {
		targets = append(targets, id)
	}
	return targets
}
