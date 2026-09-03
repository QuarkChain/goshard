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

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// XshardPool manages slave-to-slave xshard connections, indexed by full shard
// ID. Connections and slave IDs are add-only: a closed connection stays
// indexed, and dial dedup at entry is best-effort under concurrent dials
// (mutual dial may still form two connections).
type XshardPool struct {
	// mu guards all mutable pool state below. Registration
	// (registerConnection) and Close are multi-map transactions over these
	// fields plus closed, so they must stay in one critical section; do not
	// split locking per field.
	mu          sync.RWMutex
	conns       map[uint32][]*XshardConn // py: full_shard_id_to_slaves
	connections map[*XshardConn]struct{} // py: slave_connections; also tracks handshaking conns

	slaveIDs             map[string]struct{} // py: slave_ids; add-only, used for outbound dedup
	selfID               []byte              // This slave's identity.
	localFullShardIDList []uint32
	// clusterShardIDs is the immutable membership set derived from the
	// cluster-wide configured shard ids (py:
	// env.quark_chain_config.get_full_shard_ids()). A connection's shard is
	// routed only if it belongs to this set (py: slave.py:830-835).
	clusterShardIDs map[uint32]struct{}
	maxPayloadSize  uint32        // 0 disables the payload limit.
	handler         XshardHandler // Serves inbound xshard requests.
	closed          bool
	log             log.Logger
}

// Public API

// NewXshardPool creates a pool. selfID is this slave's identity. handler
// serves inbound xshard requests and must not be nil. maxPayloadSize 0
// disables the payload limit. clusterShardIDs holds the cluster-wide
// configured shard ids (py: env.quark_chain_config.get_full_shard_ids());
// a connection's shard is routed only if it is in this set.
func NewXshardPool(selfID []byte, localFullShardIDList []uint32, clusterShardIDs []uint32, maxPayloadSize uint32, handler XshardHandler, logger log.Logger) (*XshardPool, error) {
	if handler == nil {
		return nil, errors.New("xshard handler must not be nil")
	}
	if logger == nil {
		logger = log.Root()
	}
	clusterSet := make(map[uint32]struct{}, len(clusterShardIDs))
	for _, id := range clusterShardIDs {
		clusterSet[id] = struct{}{}
	}
	return &XshardPool{
		conns:                make(map[uint32][]*XshardConn),
		connections:          make(map[*XshardConn]struct{}),
		slaveIDs:             make(map[string]struct{}),
		selfID:               append([]byte(nil), selfID...),
		handler:              handler,
		localFullShardIDList: append([]uint32(nil), localFullShardIDList...),
		clusterShardIDs:      clusterSet,
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
	// Dialer.Timeout keeps bounding the dial when ctx is never cancelled.
	nc, err := (&net.Dialer{Timeout: defaultDialTimeout}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial xshard slave %s: %w", addr, err)
	}
	// Outbound identity is injected from the master-advertised SlaveInfo
	// (Python passes slave_info.id / full_shard_id_list to the constructor).
	conn, err := newXshardConn(nc, p.maxPayloadSize, p.selfID, p.localFullShardIDList, slaveInfo.ID, slaveInfo.FullShardIDList, p.handler, p.log)
	if err != nil {
		nc.Close()
		return fmt.Errorf("create xshard conn to %s: %w", addr, err)
	}

	if !p.trackConnection(conn) {
		conn.Close()
		return fmt.Errorf("xshard pool closed")
	}
	conn.Start()

	// The peer must confirm the master-advertised identity (py:885-890); the
	// PONG result is compared and discarded, never written back.
	id, shardList, err := conn.sendPing(ctx)
	if err != nil {
		p.rejectConnection(conn)
		return fmt.Errorf("ping failed for %s: %w", conn.RemoteAddr(), err)
	}
	// Python leaks the connection on mismatch; close it instead.
	if !bytes.Equal(id, slaveInfo.ID) {
		p.rejectConnection(conn)
		return fmt.Errorf("slave id mismatch for %s: expected %x, got %x", conn.RemoteAddr(), slaveInfo.ID, id)
	}
	if len(shardList) != len(slaveInfo.FullShardIDList) {
		p.rejectConnection(conn)
		return fmt.Errorf("shard list length mismatch for %s: expected %d, got %d", conn.RemoteAddr(), len(slaveInfo.FullShardIDList), len(shardList))
	}
	for i := range shardList {
		if shardList[i] != slaveInfo.FullShardIDList[i] {
			p.rejectConnection(conn)
			return fmt.Errorf("shard list mismatch for %s: expected %v, got %v", conn.RemoteAddr(), slaveInfo.FullShardIDList, shardList)
		}
	}

	// Registration is unconditional after a successful handshake (py
	// connect_to_slave dedups only at entry): re-checking slave IDs here would
	// race with the peer's inbound registering mid-handshake during a mutual
	// dial, closing both outbounds and permanently partitioning the pair.
	if err := p.registerConnection(conn); err != nil {
		return err
	}
	p.log.Info("indexed xshard connection", "remote_id", string(id), "shards", shardList)
	return nil
}

// HandleInbound takes ownership of an accepted xshard connection: it waits
// for the handshake and indexes the conn. Conns whose handshake fails or
// times out are discarded and never enter the routing index.
func (p *XshardPool) HandleInbound(nc net.Conn) {
	// Inbound identity arrives with the first PING (py:845-846 pass None).
	conn, err := newXshardConn(nc, p.maxPayloadSize, p.selfID, p.localFullShardIDList, nil, nil, p.handler, p.log)
	if err != nil {
		nc.Close()
		p.log.Error("inbound xshard conn rejected", "err", err)
		return
	}

	if !p.trackConnection(conn) {
		conn.Close()
		p.log.Warn("xshard pool closed, closing inbound conn immediately", "remote", conn.RemoteAddr())
		return
	}
	conn.Start()

	if !conn.waitUntilPingReceived() {
		p.log.Warn("inbound xshard connection closed before ping", "remote", conn.RemoteAddr())
		p.rejectConnection(conn)
		return
	}

	// Inbound is not deduplicated — a remote may have multiple connections
	// (py handle_new_connection never checks slave_ids).
	if err := p.registerConnection(conn); err != nil {
		p.log.Warn("xshard pool closed while registering inbound conn", "remote", conn.RemoteAddr())
		return
	}

	p.log.Info("indexed inbound xshard connection", "remote_id", string(conn.RemoteID()), "shards", conn.RemoteFullShardIDList())
}

// Lookup returns the connections currently indexed for a shard. They may be
// closed concurrently afterwards, so callers must tolerate failed sends.
// The returned slice is a copy holding the original pointers.
func (p *XshardPool) Lookup(fullShardID uint32) []*XshardConn {
	p.mu.RLock()
	conns := p.conns[fullShardID]
	result := make([]*XshardConn, len(conns))
	copy(result, conns)
	p.mu.RUnlock()
	return result
}

// Close closes all pool connections, including ones still handshaking
// (py close_all leaks those).
func (p *XshardPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true

	allConns := make([]*XshardConn, 0, len(p.connections))
	for conn := range p.connections {
		allConns = append(allConns, conn)
	}

	p.conns = nil
	p.connections = nil
	p.slaveIDs = nil
	p.mu.Unlock()

	for _, conn := range allConns {
		conn.Close()
	}
	p.log.Info("xshard pool closed", "connections", len(allConns))
}

// Internal implementation

// rejectConnection closes a tracked but not-yet-registered connection and
// evicts it from the tracking set. Every failure path between
// trackConnection and registerConnection must go through here, never
// through conn.Close() alone: a conn left only in the tracking set would
// linger until pool Close.
func (p *XshardPool) rejectConnection(conn *XshardConn) {
	conn.Close()

	p.mu.Lock()
	delete(p.connections, conn)
	p.mu.Unlock()
}

// registerConnection commits a verified connection into the routing index.
// It reports an error if the pool closed while the connection was handshaking,
// evicting any leftover tracking entry (normally none — Close empties the
// registries) and closing the conn.
func (p *XshardPool) registerConnection(conn *XshardConn) error {
	p.mu.Lock()
	if p.closed {
		delete(p.connections, conn)
		p.mu.Unlock()
		conn.Close()
		return fmt.Errorf("xshard pool closed")
	}

	p.addSlaveConnectionLocked(conn)
	p.mu.Unlock()
	return nil
}

// addSlaveConnectionLocked registers a connection in the slave ID registry and
// the shard routing index. The caller must hold p.mu.
func (p *XshardPool) addSlaveConnectionLocked(conn *XshardConn) {
	p.slaveIDs[string(conn.RemoteID())] = struct{}{}

	shardList := conn.RemoteFullShardIDList()
	// Filter the route keys against the cluster-wide configured shard set,
	// mirroring Python's _add_slave_connection which only indexes ids also in
	// env.quark_chain_config.get_full_shard_ids().
	seen := make(map[uint32]struct{}, len(shardList))
	for _, shardID := range shardList {
		if _, ok := p.clusterShardIDs[shardID]; !ok {
			continue
		}
		if _, dup := seen[shardID]; dup {
			continue
		}
		seen[shardID] = struct{}{}
		p.conns[shardID] = append(p.conns[shardID], conn)
	}
}

// trackConnection registers a newly created conn. It reports false if the
// pool is already closed.
func (p *XshardPool) trackConnection(conn *XshardConn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.connections[conn] = struct{}{}
	return true
}

// knownRemote reports whether expectedID is self or already known.
func (p *XshardPool) knownRemote(expectedID []byte) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.selfID) > 0 && bytes.Equal(p.selfID, expectedID) {
		return true
	}
	_, known := p.slaveIDs[string(expectedID)]
	return known
}
