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
// indexed, and a peer is dialed at most once.
type XshardPool struct {
	mu                   sync.RWMutex
	conns                map[uint32][]*xshardConn // py: full_shard_id_to_slaves
	connections          map[*xshardConn]struct{} // py: slave_connections; also tracks handshaking conns
	slaveIDs             map[string]struct{}      // py: slave_ids; add-only, used for outbound dedup
	selfID               []byte                   // This slave's identity.
	handler              XshardHandler            // Serves inbound xshard requests.
	localFullShardIDList []uint32
	maxPayloadSize       uint32 // 0 disables the payload limit.
	closed               bool
	log                  log.Logger
}

// Public API

// NewXshardPool creates a pool. selfID is this slave's identity. handler
// serves inbound xshard requests and must not be nil. maxPayloadSize 0
// disables the payload limit.
func NewXshardPool(selfID []byte, localFullShardIDList []uint32, maxPayloadSize uint32, handler XshardHandler, logger log.Logger) (*XshardPool, error) {
	if handler == nil {
		return nil, errors.New("xshard handler must not be nil")
	}
	if logger == nil {
		logger = log.Root()
	}
	return &XshardPool{
		conns:                make(map[uint32][]*xshardConn),
		connections:          make(map[*xshardConn]struct{}),
		slaveIDs:             make(map[string]struct{}),
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
	// DialContext honors ctx cancellation while Dialer.Timeout still bounds the
	// dial duration when ctx is never cancelled (keeps defaultDialTimeout's role).
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

	// Verify the remote against the advertised identity (py:885-890); the
	// PONG result is compared and discarded, never written back.
	id, shardList, err := conn.sendPing(ctx)
	if err != nil {
		// Close covers ctx cancellation, where the conn stays open otherwise.
		conn.Close()
		p.discardConnection(conn)
		return fmt.Errorf("ping failed for %s: %w", conn.RemoteAddr(), err)
	}
	// Python leaks the connection on mismatch; close it instead.
	if !bytes.Equal(id, slaveInfo.ID) {
		conn.Close()
		p.discardConnection(conn)
		return fmt.Errorf("slave id mismatch for %s: expected %x, got %x", conn.RemoteAddr(), slaveInfo.ID, id)
	}
	if len(shardList) != len(slaveInfo.FullShardIDList) {
		conn.Close()
		p.discardConnection(conn)
		return fmt.Errorf("shard list length mismatch for %s: expected %d, got %d", conn.RemoteAddr(), len(slaveInfo.FullShardIDList), len(shardList))
	}
	for i := range shardList {
		if shardList[i] != slaveInfo.FullShardIDList[i] {
			conn.Close()
			p.discardConnection(conn)
			return fmt.Errorf("shard list mismatch for %s: expected %v, got %v", conn.RemoteAddr(), slaveInfo.FullShardIDList, shardList)
		}
	}

	// Registration is unconditional after a successful handshake, mirroring
	// Python's connect_to_slave (entry check only). Re-checking slave IDs here
	// would close the outbound when the peer's inbound registers during the
	// handshake — with mutual dials both sides would close their outbound,
	// killing the only live connections and permanently partitioning the pair.
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		return fmt.Errorf("xshard pool closed")
	}
	p.addSlaveConnectionLocked(conn)
	p.mu.Unlock()
	p.log.Info("indexed xshard connection", "remote_id", string(id), "shards", shardList)
	return nil
}

// HandleInbound takes ownership of an accepted xshard connection.
//
// waitUntilPingReceived blocks until the peer's first PING, connection close,
// or the handshake timeout. It returns false if the connection closes or the
// handshake times out.
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
		// Evict the dead conn; it will never be indexed.
		p.discardConnection(conn)
		return
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		return
	}
	// Inbound is not deduplicated — a remote may have multiple connections
	// (py handle_new_connection never checks slave_ids).
	p.addSlaveConnectionLocked(conn)
	p.mu.Unlock()

	p.log.Info("indexed inbound xshard connection", "remote_id", string(conn.remoteID()), "shards", conn.remoteFullShardIDList())
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

	allConns := make([]*xshardConn, 0, len(p.connections))
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

// addSlaveConnectionLocked registers a connection in the slave ID registry and
// the shard routing index. The caller must hold p.mu.
func (p *XshardPool) addSlaveConnectionLocked(conn *xshardConn) {
	p.slaveIDs[string(conn.remoteID())] = struct{}{}

	shardList := conn.remoteFullShardIDList()
	// Shards come from the remote-declared list; Python intersects with the
	// cluster config, but that filter is unobservable since queries only use
	// config shards.
	seen := make(map[uint32]struct{}, len(shardList))
	for _, shardID := range shardList {
		if _, dup := seen[shardID]; dup {
			continue
		}
		seen[shardID] = struct{}{}
		p.conns[shardID] = append(p.conns[shardID], conn)
	}
}

// trackConnection registers a newly created conn. It reports false if the
// pool is already closed.
func (p *XshardPool) trackConnection(conn *xshardConn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.connections[conn] = struct{}{}
	return true
}

// discardConnection removes an unindexed conn from the tracking set.
func (p *XshardPool) discardConnection(conn *xshardConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.connections, conn)
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

// get returns a snapshot of connections for a shard.
func (p *XshardPool) get(fullShardID uint32) []*xshardConn {
	p.mu.RLock()
	conns := p.conns[fullShardID]
	result := make([]*xshardConn, len(conns))
	copy(result, conns)
	p.mu.RUnlock()
	return result
}
