// Package cluster implements the pyquarkchain-compatible cluster protocol wire
// library: frame codec, TCP connection management, and VirtualConnection multiplexing.
//
// Architecture (flat package, 2 parallel communication channels):
//
//	qkc/cluster/
//	  frame.go              — Frame, Metadata, ReadFrame, WriteFrame
//	  protocol.go           — ClusterOp, PeerOp, XshardOp, errors
//	  master_conn.go        — MasterConn: TCP to master
//	  master_dispatcher.go  — Dispatcher: routes by cluster_peer_id
//	  master_peer.go        — PeerConn: virtual peer connections
//	  xshard_conn.go        — XshardConn: direct TCP to other slaves
//	  xshard_pool.go        — XshardPool: connection pool
//	  slave.go              — Slave: unified entry point
//
// The Slave is the unified entry point that composes both communication channels.
package cluster

import (
	"context"
	"net"
	"sync"

	"github.com/ethereum/go-ethereum/log"
)

// Config holds the configuration for a Slave node.
type Config struct {
	MasterAddr  string     // Python master address
	OwnBranches []uint32   // Shard branches owned by this slave
	ListenAddr  string     // Address to accept xshard connections from other slaves
	Logger      log.Logger // Logger instance
}

// Slave is the unified entry point for all cluster communication.
//
// It composes two parallel communication channels:
//
//	MasterConn + Dispatcher + PeerConn
//	  Physical TCP to Python master, multiplexed by cluster_peer_id:
//	    cluster_peer_id == 0 → cluster RPC (master commands)
//	    cluster_peer_id != 0 → PeerConn (virtual P2P, routed via Dispatcher)
//
//	XshardConn + XshardPool
//	  Separate physical TCP directly to other slaves (cross-shard tx delivery).
//	  Does NOT go through master.
//
// Usage:
//
//	slave, err := NewSlave(&Config{...})
//	slave.RegisterMasterHandler(OP_ADD_ROOT_BLOCK, handler)
//	slave.Serve()
type Slave struct {
	cfg        *Config
	masterConn *MasterConn
	xshardPool *XshardPool
	dispatcher *Dispatcher

	clusterPeerIDs map[uint64]struct{}             // Registered cluster peer IDs
	peerConns      map[uint32]map[uint64]*PeerConn // branch → cluster_peer_id → conn

	mu     sync.RWMutex
	log    log.Logger
	closed bool
}

// NewSlave creates a new Slave, connects to the master, and initializes all
// communication channels. Returns an error if the master connection fails.
func NewSlave(cfg *Config) (*Slave, error) {
	if cfg.Logger == nil {
		cfg.Logger = log.New()
	}

	s := &Slave{
		cfg:            cfg,
		clusterPeerIDs: make(map[uint64]struct{}),
		peerConns:      make(map[uint32]map[uint64]*PeerConn),
		log:            cfg.Logger,
	}

	// 1. Connect to master (physical TCP, multiplexed)
	s.log.Info("connecting to master", "addr", cfg.MasterAddr)
	mc, err := NewMasterConn(cfg.MasterAddr, cfg.Logger)
	if err != nil {
		return nil, err
	}
	s.masterConn = mc

	// 2. Create xshard connection pool (separate physical TCP)
	s.xshardPool = NewXshardPool(cfg.Logger)

	// 3. Create dispatcher that routes frames by cluster_peer_id
	s.dispatcher = NewDispatcher(mc, cfg.Logger)

	// Wire dispatcher: every frame from master goes through the dispatcher
	mc.OnFrame = s.dispatcher.Dispatch

	// 4. Register default handlers
	s.registerDefaultHandlers()

	// 5. Initialize peer connection maps for each owned branch
	for _, branch := range cfg.OwnBranches {
		s.peerConns[branch] = make(map[uint64]*PeerConn)
	}

	s.log.Info("slave initialized", "branches", cfg.OwnBranches)
	return s, nil
}

// registerDefaultHandlers registers handlers for master cluster RPCs.
func (s *Slave) registerDefaultHandlers() {
	s.masterConn.RegisterHandler(OP_PING, s.handlePing)
	s.masterConn.RegisterHandler(OP_CONNECT_TO_SLAVES_REQUEST, s.handleConnectToSlaves)
	s.masterConn.RegisterHandler(OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST, s.HandleCreateClusterPeerConnection)
	s.masterConn.RegisterHandler(OP_DESTROY_CLUSTER_PEER_CONNECTION_COMMAND, s.HandleDestroyClusterPeerConnection)
}

func (s *Slave) handlePing(frame *Frame) ([]byte, error) {
	s.log.Debug("received PING from master")
	return []byte("PONG"), nil
}

func (s *Slave) handleConnectToSlaves(frame *Frame) ([]byte, error) {
	s.log.Debug("received CONNECT_TO_SLAVES_REQUEST")
	// TODO: Parse frame.Payload for slave addresses and connect
	return []byte("OK"), nil
}

func (s *Slave) HandleCreateClusterPeerConnection(frame *Frame) ([]byte, error) {
	clusterPeerID := frame.Meta.ClusterPeerID

	s.mu.Lock()
	defer s.mu.Unlock()

	s.clusterPeerIDs[clusterPeerID] = struct{}{}

	for branch := range s.peerConns {
		conn := NewPeerConn(clusterPeerID, branch, s.masterConn, s.log)
		s.peerConns[branch][clusterPeerID] = conn
		s.dispatcher.AddPeerConn(conn)
	}

	s.log.Info("created cluster peer connection", "cluster_peer_id", clusterPeerID)
	return []byte("OK"), nil
}

func (s *Slave) HandleDestroyClusterPeerConnection(frame *Frame) ([]byte, error) {
	clusterPeerID := frame.Meta.ClusterPeerID

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.clusterPeerIDs, clusterPeerID)

	for _, conns := range s.peerConns {
		if conn, ok := conns[clusterPeerID]; ok {
			conn.Close()
			delete(conns, clusterPeerID)
			s.dispatcher.RemovePeerConn(clusterPeerID)
		}
	}

	s.log.Info("destroyed cluster peer connection", "cluster_peer_id", clusterPeerID)
	return []byte("OK"), nil
}

// ── Master communication (cluster_peer_id == 0) ──────────────────────────

// SendToMaster sends a fire-and-forget command to the master.
func (s *Slave) SendToMaster(opcode byte, payload []byte) error {
	return s.masterConn.SendCommand(opcode, payload)
}

// SendRPCToMaster sends an RPC request to the master and waits for response.
func (s *Slave) SendRPCToMaster(ctx context.Context, opcode byte, payload []byte) (*Frame, error) {
	return s.masterConn.SendRPC(ctx, opcode, payload)
}

// MasterHandler is a handler function for master cluster RPC opcodes.
// It receives the decoded frame and returns the response payload bytes.
type MasterHandler = func(*Frame) ([]byte, error)

// RegisterMasterHandler registers a handler for a master cluster RPC opcode.
func (s *Slave) RegisterMasterHandler(opcode byte, handler func(*Frame) ([]byte, error)) {
	s.masterConn.RegisterHandler(opcode, handler)
}

// RegisterMasterHandlers is a convenience method to register multiple handlers
// at once. This avoids dozens of individual RegisterMasterHandler calls.
func (s *Slave) RegisterMasterHandlers(handlers map[byte]MasterHandler) {
	for opcode, handler := range handlers {
		s.masterConn.RegisterHandler(opcode, handler)
	}
}

// ── Peer communication (cluster_peer_id != 0, virtual) ───────────────────

// RegisterPeerHandler registers a handler for peer-shard P2P commands.
func (s *Slave) RegisterPeerHandler(branch uint32, opcode byte, handler func(*Frame) ([]byte, error)) {
	s.mu.RLock()
	conns, ok := s.peerConns[branch]
	s.mu.RUnlock()

	if !ok {
		s.log.Warn("branch not owned by this slave", "branch", branch)
		return
	}

	for _, conn := range conns {
		conn.RegisterHandler(opcode, handler)
	}
}

// ── Xshard communication (separate physical TCP) ─────────────────────────

// SendXshardTx sends xshard transactions to a target slave.
func (s *Slave) SendXshardTx(target FullShardID, branch uint32, payload []byte) error {
	return s.xshardPool.SendXshardTx(target, branch, payload)
}

// AddXshardConnection adds a connection to another slave.
func (s *Slave) AddXshardConnection(target FullShardID, addr string) error {
	conn, err := NewXshardConn(addr, s.log)
	if err != nil {
		return err
	}
	s.xshardPool.Add(target, conn)
	return nil
}

// ── Lifecycle ────────────────────────────────────────────────────────────

// Serve starts the slave and blocks until it encounters a fatal error.
// If ListenAddr is configured, it accepts incoming xshard connections.
func (s *Slave) Serve() error {
	if s.cfg.ListenAddr != "" {
		if err := s.startXshardServer(s.cfg.ListenAddr); err != nil {
			return err
		}
	}

	for err := range s.masterConn.Error() {
		s.log.Error("master connection error", "err", err)
		return err
	}
	return nil
}

// startXshardServer starts a TCP server to accept xshard connections.
func (s *Slave) startXshardServer(listenAddr string) error {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				s.log.Error("failed to accept xshard connection", "err", err)
				return
			}
			_ = NewXshardConnFromConn(conn, s.log)
			s.log.Info("accepted xshard connection", "addr", conn.RemoteAddr())
			// TODO: Determine target shard from handshake, store in pool
		}
	}()

	s.log.Info("xshard server started", "addr", listenAddr)
	return nil
}

// Close shuts down the slave and all its connections.
func (s *Slave) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()

	s.log.Info("shutting down slave")
	s.dispatcher.Close()
	s.xshardPool.Close()
	s.masterConn.Close()
	s.log.Info("slave shutdown complete")
}

// OwnsBranch checks if this slave owns the given branch.
func (s *Slave) OwnsBranch(branch uint32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.peerConns[branch]
	return ok
}

// ClusterPeerIDs returns all registered cluster peer IDs.
func (s *Slave) ClusterPeerIDs() []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]uint64, 0, len(s.clusterPeerIDs))
	for id := range s.clusterPeerIDs {
		ids = append(ids, id)
	}
	return ids
}

// MasterConn returns the master connection.
func (s *Slave) MasterConn() *MasterConn { return s.masterConn }

// XshardPool returns the xshard connection pool.
func (s *Slave) XshardPool() *XshardPool { return s.xshardPool }

// Dispatcher returns the dispatcher.
func (s *Slave) Dispatcher() *Dispatcher { return s.dispatcher }
