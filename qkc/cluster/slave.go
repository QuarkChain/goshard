// Package cluster implements the pyquarkchain-compatible cluster protocol wire
// library: frame codec, TCP connection management, and VirtualConnection multiplexing.
//
// # Architecture overview
//
// The package provides two parallel communication channels that together cover
// all three communication modes of a QuarkChain cluster:
//
//	Channel A — Master TCP (multiplexed)
//	  Physical TCP → Python Master, multiplexed by cluster_peer_id in Metadata:
//	    cluster_peer_id == 0  →  cluster RPC (master ↔ slave commands)
//	    cluster_peer_id != 0  →  PeerConn (virtual P2P via Dispatcher)
//
//	Channel B — Xshard TCP (dedicated)
//	  Separate physical TCP connections directly to other slave nodes.
//	  Cross-shard transaction delivery does NOT go through Master.
//
// # Entry point
//
//	rpc, err := NewSlaveRPC(&Config{
//	    MasterAddr:  "127.0.0.1:38291",
//	    OwnBranches: []uint32{0, 1},
//	    ListenAddr:  "0.0.0.0:38292",
//	})
//	if err != nil { ... }
//	defer rpc.Close()
//
//	rpc.RegisterHandlers()  // must be called before Serve()
//	rpc.Serve()             // blocks until connection error
//
// # Two-layer design
//
//	Slave     —  protocol-level: raw *Frame, opcode bytes, payload []byte  (internal)
//	SlaveRPC  —  business-level: typed Go methods, serialization           (public)
//
// Slave is internal to the cluster package.  Business code should use
// SlaveRPC exclusively — it creates and manages Slave internally.
package cluster

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// Config holds the configuration for a Slave node.
type Config struct {
	MasterAddr  string     // Python master address (host:port)
	OwnBranches []uint32   // Shard branches owned by this slave
	ListenAddr  string     // Address to accept incoming xshard connections from other slaves
	Logger      log.Logger // Logger instance (defaults to log.New())
}

// Slave is the protocol-level entry point for all cluster communication.
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
type Slave struct {
	cfg        *Config
	masterConn *MasterConn
	xshardPool *XshardPool
	dispatcher *Dispatcher

	clusterPeerIDs map[uint64]struct{}             // Registered cluster peer IDs
	peerConns      map[uint32]map[uint64]*PeerConn // branch → cluster_peer_id → conn

	// SLAVE_OP_RPC_MAP — applied to every XshardConn (outbound or inbound).
	// Set via SetXshardHandlers (called from SlaveRPC.RegisterHandlers).
	xshardHandlers   map[byte]MasterHandler
	xshardHandlersMu sync.RWMutex

	// Peer command handlers — applied to every PeerConn created by
	// HandleCreateClusterPeerConnection (mode 3, Peer→Master→Slave).
	// Caller registers handlers once via SetPeerHandlers() (typically from
	// SlaveRPC.RegisterPeerHandlers()); each new PeerConn gets them automatically.
	peerHandlers   map[byte]MasterHandler
	peerHandlersMu sync.RWMutex

	mu     sync.RWMutex
	log    log.Logger
	closed bool
}

// NewSlave creates a new Slave, connects to the master, and initializes all
// communication channels.  Returns an error if the master connection fails.
//
// No handlers are registered by default.  The caller must register handlers
// (typically via SlaveRPC.RegisterHandlers()) before messages arrive.
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

	// Start the master connection read loop (after OnFrame is configured)
	mc.Start()

	// 4. Initialize peer connection maps for each owned branch
	for _, branch := range cfg.OwnBranches {
		s.peerConns[branch] = make(map[uint64]*PeerConn)
	}

	s.log.Info("slave initialized", "branches", cfg.OwnBranches)
	return s, nil
}

// ── Handler registration (inbound: Master → Slave) ─────────────────────

// MasterHandler is a handler function for master cluster RPC opcodes.
// It receives the decoded frame and returns the response payload bytes.
// Return nil, ErrNotImplemented if the opcode is not yet wired up.
type MasterHandler = func(*Frame) ([]byte, error)

// RegisterMasterHandler registers a handler for a specific master cluster RPC
// opcode.  Panics if handler is nil.
func (s *Slave) RegisterMasterHandler(opcode byte, handler MasterHandler) {
	s.masterConn.RegisterHandler(opcode, handler)
}

// RegisterMasterHandlers is a convenience method to register multiple handlers
// at once from a map.
func (s *Slave) RegisterMasterHandlers(handlers map[byte]MasterHandler) {
	for opcode, handler := range handlers {
		s.masterConn.RegisterHandler(opcode, handler)
	}
}

// SetXshardHandlers sets the handlers that are applied to every XshardConn
// (outbound or inbound).  These are the Go equivalent of Python's
// SLAVE_OP_RPC_MAP — only 3 opcodes: PING, ADD_XSHARD_TX_LIST_REQUEST,
// BATCH_ADD_XSHARD_TX_LIST_REQUEST.
//
// Must be called before ConnectToSlaves() or Serve().
func (s *Slave) SetXshardHandlers(handlers map[byte]MasterHandler) {
	s.xshardHandlersMu.Lock()
	s.xshardHandlers = handlers
	s.xshardHandlersMu.Unlock()
}

// applyXshardHandlers registers s.xshardHandlers on an XshardConn.
func (s *Slave) applyXshardHandlers(conn *XshardConn) {
	s.xshardHandlersMu.RLock()
	defer s.xshardHandlersMu.RUnlock()
	for opcode, handler := range s.xshardHandlers {
		conn.RegisterHandler(opcode, handler)
	}
}

// ── Sending to master (outbound: Slave → Master) ───────────────────────

// SendToMaster sends a fire-and-forget command to the master (RPCID=0).
func (s *Slave) SendToMaster(opcode byte, payload []byte) error {
	return s.masterConn.SendCommand(opcode, payload)
}

// SendRPCToMaster sends an RPC request to the master and waits for the response.
func (s *Slave) SendRPCToMaster(ctx context.Context, opcode byte, payload []byte) (*Frame, error) {
	return s.masterConn.SendRPC(ctx, opcode, payload)
}

// ── Peer communication (cluster_peer_id != 0, virtual) ─────────────────

// SetPeerHandlers stores the peer command handlers that are applied to every
// PeerConn when it is created by HandleCreateClusterPeerConnection (mode 3).
//
// Must be called once before any peer connections are created.
// Typically called from SlaveRPC.RegisterPeerHandlers().
func (s *Slave) SetPeerHandlers(handlers map[byte]MasterHandler) {
	s.peerHandlersMu.Lock()
	s.peerHandlers = handlers
	s.peerHandlersMu.Unlock()
}

// applyPeerHandlers registers the stored peer handlers on a PeerConn.
func (s *Slave) applyPeerHandlers(conn *PeerConn) {
	s.peerHandlersMu.RLock()
	defer s.peerHandlersMu.RUnlock()
	for opcode, handler := range s.peerHandlers {
		conn.RegisterHandler(opcode, handler)
	}
}

// RegisterPeerHandler registers a handler for a peer-shard P2P command opcode
// on a specific branch.  It applies to all existing PeerConns on that branch,
// AND persists the handler so future PeerConns created by
// HandleCreateClusterPeerConnection also get it.
func (s *Slave) RegisterPeerHandler(branch uint32, opcode byte, handler MasterHandler) error {
	// Persist for future PeerConns
	s.peerHandlersMu.Lock()
	if s.peerHandlers == nil {
		s.peerHandlers = make(map[byte]MasterHandler)
	}
	s.peerHandlers[opcode] = handler
	s.peerHandlersMu.Unlock()

	// Apply to existing PeerConns
	s.mu.RLock()
	conns, ok := s.peerConns[branch]
	s.mu.RUnlock()

	if !ok {
		return fmt.Errorf("branch %d not owned by this slave", branch)
	}

	for _, conn := range conns {
		conn.RegisterHandler(opcode, handler)
	}
	return nil
}

// ── Cluster peer connection management ──────────────────────────────────

// HandleCreateClusterPeerConnection creates PeerConns for all owned branches
// when Master notifies that a new external peer has connected (mode 3).
// It is exported so SlaveRPC can delegate to it.
func (s *Slave) HandleCreateClusterPeerConnection(frame *Frame) ([]byte, error) {
	clusterPeerID := frame.Meta.ClusterPeerID

	s.mu.Lock()
	defer s.mu.Unlock()

	s.clusterPeerIDs[clusterPeerID] = struct{}{}

	for branch := range s.peerConns {
		conn := NewPeerConn(clusterPeerID, branch, s.masterConn, s.log)
		s.applyPeerHandlers(conn) // apply pre-registered CommandOp handlers
		s.peerConns[branch][clusterPeerID] = conn
		s.dispatcher.AddPeerConn(conn)
	}

	s.log.Info("created cluster peer connection", "cluster_peer_id", clusterPeerID)
	return serialize.SerializeToBytes(&CreateClusterPeerConnectionResponse{ErrorCode: 0})
}

// HandleDestroyClusterPeerConnection removes PeerConns for all branches
// when Master notifies that a peer has disconnected (mode 3).
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
	return nil, nil // NON-RPC, return value is ignored
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

// ── Slave↔Slave connection management (mode 2) ──────────────────────────

// ConnectToSlaves connects to the slaves listed in the CONNECT_TO_SLAVES_REQUEST
// payload.  Skips self and already-connected slaves.
//
// This is the Go equivalent of Python's SlaveConnectionManager.connect_to_slave().
//
// One result per SlaveInfo entry: empty (nil) = success, non-empty = error message.
// Each new XshardConn gets the registered xshard handlers (Python SLAVE_OP_RPC_MAP).
func (s *Slave) ConnectToSlaves(payload []byte) ([]byte, error) {
	var req ConnectToSlavesRequest
	if err := serialize.Deserialize(serialize.NewByteBuffer(payload), &req); err != nil {
		s.log.Error("deserialize ConnectToSlavesRequest failed", "err", err)
		resp := ConnectToSlavesResponse{ResultList: [][]byte{[]byte("deserialization error: " + err.Error())}}
		return serialize.SerializeToBytes(&resp)
	}

	resultList := make([][]byte, len(req.SlaveInfoList))

	for i, info := range req.SlaveInfoList {
		host := string(info.Host)
		addr := fmt.Sprintf("%s:%d", host, info.Port)

		// One connection per remote slave (not per shard — the connection
		// is indexed by all fullShardIDs that slave covers, like Python).
		if info.Port == 0 {
			resultList[i] = []byte("slave info has port 0")
			continue
		}

		conn, err := NewXshardConn(addr, s.log)
		if err != nil {
			resultList[i] = []byte(err.Error())
			s.log.Warn("failed to connect to slave", "addr", addr, "err", err)
			continue
		}

		// Register SLAVE_OP_RPC_MAP handlers (PING, ADD_XSHARD_TX_LIST, …)
		s.applyXshardHandlers(conn)

		// Start the read loop after handlers are registered
		conn.Start()

		// Index the connection by every fullShardID this slave covers
		for _, fullShardID := range info.FullShardIDList {
			target := FullShardID{
				ChainID: fullShardID >> 16,
				ShardID: fullShardID & 0xFFFF,
			}
			// RemoveTarget atomically closes old connections and cleans up the map
			s.xshardPool.RemoveTarget(target)
			s.xshardPool.Add(target, conn)
		}

		s.log.Info("connected to slave", "addr", addr, "shards", len(info.FullShardIDList))
		// resultList[i] stays nil → success (matches Python: empty str)
	}

	resp := ConnectToSlavesResponse{ResultList: resultList}
	return serialize.SerializeToBytes(&resp)
}

// ── Xshard communication (separate physical TCP) ────────────────────────

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
	s.applyXshardHandlers(conn)
	conn.Start()
	s.xshardPool.Add(target, conn)
	return nil
}

// ── Lifecycle ───────────────────────────────────────────────────────────

// Serve blocks until the master connection encounters a fatal error.
// If ListenAddr is configured, it also accepts incoming xshard connections.
func (s *Slave) Serve() error {
	if s.cfg.ListenAddr != "" {
		if err := s.startXshardServer(s.cfg.ListenAddr); err != nil {
			s.masterConn.Close()
			return err
		}
	}

	for err := range s.masterConn.Error() {
		s.log.Error("master connection error", "err", err)
		return err
	}
	return nil
}

// startXshardServer starts a TCP server to accept incoming xshard connections.
// Each accepted connection gets the registered xshard handlers (Python SLAVE_OP_RPC_MAP).
//
// TODO: perform PING/PONG handshake to learn the remote slave's ID and
// fullShardIDList, then index the connection in xshardPool accordingly
// (Python: SlaveConnectionManager.handle_new_connection).
func (s *Slave) startXshardServer(listenAddr string) error {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	go func() {
		defer listener.Close()
		defer func() {
			if r := recover(); r != nil {
				s.log.Error("xshard accept loop panic recovered", "panic", r)
			}
		}()

		for {
			conn, err := listener.Accept()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					s.log.Warn("temporary accept error, retrying", "err", err)
					continue
				}
				s.log.Error("failed to accept xshard connection", "err", err)
				return
			}
			xc := NewXshardConnFromConn(conn, s.log)
			s.applyXshardHandlers(xc)
			xc.Start()
			s.log.Info("accepted xshard connection", "addr", conn.RemoteAddr())
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
