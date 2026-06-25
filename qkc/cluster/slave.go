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
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// Config holds the configuration for a Slave node.
type Config struct {
	MasterAddr  string     // Python master address (host:port)
	ID          string     // Slave ID (matches Python slave_config.ID, ASCII bytes on wire)
	OwnBranches []uint32   // Full shard IDs owned by this slave (matches Python FULL_SHARD_ID_LIST)
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
	listener   net.Listener

	id              []byte   // Slave ID (ASCII bytes, matches Python slave_config.ID)
	fullShardIDList []uint32 // Full shard IDs owned by this slave

	clusterPeerIDs map[uint64]struct{}             // Registered cluster peer IDs
	peerConns      map[uint32]map[uint64]*PeerConn // branch → cluster_peer_id → conn

	// connectedSlaveIDs tracks slave IDs already connected via ConnectToSlaves
	// or via inbound xshard PING, matching Python SlaveConnectionManager.slave_ids.
	// Used to skip duplicate connect_to_slave calls and dedupe inbound conns.
	connectedSlaveIDs map[string]struct{}

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

	// masterHandlers are cached until the master connects (masterConn is set
	// asynchronously by acceptLoop).  When handleConn creates the MasterConn,
	// it applies these handlers before calling Start().
	masterHandlers map[byte]MasterHandler

	mu     sync.RWMutex
	log    log.Logger
	closed bool

	startOnce sync.Once // ensures acceptLoop is started at most once
}

// NewSlave creates a new Slave, starts listening for the master to connect
// (matching Python's SlaveServer which runs asyncio.start_server), and
// initializes all communication channels.
//
// The slave listens on ListenAddr for:
//   - the Python master (cluster_peer_id == 0 frames)
//   - other slave nodes (xshard traffic, cluster_peer_id != 0)
//
// No handlers are registered by default.  The caller must register handlers
// (typically via SlaveRPC.RegisterHandlers()) before messages arrive.
func NewSlave(cfg *Config) (*Slave, error) {
	if cfg.Logger == nil {
		cfg.Logger = log.New()
	}

	s := &Slave{
		cfg:               cfg,
		id:                []byte(cfg.ID),
		fullShardIDList:   cfg.OwnBranches,
		clusterPeerIDs:    make(map[uint64]struct{}),
		peerConns:         make(map[uint32]map[uint64]*PeerConn),
		connectedSlaveIDs: make(map[string]struct{}),
		masterHandlers:    make(map[byte]MasterHandler),
		log:               cfg.Logger,
	}
	// Record self in connectedSlaveIDs so ConnectToSlaves skips self even when
	// the slave_info list accidentally includes us (matches Python slave_ids init).
	s.connectedSlaveIDs[cfg.ID] = struct{}{}

	// 1. Listen for incoming connections (master + other slaves).
	//    Matches Python SlaveServer.__start_server which binds 0.0.0.0:PORT
	//    and accepts both master and slave connections.
	s.log.Info("listening for master and slave connections", "addr", cfg.ListenAddr)
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", cfg.ListenAddr, err)
	}
	s.listener = ln

	// 2. Create xshard connection pool (separate physical TCP)
	s.xshardPool = NewXshardPool(cfg.Logger)

	// 3. Create dispatcher (masterConn will be set when master connects)
	s.dispatcher = NewDispatcher(nil, cfg.Logger)

	// 4. Initialize peer connection maps for each owned branch
	for _, branch := range cfg.OwnBranches {
		s.peerConns[branch] = make(map[uint64]*PeerConn)
	}

	// NOTE: acceptLoop is NOT started here.  Callers must register handlers
	// via SlaveRPC.RegisterHandlers() first, then call Start() (or Serve(),
	// which calls Start() internally).  This avoids a race where the master
	// connects before handlers are registered.

	s.log.Info("slave initialized", "id", cfg.ID, "branches", cfg.OwnBranches, "listen", cfg.ListenAddr)
	return s, nil
}

// Start begins accepting incoming connections (master + other slaves).
// Must be called after RegisterHandlers() (typically via SlaveRPC.RegisterHandlers)
// so handlers are in place before any connection arrives.
//
// Safe to call multiple times — only the first call launches acceptLoop.
// Serve() calls Start() internally, so callers using Serve() do not need to
// call Start() explicitly.
func (s *Slave) Start() {
	s.startOnce.Do(func() { go s.acceptLoop() })
}

// acceptLoop accepts incoming connections from the listener.
// The first connection is assumed to be from the master (matching Python's
// SlaveServer which creates a MasterConnection for the master and a
// SlaveConnection for other slaves).
func (s *Slave) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return
			}
			s.log.Error("accept error", "err", err)
			return
		}
		go s.handleConn(conn)
	}
}

// handleConn processes a new incoming connection.
// If no master connection is set yet, this is treated as the master connection
// (matching Python's behavior where the master connects first).
// Otherwise it is treated as a slave-to-slave xshard connection.
func (s *Slave) handleConn(conn net.Conn) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		conn.Close()
		return
	}
	masterSet := s.masterConn != nil
	s.mu.Unlock()

	if !masterSet {
		// First connection: master.
		// Re-check under the lock to avoid a race where two concurrent
		// handleConn goroutines both see masterSet=false and both create
		// a MasterConn (the second would overwrite and leak the first).
		mc := NewMasterConnFromConn(conn, s.log)
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			mc.Close()
			return
		}
		if s.masterConn != nil {
			// Another goroutine won the race; close this duplicate.
			s.mu.Unlock()
			mc.Close()
			return
		}
		s.masterConn = mc
		// Apply any handlers registered before master connected
		for opcode, handler := range s.masterHandlers {
			mc.RegisterHandler(opcode, handler)
		}
		// Wire dispatcher before Start so readLoop never sees a nil OnFrame.
		mc.OnFrame = s.dispatcher.Dispatch
		s.dispatcher.SetMasterConn(mc)
		s.mu.Unlock()

		s.log.Info("master connected", "remote", conn.RemoteAddr())
		// Start the read loop last, after all wiring is complete.
		mc.Start()
	} else {
		// Subsequent connections: slave-to-slave xshard
		s.log.Info("slave connected", "remote", conn.RemoteAddr())
		// Wrap and apply the SLAVE_OP_RPC_MAP handlers directly from Slave.
		// (Previously this went through XshardPool.inboundHandlers, which was
		// never wired up — see BUG note.  Slave.xshardHandlers is the single
		// source of truth for both outbound and inbound xshard handlers,
		// matching Python's SlaveConnection which uses the same
		// SLAVE_OP_RPC_MAP for both directions.)
		xc := NewXshardConnFromConn(conn, s.log)
		s.applyXshardHandlers(xc)
		s.xshardPool.TrackInbound(xc)
		xc.Start()

		// Match Python SlaveConnectionManager.handle_new_connection which
		// awaits wait_until_ping_received() before indexing the conn by shard.
		// We do this in a goroutine so acceptLoop is not blocked; if the peer
		// never sends PING, the conn is closed by the timeout/peer disconnect
		// and will be cleaned up by TrackInbound's lifecycle.
		go func(xc *XshardConn) {
			if !xc.WaitUntilPingReceived() {
				s.log.Warn("inbound xshard conn closed before PING", "remote", xc.RemoteAddr())
				xc.Close()
				return
			}
			// Index by every shard the peer owns (matches Python
			// _add_slave_connection: full_shard_id_to_slaves[fid].append(slave)).
			for _, fullShardID := range xc.RemoteFullShardIDList() {
				target := FullShardID{
					ChainID: fullShardID >> 16,
					ShardID: fullShardID & 0xFFFF,
				}
				s.xshardPool.Add(target, xc)
			}
			// Register the peer id so ConnectToSlaves will skip it (matches
			// Python SlaveConnectionManager._add_slave_connection: slave_ids.add).
			remoteID := string(xc.RemoteID())
			s.mu.Lock()
			s.connectedSlaveIDs[remoteID] = struct{}{}
			s.mu.Unlock()
			s.log.Info("inbound xshard conn indexed",
				"remote", xc.RemoteAddr(), "remote_id", remoteID,
				"shards", xc.RemoteFullShardIDList())
		}(xc)
	}
}

// waitForMaster blocks until the master has connected or ctx is done.
// Used by SlaveRPC.Serve() to wait for the master before serving.
func (s *Slave) waitForMaster(ctx context.Context) error {
	for {
		s.mu.RLock()
		mc := s.masterConn
		s.mu.RUnlock()
		if mc != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// ID returns the slave's ID (ASCII bytes).
func (s *Slave) ID() []byte { return s.id }

// FullShardIDList returns the full shard IDs owned by this slave.
func (s *Slave) FullShardIDList() []uint32 { return s.fullShardIDList }

// ── Handler registration (inbound: Master → Slave) ─────────────────────

// MasterHandler is a handler function for master cluster RPC opcodes.
// It receives the decoded frame and returns the response payload bytes.
// Return nil, ErrNotImplemented if the opcode is not yet wired up.
type MasterHandler = func(*Frame) ([]byte, error)

// RegisterMasterHandler registers a handler for a specific master cluster RPC
// opcode.  Panics if handler is nil.
//
// If the master has not yet connected (masterConn is nil), the handler is
// cached in masterHandlers and applied when the master connects in handleConn.
func (s *Slave) RegisterMasterHandler(opcode byte, handler MasterHandler) {
	if handler == nil {
		panic("handler must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.masterHandlers[opcode] = handler
	if s.masterConn != nil {
		s.masterConn.RegisterHandler(opcode, handler)
	}
}

// RegisterMasterHandlers is a convenience method to register multiple handlers
// at once from a map.
func (s *Slave) RegisterMasterHandlers(handlers map[byte]MasterHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for opcode, handler := range handlers {
		if handler == nil {
			panic("handler must not be nil")
		}
		s.masterHandlers[opcode] = handler
	}
	if s.masterConn != nil {
		for opcode, handler := range handlers {
			s.masterConn.RegisterHandler(opcode, handler)
		}
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
//
// For the PING opcode (OP_PING) the registered handler is wrapped so that
// XshardConn.recordPing is invoked with the peer's id and full_shard_id_list
// BEFORE the user handler runs.  This matches Python's SlaveConnection.handle_ping
// which sets self.id / self.full_shard_id_list on the first PING.  The wrapper
// also signals pingReceived so that handleConn's WaitUntilPingReceived returns
// (matching Python SlaveConnectionManager.handle_new_connection which awaits
// wait_until_ping_received before indexing the conn into the pool).
func (s *Slave) applyXshardHandlers(conn *XshardConn) {
	s.xshardHandlersMu.RLock()
	defer s.xshardHandlersMu.RUnlock()
	for opcode, handler := range s.xshardHandlers {
		if opcode == OP_PING {
			h := handler // capture
			conn.RegisterHandler(opcode, func(frame *Frame) ([]byte, error) {
				// Best-effort recording: parse PingRequest to extract peer identity.
				// Match Python: do NOT record on empty shard list (close_with_error).
				var req PingRequest
				if err := serialize.Deserialize(serialize.NewByteBuffer(frame.Payload), &req); err == nil && len(req.FullShardIDList) > 0 {
					conn.recordPing(req.ID, req.FullShardIDList)
				}
				return h(frame)
			})
		} else {
			conn.RegisterHandler(opcode, handler)
		}
	}
}

// ── Sending to master (outbound: Slave → Master) ───────────────────────

// SendToMaster sends a fire-and-forget command to the master (RPCID=0).
func (s *Slave) SendToMaster(opcode byte, payload []byte) error {
	s.mu.RLock()
	mc := s.masterConn
	s.mu.RUnlock()
	if mc == nil {
		return fmt.Errorf("master not connected")
	}
	return mc.SendCommand(opcode, payload)
}

// SendRPCToMaster sends an RPC request to the master and waits for the response.
//
// Returns an error if the master has not connected yet.  Callers that need to
// wait for the master should use WaitForMaster() first (matching Python's
// wait_until_active() pattern).
func (s *Slave) SendRPCToMaster(ctx context.Context, opcode byte, payload []byte) (*Frame, error) {
	s.mu.RLock()
	mc := s.masterConn
	s.mu.RUnlock()
	if mc == nil {
		return nil, fmt.Errorf("master not connected")
	}
	return mc.SendRPC(ctx, opcode, payload)
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
//
// Wire compatibility: Python master's broadcast_rpc sends this request with
// metadata ClusterMetadata(ROOT_BRANCH, 0) — i.e. cluster_peer_id==0 in the
// frame header.  The actual cluster_peer_id is carried in the PAYLOAD as
// CreateClusterPeerConnectionRequest.cluster_peer_id (uint64).  Reading from
// frame.Meta.ClusterPeerID here would always yield 0 and break all PeerConn
// routing.  (Matches Python slave.py:329-330.)
func (s *Slave) HandleCreateClusterPeerConnection(frame *Frame) ([]byte, error) {
	var req CreateClusterPeerConnectionRequest
	if err := serialize.Deserialize(serialize.NewByteBuffer(frame.Payload), &req); err != nil {
		s.log.Error("failed to deserialize CreateClusterPeerConnectionRequest", "err", err)
		return nil, err
	}
	clusterPeerID := req.ClusterPeerID

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
//
// Wire compatibility: Python master's broadcast_command sends this with
// metadata ClusterMetadata(ROOT_BRANCH, 0); the cluster_peer_id is in the
// PAYLOAD (DestroyClusterPeerConnectionCommand.cluster_peer_id, uint64).
// (Matches Python slave.py:321-327.)
func (s *Slave) HandleDestroyClusterPeerConnection(frame *Frame) ([]byte, error) {
	var cmd DestroyClusterPeerConnectionCommand
	if err := serialize.Deserialize(serialize.NewByteBuffer(frame.Payload), &cmd); err != nil {
		s.log.Error("failed to deserialize DestroyClusterPeerConnectionCommand", "err", err)
		return nil, err
	}
	clusterPeerID := cmd.ClusterPeerID

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
//
// After connecting, sends a PING RPC and verifies the remote slave's id and
// full_shard_id_list match what the master advertised (matches Python behavior).
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

		// Skip self (matches Python: if slave_info.id == self.slave_server.id)
		if string(info.ID) == string(s.id) {
			resultList[i] = nil // success, self
			continue
		}

		// Skip already-connected slaves (matches Python:
		//   if slave_info.id in self.slave_ids: return "")
		s.mu.RLock()
		_, dup := s.connectedSlaveIDs[string(info.ID)]
		s.mu.RUnlock()
		if dup {
			resultList[i] = nil // success, already connected
			continue
		}

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

		// Send PING and verify the remote slave's id and shard list
		// (matches Python: id, full_shard_id_list = await slave.send_ping())
		//
		// Python's SlaveConnection.send_ping sends RootBlock(RootBlockHeader())
		// as root_tip.  We send nil (None) instead because:
		//   1. The slave-to-slave handle_ping does NOT process root_tip
		//      (only checks id and full_shard_id_list).
		//   2. Python's Optional(RootBlock) field accepts None on the wire.
		//   3. Python's send_ping itself has a TODO: "Send real root tip".
		// When full RootBlock support lands, this can send a real empty header.
		pingPayload, err := serialize.SerializeToBytes(&PingRequest{
			ID:              s.id,
			FullShardIDList: s.fullShardIDList,
			RootTip:         nil, // None — see comment above
		})
		if err != nil {
			resultList[i] = []byte("serialize ping: " + err.Error())
			conn.Close()
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := conn.SendRPC(ctx, OP_PING, pingPayload)
		cancel()
		if err != nil {
			resultList[i] = []byte("ping rpc: " + err.Error())
			conn.Close()
			continue
		}

		var pong PongResponse
		if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &pong); err != nil {
			resultList[i] = []byte("deserialize pong: " + err.Error())
			conn.Close()
			continue
		}

		// Verify remote slave's id matches what master advertised
		if string(pong.ID) != string(info.ID) {
			errMsg := fmt.Sprintf("id does not match. expect %s got %s", string(info.ID), string(pong.ID))
			resultList[i] = []byte(errMsg)
			conn.Close()
			continue
		}
		// Verify remote slave's shard list matches what master advertised
		if !shardListsEqual(pong.FullShardIDList, info.FullShardIDList) {
			errMsg := fmt.Sprintf("shard list does not match. expect %v got %v", info.FullShardIDList, pong.FullShardIDList)
			resultList[i] = []byte(errMsg)
			conn.Close()
			continue
		}

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

		s.log.Info("connected to slave", "addr", addr, "id", string(info.ID), "shards", len(info.FullShardIDList))

		// Record this slave as connected (matches Python _add_slave_connection:
		// self.slave_ids.add(slave.id)).  Also record peer identity on the conn
		// so inbound peers can identify us via RemoteID()/RemoteFullShardIDList().
		conn.recordPing(info.ID, info.FullShardIDList)
		s.mu.Lock()
		s.connectedSlaveIDs[string(info.ID)] = struct{}{}
		s.mu.Unlock()
		// resultList[i] stays nil → success (matches Python: empty str)
	}

	resp := ConnectToSlavesResponse{ResultList: resultList}
	return serialize.SerializeToBytes(&resp)
}

// shardListsEqual returns true if two full-shard-id lists contain the same elements.
func shardListsEqual(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[uint32]int, len(a))
	for _, v := range a {
		seen[v]++
	}
	for _, v := range b {
		seen[v]--
		if seen[v] < 0 {
			return false
		}
	}
	return true
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

// Serve blocks until the master connection encounters a fatal error or the
// listener is closed.
//
// The slave is already listening (NewSlave started acceptLoop), so Serve
// simply waits for a fatal error from the master connection (once it
// connects) or for the listener to close.
func (s *Slave) Serve() error {
	// Start accepting connections (idempotent — safe if Start() already called)
	s.Start()

	// Wait for the master to connect (Python master dials in after the slave
	// is listening).  Block until then or until Close().
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := s.waitForMaster(ctx); err != nil {
			s.log.Debug("waitForMaster cancelled", "err", err)
		}
	}()

	// Poll for master connection errors once masterConn is set.
	for {
		s.mu.RLock()
		mc := s.masterConn
		closed := s.closed
		s.mu.RUnlock()

		if closed {
			return nil
		}

		if mc != nil {
			// Master connected — wait for its fatal error
			err := <-mc.Error()
			if err != nil {
				s.log.Error("master connection error", "err", err)
				return err
			}
			return nil
		}

		// Master not yet connected — wait and retry
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
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
	if s.listener != nil {
		s.listener.Close()
	}
	s.dispatcher.Close()
	s.xshardPool.Close()
	s.mu.RLock()
	mc := s.masterConn
	s.mu.RUnlock()
	if mc != nil {
		mc.Close()
	}
	s.log.Info("slave shutdown complete")
}
