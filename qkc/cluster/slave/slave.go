// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// SlaveConfig holds the runtime configuration of a SlaveComm together with the
// handlers it delegates protocol work to. The slave runtime implements the
// handlers; SlaveComm only wires and owns the communication resources.
type SlaveConfig struct {
	// ID is this slave's unique identifier (e.g., []byte("S0")).
	ID []byte
	// FullShardIDList contains the shards managed by this slave.
	FullShardIDList []uint32
	// Port is the TCP port on which the slave listens for cluster connections.
	Port int
	// ClusterFullShardIDList is the cluster-wide full shard id set (py:
	// env.quark_chain_config.get_full_shard_ids()); feeds the xshard pool's
	// route filter and the MasterConn's branch validator.
	ClusterFullShardIDList []uint32
	// MaxPayloadSize limits incoming frame payload size; 0 disables the limit.
	MaxPayloadSize uint32

	// Master serves business RPCs routed through the MasterConn. Its
	// CreateShards creates the local shard runtime and reports the branches
	// it created.
	Master MasterHandler
	// Peer builds and serves slave-to-slave PeerConns for virtual cluster peers.
	Peer PeerHandler
	// Xshard serves requests received through XshardConns.
	Xshard XshardHandler

	// Logger defaults to log.Root() if nil.
	Logger log.Logger
}

// Validate returns an error if the configuration is unusable.
func (cfg *SlaveConfig) Validate() error {
	if len(cfg.ID) == 0 {
		return errors.New("slave id is required")
	}
	if len(cfg.FullShardIDList) == 0 {
		return errors.New("full shard id list is required")
	}
	if cfg.Port <= 0 {
		return errors.New("slave port must be positive")
	}

	if len(cfg.ClusterFullShardIDList) == 0 {
		return errors.New("cluster full shard id list is required")
	}

	if cfg.Master == nil {
		return errors.New("master handler must not be nil")
	}
	if cfg.Peer == nil {
		return errors.New("peer handler must not be nil")
	}
	if cfg.Xshard == nil {
		return errors.New("xshard handler must not be nil")
	}
	return nil
}

// SlaveComm owns the slave's communication resources: the listener, MasterConn,
// XshardPool and the virtual cluster-peer topology (py: SlaveServer minus the
// business state).
//
// Lifecycle: New → Start → (master connection established) → Stop.
// Start-once is an owner contract; Stop is idempotent with two legal triggers
// (owner and master-loss). Stop only initiates shutdown: it asks every owner to
// close and returns without waiting for the goroutines it unblocks to exit
// (py: shutdown issues the closes and returns, it never awaits them). No "ready"
// state exists: whether the master wire is usable is the MasterConn's own
// connection state (py: ConnectionState), never mirrored here.
type SlaveComm struct {
	cfg    SlaveConfig
	logger log.Logger

	listener net.Listener
	// master is the established master connection, published atomically by
	// runMasterConn for the first inbound and never replaced (py:
	// slave_server.master). The Send*ToMaster public APIs may run on any
	// goroutine, so publication is atomic: Load is race-free against the single
	// Store. A nil read means the master connection does not exist
	// (ErrNotActive); whether it is still open is answered by the delegate's
	// own state (BaseConn.state). This field records establishment, not
	// classification — which inbound is the master is acceptLoop's own loop-local
	// control flow.
	master atomic.Pointer[MasterConn]

	xshardPool *XshardPool

	// Peer topology, guarded by peersMu. Invariant:
	// peers[p][b] exists ⇒ p ∈ clusterPeerIDs ∧ b ∈ localBranches.
	peersMu sync.RWMutex
	// localBranches is the set of branches CreateShards reported as created
	// (py: slave_server.shards keys) — never inferred from FullShardIDList.
	localBranches map[uint32]struct{}
	// clusterPeerIDs is the set of announced virtual cluster peers (py:
	// SlaveServer.cluster_peer_ids).
	clusterPeerIDs map[uint64]struct{}
	// peers is the (cluster_peer_id, branch) → PeerConn registry (py:
	// shard.peers).
	peers map[uint64]map[uint32]*PeerConn

	// shutdownOnce guards only the shutdown notification (py:
	// shutdown_future.done() guard): close(stopped) must happen exactly once
	// even though Stop has several potentially concurrent triggers (owner Stop,
	// master loss, startup failure). The resource closes below are individually
	// idempotent and are NOT once-guarded — they repeat on every Stop call,
	// exactly as py's close_all()/server.close() do.
	shutdownOnce sync.Once
	// stopped is the shutdown notification (py: SlaveServer.shutdown_future).
	// It is closed once by Stop after every close request has been issued; it
	// does NOT wait for the goroutines unblocked by those closes to exit. The
	// slave process main and tests consume it to learn that shutdown has been
	// triggered, exactly as py awaits do_loop/get_shutdown_future.
	stopped chan struct{}
}

var _ SlaveConnHandler = (*SlaveComm)(nil)

// NewSlaveComm creates a fully-initialized but unstarted SlaveComm: the xshard
// pool, ready registries and lifecycle primitives are all constructed here, so an
// error means the object is unusable and discarded. localBranches starts empty
// and is populated by CreateShardsAndPeerConnections once the business runtime
// reports the branches it created.
func NewSlaveComm(cfg SlaveConfig) (*SlaveComm, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid slave config: %w", err)
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Root()
	}
	pool, err := NewXshardPool(cfg.ID, cfg.FullShardIDList, cfg.ClusterFullShardIDList, cfg.MaxPayloadSize, cfg.Xshard, cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("new xshard pool: %w", err)
	}
	return &SlaveComm{
		cfg:            cfg,
		localBranches:  make(map[uint32]struct{}),
		clusterPeerIDs: make(map[uint64]struct{}),
		peers:          make(map[uint64]map[uint32]*PeerConn),
		xshardPool:     pool,
		stopped:        make(chan struct{}),
		logger:         cfg.Logger,
	}, nil
}

// ── Lifecycle ────────────────────────────────────────────────────────────────

// Start begins listening for cluster connections and dispatches the event loop.
// Lifecycle contract: called exactly once by the owner, before Stop; restart is
// not supported. The xshard pool is already created by the constructor, so the
// only failure is binding the listener, after which the object must be discarded.
func (s *SlaveComm) Start() error {
	addr := net.JoinHostPort("0.0.0.0", strconv.Itoa(s.cfg.Port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	// Install before any goroutine exists that could read these fields.
	s.listener = ln

	go s.acceptLoop()

	s.logger.Info("slave server started", "addr", ln.Addr().String(), "id", string(s.cfg.ID))
	return nil
}

// Stop initiates shutdown: it asks every owner to close — the listener, the
// xshard pool, every PeerConn, and the established master connection — then
// resolves the shutdown notification and returns immediately. It does not wait
// for the goroutines blocked on those resources to exit; the closes unblock
// Accept/Read/Handshake and those goroutines exit on their own (py: shutdown
// issues the closes and returns without awaiting; in-flight handler tasks are
// never cancelled). The listener is closed first so no new connection races the
// drain.
//
// Two layers, matching py shutdown(): the resource closes are not once-guarded
// — each is individually idempotent and repeats on every Stop call (py:
// close_all()/server.close() rerun on every shutdown); only the notification
// (close(stopped)) is once, guarded by shutdownOnce (py: if not done():
// set_result). It is delivered after the closes are issued, matching py's
// observable ordering: asyncio resumes the waiter only after shutdown() has
// synchronously run its closes.
func (s *SlaveComm) Stop() {
	// Resource closes are not once-guarded: each is individually idempotent
	// and repeats on every Stop call, matching py's close_all()/server.close().
	s.listener.Close()
	s.xshardPool.Close()
	s.closeAllPeers()
	if mc := s.master.Load(); mc != nil {
		mc.Close()
	}

	// Shutdown notification (py: shutdown_future): resolve once, after every
	// close request is issued. Consumers awaiting WaitStopped observe shutdown
	// as triggered; this is not a drained-goroutine signal. shutdownOnce guards
	// only this close — sync.Once ignores later Do calls, so the notification
	// is sticky-once like py's done()/set_result pair.
	s.shutdownOnce.Do(func() {
		close(s.stopped)
		s.logger.Info("slave server stopped")
	})
}

// WaitStopped returns the shutdown notification channel (py:
// SlaveServer.get_shutdown_future). It is closed once Stop has issued every
// close request, without waiting for the goroutines they unblock to exit. The
// process main and tests consume it to learn shutdown was triggered.
func (s *SlaveComm) WaitStopped() <-chan struct{} {
	return s.stopped
}

// ── Business outbound: master sends ──────────────────────────────────────────

// SendMinorBlockHeaderToMaster reports a new minor block header to the master
// (py: SlaveServer.send_minor_block_header_to_master). Before the master
// connection exists it returns ErrNotActive (py crashes with AttributeError);
// after it closes the delegate returns ErrConnectionClosed.
func (s *SlaveComm) SendMinorBlockHeaderToMaster(ctx context.Context, req *wire.AddMinorBlockHeaderRequest) (*wire.AddMinorBlockHeaderResponse, error) {
	mc := s.master.Load()
	if mc == nil {
		return nil, conn.ErrNotActive
	}
	return mc.SendAddMinorBlockHeader(ctx, req)
}

// SendMinorBlockHeaderListToMaster reports a list of new minor block headers
// to the master (py: SlaveServer.send_minor_block_header_list_to_master).
// Before the master connection exists it returns ErrNotActive; after it
// closes the delegate returns ErrConnectionClosed.
func (s *SlaveComm) SendMinorBlockHeaderListToMaster(ctx context.Context, req *wire.AddMinorBlockHeaderListRequest) (*wire.AddMinorBlockHeaderListResponse, error) {
	mc := s.master.Load()
	if mc == nil {
		return nil, conn.ErrNotActive
	}
	return mc.SendAddMinorBlockHeaderList(ctx, req)
}

// ── Business outbound: xshard broadcasts ─────────────────────────────────────

// SendXshardTxList broadcasts an AddXshardTxListRequest to every slave
// connection serving branch (py: SlaveServer.broadcast_xshard_tx_list, remote
// leg). Local shard delivery is the caller's (Backend) responsibility. An
// empty connection set is a no-op, matching py's gather([]).
func (s *SlaveComm) SendXshardTxList(ctx context.Context, branch uint32, req *wire.AddXshardTxListRequest) error {
	for _, conn := range s.xshardPool.Lookup(branch) {
		if err := conn.SendAddXshardTxList(ctx, req); err != nil {
			return err
		}
	}
	return nil
}

// SendBatchXshardTxList broadcasts a BatchAddXshardTxListRequest to every
// slave connection serving branch.
func (s *SlaveComm) SendBatchXshardTxList(ctx context.Context, branch uint32, req *wire.BatchAddXshardTxListRequest) error {
	for _, conn := range s.xshardPool.Lookup(branch) {
		if err := conn.SendBatchAddXshardTxList(ctx, req); err != nil {
			return err
		}
	}
	return nil
}

// ── Business outbound: peer sends ────────────────────────────────────────────
//
// The business layer never holds a *PeerConn: these veneers resolve it by
// (clusterPeerID, branch) inside SlaveComm.

// SendPeerNewBlock sends a minor block to the peer's (clusterPeerID, branch)
// connection (py: PeerShardConnection.send_new_block).
func (s *SlaveComm) SendPeerNewBlock(clusterPeerID uint64, branch uint32, cmd *wire.NewBlockMinorCommand) error {
	pc, err := s.requirePeer(clusterPeerID, branch)
	if err != nil {
		return err
	}
	return pc.SendNewBlock(cmd)
}

// SendPeerNewMinorBlockHeaderList sends a new-tip header list to the peer's
// (clusterPeerID, branch) connection (py: PeerShardConnection.broadcast_new_tip).
func (s *SlaveComm) SendPeerNewMinorBlockHeaderList(clusterPeerID uint64, branch uint32, cmd *wire.NewMinorBlockHeaderListCommand) error {
	pc, err := s.requirePeer(clusterPeerID, branch)
	if err != nil {
		return err
	}
	return pc.SendNewMinorBlockHeaderList(cmd)
}

// SendPeerTransactionList sends a transaction list to the peer's
// (clusterPeerID, branch) connection (py: PeerShardConnection.broadcast_tx_list).
func (s *SlaveComm) SendPeerTransactionList(clusterPeerID uint64, branch uint32, cmd *wire.NewTransactionListCommand) error {
	pc, err := s.requirePeer(clusterPeerID, branch)
	if err != nil {
		return err
	}
	return pc.SendTransactionList(cmd)
}

// GetPeerMinorBlockList issues an active RPC to the peer's (clusterPeerID,
// branch) connection (py: write_rpc_request(GET_MINOR_BLOCK_LIST_REQUEST)).
func (s *SlaveComm) GetPeerMinorBlockList(ctx context.Context, clusterPeerID uint64, branch uint32, req *wire.GetMinorBlockListRequest) (*wire.GetMinorBlockListResponse, error) {
	pc, err := s.requirePeer(clusterPeerID, branch)
	if err != nil {
		return nil, err
	}
	return pc.GetMinorBlockList(ctx, req)
}

// GetPeerMinorBlockHeaderList issues an active RPC to the peer's
// (clusterPeerID, branch) connection (py: SyncTask.__download_block_headers).
func (s *SlaveComm) GetPeerMinorBlockHeaderList(ctx context.Context, clusterPeerID uint64, branch uint32, req *wire.GetMinorBlockHeaderListRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
	pc, err := s.requirePeer(clusterPeerID, branch)
	if err != nil {
		return nil, err
	}
	return pc.GetMinorBlockHeaderList(ctx, req)
}

// GetPeerMinorBlockHeaderListWithSkip issues an active RPC to the peer's
// (clusterPeerID, branch) connection, skipping headers already known locally.
func (s *SlaveComm) GetPeerMinorBlockHeaderListWithSkip(ctx context.Context, clusterPeerID uint64, branch uint32, req *wire.GetMinorBlockHeaderListWithSkipRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
	pc, err := s.requirePeer(clusterPeerID, branch)
	if err != nil {
		return nil, err
	}
	return pc.GetMinorBlockHeaderListWithSkip(ctx, req)
}

// ── SlaveConnHandler: master-command orchestration ───────────────────────────

// ConnectToSlaves dials every advertised slave into the xshard pool (py:
// slave_connection_manager.connect_to_slave). Per-entry failures are recorded
// in the response result list so the master connection stays up.
func (s *SlaveComm) ConnectToSlaves(req *wire.ConnectToSlavesRequest) (*wire.ConnectToSlavesResponse, error) {
	resultList := make([]wire.PrependedSizeBytes4, len(req.SlaveInfoList))
	for i := range req.SlaveInfoList {
		info := req.SlaveInfoList[i]
		if err := s.xshardPool.DialToSlave(context.Background(), info); err != nil {
			resultList[i] = wire.PrependedSizeBytes4([]byte(err.Error()))
		}
	}
	return &wire.ConnectToSlavesResponse{ResultList: resultList}, nil
}

// CreateShardsAndPeerConnections orchestrates the master's PING carrying a
// RootTip (py: handle_ping → slave_server.create_shards): MasterHandler.
// CreateShards owns the creation decision and reports the branches it
// created; every reported branch is recorded in localBranches and equipped
// with a PeerConn for each announced cluster peer (py:
// Shard.create_peer_shard_connections). The communication layer never
// re-derives the creation decision from the RootTip or FullShardIDList; a
// branch already in localBranches is skipped.
func (s *SlaveComm) CreateShardsAndPeerConnections(rootTip *wire.RawBytes) error {
	// A business failure fails the PING before any topology change.
	createdBranches, err := s.cfg.Master.CreateShards(rootTip)
	if err != nil {
		return err
	}
	if len(createdBranches) == 0 {
		return nil
	}

	s.peersMu.Lock()
	newBranches := make([]uint32, 0, len(createdBranches))
	for _, branch := range createdBranches {
		if _, exists := s.localBranches[branch]; exists {
			continue
		}
		s.localBranches[branch] = struct{}{}
		newBranches = append(newBranches, branch)
	}
	peers := make([]uint64, 0, len(s.clusterPeerIDs))
	for id := range s.clusterPeerIDs {
		peers = append(peers, id)
	}
	s.peersMu.Unlock()

	for _, branch := range newBranches {
		for _, id := range peers {
			if _, err := s.addPeerConnection(id, branch); err != nil {
				s.logger.Error("equip peer connection failed", "cluster_peer_id", id, "branch", branch, "err", err)
			}
		}
	}
	return nil
}

// CreateClusterPeerConnection registers a new cluster peer and creates a
// PeerConn for it on every currently-created local branch (py: slave.py
// CREATE_CLUSTER_PEER_CONNECTION). It always succeeds from the master's point
// of view (error_code 0); duplicates are logged and skipped. Branches not
// created yet are equipped later by CreateShardsAndPeerConnections.
func (s *SlaveComm) CreateClusterPeerConnection(req *wire.CreateClusterPeerConnectionRequest) (*wire.CreateClusterPeerConnectionResponse, error) {
	id := req.ClusterPeerID

	s.peersMu.Lock()
	s.clusterPeerIDs[id] = struct{}{}
	branches := make([]uint32, 0, len(s.localBranches))
	for branch := range s.localBranches {
		branches = append(branches, branch)
	}
	s.peersMu.Unlock()

	for _, branch := range branches {
		created, err := s.addPeerConnection(id, branch)
		if err != nil {
			s.logger.Error("create peer connection failed", "cluster_peer_id", id, "branch", branch, "err", err)
			continue
		}
		if !created {
			s.logger.Error("duplicated create cluster peer connection", "cluster_peer_id", id, "branch", branch)
		}
	}
	return &wire.CreateClusterPeerConnectionResponse{}, nil
}

// DestroyClusterPeerConnection deregisters the cluster peer and closes every
// PeerConn of it (py: slave.py DESTROY_CLUSTER_PEER_CONNECTION). Fire-and-
// forget; destroying an unknown id is a no-op. Connections are closed outside
// peersMu.
func (s *SlaveComm) DestroyClusterPeerConnection(req *wire.DestroyClusterPeerConnectionCommand) error {
	id := req.ClusterPeerID

	s.peersMu.Lock()
	delete(s.clusterPeerIDs, id)
	bm, ok := s.peers[id]
	if ok {
		delete(s.peers, id)
	}
	s.peersMu.Unlock()
	if !ok {
		return nil
	}
	for _, pc := range bm {
		pc.Close()
	}
	return nil
}

// LookupPeer routes virtual peer frames from the master to the PeerConn
// serving (cluster_peer_id, branch), or nil when there is none (py:
// NULL_CONNECTION).
func (s *SlaveComm) LookupPeer(clusterPeerID uint64, branch uint32) *PeerConn {
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	if bm, ok := s.peers[clusterPeerID]; ok {
		return bm[branch]
	}
	return nil
}

// ── Internals ────────────────────────────────────────────────────────────────

// acceptLoop accepts inbound TCP connections and dispatches them. It is the
// single owner of connection classification and encodes py's "if not
// self.master" as its own serialized control flow: the first accepted
// connection is always the master, every later one is xshard. The claim is a
// loop-local flag, not shared state — no other goroutine classifies
// connections, so no lock or atomic is needed. This matches py, where the
// first handler's synchronous "self.master = ..." assignment likewise
// classifies the first connection before any second connection is processed.
// Stop closes the listener, which makes Accept return and this loop exit.
func (s *SlaveComm) acceptLoop() {
	masterClaimed := false
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Listener closed by Stop: stop accepting. Any other error is
			// transient and retried.
			if errors.Is(err, net.ErrClosed) {
				return
			}
			s.logger.Warn("accept failed", "err", err)
			continue
		}

		if !masterClaimed {
			masterClaimed = true
			go s.runMasterConn(conn)
			continue
		}
		go s.runXshardConn(conn)
	}
}

// runMasterConn runs the first inbound connection as the MasterConn and blocks
// until that connection is gone. The master pointer is stored before Start so
// any frame the readLoop processes sees an established master. Its own closure
// (master loss) triggers Stop; an external Stop reaches it by directly closing
// the established MasterConn in Stop, which unblocks this read. It is not joined
// or waited on by Stop: it holds nothing Stop waits for, and Stop is non-blocking.
func (s *SlaveComm) runMasterConn(conn net.Conn) {
	mc, err := NewMasterConn(MasterConnConfig{
		Conn:                 conn,
		MaxPayloadSize:       s.cfg.MaxPayloadSize,
		LocalID:              s.cfg.ID,
		LocalFullShardIDList: s.cfg.FullShardIDList,
		ClusterShardIDs:      s.cfg.ClusterFullShardIDList,
		SlaveConnHandler:     s,
		Handler:              s.cfg.Master,
		Logger:               s.logger,
	})
	if err != nil {
		conn.Close()
		s.logger.Error("failed to create master connection", "err", err)
		s.Stop()
		return
	}

	s.master.Store(mc)
	mc.Start()
	s.logger.Info("master connection established", "remote", conn.RemoteAddr())

	<-mc.WaitUntilClosed()
	s.logger.Info("master connection closed")
	s.Stop()
}

// runXshardConn hands a subsequent inbound connection to the pool, which owns
// the full inbound lifecycle: handshake, indexing and cleanup on close. Stop
// closes the pool, which closes the tracked connections and unblocks this
// handshake, so it exits on its own.
func (s *SlaveComm) runXshardConn(conn net.Conn) {
	s.logger.Info("accepted xshard connection", "remote", conn.RemoteAddr())
	s.xshardPool.HandleInbound(conn)
}

// requirePeer resolves the (clusterPeerID, branch) connection, or fails with
// the NULL_CONNECTION case: no sendable connection exists.
func (s *SlaveComm) requirePeer(clusterPeerID uint64, branch uint32) (*PeerConn, error) {
	if pc := s.LookupPeer(clusterPeerID, branch); pc != nil {
		return pc, nil
	}
	return nil, fmt.Errorf("no peer connection for cluster_peer_id %d branch 0x%x", clusterPeerID, branch)
}

// addPeerConnection is the single construction path for every PeerConn: it
// builds, starts and registers (clusterPeerID, branch), reporting created
// false when the pair already exists. Ownership stays with SlaveComm; callers
// are master-command dispatchers, so the master connection is always published
// when this runs. It does not gate on Stop: a handler already in flight when
// the registry drains may still complete one registration — the same terminal
// window py accepts (in-flight handler tasks are never cancelled).
func (s *SlaveComm) addPeerConnection(clusterPeerID uint64, branch uint32) (created bool, err error) {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()
	bm, ok := s.peers[clusterPeerID]
	if !ok {
		bm = make(map[uint32]*PeerConn)
	}
	if _, exists := bm[branch]; exists {
		return false, nil
	}
	pc, err := NewPeerConn(clusterPeerID, branch, s.master.Load(), s.cfg.Peer, s.logger)
	if err != nil {
		return false, err
	}
	pc.Start()
	bm[branch] = pc
	s.peers[clusterPeerID] = bm
	return true, nil
}

// closeAllPeers removes and closes every registered PeerConn and clears the
// known-peer set (py: MasterConnection.close, the master-loss leg). PeerConns
// recorded later by an in-flight handler are a terminal best-effort residue the
// process is about to exit with (py semantics).
func (s *SlaveComm) closeAllPeers() {
	s.peersMu.Lock()
	var all []*PeerConn
	for _, bm := range s.peers {
		for _, pc := range bm {
			all = append(all, pc)
		}
	}
	s.peers = make(map[uint64]map[uint32]*PeerConn)
	s.clusterPeerIDs = make(map[uint64]struct{})
	s.peersMu.Unlock()
	for _, pc := range all {
		pc.Close()
	}
}
