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
	"github.com/ethereum/go-ethereum/qkc/types"
)

// MasterBackend defines the business operations used by SlaveComm.
// It is implemented by the external slave runtime; SlaveComm consumes
// these operations and performs the communication-side orchestration.
type MasterBackend interface {
	// ShardCreator creates the business runtime's shards for a root tip
	// and returns the newly-created branches.
	ShardCreator(rootTip *types.RootBlock) ([]uint32, error)

	// ── business RPCs ──
	Mine(req *wire.MineRequest) (*wire.MineResponse, error)
	GenTx(req *wire.GenTxRequest) (*wire.GenTxResponse, error)
	AddRootBlock(req *wire.AddRootBlockRequest) (*wire.AddRootBlockResponse, error)
	GetEcoInfoList(req *wire.GetEcoInfoListRequest) (*wire.GetEcoInfoListResponse, error)
	GetNextBlockToMine(req *wire.GetNextBlockToMineRequest) (*wire.GetNextBlockToMineResponse, error)
	AddMinorBlock(req *wire.AddMinorBlockRequest) (*wire.AddMinorBlockResponse, error)
	GetUnconfirmedHeaders(req *wire.GetUnconfirmedHeadersRequest) (*wire.GetUnconfirmedHeadersResponse, error)
	GetAccountData(req *wire.GetAccountDataRequest) (*wire.GetAccountDataResponse, error)
	AddTransaction(req *wire.AddTransactionRequest) (*wire.AddTransactionResponse, error)
	GetMinorBlock(req *wire.GetMinorBlockRequest) (*wire.GetMinorBlockResponse, error)
	GetTransaction(req *wire.GetTransactionRequest) (*wire.GetTransactionResponse, error)
	SyncMinorBlockList(req *wire.SyncMinorBlockListRequest) (*wire.SyncMinorBlockListResponse, error)
	ExecuteTransaction(req *wire.ExecuteTransactionRequest) (*wire.ExecuteTransactionResponse, error)
	GetTransactionReceipt(req *wire.GetTransactionReceiptRequest) (*wire.GetTransactionReceiptResponse, error)
	GetTransactionListByAddress(req *wire.GetTransactionListByAddressRequest) (*wire.GetTransactionListByAddressResponse, error)
	GetLogs(req *wire.GetLogRequest) (*wire.GetLogResponse, error)
	EstimateGas(req *wire.EstimateGasRequest) (*wire.EstimateGasResponse, error)
	GetStorageAt(req *wire.GetStorageRequest) (*wire.GetStorageResponse, error)
	GetCode(req *wire.GetCodeRequest) (*wire.GetCodeResponse, error)
	GasPrice(req *wire.GasPriceRequest) (*wire.GasPriceResponse, error)
	GetWork(req *wire.GetWorkRequest) (*wire.GetWorkResponse, error)
	SubmitWork(req *wire.SubmitWorkRequest) (*wire.SubmitWorkResponse, error)
	CheckMinorBlock(req *wire.CheckMinorBlockRequest) (*wire.CheckMinorBlockResponse, error)
	GetAllTransactions(req *wire.GetAllTransactionsRequest) (*wire.GetAllTransactionsResponse, error)
	GetRootChainStakes(req *wire.GetRootChainStakesRequest) (*wire.GetRootChainStakesResponse, error)
	GetTotalBalance(req *wire.GetTotalBalanceRequest) (*wire.GetTotalBalanceResponse, error)
}

// ── masterHandler facade ─────────────────────────────────────────────────────
//
// masterHandler is the MasterHandler SlaveComm installs into MasterConn. It
// combines SlaveComm's communication orchestration with the external business
// backend: the topology & shard-activation commands below are served by
// SlaveComm itself; the business RPCs are embedded MasterBackend methods.
type masterHandler struct {
	MasterBackend
	comm *SlaveComm
}

var _ MasterHandler = (*masterHandler)(nil)

// newMasterHandler builds the MasterHandler MasterConn is configured with.
func (s *SlaveComm) newMasterHandler() MasterHandler {
	return &masterHandler{
		MasterBackend: s.cfg.Master,
		comm:          s,
	}
}

// ── topology & shard activation: served by SlaveComm ──

// CreateShards handles a PING root tip: it creates the local shards and equips
// them with PeerConns.
func (h *masterHandler) CreateShards(rootTip *types.RootBlock) error {
	return h.comm.createShards(rootTip)
}

// ConnectToSlaves dials the advertised slaves into the xshard pool.
func (h *masterHandler) ConnectToSlaves(req *wire.ConnectToSlavesRequest) (*wire.ConnectToSlavesResponse, error) {
	return h.comm.connectToSlaves(req)
}

// CreateClusterPeerConnection registers a new cluster peer and creates a
// PeerConn for it on every local branch.
func (h *masterHandler) CreateClusterPeerConnection(req *wire.CreateClusterPeerConnectionRequest) (*wire.CreateClusterPeerConnectionResponse, error) {
	return h.comm.createClusterPeerConnection(req)
}

// DestroyClusterPeerConnection removes a cluster peer and closes its PeerConns.
func (h *masterHandler) DestroyClusterPeerConnection(req *wire.DestroyClusterPeerConnectionCommand) error {
	return h.comm.destroyClusterPeerConnection(req)
}

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
	// ClusterFullShardIDList is the cluster-wide shard id set (py:
	// get_full_shard_ids()); feeds the xshard pool route filter and the
	// MasterConn branch validator.
	ClusterFullShardIDList []uint32
	// MaxPayloadSize limits incoming frame payload size; 0 disables the limit.
	MaxPayloadSize uint32

	// Master handles business RPCs routed through MasterConn.
	Master MasterBackend
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

// SlaveComm owns the slave's communication resources: the listener,
// MasterConn, XshardPool, and virtual cluster-peer topology.
//
// Lifecycle: New → Start → Stop. Start is called once by the owner.
// Stop is idempotent and returns without waiting for goroutines to exit.
type SlaveComm struct {
	cfg    SlaveConfig
	logger log.Logger

	listener net.Listener
	// master is the established MasterConn (py: slave_server.master), published
	// atomically by runMasterConn for the first inbound and never replaced. The
	// Send*ToMaster APIs may run on any goroutine, so publication is atomic: Load
	// is race-free against the single Store. nil means not established
	// (ErrNotActive); open/closed is the delegate's own state. This records
	// establishment, not classification — which inbound is the master is
	// acceptLoop's loop-local control flow.
	master atomic.Pointer[MasterConn]

	xshardPool *XshardPool

	// Peer topology, guarded by peersMu. Invariant:
	// peers[p][b] exists ⇒ p ∈ clusterPeerIDs ∧ b ∈ localBranches.
	peersMu sync.RWMutex
	// localBranches is the set of branches currently served by this slave.
	localBranches map[uint32]struct{}
	// clusterPeerIDs is the set of announced virtual cluster peers (py:
	// SlaveServer.cluster_peer_ids).
	clusterPeerIDs map[uint64]struct{}
	// peers is the (cluster_peer_id, branch) → PeerConn registry (py: shard.peers).
	peers map[uint64]map[uint32]*PeerConn

	// shutdownOnce guards only the shutdown notification (py: shutdown_future.done()):
	// close(stopped) must happen exactly once across Stop's concurrent triggers (owner,
	// master loss, startup failure). The resource closes below are NOT once-guarded —
	// each is individually idempotent and repeats on every Stop call, as in py.
	shutdownOnce sync.Once
	// stopped is the shutdown notification (py: SlaveServer.shutdown_future), closed
	// once every close request has been issued, without waiting for the goroutines
	// those closes unblock. Consumers (process main, tests) read it to learn shutdown
	// was triggered.
	stopped chan struct{}
}

var _ PeerResolver = (*SlaveComm)(nil)

// NewSlaveComm constructs a fully-initialized but unstarted SlaveComm. An error
// here means the object is unusable and discarded.
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

// Start binds the listener and dispatches the event loop. Owner contract: called
// exactly once, before Stop; restart is unsupported. The only failure is binding
// the listener, after which the object must be discarded.
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

// Stop initiates shutdown and returns without waiting for goroutines to exit.
// Resource closes are intentionally not once-guarded (each is individually
// idempotent); only the shutdown notification is once-guarded.
//
// stopped is closed before loading master so runMasterConn can detect a MasterConn
// published after Stop has already observed master as nil, and close it in its own
// post-publication compensation.
func (s *SlaveComm) Stop() {
	s.shutdownOnce.Do(func() {
		close(s.stopped)
	})
	if s.listener != nil {
		s.listener.Close()
	}
	s.xshardPool.Close()
	s.closeAllPeers()
	if mc := s.master.Load(); mc != nil {
		mc.Close()
	}
	s.logger.Info("slave server stopped")
}

// WaitStopped returns the shutdown notification channel (py: get_shutdown_future),
// closed once Stop has issued every close request, without waiting for the
// goroutines those closes unblock.
func (s *SlaveComm) WaitStopped() <-chan struct{} {
	return s.stopped
}

// ── Business outbound: master sends ──────────────────────────────────────────

// SendMinorBlockHeaderToMaster reports a new minor block header (py:
// send_minor_block_header_to_master). Returns ErrNotActive before the master
// connection exists and ErrConnectionClosed after it closes.
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

// SendXshardTxList broadcasts an AddXshardTxListRequest to every slave connection
// serving branch (py: broadcast_xshard_tx_list, remote leg); local delivery is the
// caller's. An empty connection set is a no-op, matching py's gather([]).
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
// The business layer never holds a *PeerConn; these veneers resolve it by
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

// LookupPeer routes virtual peer frames from the master to the PeerConn serving
// (cluster_peer_id, branch), or nil when there is none (py: NULL_CONNECTION).
func (s *SlaveComm) LookupPeer(clusterPeerID uint64, branch uint32) *PeerConn {
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	if bm, ok := s.peers[clusterPeerID]; ok {
		return bm[branch]
	}
	return nil
}

// ── Internals ────────────────────────────────────────────────────────────────

// connectToSlaves dials every advertised slave into the xshard pool.
// Per-entry failures are recorded in the response result list so the master
// connection stays up.
func (s *SlaveComm) connectToSlaves(req *wire.ConnectToSlavesRequest) (*wire.ConnectToSlavesResponse, error) {
	resultList := make([]wire.PrependedSizeBytes4, len(req.SlaveInfoList))
	for i := range req.SlaveInfoList {
		info := req.SlaveInfoList[i]
		if err := s.xshardPool.DialToSlave(context.Background(), info); err != nil {
			resultList[i] = wire.PrependedSizeBytes4([]byte(err.Error()))
		}
	}
	return &wire.ConnectToSlavesResponse{ResultList: resultList}, nil
}

// createShards records newly-created branches in localBranches and equips
// each with a PeerConn for every announced cluster peer. Existing branches
// are skipped.
func (s *SlaveComm) createShards(rootTip *types.RootBlock) error {
	// A business failure fails the PING before any topology change.
	createdBranches, err := s.cfg.Master.ShardCreator(rootTip)
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
	// Snapshot the announced cluster peers to equip each new branch with.
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

// createClusterPeerConnection registers a new cluster peer and creates a
// PeerConn for it on every currently-created local branch. It always succeeds
// from the master's point of view (error_code 0); duplicates are logged and
// skipped. Branches not created yet are equipped later by createShards.
func (s *SlaveComm) createClusterPeerConnection(req *wire.CreateClusterPeerConnectionRequest) (*wire.CreateClusterPeerConnectionResponse, error) {
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

// destroyClusterPeerConnection deregisters the cluster peer and closes every
// PeerConn of it. Fire-and-forget; destroying an unknown id is a no-op.
// Connections are closed outside peersMu.
func (s *SlaveComm) destroyClusterPeerConnection(req *wire.DestroyClusterPeerConnectionCommand) error {
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

// acceptLoop accepts inbound connections and is the single owner of
// classification, encoding py's "if not self.master" as its own serialized
// control flow: the first accepted connection is always the master, every later
// one is xshard. The claim is a loop-local flag — no other goroutine classifies
// connections — so no lock or atomic is needed. Stop closes the listener, which
// makes Accept return and this loop exit.
func (s *SlaveComm) acceptLoop() {
	masterClaimed := false
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Listener closed by Stop; other errors are transient and retried.
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
// until that connection is gone. The pointer is stored before Start so any frame
// the readLoop processes sees an established master. Master loss triggers Stop; an
// external Stop reaches here by closing the established MasterConn directly. Not
// joined or waited on by Stop: it holds nothing Stop waits for, and Stop is
// non-blocking.
func (s *SlaveComm) runMasterConn(conn net.Conn) {
	mc, err := NewMasterConn(MasterConnConfig{
		Conn:                 conn,
		MaxPayloadSize:       s.cfg.MaxPayloadSize,
		LocalID:              s.cfg.ID,
		LocalFullShardIDList: s.cfg.FullShardIDList,
		ClusterShardIDs:      s.cfg.ClusterFullShardIDList,
		Handler:              s.newMasterHandler(),
		PeerResolver:         s,
		Logger:               s.logger,
	})
	if err != nil {
		conn.Close()
		s.logger.Error("failed to create master connection", "err", err)
		s.Stop()
		return
	}

	s.master.Store(mc)
	// If Stop resolved while this connection was being established, the master
	// pointer was published too late for Stop to close it (its Load saw nil).
	// Compensate here: close the just-published MasterConn and never Start it,
	// so no net.Conn / readLoop is owned beyond a resolved shutdown.
	select {
	case <-s.stopped:
		mc.Close()
		return
	default:
	}

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

// requirePeer resolves the (clusterPeerID, branch) connection or reports the
// NULL_CONNECTION case: no sendable connection exists.
func (s *SlaveComm) requirePeer(clusterPeerID uint64, branch uint32) (*PeerConn, error) {
	if pc := s.LookupPeer(clusterPeerID, branch); pc != nil {
		return pc, nil
	}
	return nil, fmt.Errorf("no peer connection for cluster_peer_id %d branch 0x%x", clusterPeerID, branch)
}

// addPeerConnection is the single construction path for every PeerConn: it builds,
// starts and registers (clusterPeerID, branch), reporting created=false on a
// duplicate. Ownership stays with SlaveComm; callers are master-command
// dispatchers, so the master connection is always published here. It does not gate
// on Stop: an in-flight handler may complete one registration after the registry
// drains — the terminal window py accepts.
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

// closeAllPeers removes and closes every registered PeerConn and clears the known
// peer set (py: MasterConnection.close, the master-loss leg). PeerConns recorded
// later by an in-flight handler are a terminal best-effort residue the process is
// about to exit with (py semantics). Close happens outside peersMu.
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
