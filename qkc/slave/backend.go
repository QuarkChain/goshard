// Copyright 2026-2027, QuarkChain.

// Package slave runs one slave identity's shards and communication resources
// inside a single process, mirroring pyquarkchain's SlaveServer. The slave owns
// the network listener, the master connection, the xshard pool, and the shards;
// persistent state lives in the per-shard chaindbs.
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
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/conn"
	"github.com/ethereum/go-ethereum/qkc/shard"
	"github.com/ethereum/go-ethereum/qkc/slaveconn"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/qkc/wire"
)

// SlaveBackend hosts one slave identity: its shards, keyed by branch as in
// pyquarkchain's SlaveServer, and its communication resources (listener, master
// connection, xshard pool, virtual cluster-peer topology).
type SlaveBackend struct {
	// ── Identity & shard registry ──────────────────────────────────────────
	// ID is this slave's identity, used as the wire-protocol identity.
	ID     string
	shards map[account.Branch]*shard.Shard
	order  []account.Branch // boot (config) order, for deterministic iteration and shutdown

	// ── Network config, derived from the SlaveContext at construction ─────
	localFullShardIDList []uint32
	clusterShardIDs      []uint32
	port                 int
	maxPayloadSize       uint32 // 0 disables the payload limit.

	// ── Communication resources ───────────────────────────────────────────
	listener net.Listener
	// master is the established master connection; nil until published, never replaced.
	master     atomic.Pointer[slaveconn.MasterConn]
	xshardPool *slaveconn.XshardPool

	// ── Virtual cluster-peer topology ─────────────────────────────────────
	// clusterPeerIDs is the announced cluster-peer set, guarded by clusterPeersMu;
	// CreateShards is its designated reader once implemented.
	clusterPeersMu sync.Mutex
	clusterPeerIDs map[uint64]struct{}

	// ── Shutdown ──────────────────────────────────────────────────────────
	stopped  chan struct{}
	stopOnce sync.Once
	stopErr  error

	// ── Logger ────────────────────────────────────────────────────────────
	logger log.Logger
}

// Compile-time interface conformance.
var (
	_ slaveconn.MasterHandler = (*SlaveBackend)(nil)
	_ slaveconn.PeerResolver  = (*SlaveBackend)(nil)
	_ slaveconn.XshardHandler = (*SlaveBackend)(nil)
	_ shard.Sender            = (*SlaveBackend)(nil)
)

// Options is the slave backend's injection surface — process-level dependencies
// the backend and its connections use but the cluster config does not carry.
type Options struct {
	// Logger is the process logger the slave and its connections log through.
	// nil means log.Root().
	Logger log.Logger

	// MaxPayloadSize bounds a single frame's payload on the master and xshard
	// connections; 0 disables the limit. A Go-side guard with no pyquarkchain
	// counterpart, so it has no config-derived default.
	MaxPayloadSize uint32
}

// New boots every shard the context's slave owns, eagerly and in config order,
// and assembles the communication resources (xshard pool; the listener is bound
// later by Start). Eager shard construction is interim scaffolding: with no
// master in the cluster yet it is the only way to bring the shards up; the
// PING(root_tip)-driven path is stubbed in CreateShards below.
//
// On any failure the shards already started are stopped and their databases
// closed before the error returns, so the datadir stays reopenable.
func New(ctx *config.SlaveContext, rootGenesis *types.RootBlockHeader, opts Options, shardOpts shard.Options) (*SlaveBackend, error) {
	// Port is config data (ctx.Slave.Port), not an injection seam — but a zero
	// port is never a valid listen address, and SlaveContext can be hand-built
	// (tests, callers that skip ClusterConfig.Validate), so reject it here before
	// any shard or socket is opened.
	if ctx.Slave.Port == 0 {
		return nil, fmt.Errorf("slave %s: invalid port 0", ctx.ID)
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.Root()
	}
	b := &SlaveBackend{
		ID:                   ctx.ID,
		shards:               make(map[account.Branch]*shard.Shard, len(ctx.FullShardIDs())),
		localFullShardIDList: append([]uint32(nil), ctx.FullShardIDs()...),
		port:                 int(ctx.Slave.Port),
		maxPayloadSize:       opts.MaxPayloadSize,
		clusterShardIDs:      ctx.Quarkchain.GetGenesisShardIds(),
		clusterPeerIDs:       make(map[uint64]struct{}),
		stopped:              make(chan struct{}),
		logger:               logger,
	}

	pool, err := slaveconn.NewXshardPool([]byte(b.ID), b.localFullShardIDList, b.clusterShardIDs, b.maxPayloadSize, b, b.logger)
	if err != nil {
		return nil, fmt.Errorf("slave %s: new xshard pool: %w", b.ID, err)
	}
	b.xshardPool = pool

	for _, id := range ctx.FullShardIDs() {
		branch := account.NewBranch(id)
		s, err := shard.New(ctx, branch, rootGenesis, ctx.DBPathRoot, shardOpts, b)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("slave %s: %w", b.ID, err), b.Stop())
		}
		b.shards[branch] = s
		b.order = append(b.order, branch)

		height, _ := s.Chain().Head()
		b.logger.Info("shard started", "shard", fmt.Sprintf("0x%08x", id), "genesis", s.Chain().GenesisHash(), "head", height)
	}
	return b, nil
}

// Shard returns the shard hosting branch, or nil if this slave does not own it.
func (b *SlaveBackend) Shard(branch account.Branch) *shard.Shard {
	return b.shards[branch]
}

// Shards returns the hosted shards in boot (config) order.
func (b *SlaveBackend) Shards() []*shard.Shard {
	out := make([]*shard.Shard, 0, len(b.order))
	for _, branch := range b.order {
		out = append(out, b.shards[branch])
	}
	return out
}

// ── Lifecycle ────────────────────────────────────────────────────────────────

// Start binds the listener and dispatches the accept loop. On bind failure the
// backend is stopped, so a partially started backend cannot leak (Stop is
// idempotent). Owner contract: called exactly once.
func (b *SlaveBackend) Start() error {
	addr := net.JoinHostPort("0.0.0.0", strconv.Itoa(b.port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		b.Stop()
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	b.listener = ln
	go b.acceptLoop()
	b.logger.Info("slave server started", "addr", ln.Addr().String(), "id", b.ID)
	return nil
}

// Stop initiates shutdown and returns without waiting for goroutines to exit.
// The whole sequence — close(stopped), then the listener, xshard pool, shards
// (peers, then chain, then database), then the master — runs exactly once:
// stopped is closed first so runMasterConn can detect a MasterConn published too
// late to be closed here, and so each shard's AddPeer refuses registrations
// racing with its peer drain. Shards are stopped before the master because their
// PeerConns tunnel outbound frames through it.
func (b *SlaveBackend) Stop() error {
	b.stopOnce.Do(func() {
		close(b.stopped)
		if b.listener != nil {
			b.listener.Close()
		}
		if b.xshardPool != nil {
			b.xshardPool.Close()
		}
		// Stop the shards (each closes its peers, then its chain, then its
		// database) before closing the master: PeerConns tunnel outbound frames
		// through it.
		var errs []error
		for _, branch := range b.order {
			if err := b.shards[branch].Stop(); err != nil {
				errs = append(errs, fmt.Errorf("stop shard 0x%08x: %w", branch.GetFullShardID(), err))
			}
		}
		if mc := b.master.Load(); mc != nil {
			mc.Close()
		}
		b.stopErr = errors.Join(errs...)
		b.logger.Info("slave server stopped")
	})
	return b.stopErr
}

// Done returns the shutdown notification channel (py: get_shutdown_future):
// closed at the top of Stop, before the resource closes run. A resolved channel
// means shutdown was triggered, not that every goroutine has exited.
func (b *SlaveBackend) Done() <-chan struct{} {
	return b.stopped
}

// ── Accept loop & connection dispatch ────────────────────────────────────────

// acceptLoop classifies inbound connections: the first is the master, later ones are xshard.
func (b *SlaveBackend) acceptLoop() {
	masterClaimed := false
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			b.logger.Warn("accept failed", "err", err)
			continue
		}

		if !masterClaimed {
			masterClaimed = true
			go b.runMasterConn(conn)
			continue
		}
		go b.runXshardConn(conn)
	}
}

// runMasterConn runs the first inbound connection as the master; master loss triggers Stop.
func (b *SlaveBackend) runMasterConn(conn net.Conn) {
	mc, err := slaveconn.NewMasterConn(slaveconn.MasterConnConfig{
		Conn:                 conn,
		MaxPayloadSize:       b.maxPayloadSize,
		LocalID:              []byte(b.ID),
		LocalFullShardIDList: b.localFullShardIDList,
		ClusterShardIDs:      b.clusterShardIDs,
		Handler:              b,
		PeerResolver:         b,
		Logger:               b.logger,
	})
	if err != nil {
		conn.Close()
		b.logger.Error("failed to create master connection", "err", err)
		b.Stop()
		return
	}

	b.master.Store(mc)
	// If Stop resolved while this connection was being established, the master
	// pointer was published too late for Stop to close it (its Load saw nil).
	// Compensate here: close the just-published MasterConn and never Start it.
	select {
	case <-b.stopped:
		mc.Close()
		return
	default:
	}

	mc.Start()
	b.logger.Info("master connection established", "remote", conn.RemoteAddr())

	<-mc.WaitUntilClosed()
	b.logger.Info("master connection closed")
	b.Stop()
}

// runXshardConn hands a subsequent inbound connection to the xshard pool.
func (b *SlaveBackend) runXshardConn(conn net.Conn) {
	b.logger.Info("accepted xshard connection", "remote", conn.RemoteAddr())
	b.xshardPool.HandleInbound(conn)
}

// ── PeerResolver ─────────────────────────────────────────────────────────────

// LookupPeer returns the PeerConn serving (clusterPeerID, branch), or nil when none exists.
func (b *SlaveBackend) LookupPeer(clusterPeerID uint64, branch uint32) *slaveconn.PeerConn {
	if s := b.shards[account.NewBranch(branch)]; s != nil {
		return s.Peer(clusterPeerID)
	}
	return nil
}

// ── MasterHandler: topology & shard activation ───────────────────────────────

// CreateShards creates the shards made eligible by the root tip and wires them
// to the announced cluster peers. Unimplemented: it returns nil because an
// error here would close the master connection (PING path). Shards are
// immutable after New until it is implemented.
//
// TODO(create-shards): guard shards/order/clusterPeerIDs with a topology mutex,
// and wire new shards to the announced peers before registering them.
func (b *SlaveBackend) CreateShards(*types.RootBlock) error {
	return nil
}

// CreateClusterPeerConnection registers the cluster peer and wires a PeerConn on every local shard.
func (b *SlaveBackend) CreateClusterPeerConnection(req *wire.CreateClusterPeerConnectionRequest) (*wire.CreateClusterPeerConnectionResponse, error) {
	id := req.ClusterPeerID

	b.clusterPeersMu.Lock()
	b.clusterPeerIDs[id] = struct{}{}
	b.clusterPeersMu.Unlock()

	mc := b.master.Load()
	for _, branch := range b.order {
		if !b.wireClusterPeer(b.shards[branch], id, mc) {
			break // shutdown: every remaining shard would be refused too
		}
	}
	return &wire.CreateClusterPeerConnectionResponse{}, nil
}

// wireClusterPeer wires clusterPeerID onto s, returning false only when the
// shard is stopped; mc must be the established master connection.
func (b *SlaveBackend) wireClusterPeer(s *shard.Shard, clusterPeerID uint64, mc *slaveconn.MasterConn) bool {
	if mc == nil {
		b.logger.Error("cannot wire cluster peer without an established master connection",
			"cluster_peer_id", clusterPeerID, "branch", s.Branch.GetFullShardID())
		return true
	}
	created, err := s.AddPeer(clusterPeerID, mc)
	switch {
	case errors.Is(err, shard.ErrStopped):
		return false
	case err != nil:
		b.logger.Error("create peer connection failed", "cluster_peer_id", clusterPeerID, "branch", s.Branch.GetFullShardID(), "err", err)
	case !created:
		b.logger.Error("duplicated create cluster peer connection", "cluster_peer_id", clusterPeerID, "branch", s.Branch.GetFullShardID())
	}
	return true
}

// DestroyClusterPeerConnection removes the cluster peer and closes its PeerConns; unknown ids are a no-op.
func (b *SlaveBackend) DestroyClusterPeerConnection(req *wire.DestroyClusterPeerConnectionCommand) error {
	id := req.ClusterPeerID

	b.clusterPeersMu.Lock()
	delete(b.clusterPeerIDs, id)
	b.clusterPeersMu.Unlock()

	for _, branch := range b.order {
		b.shards[branch].RemovePeer(id)
	}
	return nil
}

// ConnectToSlaves dials every advertised slave concurrently (each dial
// timeout-bounded) and reports per-entry failures in the result list,
// preserving the request order.
func (b *SlaveBackend) ConnectToSlaves(req *wire.ConnectToSlavesRequest) (*wire.ConnectToSlavesResponse, error) {
	resultList := make([]wire.PrependedSizeBytes4, len(req.SlaveInfoList))

	var wg sync.WaitGroup
	for i := range req.SlaveInfoList {
		wg.Add(1)
		go func(i int, info wire.SlaveInfo) {
			defer wg.Done()

			if err := b.xshardPool.DialToSlave(context.Background(), info); err != nil {
				b.logger.Warn("connect to slave failed", "remote_id", string(info.ID), "err", err)
				resultList[i] = wire.PrependedSizeBytes4([]byte(err.Error()))
			}
		}(i, req.SlaveInfoList[i])
	}
	wg.Wait()
	return &wire.ConnectToSlavesResponse{ResultList: resultList}, nil
}

// ── Outbound: master sends (shard.Sender) ────────────────────────────────────

// SendMinorBlockHeaderToMaster reports a new minor block header to the master;
// ErrNotActive before the master connection exists, ErrConnectionClosed after it closes.
func (b *SlaveBackend) SendMinorBlockHeaderToMaster(ctx context.Context, req *wire.AddMinorBlockHeaderRequest) (*wire.AddMinorBlockHeaderResponse, error) {
	mc := b.master.Load()
	if mc == nil {
		return nil, conn.ErrNotActive
	}
	return mc.SendAddMinorBlockHeader(ctx, req)
}

// SendMinorBlockHeaderListToMaster reports a list of new minor block headers to the master.
func (b *SlaveBackend) SendMinorBlockHeaderListToMaster(ctx context.Context, req *wire.AddMinorBlockHeaderListRequest) (*wire.AddMinorBlockHeaderListResponse, error) {
	mc := b.master.Load()
	if mc == nil {
		return nil, conn.ErrNotActive
	}
	return mc.SendAddMinorBlockHeaderList(ctx, req)
}

// ── Outbound: xshard broadcasts (shard.Sender) ───────────────────────────────

// broadcastToBranch sends concurrently to every xshard connection serving
// branch and aggregates every send failure; an empty connection set is a no-op.
func (b *SlaveBackend) broadcastToBranch(branch uint32, send func(*slaveconn.XshardConn) error) error {
	conns := b.xshardPool.Lookup(branch)
	errs := make([]error, len(conns))

	var wg sync.WaitGroup
	for i, c := range conns {
		wg.Add(1)
		go func(i int, c *slaveconn.XshardConn) {
			defer wg.Done()

			if err := send(c); err != nil {
				errs[i] = fmt.Errorf("xshard conn#%d (remote=%x): %w", i, c.RemoteID(), err)
			}
		}(i, c)
	}

	wg.Wait()
	return errors.Join(errs...)
}

// SendXshardTxList broadcasts req to every connection serving branch; branch
// is the routing key and must match req.Branch.
func (b *SlaveBackend) SendXshardTxList(ctx context.Context, branch uint32, req *wire.AddXshardTxListRequest) error {
	return b.broadcastToBranch(branch, func(c *slaveconn.XshardConn) error {
		return c.SendAddXshardTxList(ctx, req)
	})
}

// SendBatchXshardTxList broadcasts req to every connection serving branch;
// branch is the routing key (the batch request carries no branch of its own).
func (b *SlaveBackend) SendBatchXshardTxList(ctx context.Context, branch uint32, req *wire.BatchAddXshardTxListRequest) error {
	return b.broadcastToBranch(branch, func(c *slaveconn.XshardConn) error {
		return c.SendBatchAddXshardTxList(ctx, req)
	})
}

// ── MasterHandler: business RPCs (stubs returning ErrHandlerNotImplemented) ──

func (b *SlaveBackend) Mine(*wire.MineRequest) (*wire.MineResponse, error) {
	return nil, fmt.Errorf("Mine: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GenTx(*wire.GenTxRequest) (*wire.GenTxResponse, error) {
	return nil, fmt.Errorf("GenTx: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) AddRootBlock(*wire.AddRootBlockRequest) (*wire.AddRootBlockResponse, error) {
	return nil, fmt.Errorf("AddRootBlock: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetEcoInfoList(*wire.GetEcoInfoListRequest) (*wire.GetEcoInfoListResponse, error) {
	return nil, fmt.Errorf("GetEcoInfoList: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetNextBlockToMine(*wire.GetNextBlockToMineRequest) (*wire.GetNextBlockToMineResponse, error) {
	return nil, fmt.Errorf("GetNextBlockToMine: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) AddMinorBlock(*wire.AddMinorBlockRequest) (*wire.AddMinorBlockResponse, error) {
	return nil, fmt.Errorf("AddMinorBlock: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetUnconfirmedHeaders(*wire.GetUnconfirmedHeadersRequest) (*wire.GetUnconfirmedHeadersResponse, error) {
	return nil, fmt.Errorf("GetUnconfirmedHeaders: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetAccountData(*wire.GetAccountDataRequest) (*wire.GetAccountDataResponse, error) {
	return nil, fmt.Errorf("GetAccountData: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) AddTransaction(*wire.AddTransactionRequest) (*wire.AddTransactionResponse, error) {
	return nil, fmt.Errorf("AddTransaction: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetMinorBlock(*wire.GetMinorBlockRequest) (*wire.GetMinorBlockResponse, error) {
	return nil, fmt.Errorf("GetMinorBlock: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetTransaction(*wire.GetTransactionRequest) (*wire.GetTransactionResponse, error) {
	return nil, fmt.Errorf("GetTransaction: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) SyncMinorBlockList(*wire.SyncMinorBlockListRequest) (*wire.SyncMinorBlockListResponse, error) {
	return nil, fmt.Errorf("SyncMinorBlockList: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) ExecuteTransaction(*wire.ExecuteTransactionRequest) (*wire.ExecuteTransactionResponse, error) {
	return nil, fmt.Errorf("ExecuteTransaction: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetTransactionReceipt(*wire.GetTransactionReceiptRequest) (*wire.GetTransactionReceiptResponse, error) {
	return nil, fmt.Errorf("GetTransactionReceipt: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetTransactionListByAddress(*wire.GetTransactionListByAddressRequest) (*wire.GetTransactionListByAddressResponse, error) {
	return nil, fmt.Errorf("GetTransactionListByAddress: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetLogs(*wire.GetLogRequest) (*wire.GetLogResponse, error) {
	return nil, fmt.Errorf("GetLogs: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) EstimateGas(*wire.EstimateGasRequest) (*wire.EstimateGasResponse, error) {
	return nil, fmt.Errorf("EstimateGas: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetStorageAt(*wire.GetStorageRequest) (*wire.GetStorageResponse, error) {
	return nil, fmt.Errorf("GetStorageAt: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetCode(*wire.GetCodeRequest) (*wire.GetCodeResponse, error) {
	return nil, fmt.Errorf("GetCode: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GasPrice(*wire.GasPriceRequest) (*wire.GasPriceResponse, error) {
	return nil, fmt.Errorf("GasPrice: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetWork(*wire.GetWorkRequest) (*wire.GetWorkResponse, error) {
	return nil, fmt.Errorf("GetWork: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) SubmitWork(*wire.SubmitWorkRequest) (*wire.SubmitWorkResponse, error) {
	return nil, fmt.Errorf("SubmitWork: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) CheckMinorBlock(*wire.CheckMinorBlockRequest) (*wire.CheckMinorBlockResponse, error) {
	return nil, fmt.Errorf("CheckMinorBlock: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetAllTransactions(*wire.GetAllTransactionsRequest) (*wire.GetAllTransactionsResponse, error) {
	return nil, fmt.Errorf("GetAllTransactions: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetRootChainStakes(*wire.GetRootChainStakesRequest) (*wire.GetRootChainStakesResponse, error) {
	return nil, fmt.Errorf("GetRootChainStakes: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) GetTotalBalance(*wire.GetTotalBalanceRequest) (*wire.GetTotalBalanceResponse, error) {
	return nil, fmt.Errorf("GetTotalBalance: %w", conn.ErrHandlerNotImplemented)
}

// ── XshardHandler ────────────────────────────────────────────────────────────

func (b *SlaveBackend) AddXshardTxList(*wire.AddXshardTxListRequest) (*wire.AddXshardTxListResponse, error) {
	return nil, fmt.Errorf("AddXshardTxList: %w", conn.ErrHandlerNotImplemented)
}

func (b *SlaveBackend) BatchAddXshardTxList(*wire.BatchAddXshardTxListRequest) (*wire.BatchAddXshardTxListResponse, error) {
	return nil, fmt.Errorf("BatchAddXshardTxList: %w", conn.ErrHandlerNotImplemented)
}
