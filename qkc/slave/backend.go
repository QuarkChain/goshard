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
	ID     string
	shards map[account.Branch]*shard.Shard
	order  []account.Branch // boot (config) order, for deterministic iteration and shutdown

	// Identity and network config, derived from the SlaveContext at construction.
	selfID               []byte
	localFullShardIDList []uint32
	clusterShardIDs      []uint32
	port                 int
	maxPayloadSize       uint32 // 0 disables the payload limit.
	logger               log.Logger

	listener net.Listener
	// master is the established MasterConn (py: slave_server.master), published
	// atomically by runMasterConn for the first inbound and never replaced. A nil
	// load means not established (ErrNotActive).
	master     atomic.Pointer[slaveconn.MasterConn]
	xshardPool *slaveconn.XshardPool

	// clusterPeerIDs is the announced virtual cluster-peer set (py:
	// SlaveServer.cluster_peer_ids, slave.py:909), guarded by clusterPeersMu. A
	// peer is recorded the moment the master announces it, before any shard may
	// hold a PeerConn for it, so a shard created later can still be wired to
	// peers announced earlier. It has no reader yet: CreateShards (below) is the
	// designated consumer once it lands (py: create_shards reads this set,
	// slave.py:939-941).
	clusterPeersMu sync.Mutex
	clusterPeerIDs map[uint64]struct{}

	stopped  chan struct{}
	stopOnce sync.Once
	stopErr  error
}

// Compile-time interface conformance.
var (
	_ slaveconn.MasterHandler = (*SlaveBackend)(nil)
	_ slaveconn.PeerResolver  = (*SlaveBackend)(nil)
	_ slaveconn.XshardHandler = (*SlaveBackend)(nil)
	_ shard.Sender            = (*SlaveBackend)(nil)
)

// New boots every shard the context's slave owns, eagerly and in config order,
// and assembles the communication resources (xshard pool; the listener is bound
// later by Start). Eager shard construction is interim scaffolding: with no
// master in the cluster yet it is the only way to bring the shards up; the
// PING(root_tip)-driven path is stubbed in CreateShards below.
//
// On any failure the shards already started are stopped and their databases
// closed before the error returns, so the datadir stays reopenable.
func New(ctx *config.SlaveContext, rootGenesis *types.RootBlockHeader, opts shard.Options) (*SlaveBackend, error) {
	b := &SlaveBackend{
		ID:                   ctx.ID,
		shards:               make(map[account.Branch]*shard.Shard, len(ctx.FullShardIDs())),
		selfID:               []byte(ctx.ID),
		localFullShardIDList: append([]uint32(nil), ctx.FullShardIDs()...),
		port:                 int(ctx.Slave.Port),
		clusterShardIDs:      ctx.Quarkchain.GetGenesisShardIds(),
		clusterPeerIDs:       make(map[uint64]struct{}),
		stopped:              make(chan struct{}),
		logger:               log.Root(),
	}

	pool, err := slaveconn.NewXshardPool(b.selfID, b.localFullShardIDList, b.clusterShardIDs, b.maxPayloadSize, b, b.logger)
	if err != nil {
		return nil, fmt.Errorf("slave %s: new xshard pool: %w", b.ID, err)
	}
	b.xshardPool = pool

	for _, id := range ctx.FullShardIDs() {
		branch := account.NewBranch(id)
		s, err := shard.New(ctx, branch, rootGenesis, ctx.DBPathRoot, opts, b)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("slave %s: %w", b.ID, err), b.Stop())
		}
		b.shards[branch] = s
		b.order = append(b.order, branch)

		height, _ := s.Chain().Head()
		log.Info("shard started", "shard", fmt.Sprintf("0x%08x", id), "genesis", s.Chain().GenesisHash(), "head", height)
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

// acceptLoop accepts inbound connections and is the single owner of
// classification: the first accepted connection is always the master, every later
// one is xshard. The claim is loop-local, so no lock or atomic is needed. Stop
// closes the listener, which makes Accept return and this loop exit.
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

// runMasterConn runs the first inbound connection as the MasterConn and blocks
// until that connection is gone. The pointer is stored before Start so any frame
// the readLoop processes sees an established master. Master loss triggers Stop.
func (b *SlaveBackend) runMasterConn(conn net.Conn) {
	mc, err := slaveconn.NewMasterConn(slaveconn.MasterConnConfig{
		Conn:                 conn,
		MaxPayloadSize:       b.maxPayloadSize,
		LocalID:              b.selfID,
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

// runXshardConn hands a subsequent inbound connection to the pool, which owns the
// full inbound lifecycle.
func (b *SlaveBackend) runXshardConn(conn net.Conn) {
	b.logger.Info("accepted xshard connection", "remote", conn.RemoteAddr())
	b.xshardPool.HandleInbound(conn)
}

// ── PeerResolver ─────────────────────────────────────────────────────────────

// LookupPeer routes virtual peer frames from the master to the PeerConn serving
// (cluster_peer_id, branch), or nil when there is none (py: NULL_CONNECTION).
func (b *SlaveBackend) LookupPeer(clusterPeerID uint64, branch uint32) *slaveconn.PeerConn {
	if s := b.shards[account.NewBranch(branch)]; s != nil {
		return s.Peer(clusterPeerID)
	}
	return nil
}

// ── MasterHandler: topology & shard activation ───────────────────────────────

// CreateShards creates the shards made eligible by the given root tip, then
// wires each new shard's peer connections to the cluster's current peers (py:
// SlaveServer.create_shards, slave.py:933-960). The master triggers it through
// handlePing(PING(root_tip)); it is intentionally unimplemented — shards are
// currently booted eagerly by New, and landing this requires the deferred work
// recorded below.
func (b *SlaveBackend) CreateShards(*types.RootBlock) error {
	// TODO(create-shards): implement py slave.py:933-960 as a pure lifecycle
	// pass over the shard set — no genesis/db/chain logic here, that belongs to
	// the shard layer. Deferred prerequisites from the 2026-09-29 audit:
	//   - make shards/order dynamic under a single topology mutex guarding
	//     shards + order + clusterPeerIDs as ONE state unit: py gets the total
	//     order on PING/CREATE_PEER/DESTROY_PEER for free from its event loop
	//     serializing every master request; Go's per-request dispatch goroutines
	//     must rebuild it explicitly (interleavings N1-N6, see the work log);
	//   - wire each new shard to b.clusterPeerIDs via wireClusterPeer, then
	//     register it last (wire-then-register, py: slave.py:946), so
	//     routeFrame/LookupPeer never observe a half-wired shard;
	//   - gate on b.stopped at entry and surface the error through handlePing,
	//     so a shutting-down backend refuses creation (py closes the connection,
	//     slave.py:177-180);
	//   - eligibility vs the genesis root height, db recovery, genesis header
	//     report and miner start belong to the shard layer (py:
	//     init_from_root_block, shard.py:619-639), not here;
	//   - b.shards is immutable after New until this lands — that is what makes
	//     the lock-free Shard/Shards/LookupPeer and topology handlers safe.
	return nil
}

// CreateClusterPeerConnection registers a new cluster peer and creates a PeerConn
// for it on every local shard. It always succeeds from the master's point of view
// (error_code 0); duplicates are logged and skipped.
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

// wireClusterPeer creates and registers the PeerConn of clusterPeerID on s. It
// reports false only when the shard refused because it is shutting down
// (shard.ErrStopped), so the caller can stop wiring the remaining shards; every
// other failure — duplicates, construction errors, a master connection that is
// not established — is logged and swallowed, matching py's per-connection skip
// (slave.py:334-368, shard.py:596-617). This is the single wiring seam the
// future CreateShards reuses to wire new shards to the announced peers.
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

// DestroyClusterPeerConnection removes the cluster peer and closes every PeerConn
// of it. Fire-and-forget; destroying an unknown id is a no-op.
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

// ConnectToSlaves dials every advertised slave into the xshard pool, reporting
// per-entry failures in the response result list so the master connection stays
// up.
func (b *SlaveBackend) ConnectToSlaves(req *wire.ConnectToSlavesRequest) (*wire.ConnectToSlavesResponse, error) {
	resultList := make([]wire.PrependedSizeBytes4, len(req.SlaveInfoList))
	for i := range req.SlaveInfoList {
		info := req.SlaveInfoList[i]
		if err := b.xshardPool.DialToSlave(context.Background(), info); err != nil {
			b.logger.Warn("connect to slave failed", "remote_id", string(info.ID), "err", err)
			resultList[i] = wire.PrependedSizeBytes4([]byte(err.Error()))
		}
	}
	return &wire.ConnectToSlavesResponse{ResultList: resultList}, nil
}

// ── Outbound: master sends (shard.Sender) ────────────────────────────────────

// SendMinorBlockHeaderToMaster reports a new minor block header (py:
// send_minor_block_header_to_master). Returns ErrNotActive before the master
// connection exists and ErrConnectionClosed after it closes.
func (b *SlaveBackend) SendMinorBlockHeaderToMaster(ctx context.Context, req *wire.AddMinorBlockHeaderRequest) (*wire.AddMinorBlockHeaderResponse, error) {
	mc := b.master.Load()
	if mc == nil {
		return nil, conn.ErrNotActive
	}
	return mc.SendAddMinorBlockHeader(ctx, req)
}

// SendMinorBlockHeaderListToMaster reports a list of new minor block headers to
// the master (py: SlaveServer.send_minor_block_header_list_to_master).
func (b *SlaveBackend) SendMinorBlockHeaderListToMaster(ctx context.Context, req *wire.AddMinorBlockHeaderListRequest) (*wire.AddMinorBlockHeaderListResponse, error) {
	mc := b.master.Load()
	if mc == nil {
		return nil, conn.ErrNotActive
	}
	return mc.SendAddMinorBlockHeaderList(ctx, req)
}

// ── Outbound: xshard broadcasts (shard.Sender) ───────────────────────────────

// broadcastToBranch concurrently sends to every xshard connection serving branch
// (py: broadcast_xshard_tx_list / batch_broadcast_xshard_tx_list). The delivery
// set is the full snapshot: all connections are attempted even if some sends
// fail. The call waits for all sends and returns nil only if every send
// succeeds; otherwise all errors are aggregated. An empty connection set is a
// no-op, matching Python's gather([]).
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

// SendXshardTxList broadcasts an AddXshardTxListRequest to every slave connection
// serving branch (py: broadcast_xshard_tx_list, remote leg); local delivery is the
// caller's. Delivery is attempt-all and the result is binary: nil iff every
// connection acknowledged. An empty connection set is a no-op.
func (b *SlaveBackend) SendXshardTxList(ctx context.Context, branch uint32, req *wire.AddXshardTxListRequest) error {
	return b.broadcastToBranch(branch, func(c *slaveconn.XshardConn) error {
		return c.SendAddXshardTxList(ctx, req)
	})
}

// SendBatchXshardTxList broadcasts a BatchAddXshardTxListRequest to every slave
// connection serving branch, with the same attempt-all and binary-result
// semantics (py: batch_broadcast_xshard_tx_list).
func (b *SlaveBackend) SendBatchXshardTxList(ctx context.Context, branch uint32, req *wire.BatchAddXshardTxListRequest) error {
	return b.broadcastToBranch(branch, func(c *slaveconn.XshardConn) error {
		return c.SendBatchAddXshardTxList(ctx, req)
	})
}

// ── MasterHandler: business RPCs ─────────────────────────────────────────────
//
// These remain stubs until the shard chain implementation lands. They return
// ErrHandlerNotImplemented rather than faking success.

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
