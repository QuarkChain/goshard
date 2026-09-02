// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// PeerResolver resolves a virtual peer frame to the PeerConn serving
// (cluster_peer_id, branch). A nil result means no such peer (Python
// NULL_CONNECTION): the frame is consumed and dropped by the caller.
// The registry and lookup implementation are owned by the upper runtime/service
// layer and are not part of the communication layer.
type PeerResolver interface {
	LookupPeer(clusterPeerID uint64, branch uint32) *PeerConn
}

// SlaveConnHandler handles master commands for slave-to-slave connections.
// ConnectToSlaves is pure communication control and is owned by the xshard
// pool, so it is kept separate from MasterHandler.
type SlaveConnHandler interface {
	// ConnectToSlaves connects to the slaves advertised by the master.
	ConnectToSlaves(req *wire.ConnectToSlavesRequest) (*wire.ConnectToSlavesResponse, error)
}

// MasterHandler handles master commands that operate on runtime-owned state.
// It is implemented by the composition layer and injected at construction.
//
// Handler implementations must be safe for concurrent calls.
// Errors are reserved for connection-level failures; business failures should
// be encoded in the response ErrorCode.
type MasterHandler interface {
	// CreateShards initializes or updates shard runtime state from the master's PING.
	CreateShards(rootTip *wire.RawBytes) error

	// CreateClusterPeerConnection and DestroyClusterPeerConnection manage
	// peer connections owned by the shard runtime.
	CreateClusterPeerConnection(req *wire.CreateClusterPeerConnectionRequest) (*wire.CreateClusterPeerConnectionResponse, error)
	DestroyClusterPeerConnection(req *wire.DestroyClusterPeerConnectionCommand) error

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

// MasterConnConfig configures a MasterConn. Conn, Handler and PeerResolver are
// required; Logger defaults to log.Root().
type MasterConnConfig struct {
	// Conn is the accepted TCP connection from the master. The slave never
	// dials the master (py: MasterServer connects, SlaveServer listens).
	Conn net.Conn

	// MaxPayloadSize limits frame payload size; 0 disables the limit.
	MaxPayloadSize uint32

	// LocalID and LocalFullShardIDList identify this slave; they come from
	// SlaveConfig and are echoed in PONG (py: Pong(self.slave_server.id, ...)).
	// The slave never adopts identity from the master's PING.
	LocalID              []byte
	LocalFullShardIDList []uint32

	// ClusterShardIDs is the cluster-wide configured full shard id set
	// (py: env.quark_chain_config.get_full_shard_ids()). routeFrame uses it to
	// reject frames from a master for a branch outside the global config, which
	// is fatal for the connection (py: slave.py:123-129 close_with_error).
	ClusterShardIDs []uint32

	// SlaveConnHandler serves the slave-to-slave topology command
	// CONNECT_TO_SLAVES (required). It is separate from Handler: the xshard
	// topology is communication-owned, while Handler is the runtime/business
	// boundary.
	SlaveConnHandler SlaveConnHandler

	// Handler serves master commands that operate on runtime-owned state.
	// The composition layer implements it.
	Handler MasterHandler

	// PeerResolver resolves virtual peer frames to their PeerConn (required).
	// It is the minimal routing capability required for forwarding frames
	// received from the master.
	PeerResolver PeerResolver

	// Logger defaults to log.Root() if nil.
	Logger log.Logger
}

// MasterConn represents the slave-side TCP connection to the cluster master.
// It corresponds to Python's quarkchain.cluster.slave.MasterConnection and uses
// 12-byte ClusterMetadata framing.
//
// MasterConn is the slave's single connection to the master: it dispatches
// master commands (business operations delegated to MasterHandler) and routes
// virtual peer frames through PeerResolver.
type MasterConn struct {
	*conn.BaseConn

	handler              MasterHandler
	slaveConnHandler     SlaveConnHandler
	localID              []byte
	localFullShardIDList []uint32
	// clusterShardIDs is the cluster-wide configured full shard id set
	// (py: env.quark_chain_config.get_full_shard_ids()); a frame for a branch
	// outside it closes the connection (see routeFrame).
	clusterShardIDs map[uint32]struct{}

	// peerResolver resolves virtual peer frames to their PeerConn (Python:
	// MasterConnection.get_connection_to_forward, slave.py:116-148). Never nil
	// on a started MasterConn.
	peerResolver PeerResolver
}

// NewMasterConn wraps an accepted net.Conn from the master.
// The caller is responsible for calling Start().
func NewMasterConn(cfg MasterConnConfig) (*MasterConn, error) {
	if cfg.Conn == nil {
		return nil, errors.New("master connection must not be nil")
	}
	if cfg.SlaveConnHandler == nil {
		return nil, errors.New("master slave conn handler must not be nil")
	}
	if cfg.Handler == nil {
		return nil, errors.New("master handler must not be nil")
	}
	if cfg.PeerResolver == nil {
		return nil, errors.New("master peer resolver must not be nil")
	}
	readFrame := func(r io.Reader) (*wire.Frame, error) {
		return wire.ReadFrame(r, cfg.MaxPayloadSize)
	}

	clusterShardIDs := make(map[uint32]struct{}, len(cfg.ClusterShardIDs))
	for _, id := range cfg.ClusterShardIDs {
		clusterShardIDs[id] = struct{}{}
	}

	mc := &MasterConn{
		slaveConnHandler:     cfg.SlaveConnHandler,
		handler:              cfg.Handler,
		localID:              append([]byte(nil), cfg.LocalID...),
		localFullShardIDList: append([]uint32(nil), cfg.LocalFullShardIDList...),
		clusterShardIDs:      clusterShardIDs,
		peerResolver:         cfg.PeerResolver,
	}

	// Forwarder: route cluster_peer_id != 0 frames to virtual PeerConns.
	// routeFrame returns false for master-local traffic so MasterConn handles
	// it normally. The forwarder runs on the reader goroutine; it enqueues
	// frames without blocking (the PeerConn inbound queue is unbounded).
	forwarder := mc.routeFrame

	mc.BaseConn = conn.NewBaseConn(conn.Config{
		Transport: conn.NewTCPTransport(cfg.Conn, readFrame, wire.WriteFrame),
		Serializers: map[byte]*conn.OpSerializer{
			// §1 Cluster initialisation
			byte(wire.ClusterOpPing):                         conn.OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong)),
			byte(wire.ClusterOpConnectToSlavesRequest):       conn.OpSerializerFor[wire.ConnectToSlavesRequest, wire.ConnectToSlavesResponse](byte(wire.ClusterOpConnectToSlavesResponse)),
			byte(wire.ClusterOpAddRootBlockRequest):          conn.OpSerializerFor[wire.AddRootBlockRequest, wire.AddRootBlockResponse](byte(wire.ClusterOpAddRootBlockResponse)),
			byte(wire.ClusterOpGetEcoInfoListRequest):        conn.OpSerializerFor[wire.GetEcoInfoListRequest, wire.GetEcoInfoListResponse](byte(wire.ClusterOpGetEcoInfoListResponse)),
			byte(wire.ClusterOpGetNextBlockToMineRequest):    conn.OpSerializerFor[wire.GetNextBlockToMineRequest, wire.GetNextBlockToMineResponse](byte(wire.ClusterOpGetNextBlockToMineResponse)),
			byte(wire.ClusterOpGetUnconfirmedHeadersRequest): conn.OpSerializerFor[wire.GetUnconfirmedHeadersRequest, wire.GetUnconfirmedHeadersResponse](byte(wire.ClusterOpGetUnconfirmedHeadersResponse)),
			byte(wire.ClusterOpGetAccountDataRequest):        conn.OpSerializerFor[wire.GetAccountDataRequest, wire.GetAccountDataResponse](byte(wire.ClusterOpGetAccountDataResponse)),
			byte(wire.ClusterOpAddTransactionRequest):        conn.OpSerializerFor[wire.AddTransactionRequest, wire.AddTransactionResponse](byte(wire.ClusterOpAddTransactionResponse)),

			// §2 Slave → Master (mining)
			byte(wire.ClusterOpAddMinorBlockHeaderRequest): conn.OpSerializerFor[wire.AddMinorBlockHeaderRequest, wire.AddMinorBlockHeaderResponse](byte(wire.ClusterOpAddMinorBlockHeaderResponse)),

			// §4 Master → Slave (sync / virtual conns)
			byte(wire.ClusterOpSyncMinorBlockListRequest):          conn.OpSerializerFor[wire.SyncMinorBlockListRequest, wire.SyncMinorBlockListResponse](byte(wire.ClusterOpSyncMinorBlockListResponse)),
			byte(wire.ClusterOpAddMinorBlockRequest):               conn.OpSerializerFor[wire.AddMinorBlockRequest, wire.AddMinorBlockResponse](byte(wire.ClusterOpAddMinorBlockResponse)),
			byte(wire.ClusterOpCreateClusterPeerConnectionRequest): conn.OpSerializerFor[wire.CreateClusterPeerConnectionRequest, wire.CreateClusterPeerConnectionResponse](byte(wire.ClusterOpCreateClusterPeerConnectionResponse)),
			// 0 = non-RPC placeholder: ignored by Config validation and never
			// read at runtime (see NonRPCOps below).
			byte(wire.ClusterOpDestroyClusterPeerConnectionCommand): conn.OpSerializerFor[wire.DestroyClusterPeerConnectionCommand, wire.DestroyClusterPeerConnectionCommand](0),
			byte(wire.ClusterOpGetMinorBlockRequest):                conn.OpSerializerFor[wire.GetMinorBlockRequest, wire.GetMinorBlockResponse](byte(wire.ClusterOpGetMinorBlockResponse)),
			byte(wire.ClusterOpGetTransactionRequest):               conn.OpSerializerFor[wire.GetTransactionRequest, wire.GetTransactionResponse](byte(wire.ClusterOpGetTransactionResponse)),

			// §6 Master → Slave (JSON-RPC-like)
			byte(wire.ClusterOpExecuteTransactionRequest):          conn.OpSerializerFor[wire.ExecuteTransactionRequest, wire.ExecuteTransactionResponse](byte(wire.ClusterOpExecuteTransactionResponse)),
			byte(wire.ClusterOpGetTransactionReceiptRequest):       conn.OpSerializerFor[wire.GetTransactionReceiptRequest, wire.GetTransactionReceiptResponse](byte(wire.ClusterOpGetTransactionReceiptResponse)),
			byte(wire.ClusterOpMineRequest):                        conn.OpSerializerFor[wire.MineRequest, wire.MineResponse](byte(wire.ClusterOpMineResponse)),
			byte(wire.ClusterOpGenTxRequest):                       conn.OpSerializerFor[wire.GenTxRequest, wire.GenTxResponse](byte(wire.ClusterOpGenTxResponse)),
			byte(wire.ClusterOpGetTransactionListByAddressRequest): conn.OpSerializerFor[wire.GetTransactionListByAddressRequest, wire.GetTransactionListByAddressResponse](byte(wire.ClusterOpGetTransactionListByAddressResponse)),
			byte(wire.ClusterOpGetLogRequest):                      conn.OpSerializerFor[wire.GetLogRequest, wire.GetLogResponse](byte(wire.ClusterOpGetLogResponse)),
			byte(wire.ClusterOpEstimateGasRequest):                 conn.OpSerializerFor[wire.EstimateGasRequest, wire.EstimateGasResponse](byte(wire.ClusterOpEstimateGasResponse)),
			byte(wire.ClusterOpGetStorageRequest):                  conn.OpSerializerFor[wire.GetStorageRequest, wire.GetStorageResponse](byte(wire.ClusterOpGetStorageResponse)),
			byte(wire.ClusterOpGetCodeRequest):                     conn.OpSerializerFor[wire.GetCodeRequest, wire.GetCodeResponse](byte(wire.ClusterOpGetCodeResponse)),
			byte(wire.ClusterOpGasPriceRequest):                    conn.OpSerializerFor[wire.GasPriceRequest, wire.GasPriceResponse](byte(wire.ClusterOpGasPriceResponse)),
			byte(wire.ClusterOpGetWorkRequest):                     conn.OpSerializerFor[wire.GetWorkRequest, wire.GetWorkResponse](byte(wire.ClusterOpGetWorkResponse)),
			byte(wire.ClusterOpSubmitWorkRequest):                  conn.OpSerializerFor[wire.SubmitWorkRequest, wire.SubmitWorkResponse](byte(wire.ClusterOpSubmitWorkResponse)),

			// §7 Slave → Master (block list)
			byte(wire.ClusterOpAddMinorBlockHeaderListRequest): conn.OpSerializerFor[wire.AddMinorBlockHeaderListRequest, wire.AddMinorBlockHeaderListResponse](byte(wire.ClusterOpAddMinorBlockHeaderListResponse)),

			// §8 Master → Slave (JRPC & staking)
			byte(wire.ClusterOpCheckMinorBlockRequest):    conn.OpSerializerFor[wire.CheckMinorBlockRequest, wire.CheckMinorBlockResponse](byte(wire.ClusterOpCheckMinorBlockResponse)),
			byte(wire.ClusterOpGetAllTransactionsRequest): conn.OpSerializerFor[wire.GetAllTransactionsRequest, wire.GetAllTransactionsResponse](byte(wire.ClusterOpGetAllTransactionsResponse)),
			byte(wire.ClusterOpGetRootChainStakesRequest): conn.OpSerializerFor[wire.GetRootChainStakesRequest, wire.GetRootChainStakesResponse](byte(wire.ClusterOpGetRootChainStakesResponse)),
			byte(wire.ClusterOpGetTotalBalanceRequest):    conn.OpSerializerFor[wire.GetTotalBalanceRequest, wire.GetTotalBalanceResponse](byte(wire.ClusterOpGetTotalBalanceResponse)),
		},
		Handlers: map[byte]conn.TypedHandler{
			// ── Communication handlers ─────────────────────────────────────
			byte(wire.ClusterOpPing): mc.handlePing,

			// ── Inbound handlers (delegated to MasterHandler / service layer) ─
			byte(wire.ClusterOpCreateClusterPeerConnectionRequest):  mc.handleCreateClusterPeerConnection,
			byte(wire.ClusterOpDestroyClusterPeerConnectionCommand): mc.handleDestroyClusterPeerConnection,
			byte(wire.ClusterOpConnectToSlavesRequest):              mc.handleConnectToSlaves,
			byte(wire.ClusterOpMineRequest):                         mc.handleMine,
			byte(wire.ClusterOpGenTxRequest):                        mc.handleGenTx,
			byte(wire.ClusterOpAddRootBlockRequest):                 mc.handleAddRootBlock,
			byte(wire.ClusterOpGetEcoInfoListRequest):               mc.handleGetEcoInfoList,
			byte(wire.ClusterOpGetNextBlockToMineRequest):           mc.handleGetNextBlockToMine,
			byte(wire.ClusterOpAddMinorBlockRequest):                mc.handleAddMinorBlock,
			byte(wire.ClusterOpGetUnconfirmedHeadersRequest):        mc.handleGetUnconfirmedHeaders,
			byte(wire.ClusterOpGetAccountDataRequest):               mc.handleGetAccountData,
			byte(wire.ClusterOpAddTransactionRequest):               mc.handleAddTransaction,
			byte(wire.ClusterOpGetMinorBlockRequest):                mc.handleGetMinorBlock,
			byte(wire.ClusterOpGetTransactionRequest):               mc.handleGetTransaction,
			byte(wire.ClusterOpSyncMinorBlockListRequest):           mc.handleSyncMinorBlockList,
			byte(wire.ClusterOpExecuteTransactionRequest):           mc.handleExecuteTransaction,
			byte(wire.ClusterOpGetTransactionReceiptRequest):        mc.handleGetTransactionReceipt,
			byte(wire.ClusterOpGetTransactionListByAddressRequest):  mc.handleGetTransactionListByAddress,
			byte(wire.ClusterOpGetLogRequest):                       mc.handleGetLogs,
			byte(wire.ClusterOpEstimateGasRequest):                  mc.handleEstimateGas,
			byte(wire.ClusterOpGetStorageRequest):                   mc.handleGetStorageAt,
			byte(wire.ClusterOpGetCodeRequest):                      mc.handleGetCode,
			byte(wire.ClusterOpGasPriceRequest):                     mc.handleGasPrice,
			byte(wire.ClusterOpGetWorkRequest):                      mc.handleGetWork,
			byte(wire.ClusterOpSubmitWorkRequest):                   mc.handleSubmitWork,
			byte(wire.ClusterOpCheckMinorBlockRequest):              mc.handleCheckMinorBlock,
			byte(wire.ClusterOpGetAllTransactionsRequest):           mc.handleGetAllTransactions,
			byte(wire.ClusterOpGetRootChainStakesRequest):           mc.handleGetRootChainStakes,
			byte(wire.ClusterOpGetTotalBalanceRequest):              mc.handleGetTotalBalance,
		},
		NonRPCOps: map[byte]struct{}{
			byte(wire.ClusterOpDestroyClusterPeerConnectionCommand): {},
		},
		Forwarder: forwarder,
		Logger:    cfg.Logger,
	})
	return mc, nil
}

// LocalID returns this slave's ID used in PONG responses.
func (mc *MasterConn) LocalID() []byte {
	return append([]byte(nil), mc.localID...)
}

// LocalFullShardIDList returns this slave's full shard ID list used in PONG responses.
func (mc *MasterConn) LocalFullShardIDList() []uint32 {
	return append([]uint32(nil), mc.localFullShardIDList...)
}

// SendAddMinorBlockHeader sends AddMinorBlockHeaderRequest to the master and
// returns the parsed response.
func (mc *MasterConn) SendAddMinorBlockHeader(ctx context.Context, req *wire.AddMinorBlockHeaderRequest) (*wire.AddMinorBlockHeaderResponse, error) {
	payload, err := serialize.SerializeToBytes(req)
	if err != nil {
		return nil, fmt.Errorf("serialize AddMinorBlockHeaderRequest: %w", err)
	}
	resp, err := mc.SendRPCMeta(ctx, byte(wire.ClusterOpAddMinorBlockHeaderRequest), payload, wire.ClusterMetadata{})
	if err != nil {
		return nil, err
	}
	r, ok := resp.(*wire.AddMinorBlockHeaderResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected AddMinorBlockHeader response %T", resp)
	}
	return r, nil
}

// SendAddMinorBlockHeaderList sends AddMinorBlockHeaderListRequest to the master
// and returns the parsed response.
func (mc *MasterConn) SendAddMinorBlockHeaderList(ctx context.Context, req *wire.AddMinorBlockHeaderListRequest) (*wire.AddMinorBlockHeaderListResponse, error) {
	payload, err := serialize.SerializeToBytes(req)
	if err != nil {
		return nil, fmt.Errorf("serialize AddMinorBlockHeaderListRequest: %w", err)
	}
	resp, err := mc.SendRPCMeta(ctx, byte(wire.ClusterOpAddMinorBlockHeaderListRequest), payload, wire.ClusterMetadata{})
	if err != nil {
		return nil, err
	}
	r, ok := resp.(*wire.AddMinorBlockHeaderListResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected AddMinorBlockHeaderList response %T", resp)
	}
	return r, nil
}

// ── Communication handlers ─────────────────────────────────────────────

// handlePing serves the master's PING, which has two roles:
//
//   - Protocol handshake: reply with this slave's identity. The PONG is built
//     here because it is pure protocol framing
//     (py: MasterConnection.handle_ping -> Pong(self.slave_server.id, ...)).
//   - Runtime notification: a RootTip asks the runtime to create/update shards.
//     That business logic is delegated to MasterHandler
//     (py: await self.slave_server.create_shards(ping.root_tip)); MasterConn
//     keeps only the delegation.
//     That business logic is delegated to MasterHandler because shard
//     lifecycle belongs to the runtime, not MasterConn.
func (mc *MasterConn) handlePing(req any) (any, error) {
	ping := req.(*wire.PingRequest)
	if ping.RootTip != nil {
		if err := mc.handler.CreateShards(ping.RootTip); err != nil {
			return nil, err
		}
	}
	return &wire.PongResponse{
		ID:              append([]byte(nil), mc.localID...),
		FullShardIDList: append([]uint32(nil), mc.localFullShardIDList...),
	}, nil
}

// ── Frame routing ───────────────────────────────────────────────────────

// routeFrame handles frames addressed to virtual peer connections.
// cluster_peer_id == 0 is master-local traffic and returns false so the
// normal MasterConn dispatcher handles it. Peer traffic is validated and
// forwarded to the corresponding PeerConn.
// A branch outside the GLOBAL configured shard set is fatal for the
// connection (py: slave.py:123-129 close_with_error); a branch that is
// globally valid but not owned/created locally, or an unknown peer id,
// follows Python's NULL_CONNECTION semantics (slave.py:131-146): the
// frame is consumed and dropped without closing the connection.
func (mc *MasterConn) routeFrame(frame *wire.Frame) bool {
	if frame.Meta.ClusterPeerID == 0 {
		return false
	}

	if _, ok := mc.clusterShardIDs[frame.Meta.Branch]; !ok {
		mc.Logger().Error(
			"incorrect forwarding branch",
			"branch", fmt.Sprintf("0x%x", frame.Meta.Branch),
		)
		mc.Close()
		return true
	}

	pc := mc.peerResolver.LookupPeer(frame.Meta.ClusterPeerID, frame.Meta.Branch)
	if pc == nil {
		// Covers both "shard valid globally but not created locally"
		// (slave.py:131-134) and "peer not found" (slave.py:136-146): drop,
		// keep the connection.
		mc.Logger().Warn("dropping frame for unknown virtual peer connection",
			"cluster_peer_id", frame.Meta.ClusterPeerID, "branch", frame.Meta.Branch)
		return true
	}

	pc.HandleFrame(frame)
	return true
}

// ── Inbound handler dispatch (delegated to MasterHandler) ───────────────

func (mc *MasterConn) handleCreateClusterPeerConnection(req any) (any, error) {
	return mc.handler.CreateClusterPeerConnection(req.(*wire.CreateClusterPeerConnectionRequest))
}

func (mc *MasterConn) handleDestroyClusterPeerConnection(req any) (any, error) {
	return nil, mc.handler.DestroyClusterPeerConnection(req.(*wire.DestroyClusterPeerConnectionCommand))
}

func (mc *MasterConn) handleConnectToSlaves(req any) (any, error) {
	return mc.slaveConnHandler.ConnectToSlaves(req.(*wire.ConnectToSlavesRequest))
}

func (mc *MasterConn) handleMine(req any) (any, error) {
	return mc.handler.Mine(req.(*wire.MineRequest))
}

func (mc *MasterConn) handleGenTx(req any) (any, error) {
	return mc.handler.GenTx(req.(*wire.GenTxRequest))
}

func (mc *MasterConn) handleAddRootBlock(req any) (any, error) {
	return mc.handler.AddRootBlock(req.(*wire.AddRootBlockRequest))
}

func (mc *MasterConn) handleGetEcoInfoList(req any) (any, error) {
	return mc.handler.GetEcoInfoList(req.(*wire.GetEcoInfoListRequest))
}

func (mc *MasterConn) handleGetNextBlockToMine(req any) (any, error) {
	return mc.handler.GetNextBlockToMine(req.(*wire.GetNextBlockToMineRequest))
}

func (mc *MasterConn) handleAddMinorBlock(req any) (any, error) {
	return mc.handler.AddMinorBlock(req.(*wire.AddMinorBlockRequest))
}

func (mc *MasterConn) handleGetUnconfirmedHeaders(req any) (any, error) {
	return mc.handler.GetUnconfirmedHeaders(req.(*wire.GetUnconfirmedHeadersRequest))
}

func (mc *MasterConn) handleGetAccountData(req any) (any, error) {
	return mc.handler.GetAccountData(req.(*wire.GetAccountDataRequest))
}

func (mc *MasterConn) handleAddTransaction(req any) (any, error) {
	return mc.handler.AddTransaction(req.(*wire.AddTransactionRequest))
}

func (mc *MasterConn) handleGetMinorBlock(req any) (any, error) {
	return mc.handler.GetMinorBlock(req.(*wire.GetMinorBlockRequest))
}

func (mc *MasterConn) handleGetTransaction(req any) (any, error) {
	return mc.handler.GetTransaction(req.(*wire.GetTransactionRequest))
}

func (mc *MasterConn) handleSyncMinorBlockList(req any) (any, error) {
	return mc.handler.SyncMinorBlockList(req.(*wire.SyncMinorBlockListRequest))
}

func (mc *MasterConn) handleExecuteTransaction(req any) (any, error) {
	return mc.handler.ExecuteTransaction(req.(*wire.ExecuteTransactionRequest))
}

func (mc *MasterConn) handleGetTransactionReceipt(req any) (any, error) {
	return mc.handler.GetTransactionReceipt(req.(*wire.GetTransactionReceiptRequest))
}

func (mc *MasterConn) handleGetTransactionListByAddress(req any) (any, error) {
	return mc.handler.GetTransactionListByAddress(req.(*wire.GetTransactionListByAddressRequest))
}

func (mc *MasterConn) handleGetLogs(req any) (any, error) {
	return mc.handler.GetLogs(req.(*wire.GetLogRequest))
}

func (mc *MasterConn) handleEstimateGas(req any) (any, error) {
	return mc.handler.EstimateGas(req.(*wire.EstimateGasRequest))
}

func (mc *MasterConn) handleGetStorageAt(req any) (any, error) {
	return mc.handler.GetStorageAt(req.(*wire.GetStorageRequest))
}

func (mc *MasterConn) handleGetCode(req any) (any, error) {
	return mc.handler.GetCode(req.(*wire.GetCodeRequest))
}

func (mc *MasterConn) handleGasPrice(req any) (any, error) {
	return mc.handler.GasPrice(req.(*wire.GasPriceRequest))
}

func (mc *MasterConn) handleGetWork(req any) (any, error) {
	return mc.handler.GetWork(req.(*wire.GetWorkRequest))
}

func (mc *MasterConn) handleSubmitWork(req any) (any, error) {
	return mc.handler.SubmitWork(req.(*wire.SubmitWorkRequest))
}

func (mc *MasterConn) handleCheckMinorBlock(req any) (any, error) {
	return mc.handler.CheckMinorBlock(req.(*wire.CheckMinorBlockRequest))
}

func (mc *MasterConn) handleGetAllTransactions(req any) (any, error) {
	return mc.handler.GetAllTransactions(req.(*wire.GetAllTransactionsRequest))
}

func (mc *MasterConn) handleGetRootChainStakes(req any) (any, error) {
	return mc.handler.GetRootChainStakes(req.(*wire.GetRootChainStakesRequest))
}

func (mc *MasterConn) handleGetTotalBalance(req any) (any, error) {
	return mc.handler.GetTotalBalance(req.(*wire.GetTotalBalanceRequest))
}
