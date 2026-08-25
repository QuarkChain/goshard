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

// MasterHandler serves inbound RPCs from the master. It is implemented by
// the service layer and injected at construction.
//
// Communication-layer messages that MasterConn handles itself (PING,
// cluster peer connection management) never reach this interface.
// ConnectToSlaves is delegated: its execution needs the XShardPool owned by
// the future SlaveService (py: slave_connection_manager.connect_to_slave).
//
// Handler implementations must be safe for concurrent calls.
//
// The error return is reserved for connection-level failures: returning an
// error closes the connection (py: close_with_error). Business failures must
// be encoded in the response ErrorCode field.
type MasterHandler interface {
	ConnectToSlaves(req *wire.ConnectToSlavesRequest) (*wire.ConnectToSlavesResponse, error)
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

// MasterConnConfig configures a MasterConn. Conn, Handler and PeerRuntime are
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

	// Handler serves inbound RPCs (required). Business RPCs are delegated here;
	// the future SlaveService implements them with its own XshardPool.
	Handler MasterHandler

	// PeerRuntime is the required dependency on the runtime owning the shards
	// and peer registry; "no shards yet" is an empty shard set inside it, never
	// nil. The future SlaveService provides it; tests inject a fake.
	PeerRuntime PeerRuntime

	// Logger defaults to log.Root() if nil.
	Logger log.Logger
}

// MasterConn represents the slave-side TCP connection to the cluster master.
// It corresponds to Python's quarkchain.cluster.slave.MasterConnection and uses
// 12-byte ClusterMetadata framing.
//
// MasterConn is the entry point of the slave: every other connection
// (slave-to-slave xshard, cluster peers) is created on the master's command
// through this connection.
type MasterConn struct {
	*conn.BaseConn

	handler              MasterHandler
	localID              []byte
	localFullShardIDList []uint32

	// peerRuntime is the required dependency on the runtime owning the shards
	// and peer registry (Python: MasterConnection.slave_server). Never nil on
	// a started MasterConn.
	peerRuntime PeerRuntime
}

// NewMasterConn wraps an accepted net.Conn from the master.
// The caller is responsible for calling Start().
func NewMasterConn(cfg MasterConnConfig) (*MasterConn, error) {
	if cfg.Conn == nil {
		return nil, errors.New("master connection must not be nil")
	}
	if cfg.Handler == nil {
		return nil, errors.New("master handler must not be nil")
	}
	if cfg.PeerRuntime == nil {
		return nil, errors.New("master peer runtime must not be nil")
	}
	readFrame := func(r io.Reader) (*wire.Frame, error) {
		return wire.ReadFrame(r, cfg.MaxPayloadSize)
	}

	mc := &MasterConn{
		handler:              cfg.Handler,
		localID:              append([]byte(nil), cfg.LocalID...),
		localFullShardIDList: append([]uint32(nil), cfg.LocalFullShardIDList...),
		peerRuntime:          cfg.PeerRuntime,
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
			byte(wire.ClusterOpSyncMinorBlockListRequest):           conn.OpSerializerFor[wire.SyncMinorBlockListRequest, wire.SyncMinorBlockListResponse](byte(wire.ClusterOpSyncMinorBlockListResponse)),
			byte(wire.ClusterOpAddMinorBlockRequest):                conn.OpSerializerFor[wire.AddMinorBlockRequest, wire.AddMinorBlockResponse](byte(wire.ClusterOpAddMinorBlockResponse)),
			byte(wire.ClusterOpCreateClusterPeerConnectionRequest):  conn.OpSerializerFor[wire.CreateClusterPeerConnectionRequest, wire.CreateClusterPeerConnectionResponse](byte(wire.ClusterOpCreateClusterPeerConnectionResponse)),
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
			byte(wire.ClusterOpPing):                                mc.handlePing,
			byte(wire.ClusterOpCreateClusterPeerConnectionRequest):  mc.handleCreateClusterPeerConnection,
			byte(wire.ClusterOpDestroyClusterPeerConnectionCommand): mc.handleDestroyClusterPeerConnection,

			// ── Delegated handlers (MasterHandler / service layer) ─────────
			byte(wire.ClusterOpConnectToSlavesRequest):             mc.delegateConnectToSlaves,
			byte(wire.ClusterOpMineRequest):                        mc.delegateMine,
			byte(wire.ClusterOpGenTxRequest):                       mc.delegateGenTx,
			byte(wire.ClusterOpAddRootBlockRequest):                mc.delegateAddRootBlock,
			byte(wire.ClusterOpGetEcoInfoListRequest):              mc.delegateGetEcoInfoList,
			byte(wire.ClusterOpGetNextBlockToMineRequest):          mc.delegateGetNextBlockToMine,
			byte(wire.ClusterOpAddMinorBlockRequest):               mc.delegateAddMinorBlock,
			byte(wire.ClusterOpGetUnconfirmedHeadersRequest):       mc.delegateGetUnconfirmedHeaders,
			byte(wire.ClusterOpGetAccountDataRequest):              mc.delegateGetAccountData,
			byte(wire.ClusterOpAddTransactionRequest):              mc.delegateAddTransaction,
			byte(wire.ClusterOpGetMinorBlockRequest):               mc.delegateGetMinorBlock,
			byte(wire.ClusterOpGetTransactionRequest):              mc.delegateGetTransaction,
			byte(wire.ClusterOpSyncMinorBlockListRequest):          mc.delegateSyncMinorBlockList,
			byte(wire.ClusterOpExecuteTransactionRequest):          mc.delegateExecuteTransaction,
			byte(wire.ClusterOpGetTransactionReceiptRequest):       mc.delegateGetTransactionReceipt,
			byte(wire.ClusterOpGetTransactionListByAddressRequest): mc.delegateGetTransactionListByAddress,
			byte(wire.ClusterOpGetLogRequest):                      mc.delegateGetLogs,
			byte(wire.ClusterOpEstimateGasRequest):                 mc.delegateEstimateGas,
			byte(wire.ClusterOpGetStorageRequest):                  mc.delegateGetStorageAt,
			byte(wire.ClusterOpGetCodeRequest):                     mc.delegateGetCode,
			byte(wire.ClusterOpGasPriceRequest):                    mc.delegateGasPrice,
			byte(wire.ClusterOpGetWorkRequest):                     mc.delegateGetWork,
			byte(wire.ClusterOpSubmitWorkRequest):                  mc.delegateSubmitWork,
			byte(wire.ClusterOpCheckMinorBlockRequest):             mc.delegateCheckMinorBlock,
			byte(wire.ClusterOpGetAllTransactionsRequest):          mc.delegateGetAllTransactions,
			byte(wire.ClusterOpGetRootChainStakesRequest):          mc.delegateGetRootChainStakes,
			byte(wire.ClusterOpGetTotalBalanceRequest):             mc.delegateGetTotalBalance,
		},
		NonRPCOps: map[byte]struct{}{
			byte(wire.ClusterOpDestroyClusterPeerConnectionCommand): {},
		},
		Forwarder: forwarder,
		Logger:    cfg.Logger,
	})

	// Cascade teardown: on any shutdown path delegate the peer cascade to the
	// runtime (Python: MasterConnection.close, slave.py:155-162).
	go func() {
		<-mc.WaitUntilClosed()
		mc.peerRuntime.CloseAllPeers()
	}()
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

// handlePing responds to the master's PING with this slave's identity.
// Python: MasterConnection.handle_ping -> Pong(self.slave_server.id, ...).
func (mc *MasterConn) handlePing(req any) (any, error) {
	ping := req.(*wire.PingRequest)

	if ping.RootTip != nil {
		// TODO: create/update shard runtime from root tip when core.RootBlock
		// is ported (py: await self.slave_server.create_shards(ping.root_tip)).
	}

	return &wire.PongResponse{
		ID:              append([]byte(nil), mc.localID...),
		FullShardIDList: append([]uint32(nil), mc.localFullShardIDList...),
	}, nil
}

// handleCreateClusterPeerConnection parses CREATE and delegates PeerConn
// creation to the runtime. An empty shard set in the runtime makes it a no-op
// while still returning error_code=0 (Python: slave.py:329-370).
func (mc *MasterConn) handleCreateClusterPeerConnection(req any) (any, error) {
	create := req.(*wire.CreateClusterPeerConnectionRequest)
	mc.peerRuntime.CreatePeerConns(create.ClusterPeerID)
	return &wire.CreateClusterPeerConnectionResponse{ErrorCode: 0}, nil
}

// handleDestroyClusterPeerConnection is a fire-and-forget command delegating
// peer teardown to the runtime (Python: slave.py:321-327).
func (mc *MasterConn) handleDestroyClusterPeerConnection(req any) (any, error) {
	destroy := req.(*wire.DestroyClusterPeerConnectionCommand)
	mc.peerRuntime.DestroyPeerConns(destroy.ClusterPeerID)
	return nil, nil
}

// Close shuts down the connection and delegates the peer cascade to the
// runtime. It shadows BaseConn.Close so teardown is synchronous.
func (mc *MasterConn) Close() {
	mc.peerRuntime.CloseAllPeers()
	mc.BaseConn.Close()
}

// ── Frame routing ───────────────────────────────────────────────────────

// routeFrame is the forwarder installed on BaseConn: cluster_peer_id == 0 is
// master-local (dispatch normally); peer traffic is routed through the runtime
// and handed to the matching PeerConn. A LookupPeer miss — including an empty
// shard set — is Python's NULL_CONNECTION semantics (slave.py:131-146): the
// frame is consumed and dropped, no new error is produced.
func (mc *MasterConn) routeFrame(frame *wire.Frame) bool {
	if frame.Meta.ClusterPeerID == 0 {
		return false
	}

	pc := mc.peerRuntime.LookupPeer(frame.Meta.ClusterPeerID, frame.Meta.Branch)
	if pc == nil {
		mc.Logger().Warn("dropping frame for unknown virtual peer connection",
			"cluster_peer_id", frame.Meta.ClusterPeerID, "branch", frame.Meta.Branch)
		return true
	}

	pc.HandleFrame(frame)
	return true
}

// ── Delegated handler dispatch ─────────────────────────────────────────

func (mc *MasterConn) delegateConnectToSlaves(req any) (any, error) {
	return mc.handler.ConnectToSlaves(req.(*wire.ConnectToSlavesRequest))
}

func (mc *MasterConn) delegateMine(req any) (any, error) {
	return mc.handler.Mine(req.(*wire.MineRequest))
}

func (mc *MasterConn) delegateGenTx(req any) (any, error) {
	return mc.handler.GenTx(req.(*wire.GenTxRequest))
}

func (mc *MasterConn) delegateAddRootBlock(req any) (any, error) {
	return mc.handler.AddRootBlock(req.(*wire.AddRootBlockRequest))
}

func (mc *MasterConn) delegateGetEcoInfoList(req any) (any, error) {
	return mc.handler.GetEcoInfoList(req.(*wire.GetEcoInfoListRequest))
}

func (mc *MasterConn) delegateGetNextBlockToMine(req any) (any, error) {
	return mc.handler.GetNextBlockToMine(req.(*wire.GetNextBlockToMineRequest))
}

func (mc *MasterConn) delegateAddMinorBlock(req any) (any, error) {
	return mc.handler.AddMinorBlock(req.(*wire.AddMinorBlockRequest))
}

func (mc *MasterConn) delegateGetUnconfirmedHeaders(req any) (any, error) {
	return mc.handler.GetUnconfirmedHeaders(req.(*wire.GetUnconfirmedHeadersRequest))
}

func (mc *MasterConn) delegateGetAccountData(req any) (any, error) {
	return mc.handler.GetAccountData(req.(*wire.GetAccountDataRequest))
}

func (mc *MasterConn) delegateAddTransaction(req any) (any, error) {
	return mc.handler.AddTransaction(req.(*wire.AddTransactionRequest))
}

func (mc *MasterConn) delegateGetMinorBlock(req any) (any, error) {
	return mc.handler.GetMinorBlock(req.(*wire.GetMinorBlockRequest))
}

func (mc *MasterConn) delegateGetTransaction(req any) (any, error) {
	return mc.handler.GetTransaction(req.(*wire.GetTransactionRequest))
}

func (mc *MasterConn) delegateSyncMinorBlockList(req any) (any, error) {
	return mc.handler.SyncMinorBlockList(req.(*wire.SyncMinorBlockListRequest))
}

func (mc *MasterConn) delegateExecuteTransaction(req any) (any, error) {
	return mc.handler.ExecuteTransaction(req.(*wire.ExecuteTransactionRequest))
}

func (mc *MasterConn) delegateGetTransactionReceipt(req any) (any, error) {
	return mc.handler.GetTransactionReceipt(req.(*wire.GetTransactionReceiptRequest))
}

func (mc *MasterConn) delegateGetTransactionListByAddress(req any) (any, error) {
	return mc.handler.GetTransactionListByAddress(req.(*wire.GetTransactionListByAddressRequest))
}

func (mc *MasterConn) delegateGetLogs(req any) (any, error) {
	return mc.handler.GetLogs(req.(*wire.GetLogRequest))
}

func (mc *MasterConn) delegateEstimateGas(req any) (any, error) {
	return mc.handler.EstimateGas(req.(*wire.EstimateGasRequest))
}

func (mc *MasterConn) delegateGetStorageAt(req any) (any, error) {
	return mc.handler.GetStorageAt(req.(*wire.GetStorageRequest))
}

func (mc *MasterConn) delegateGetCode(req any) (any, error) {
	return mc.handler.GetCode(req.(*wire.GetCodeRequest))
}

func (mc *MasterConn) delegateGasPrice(req any) (any, error) {
	return mc.handler.GasPrice(req.(*wire.GasPriceRequest))
}

func (mc *MasterConn) delegateGetWork(req any) (any, error) {
	return mc.handler.GetWork(req.(*wire.GetWorkRequest))
}

func (mc *MasterConn) delegateSubmitWork(req any) (any, error) {
	return mc.handler.SubmitWork(req.(*wire.SubmitWorkRequest))
}

func (mc *MasterConn) delegateCheckMinorBlock(req any) (any, error) {
	return mc.handler.CheckMinorBlock(req.(*wire.CheckMinorBlockRequest))
}

func (mc *MasterConn) delegateGetAllTransactions(req any) (any, error) {
	return mc.handler.GetAllTransactions(req.(*wire.GetAllTransactionsRequest))
}

func (mc *MasterConn) delegateGetRootChainStakes(req any) (any, error) {
	return mc.handler.GetRootChainStakes(req.(*wire.GetRootChainStakesRequest))
}

func (mc *MasterConn) delegateGetTotalBalance(req any) (any, error) {
	return mc.handler.GetTotalBalance(req.(*wire.GetTotalBalanceRequest))
}
