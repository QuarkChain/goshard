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

// SlaveConnHandler handles communication-layer operations dispatched by
// MasterConn. Implementations may coordinate shard, peer, and xshard
// connection lifecycle.
type SlaveConnHandler interface {
	// CreateShardsAndPeerConnections creates local shards through the business
	// handler and equips every newly created branch with PeerConns.
	//
	// The concrete orchestration is implemented outside MasterConn.
	CreateShardsAndPeerConnections(rootTip *wire.RawBytes) error

	// ConnectToSlaves connects to the slaves advertised by the master.
	ConnectToSlaves(req *wire.ConnectToSlavesRequest) (*wire.ConnectToSlavesResponse, error)

	// CreateClusterPeerConnection creates PeerConns for the given cluster peer
	// on all current local branches.
	CreateClusterPeerConnection(req *wire.CreateClusterPeerConnectionRequest) (*wire.CreateClusterPeerConnectionResponse, error)

	// DestroyClusterPeerConnection removes the given cluster peer and closes
	// its PeerConns.
	DestroyClusterPeerConnection(req *wire.DestroyClusterPeerConnectionCommand) error
}

// MasterHandler handles master commands that operate on business-owned
// runtime state. It is implemented by the composition layer and injected
// into SlaveComm.
type MasterHandler interface {
	// CreateShards creates the local shards the master's PING RootTip makes
	// eligible and returns the full shard ids it actually created in this
	// call, which become this slave's new local branches.
	//
	// The handler owns the shard-creation decision (py: slave_server.create_shards):
	// it decodes the RootTip, restricts to the shards this slave covers and
	// those having a GENESIS config, skips shards already created, and keeps
	// only those whose GENESIS.ROOT_HEIGHT the root height has reached. One
	// call may therefore create zero, one or several shards. Returning an
	// empty slice is the normal "nothing became eligible" outcome.
	//
	// The return value is a Go-internal contract, not a wire field: Python
	// shares the created set through slave_server.shards, which the Go
	// business/communication boundary cannot read, so the fact is handed
	// over explicitly here.
	CreateShards(rootTip *wire.RawBytes) ([]uint32, error)

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

// MasterConnConfig configures a MasterConn. Conn, SlaveConnHandler and Handler
// are required; Logger defaults to log.Root().
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

	// SlaveConnHandler handles communication-layer operations dispatched by
	// MasterConn, including topology and peer-connection lifecycle commands.
	// Its concrete implementation may be provided by SlaveComm.
	SlaveConnHandler SlaveConnHandler

	// Handler handles master commands that operate on runtime-owned state.
	// The concrete implementation is provided by the composition layer.
	Handler MasterHandler

	// Logger defaults to log.Root() if nil.
	Logger log.Logger
}

// MasterConn represents the slave-side TCP connection to the cluster master.
// It corresponds to Python's quarkchain.cluster.slave.MasterConnection and
// uses 12-byte ClusterMetadata framing.
type MasterConn struct {
	*conn.BaseConn

	handler              MasterHandler
	slaveConnHandler     SlaveConnHandler
	localID              []byte
	localFullShardIDList []uint32
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
	readFrame := func(r io.Reader) (*wire.Frame, error) {
		return wire.ReadFrame(r, cfg.MaxPayloadSize)
	}

	mc := &MasterConn{
		slaveConnHandler:     cfg.SlaveConnHandler,
		handler:              cfg.Handler,
		localID:              append([]byte(nil), cfg.LocalID...),
		localFullShardIDList: append([]uint32(nil), cfg.LocalFullShardIDList...),
	}

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
		// Forwarder stays nil: routing peer traffic (cluster_peer_id != 0)
		// to virtual PeerConns is PR6 (Dispatcher as the frame consumer).
		// Until then, any peer frame (CommandOp opcode) is unregistered and
		// closes the connection — MasterConn must not receive peer traffic.
		Logger: cfg.Logger,
	})
	return mc, nil
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

// handlePing handles the master's PING.
//
// It replies with this slave's identity and, when RootTip is present,
// triggers shard creation/update.
// (py: MasterConnection.handle_ping)
func (mc *MasterConn) handlePing(req any) (any, error) {
	ping := req.(*wire.PingRequest)
	if ping.RootTip != nil {
		if err := mc.slaveConnHandler.CreateShardsAndPeerConnections(ping.RootTip); err != nil {
			return nil, err
		}
	}
	return &wire.PongResponse{
		ID:              append([]byte(nil), mc.localID...),
		FullShardIDList: append([]uint32(nil), mc.localFullShardIDList...),
	}, nil
}

// ── Inbound handler dispatch ─────────────────────────────────────────────
// Business RPCs go to MasterHandler; communication/topology commands go to SlaveConnHandler.

func (mc *MasterConn) handleCreateClusterPeerConnection(req any) (any, error) {
	return mc.slaveConnHandler.CreateClusterPeerConnection(req.(*wire.CreateClusterPeerConnectionRequest))
}

func (mc *MasterConn) handleDestroyClusterPeerConnection(req any) (any, error) {
	return nil, mc.slaveConnHandler.DestroyClusterPeerConnection(req.(*wire.DestroyClusterPeerConnectionCommand))
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
