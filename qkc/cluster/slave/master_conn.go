// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// MasterConn represents the slave-side TCP connection to the cluster master.
// It corresponds to Python's quarkchain.cluster.slave.MasterConnection and uses
// 12-byte ClusterMetadata framing.
//
// Architecture:
//
//	MasterConn embeds *conn.BaseConn
//
// All master→slave ClusterOp handlers are registered during construction.
// Business handlers that depend on unported components (Shard, StateDB, etc.)
// are implemented as stubs that return ErrHandlerNotImplemented to fail fast.
type MasterConn struct {
	*conn.BaseConn

	localID              []byte
	localFullShardIDList []uint32
}

// NewMasterConn dials the master at addr and returns a MasterConn.
// maxPayloadSize controls frame payload size limit; 0 disables the limit.
// localID and localFullShardIDList identify this slave and are used in PONG.
func NewMasterConn(addr string, maxPayloadSize uint32, localID []byte, localFullShardIDList []uint32, logger log.Logger) (*MasterConn, error) {
	cn, err := net.DialTimeout("tcp", addr, defaultDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial master %s: %w", addr, err)
	}
	return newMasterConn(cn, maxPayloadSize, localID, localFullShardIDList, logger), nil
}

// NewMasterConnFromConn wraps an accepted net.Conn as a MasterConn.
// maxPayloadSize controls frame payload size limit; 0 disables the limit.
func NewMasterConnFromConn(cn net.Conn, maxPayloadSize uint32, localID []byte, localFullShardIDList []uint32, logger log.Logger) *MasterConn {
	return newMasterConn(cn, maxPayloadSize, localID, localFullShardIDList, logger)
}

func newMasterConn(cn net.Conn, maxPayloadSize uint32, localID []byte, localFullShardIDList []uint32, logger log.Logger) *MasterConn {
	readFrame := func(r io.Reader) (*wire.Frame, error) {
		return wire.ReadFrame(r, maxPayloadSize)
	}
	mc := &MasterConn{
		BaseConn:             conn.NewBaseConnFromConn(cn, readFrame, wire.WriteFrame, logger),
		localID:              append([]byte(nil), localID...),
		localFullShardIDList: append([]uint32(nil), localFullShardIDList...),
	}

	mc.registerOpSerializers()
	mc.registerHandlers()

	return mc
}

// registerOpSerializers registers one serializer per RPC pair, keyed by the
// request opcode. BaseConn.RegisterOpSerializers installs each serializer
// under both its request opcode and its ResponseOpCode, so inbound response
// payloads can be deserialized without a second registration.
func (mc *MasterConn) registerOpSerializers() {
	mc.BaseConn.RegisterOpSerializers(map[byte]*conn.OpSerializer{
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

		// §3 Slave ↔ Slave (xshard direct)
		byte(wire.ClusterOpAddXshardTxListRequest): conn.OpSerializerFor[wire.AddXshardTxListRequest, wire.AddXshardTxListResponse](byte(wire.ClusterOpAddXshardTxListResponse)),

		// §4 Master → Slave (sync / virtual conns)
		byte(wire.ClusterOpSyncMinorBlockListRequest):           conn.OpSerializerFor[wire.SyncMinorBlockListRequest, wire.SyncMinorBlockListResponse](byte(wire.ClusterOpSyncMinorBlockListResponse)),
		byte(wire.ClusterOpAddMinorBlockRequest):                conn.OpSerializerFor[wire.AddMinorBlockRequest, wire.AddMinorBlockResponse](byte(wire.ClusterOpAddMinorBlockResponse)),
		byte(wire.ClusterOpCreateClusterPeerConnectionRequest):  conn.OpSerializerFor[wire.CreateClusterPeerConnectionRequest, wire.CreateClusterPeerConnectionResponse](byte(wire.ClusterOpCreateClusterPeerConnectionResponse)),
		byte(wire.ClusterOpDestroyClusterPeerConnectionCommand): conn.OpSerializerFor[wire.DestroyClusterPeerConnectionCommand, wire.DestroyClusterPeerConnectionCommand](byte(wire.ClusterOpDestroyClusterPeerConnectionCommand)),
		byte(wire.ClusterOpGetMinorBlockRequest):                conn.OpSerializerFor[wire.GetMinorBlockRequest, wire.GetMinorBlockResponse](byte(wire.ClusterOpGetMinorBlockResponse)),
		byte(wire.ClusterOpGetTransactionRequest):               conn.OpSerializerFor[wire.GetTransactionRequest, wire.GetTransactionResponse](byte(wire.ClusterOpGetTransactionResponse)),

		// §5 Slave ↔ Slave (xshard batch)
		byte(wire.ClusterOpBatchAddXshardTxListRequest): conn.OpSerializerFor[wire.BatchAddXshardTxListRequest, wire.BatchAddXshardTxListResponse](byte(wire.ClusterOpBatchAddXshardTxListResponse)),

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
	})
}

// registerHandlers registers all master→slave RPC handlers and marks the
// fire-and-forget opcodes as non-RPC.
func (mc *MasterConn) registerHandlers() {
	mc.BaseConn.RegisterTypedHandlers(map[byte]conn.TypedHandler{
		// ── Communication handlers ─────────────────────────────────────
		byte(wire.ClusterOpPing):                                mc.handlePing,
		byte(wire.ClusterOpCreateClusterPeerConnectionRequest):  mc.handleCreateClusterPeerConnection,
		byte(wire.ClusterOpDestroyClusterPeerConnectionCommand): mc.handleDestroyClusterPeerConnection,

		// ── Migration stubs ─────────────────────────────────────────────
		// These handlers exist only to preserve protocol compatibility.
		// Real implementations must be added outside the connection layer.
		// After migration, remove these stub registrations and handlers.

		byte(wire.ClusterOpConnectToSlavesRequest): mc.handleConnectToSlaves,

		byte(wire.ClusterOpMineRequest):                        mc.handleMine,
		byte(wire.ClusterOpGenTxRequest):                       mc.handleGenTx,
		byte(wire.ClusterOpAddRootBlockRequest):                mc.handleAddRootBlock,
		byte(wire.ClusterOpGetEcoInfoListRequest):              mc.handleGetEcoInfoList,
		byte(wire.ClusterOpGetNextBlockToMineRequest):          mc.handleGetNextBlockToMine,
		byte(wire.ClusterOpAddMinorBlockRequest):               mc.handleAddMinorBlock,
		byte(wire.ClusterOpGetUnconfirmedHeadersRequest):       mc.handleGetUnconfirmedHeaders,
		byte(wire.ClusterOpGetAccountDataRequest):              mc.handleGetAccountData,
		byte(wire.ClusterOpAddTransactionRequest):              mc.handleAddTransaction,
		byte(wire.ClusterOpGetMinorBlockRequest):               mc.handleGetMinorBlock,
		byte(wire.ClusterOpGetTransactionRequest):              mc.handleGetTransaction,
		byte(wire.ClusterOpSyncMinorBlockListRequest):          mc.handleSyncMinorBlockList,
		byte(wire.ClusterOpExecuteTransactionRequest):          mc.handleExecuteTransaction,
		byte(wire.ClusterOpGetTransactionReceiptRequest):       mc.handleGetTransactionReceipt,
		byte(wire.ClusterOpGetTransactionListByAddressRequest): mc.handleGetTransactionListByAddress,
		byte(wire.ClusterOpGetLogRequest):                      mc.handleGetLogs,
		byte(wire.ClusterOpEstimateGasRequest):                 mc.handleEstimateGas,
		byte(wire.ClusterOpGetStorageRequest):                  mc.handleGetStorageAt,
		byte(wire.ClusterOpGetCodeRequest):                     mc.handleGetCode,
		byte(wire.ClusterOpGasPriceRequest):                    mc.handleGasPrice,
		byte(wire.ClusterOpGetWorkRequest):                     mc.handleGetWork,
		byte(wire.ClusterOpSubmitWorkRequest):                  mc.handleSubmitWork,
		byte(wire.ClusterOpCheckMinorBlockRequest):             mc.handleCheckMinorBlock,
		byte(wire.ClusterOpGetAllTransactionsRequest):          mc.handleGetAllTransactions,
		byte(wire.ClusterOpGetRootChainStakesRequest):          mc.handleGetRootChainStakes,
		byte(wire.ClusterOpGetTotalBalanceRequest):             mc.handleGetTotalBalance,
	})

	mc.BaseConn.RegisterNonRPCOps([]byte{
		byte(wire.ClusterOpDestroyClusterPeerConnectionCommand),
	})
}

// LocalID returns this slave's ID used in PONG responses.
func (mc *MasterConn) LocalID() []byte {
	return append([]byte(nil), mc.localID...)
}

// LocalFullShardIDList returns this slave's full shard ID list used in PONG responses.
func (mc *MasterConn) LocalFullShardIDList() []uint32 {
	return append([]uint32(nil), mc.localFullShardIDList...)
}

// handlePing responds to the master's PING with this slave's identity.
// Python: MasterConnection.handle_ping -> Pong(self.slave_server.id, ...).
func (mc *MasterConn) handlePing(req any) (any, error) {
	ping := req.(*wire.PingRequest)

	if ping.RootTip != nil {
		// TODO: create/update shard runtime from root tip. when core.RootBlock is ported, use ping.root_tip to drive shard creation.
	}

	return &wire.PongResponse{
		ID:              append([]byte(nil), mc.localID...),
		FullShardIDList: append([]uint32(nil), mc.localFullShardIDList...),
	}, nil
}

// handleCreateClusterPeerConnection creates virtual peer connections for all shards.
// Python: returns CreateClusterPeerConnectionResponse(error_code=0) on success.
func (mc *MasterConn) handleCreateClusterPeerConnection(req any) (any, error) {
	_ = req.(*wire.CreateClusterPeerConnectionRequest)
	// TODO: create PeerShardConnection instances and wire with the dispatcher (PR6).
	return nil, conn.ErrHandlerNotImplemented
}

// handleDestroyClusterPeerConnection is a fire-and-forget command to tear down
// a virtual peer connection. No response is sent.
func (mc *MasterConn) handleDestroyClusterPeerConnection(req any) (any, error) {
	_ = req.(*wire.DestroyClusterPeerConnectionCommand)
	// TODO: notify dispatcher / close peer shard connections (PR6).
	return nil, nil
}

// SetForwarder installs a raw-frame forwarder hook for peer traffic
// (cluster_peer_id != 0). The Dispatcher uses this to route frames to
// virtual PeerConns.
func (mc *MasterConn) SetForwarder(f func(*wire.Frame) bool) {
	mc.BaseConn.SetForwarder(f)
}

// ForwardFrame writes a raw frame to the underlying TCP transport. It is used
// by virtual PeerConns to send responses back to the master.
func (mc *MasterConn) ForwardFrame(f *wire.Frame) error {
	return mc.BaseConn.SubmitFrame(f)
}

// SendRPCMeta sends a request with ClusterMetadata and waits for the response.
func (mc *MasterConn) SendRPCMeta(ctx context.Context, opcode byte, payload []byte, meta wire.ClusterMetadata) (*wire.Frame, error) {
	return mc.BaseConn.SendRPCMeta(ctx, opcode, payload, meta)
}

// SendAddMinorBlockHeader sends AddMinorBlockHeaderRequest to the master and
// returns the parsed response.
func (mc *MasterConn) SendAddMinorBlockHeader(ctx context.Context, req *wire.AddMinorBlockHeaderRequest) (*wire.AddMinorBlockHeaderResponse, error) {
	payload, err := serialize.SerializeToBytes(req)
	if err != nil {
		return nil, fmt.Errorf("serialize AddMinorBlockHeaderRequest: %w", err)
	}
	frame, err := mc.SendRPCMeta(ctx, byte(wire.ClusterOpAddMinorBlockHeaderRequest), payload, wire.ClusterMetadata{})
	if err != nil {
		return nil, err
	}
	var resp wire.AddMinorBlockHeaderResponse
	if err := serialize.DeserializeFromBytes(frame.Payload, &resp); err != nil {
		return nil, fmt.Errorf("deserialize AddMinorBlockHeaderResponse: %w", err)
	}
	return &resp, nil
}

// SendAddMinorBlockHeaderList sends AddMinorBlockHeaderListRequest to the master
// and returns the parsed response.
func (mc *MasterConn) SendAddMinorBlockHeaderList(ctx context.Context, req *wire.AddMinorBlockHeaderListRequest) (*wire.AddMinorBlockHeaderListResponse, error) {
	payload, err := serialize.SerializeToBytes(req)
	if err != nil {
		return nil, fmt.Errorf("serialize AddMinorBlockHeaderListRequest: %w", err)
	}
	frame, err := mc.SendRPCMeta(ctx, byte(wire.ClusterOpAddMinorBlockHeaderListRequest), payload, wire.ClusterMetadata{})
	if err != nil {
		return nil, err
	}
	var resp wire.AddMinorBlockHeaderListResponse
	if err := serialize.DeserializeFromBytes(frame.Payload, &resp); err != nil {
		return nil, fmt.Errorf("deserialize AddMinorBlockHeaderListResponse: %w", err)
	}
	return &resp, nil
}

// ── Migration stubs ─────────────────────────────────────────────

// handleConnectToSlaves accepts a list of slaves to connect to.
// Python: returns ConnectToSlavesResponse with one empty bytes result per slave.
// Stub: returns ErrHandlerNotImplemented to fail fast.
func (mc *MasterConn) handleConnectToSlaves(req any) (any, error) {
	_ = req.(*wire.ConnectToSlavesRequest)

	// TODO: delegate to SlaveServer.slave_connection_manager.connect_to_slave.
	return nil, conn.ErrHandlerNotImplemented
}

// handleMine starts or stops mining.
// Python: MineResponse(error_code=0).
func (mc *MasterConn) handleMine(req any) (any, error) {
	_ = req.(*wire.MineRequest)

	// TODO: delegate to SlaveComm.start_mining / stop_mining.
	mc.Logger().Warn("Mine stub invoked — mining command (not implemented)", "remote", mc.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleGenTx generates transactions.
// Python: GenTxResponse(error_code=0).
func (mc *MasterConn) handleGenTx(req any) (any, error) {
	_ = req.(*wire.GenTxRequest)
	// TODO: delegate to SlaveComm.create_transactions.
	mc.Logger().Warn("GenTx stub invoked — transaction generation will be discarded", "remote", mc.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleAddRootBlock processes a root block from the master.
// Python: returns AddRootBlockResponse(error_code=0, switched=False) on success.
func (mc *MasterConn) handleAddRootBlock(req any) (any, error) {
	_ = req.(*wire.AddRootBlockRequest)
	// TODO: delegate to shard.add_root_block and SlaveComm.create_shards.
	mc.Logger().Warn("AddRootBlock stub invoked — root block will be discarded", "remote", mc.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetEcoInfoList returns economic info for all initialized shards.
// Python: returns empty list when no shards are initialized.
func (mc *MasterConn) handleGetEcoInfoList(req any) (any, error) {
	_ = req.(*wire.GetEcoInfoListRequest)
	// TODO: collect real EcoInfo from shard states.
	mc.Logger().Warn("GetEcoInfoList stub invoked — returning empty list", "remote", mc.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetNextBlockToMine returns a block template for the requested branch.
// Python requires the shard to exist; without shard runtime we return not-found.
func (mc *MasterConn) handleGetNextBlockToMine(req any) (any, error) {
	_ = req.(*wire.GetNextBlockToMineRequest)
	// TODO: delegate to shard.state.create_block_to_mine.
	return nil, conn.ErrHandlerNotImplemented
}

// handleAddMinorBlock adds a JRPC-mined minor block.
// Python: returns AddMinorBlockResponse(error_code=0) on success.
func (mc *MasterConn) handleAddMinorBlock(req any) (any, error) {
	_ = req.(*wire.AddMinorBlockRequest)
	// TODO: deserialize MinorBlock and delegate to shard.add_block.
	mc.Logger().Warn("AddMinorBlock stub invoked — minor block will be discarded", "remote", mc.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetUnconfirmedHeaders returns unconfirmed headers per shard.
// Python: returns empty list when no shards are initialized.
func (mc *MasterConn) handleGetUnconfirmedHeaders(req any) (any, error) {
	_ = req.(*wire.GetUnconfirmedHeadersRequest)
	// TODO: collect real HeadersInfo from shard states.
	mc.Logger().Warn("GetUnconfirmedHeaders stub invoked — returning empty list", "remote", mc.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetAccountData returns account data across shards.
// Python: returns empty list when there are no shards for the address.
func (mc *MasterConn) handleGetAccountData(req any) (any, error) {
	_ = req.(*wire.GetAccountDataRequest)
	// TODO: delegate to SlaveComm.get_account_data.
	mc.Logger().Warn("GetAccountData stub invoked — returning empty list", "remote", mc.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleAddTransaction adds a transaction to the tx pool.
// Python: returns AddTransactionResponse(error_code=0) on success.
func (mc *MasterConn) handleAddTransaction(req any) (any, error) {
	_ = req.(*wire.AddTransactionRequest)
	// TODO: delegate to SlaveComm.add_tx.
	mc.Logger().Warn("AddTransaction stub invoked — transaction will be discarded", "remote", mc.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetMinorBlock fetches a minor block by hash or height.
// Python returns error_code=1 with an empty block when not found.
func (mc *MasterConn) handleGetMinorBlock(req any) (any, error) {
	_ = req.(*wire.GetMinorBlockRequest)
	// TODO: delegate to SlaveComm.get_minor_block_by_hash / by_height.
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetTransaction fetches a transaction by hash.
// Python returns error_code=1 with an empty block when not found.
func (mc *MasterConn) handleGetTransaction(req any) (any, error) {
	_ = req.(*wire.GetTransactionRequest)
	// TODO: delegate to SlaveComm.get_transaction_by_hash.
	return nil, conn.ErrHandlerNotImplemented
}

// handleSyncMinorBlockList downloads and applies a list of minor blocks.
// Python returns error_code=0 with empty data when the input list is empty.
func (mc *MasterConn) handleSyncMinorBlockList(req any) (any, error) {
	r := req.(*wire.SyncMinorBlockListRequest)
	_ = r
	// TODO: delegate to SlaveComm.add_block_list_for_sync.
	mc.Logger().Warn("SyncMinorBlockList stub invoked — block list will be discarded", "remote", mc.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleExecuteTransaction executes a transaction and returns the result.
// Python returns error_code=1 when execution fails (e.g. shard missing).
func (mc *MasterConn) handleExecuteTransaction(req any) (any, error) {
	_ = req.(*wire.ExecuteTransactionRequest)
	// TODO: delegate to SlaveComm.execute_tx.
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetTransactionReceipt fetches a transaction receipt.
// Python returns error_code=1 with empty block/receipt when not found.
func (mc *MasterConn) handleGetTransactionReceipt(req any) (any, error) {
	_ = req.(*wire.GetTransactionReceiptRequest)
	// TODO: delegate to SlaveComm.get_transaction_receipt.
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetTransactionListByAddress returns transactions for an address.
// Python returns error_code=1 with empty lists when the shard is missing.
func (mc *MasterConn) handleGetTransactionListByAddress(req any) (any, error) {
	_ = req.(*wire.GetTransactionListByAddressRequest)
	// TODO: delegate to SlaveComm.get_transaction_list_by_address.
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetLogs returns logs matching the filter.
// Python returns error_code=1 with empty logs when the shard is missing.
func (mc *MasterConn) handleGetLogs(req any) (any, error) {
	_ = req.(*wire.GetLogRequest)
	// TODO: delegate to SlaveComm.get_logs.
	return nil, conn.ErrHandlerNotImplemented
}

// handleEstimateGas estimates gas for a transaction.
// Python returns error_code=1 when estimation fails (e.g. shard missing).
func (mc *MasterConn) handleEstimateGas(req any) (any, error) {
	_ = req.(*wire.EstimateGasRequest)
	// TODO: delegate to SlaveComm.estimate_gas.
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetStorageAt reads storage at the given address/key.
// Python returns error_code=1 with a zero result when the shard is missing.
func (mc *MasterConn) handleGetStorageAt(req any) (any, error) {
	_ = req.(*wire.GetStorageRequest)
	// TODO: delegate to SlaveComm.get_storage_at.
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetCode reads code at the given address.
// Python returns error_code=1 with empty bytes when the shard is missing.
func (mc *MasterConn) handleGetCode(req any) (any, error) {
	_ = req.(*wire.GetCodeRequest)
	// TODO: delegate to SlaveComm.get_code.
	return nil, conn.ErrHandlerNotImplemented
}

// handleGasPrice returns the gas price for a token on a branch.
// Python returns error_code=1 with result 0 when the shard is missing.
func (mc *MasterConn) handleGasPrice(req any) (any, error) {
	_ = req.(*wire.GasPriceRequest)
	// TODO: delegate to SlaveComm.gas_price.
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetWork returns mining work.
// Python returns error_code=1 when work cannot be produced.
func (mc *MasterConn) handleGetWork(req any) (any, error) {
	_ = req.(*wire.GetWorkRequest)
	// TODO: delegate to SlaveComm.get_work.
	return nil, conn.ErrHandlerNotImplemented
}

// handleSubmitWork submits mining work.
// Python returns error_code=1, success=False when submission fails.
func (mc *MasterConn) handleSubmitWork(req any) (any, error) {
	_ = req.(*wire.SubmitWorkRequest)
	// TODO: delegate to SlaveComm.submit_work.
	return nil, conn.ErrHandlerNotImplemented
}

// handleCheckMinorBlock validates a minor block header.
// Python returns CheckMinorBlockResponse(error_code=0) when the block is valid,
// and error_code=errno.EBADMSG when the shard is missing or validation fails.
// This stub returns ErrorCode=1 to signal "not implemented / cannot validate".
func (mc *MasterConn) handleCheckMinorBlock(req any) (any, error) {
	_ = req.(*wire.CheckMinorBlockRequest)
	// TODO: delegate to shard.check_minor_block_by_header.
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetAllTransactions returns all transactions in the mempool.
// Python returns error_code=1 with empty lists when the shard is missing.
func (mc *MasterConn) handleGetAllTransactions(req any) (any, error) {
	_ = req.(*wire.GetAllTransactionsRequest)
	// TODO: delegate to SlaveComm.get_all_transactions.
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetRootChainStakes reads root-chain stake info.
// Python returns GetRootChainStakesResponse(0, stakes, signer).
func (mc *MasterConn) handleGetRootChainStakes(req any) (any, error) {
	_ = req.(*wire.GetRootChainStakesRequest)
	// TODO: delegate to SlaveComm.get_root_chain_stakes.
	mc.Logger().Warn("GetRootChainStakes stub invoked — returning zero values", "remote", mc.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleGetTotalBalance returns the total token balance across accounts.
// Python catches exceptions and returns GetTotalBalanceResponse(1, 0, b"").
func (mc *MasterConn) handleGetTotalBalance(req any) (any, error) {
	_ = req.(*wire.GetTotalBalanceRequest)
	// TODO: delegate to SlaveComm.get_total_balance.
	return nil, conn.ErrHandlerNotImplemented
}
