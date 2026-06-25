// Copyright 2026-2027, QuarkChain.
//
// Package cluster: wire-format message structs for every cluster RPC opcode.
//
// Each struct matches a pyquarkchain Serializable from cluster/rpc.py or
// cluster/p2p_commands.py.  Fields are annotated with qkc/serialize/ struct
// tags so SerializeToBytes / Deserialize produce byte-compatible output.
//
// Tag conventions (see qkc/serialize/typecache.go):
//
//	bytesizeofslicelen:"4"  4-byte big-endian length prefix for slices
//	                        (matches Python PrependedSizeListSerializer(4, …))
//	ser:"nil"              nullable pointer — 1-byte presence marker
//	                        (matches Python Optional(…))
//	ser:"-"                ignored field (not serialized)
//
// ---------------------------------------------------------------------------
// Organisation follows pyquarkchain’s cluster/rpc.py grouping:
//
//	§1  Cluster initialisation     (PING, CONNECT_TO_SLAVES)
//	§2  Virtual connection mgmt    (CREATE/DESTROY_CLUSTER_PEER)
//	§3  Mining / test commands     (MINE, GEN_TX, GET_WORK, SUBMIT_WORK)
//	§4  Blockchain updates         (ADD_ROOT_BLOCK, ADD_MINOR_BLOCK, …)
//	§5  Blockchain queries         (GET_ACCOUNT_DATA, EXECUTE_TX, …)
//	§6  Cross-shard (Slave↔Slave)  (ADD_XSHARD_TX_LIST, …)
//	§7  P2P commands               (HELLO, NEW_MINOR_BLOCK_HEADER, …)
//	§8  P2P query commands         (GET_MINOR_BLOCK_LIST, …)
//
// Types that need porting from pyquarkchain (MinorBlockHeader, Address, …)
// are marked with TODO and the struct body is commented out until those types
// land.  Fields that use primitive Go types are defined now so the wire layout
// is documented even before the handler plumbing is complete.
// ---------------------------------------------------------------------------
package cluster

import (
	"math/big"

	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// =============================================================================
// §1  Cluster initialisation
// =============================================================================

// RootBlock is an opaque placeholder for Python's RootBlock.
//
// The full RootBlock structure (RootBlockHeader + minor_block_header_list +
// tracking_data) is not yet defined in Go.  This placeholder stores the raw
// serialized bytes so that PingRequest can be deserialized without parsing
// the RootBlock contents, and re-serialized byte-identically.
//
// When shard-state initialization is implemented, this type will be replaced
// with a full struct definition.
type RootBlock struct {
	raw []byte
}

// Serialize implements serialize.Serializable.
func (rb *RootBlock) Serialize(w *[]byte) error {
	*w = append(*w, rb.raw...)
	return nil
}

// Deserialize implements serialize.Serializable.
// Reads all remaining bytes from the buffer.
func (rb *RootBlock) Deserialize(bb *serialize.ByteBuffer) error {
	remaining, err := bb.ReadRemaining()
	if err != nil {
		return err
	}
	rb.raw = remaining
	return nil
}

// IsNil returns true if the RootBlock contains no data.
func (rb *RootBlock) IsNil() bool {
	return len(rb.raw) == 0
}

// PingRequest is sent by Master to initialise slave shard state.
// Wire: [4B id] [4B full_shard_id_list] [nil? root_tip as RootBlock]
type PingRequest struct {
	ID              []byte     `bytesizeofslicelen:"4"`
	FullShardIDList []uint32   `bytesizeofslicelen:"4"`
	RootTip         *RootBlock `ser:"nil"`
}

// PongResponse is the slave's reply to PING.
type PongResponse struct {
	ID              []byte   `bytesizeofslicelen:"4"`
	FullShardIDList []uint32 `bytesizeofslicelen:"4"`
}

// SlaveInfo describes a slave node for cross-shard connection setup.
type SlaveInfo struct {
	ID              []byte   `bytesizeofslicelen:"4"`
	Host            []byte   `bytesizeofslicelen:"4"`
	Port            uint16   ``
	FullShardIDList []uint32 `bytesizeofslicelen:"4"`
}

// ConnectToSlavesRequest is sent by Master to instruct a slave to connect
// to other slaves (mode 2 — Slave↔Slave direct TCP).
type ConnectToSlavesRequest struct {
	SlaveInfoList []SlaveInfo `bytesizeofslicelen:"4"`
}

// ConnectToSlavesResponse — empty result string = success.
type ConnectToSlavesResponse struct {
	ResultList [][]byte `bytesizeofslicelen:"4"`
}

// =============================================================================
// §2  Virtual connection management (mode 3 — Peer→Master→Slave)
// =============================================================================

// CreateClusterPeerConnectionRequest is sent by Master when an external peer
// connects.  The slave creates a PeerConn for each owned branch.
type CreateClusterPeerConnectionRequest struct {
	ClusterPeerID uint64
}

// CreateClusterPeerConnectionResponse is the acknowledgement.
type CreateClusterPeerConnectionResponse struct {
	ErrorCode uint32
}

// DestroyClusterPeerConnectionCommand — fire-and-forget (NON-RPC).
type DestroyClusterPeerConnectionCommand struct {
	ClusterPeerID uint64
}

// =============================================================================
// §3  Mining / test commands
// =============================================================================

// ArtificialTxConfig controls auto-generated transaction parameters.
type ArtificialTxConfig struct {
	TargetRootBlockTime  uint32
	TargetMinorBlockTime uint32
}

// MineRequest — start/stop mining on slaves.
type MineRequest struct {
	ArtificialTxConfig ArtificialTxConfig
	Mining             bool
}

// MineResponse is the acknowledgement.
type MineResponse struct {
	ErrorCode uint32
}

// GenTxResponse is the acknowledgement for transaction generation.
type GenTxResponse struct {
	ErrorCode uint32
}

// TODO: GenTxRequest needs TypedTransaction
// type GenTxRequest struct {
// 	NumTxPerShard uint32
// 	XShardPercent uint32
// 	Tx            *TypedTransaction
// }

// =============================================================================
// §3a  GetWork / SubmitWork (PoW mining)
// =============================================================================

// GetWorkResponse returns a mining work package.
type GetWorkResponse struct {
	ErrorCode  uint32
	HeaderHash [32]byte // hash256
	Height     uint64
	Difficulty *big.Int // biguint
}

// SubmitWorkResponse is the acknowledgement for a submitted solution.
type SubmitWorkResponse struct {
	ErrorCode uint32
	Success   bool
}

// TODO: GetWorkRequest needs Branch, Address
// type GetWorkRequest struct {
// 	Branch      uint32  // Branch
// 	CoinbaseAddr *[20]byte `ser:"nil"` // Address
// }

// TODO: SubmitWorkRequest needs Branch, signature
// type SubmitWorkRequest struct {
// 	Branch     uint32
// 	HeaderHash [32]byte
// 	Nonce      uint64
// 	Mixhash    [32]byte
// 	Signature  *[65]byte `ser:"nil"`
// }

// =============================================================================
// §4  Blockchain updates
// =============================================================================

// AddMinorBlockRequest is for adding blocks mined through JRPC.
type AddMinorBlockRequest struct {
	MinorBlockData []byte `bytesizeofslicelen:"4"`
}

// AddMinorBlockResponse is the acknowledgement.
type AddMinorBlockResponse struct {
	ErrorCode uint32
}

// GetUnconfirmedHeadersRequest requests minor block headers to build a new
// root block.  Empty body.
type GetUnconfirmedHeadersRequest struct {
}

// TODO: AddRootBlockRequest needs RootBlock
// type AddRootBlockRequest struct {
// 	RootBlock    *RootBlock
// 	ExpectSwitch bool
// }

// TODO: AddRootBlockResponse
// type AddRootBlockResponse struct {
// 	ErrorCode uint32
// 	Switched  bool
// }

// TODO: AddMinorBlockHeaderRequest needs MinorBlockHeader, TokenBalanceMap, ShardStats
// type AddMinorBlockHeaderRequest struct {
// 	MinorBlockHeader  *MinorBlockHeader `ser:"nil"`
// 	TxCount           uint32
// 	XShardTxCount     uint32
// 	CoinbaseAmountMap TokenBalanceMap
// 	ShardStats        *ShardStats
// }

// TODO: AddMinorBlockHeaderResponse needs ArtificialTxConfig
// type AddMinorBlockHeaderResponse struct {
// 	ErrorCode          uint32
// 	ArtificialTxConfig ArtificialTxConfig
// }

// TODO: AddMinorBlockHeaderListRequest needs MinorBlockHeader, TokenBalanceMap
// type AddMinorBlockHeaderListRequest struct {
// 	MinorBlockHeaderList    []*MinorBlockHeader `bytesizeofslicelen:"4"`
// 	CoinbaseAmountMapList   []TokenBalanceMap   `bytesizeofslicelen:"4"`
// }

// TODO: AddMinorBlockHeaderListResponse
// type AddMinorBlockHeaderListResponse struct {
// 	ErrorCode uint32
// }

// TODO: CheckMinorBlockRequest needs MinorBlockHeader
// type CheckMinorBlockRequest struct {
// 	MinorBlockHeader *MinorBlockHeader
// }

// TODO: CheckMinorBlockResponse
// type CheckMinorBlockResponse struct {
// 	ErrorCode uint32
// }

// TODO: SyncMinorBlockListRequest — master asks slave to sync blocks from peer
// type SyncMinorBlockListRequest struct {
// 	MinorBlockHashList [][32]byte `bytesizeofslicelen:"4"`
// 	Branch             uint32
// 	ClusterPeerID      uint64
// }

// TODO: SyncMinorBlockListResponse — returns coinbase map + optional ShardStats
// type SyncMinorBlockListResponse struct {
// 	ErrorCode        uint32
// 	BlockCoinbaseMap map[[32]byte]TokenBalanceMap  // PrependedSizeMapSerializer(4, hash256, TokenBalanceMap)
// 	ShardStats       *ShardStats `ser:"nil"`
// }

// =============================================================================
// §5  Blockchain queries (master → slave)
// =============================================================================

// TODO: GetEcoInfoListRequest — empty body
// type GetEcoInfoListRequest struct{}

// EcoInfo contains the economic info for a shard.
// TODO: complete fields (height, coinbase_amount, difficulty, …)
type EcoInfo struct {
	Branch uint32
	// Height                          uint64
	// CoinbaseAmount                  *big.Int // uint256
	// Difficulty                      *big.Int // biguint
	// UnconfirmedHeadersCoinbaseAmount *big.Int // uint256
}

// TODO: GetEcoInfoListResponse
// type GetEcoInfoListResponse struct {
// 	ErrorCode    uint32
// 	EcoInfoList  []EcoInfo `bytesizeofslicelen:"4"`
// }

// TODO: GetNextBlockToMineRequest needs Branch, Address
// type GetNextBlockToMineRequest struct {
// 	Branch              uint32
// 	Address             [20]byte
// 	ArtificialTxConfig  ArtificialTxConfig
// }

// TODO: GetNextBlockToMineResponse needs MinorBlock
// type GetNextBlockToMineResponse struct {
// 	ErrorCode uint32
// 	Block     *MinorBlock
// }

// TODO: GetAccountDataRequest needs Address
// type GetAccountDataRequest struct {
// 	Address     [20]byte
// 	BlockHeight *uint64 `ser:"nil"`
// }

// TODO: GetAccountDataResponse needs AccountBranchData
// type GetAccountDataResponse struct {
// 	ErrorCode              uint32
// 	AccountBranchDataList  []AccountBranchData `bytesizeofslicelen:"4"`
// }

// TODO: AddTransactionRequest needs TypedTransaction
// type AddTransactionRequest struct {
// 	Tx *TypedTransaction
// }

// TODO: AddTransactionResponse
// type AddTransactionResponse struct {
// 	ErrorCode uint32
// }

// TODO: ExecuteTransactionRequest needs TypedTransaction, Address
// type ExecuteTransactionRequest struct {
// 	Tx          *TypedTransaction
// 	FromAddress [20]byte
// 	BlockHeight *uint64 `ser:"nil"`
// }

// TODO: ExecuteTransactionResponse
// type ExecuteTransactionResponse struct {
// 	ErrorCode uint32
// 	Result    []byte `bytesizeofslicelen:"4"`
// }

// TODO: GetTransactionReceiptRequest needs Address-like types
// type GetTransactionReceiptRequest struct {
// 	TxHash [32]byte
// 	Branch uint32
// }

// TODO: GetTransactionReceiptResponse needs MinorBlock, TransactionReceipt
// type GetTransactionReceiptResponse struct {
// 	ErrorCode  uint32
// 	MinorBlock *MinorBlock
// 	Index      uint32
// 	Receipt    *TransactionReceipt
// }

// TODO: GetMinorBlockRequest needs Branch
// type GetMinorBlockRequest struct {
// 	Branch         uint32
// 	MinorBlockHash [32]byte
// 	Height         uint64
// 	NeedExtraInfo  bool
// }

// TODO: GetMinorBlockResponse needs MinorBlock, MinorBlockExtraInfo
// type GetMinorBlockResponse struct {
// 	ErrorCode  uint32
// 	MinorBlock *MinorBlock
// 	ExtraInfo  *MinorBlockExtraInfo `ser:"nil"`
// }

// TODO: GetTransactionRequest
// type GetTransactionRequest struct {
// 	TxHash [32]byte
// 	Branch uint32
// }

// TODO: GetTransactionResponse needs MinorBlock
// type GetTransactionResponse struct {
// 	ErrorCode  uint32
// 	MinorBlock *MinorBlock
// 	Index      uint32
// }

// TODO: GetTransactionListByAddressRequest needs Address
// type GetTransactionListByAddressRequest struct {
// 	Address          [20]byte
// 	TransferTokenID  *uint64 `ser:"nil"`
// 	Start            []byte  `bytesizeofslicelen:"4"`
// 	Limit            uint32
// }

// TODO: GetTransactionListByAddressResponse needs TransactionDetail
// type GetTransactionListByAddressResponse struct {
// 	ErrorCode uint32
// 	TxList    []TransactionDetail `bytesizeofslicelen:"4"`
// 	Next      []byte              `bytesizeofslicelen:"4"`
// }

// TODO: GetAllTransactionsRequest needs Branch
// type GetAllTransactionsRequest struct {
// 	Branch uint32
// 	Start  []byte `bytesizeofslicelen:"4"`
// 	Limit  uint32
// }

// TODO: GetAllTransactionsResponse needs TransactionDetail
// type GetAllTransactionsResponse struct {
// 	ErrorCode uint32
// 	TxList    []TransactionDetail `bytesizeofslicelen:"4"`
// 	Next      []byte              `bytesizeofslicelen:"4"`
// }

// TODO: GetLogRequest needs Address, Branch
// type GetLogRequest struct {
// 	Branch     uint32
// 	Addresses  [][20]byte     `bytesizeofslicelen:"4"`
// 	Topics     [][][32]byte   `bytesizeofslicelen:"4"`
// 	StartBlock uint64
// 	EndBlock   uint64
// }

// TODO: GetLogResponse needs Log
// type GetLogResponse struct {
// 	ErrorCode uint32
// 	Logs      []Log `bytesizeofslicelen:"4"`
// }

// TODO: EstimateGasRequest needs TypedTransaction, Address
// type EstimateGasRequest struct {
// 	Tx          *TypedTransaction
// 	FromAddress [20]byte
// }

// EstimateGasResponse is the gas estimate result.
type EstimateGasResponse struct {
	ErrorCode uint32
	Result    uint32
}

// TODO: GetStorageRequest needs Address
// type GetStorageRequest struct {
// 	Address     [20]byte
// 	Key         *big.Int // uint256
// 	BlockHeight *uint64  `ser:"nil"`
// }

// GetStorageResponse is the storage value at the given key.
type GetStorageResponse struct {
	ErrorCode uint32
	Result    [32]byte
}

// TODO: GetCodeRequest needs Address
// type GetCodeRequest struct {
// 	Address     [20]byte
// 	BlockHeight *uint64 `ser:"nil"`
// }

// GetCodeResponse is the contract bytecode.
type GetCodeResponse struct {
	ErrorCode uint32
	Result    []byte `bytesizeofslicelen:"4"`
}

// TODO: GasPriceRequest needs Branch
// type GasPriceRequest struct {
// 	Branch  uint32
// 	TokenID uint64
// }

// GasPriceResponse is the current gas price.
type GasPriceResponse struct {
	ErrorCode uint32
	Result    uint64
}

// GetRootChainStakesResponse is the stake info for an address.
type GetRootChainStakesResponse struct {
	ErrorCode uint32
	Stakes    *big.Int // biguint
	Signer    [20]byte
}

// TODO: GetRootChainStakesRequest needs Address
// type GetRootChainStakesRequest struct {
// 	Address        [20]byte
// 	MinorBlockHash [32]byte
// }

// TODO: GetTotalBalanceRequest — complex, needs Branch, Address types
// type GetTotalBalanceRequest struct {
// 	Branch         uint32
// 	Start          *[32]byte `ser:"nil"`
// 	TokenID        uint64
// 	Limit          uint32
// 	MinorBlockHash [32]byte
// 	RootBlockHash  *[32]byte `ser:"nil"`
// }

// GetTotalBalanceResponse is the total balance query result.
type GetTotalBalanceResponse struct {
	ErrorCode    uint32
	TotalBalance *big.Int // biguint
	Next         []byte   `bytesizeofslicelen:"4"`
}

// =============================================================================
// §6  Cross-shard (Slave ↔ Slave, direct TCP)
// =============================================================================

// TODO: AddXshardTxListRequest needs Branch, CrossShardTransactionList
// type AddXshardTxListRequest struct {
// 	Branch         uint32
// 	MinorBlockHash [32]byte
// 	TxList         *CrossShardTransactionList
// }

// AddXshardTxListResponse is the acknowledgement for xshard delivery.
type AddXshardTxListResponse struct {
	ErrorCode uint32
}

// TODO: BatchAddXshardTxListRequest
// type BatchAddXshardTxListRequest struct {
// 	AddXshardTxListRequestList []AddXshardTxListRequest `bytesizeofslicelen:"4"`
// }

// BatchAddXshardTxListResponse is the acknowledgement for batch xshard delivery.
type BatchAddXshardTxListResponse struct {
	ErrorCode uint32
}

// =============================================================================
// §7  P2P commands (CommandOp, cluster_peer_id != 0)
// =============================================================================

// TODO: NewMinorBlockHeaderListCommand needs RootBlockHeader, MinorBlockHeader
// type NewMinorBlockHeaderListCommand struct {
// 	RootBlockHeader       *RootBlockHeader
// 	MinorBlockHeaderList  []*MinorBlockHeader `bytesizeofslicelen:"4"`
// }

// TODO: NewTransactionListCommand needs TypedTransaction
// type NewTransactionListCommand struct {
// 	TransactionList []*TypedTransaction `bytesizeofslicelen:"4"`
// }

// TODO: NewBlockMinorCommand needs MinorBlock
// type NewBlockMinorCommand struct {
// 	Block *MinorBlock
// }

// =============================================================================
// §8  P2P query commands
// =============================================================================

// TODO: GetMinorBlockHeaderListRequest needs hash256
// type GetMinorBlockHeaderListRequest struct {
// 	BlockHash [32]byte
// 	Branch    uint32
// 	Limit     uint32
// 	Direction uint8 // 0=GENESIS, 1=TIP
// }

// TODO: GetMinorBlockHeaderListResponse needs RootBlockHeader, MinorBlockHeader
// type GetMinorBlockHeaderListResponse struct {
// 	RootTip         *RootBlockHeader
// 	HeaderTip       *MinorBlockHeader
// 	BlockHeaderList []*MinorBlockHeader `bytesizeofslicelen:"4"`
// }

// TODO: GetMinorBlockListRequest
// type GetMinorBlockListRequest struct {
// 	MinorBlockHashList [][32]byte `bytesizeofslicelen:"4"`
// }

// TODO: GetMinorBlockListResponse needs MinorBlock
// type GetMinorBlockListResponse struct {
// 	MinorBlockList []*MinorBlock `bytesizeofslicelen:"4"`
// }
