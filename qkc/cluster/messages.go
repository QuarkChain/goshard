// Copyright 2026-2027, QuarkChain.
package cluster

// =============================================================================
// Message Structure Definitions
// =============================================================================
// These structs define the payload for each cluster RPC opcode.
// They use qkc/serialize/ tags for automatic byte-compatible serialization
// with pyquarkchain.
//
// Tag conventions:
//   bytesizeofslicelen:"4"  → 4-byte length prefix (like Python PrependedSizeListSerializer(4, ...))
//   ser:"nil"               → nullable pointer field (like Python Optional(...))
//   ser:"-"                 → ignored field (not serialized)
//
// =============================================================================

// =============================================================================
// OP_PING / OP_PONG
// =============================================================================

// PingRequest is sent by Master to initialize slave shard state.
type PingRequest struct {
	ID              []byte   `bytesizeofslicelen:"4"` // slave ID
	FullShardIDList []uint32 `bytesizeofslicelen:"4"`
	// TODO: RootTip *RootBlock `ser:"nil"`
}

// PongResponse is sent by Slave in response to PING.
type PongResponse struct {
	ID              []byte   `bytesizeofslicelen:"4"` // slave ID
	FullShardIDList []uint32 `bytesizeofslicelen:"4"`
}

// =============================================================================
// OP_CONNECT_TO_SLAVES_REQUEST / OP_CONNECT_TO_SLAVES_RESPONSE
// =============================================================================

// SlaveInfo describes a slave node for cross-shard connection setup.
type SlaveInfo struct {
	ID              []byte `bytesizeofslicelen:"4"` // slave ID
	Host            []byte `bytesizeofslicelen:"4"`
	Port            uint16
	FullShardIDList []uint32 `bytesizeofslicelen:"4"`
}

// ConnectToSlavesRequest is sent by Master to instruct a slave to connect to other slaves.
type ConnectToSlavesRequest struct {
	SlaveInfoList []SlaveInfo `bytesizeofslicelen:"4"`
}

// ConnectToSlavesResponse is sent by Slave to confirm connections.
type ConnectToSlavesResponse struct {
	ResultList [][]byte `bytesizeofslicelen:"4"`
}

// =============================================================================
// OP_ADD_MINOR_BLOCK_REQUEST / OP_ADD_MINOR_BLOCK_RESPONSE
// =============================================================================

// AddMinorBlockRequest is for adding blocks mined through JRPC.
type AddMinorBlockRequest struct {
	MinorBlockData []byte `bytesizeofslicelen:"4"`
}

// AddMinorBlockResponse is the response for AddMinorBlockRequest.
type AddMinorBlockResponse struct {
	ErrorCode uint32
}

// =============================================================================
// OP_ADD_MINOR_BLOCK_HEADER_REQUEST / OP_ADD_MINOR_BLOCK_HEADER_RESPONSE
// =============================================================================

// AddMinorBlockHeaderRequest notifies master about a successfully added minor block.
// Piggybacks ShardStats in the same request.
//
// TODO: need MinorBlockHeader、TokenBalanceMap、ShardStats
// type AddMinorBlockHeaderRequest struct {
// 	MinorBlockHeader  *MinorBlockHeader `ser:"nil"`
// 	TxCount           uint32
// 	XShardTxCount     uint32
// 	CoinbaseAmountMap TokenBalanceMap
// 	ShardStats        ShardStats
// }

// AddMinorBlockHeaderResponse is the response for AddMinorBlockHeaderRequest.
//
// TODO: need ArtificialTxConfig
// type AddMinorBlockHeaderResponse struct {
// 	ErrorCode          uint32
// 	ArtificialTxConfig ArtificialTxConfig
// }

// =============================================================================
// OP_GET_UNCONFIRMED_HEADERS_REQUEST / OP_GET_UNCONFIRMED_HEADERS_RESPONSE
// =============================================================================

// GetUnconfirmedHeadersRequest requests minor block headers to build a new root block.
type GetUnconfirmedHeadersRequest struct {
}

// =============================================================================
// OP_GET_ECO_INFO_LIST_REQUEST / OP_GET_ECO_INFO_LIST_RESPONSE
// =============================================================================

// EcoInfo contains the economic info for a shard.
type EcoInfo struct {
	Branch uint32
	// TODO: others
}
