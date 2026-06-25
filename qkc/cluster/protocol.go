// Package cluster implements the pyquarkchain-compatible cluster protocol wire
// library: frame codec, TCP connection management, and VirtualConnection multiplexing.
//
// Opcode ranges (matching pyquarkchain wire values):
//
//	0x00 - 0x14  CommandOp   (peer-shard P2P, cluster_peer_id != 0)
//	0x81 - 0xC4  ClusterOp   (master-slave + xshard, cluster_peer_id == 0)
//	CLUSTER_OP_BASE = 128 in pyquarkchain rpc.py
//
// # REQUEST vs RESPONSE opcodes
//
// Every ClusterOp comes in REQUEST/RESPONSE pairs:  RESPONSE = REQUEST + 1.
//
//	OP_PING     = 0x81  (REQUEST — needs handler registration)
//	OP_PONG     = 0x82  (RESPONSE — auto-generated, no handler needed)
//
// MasterConn.Handle() auto-generates the response opcode:
//
//	resp.Opcode = frame.Opcode + 1
//
// The RESPONSE constants exist for documentation, type-checking, and
// response-matching on the sender side.  They are NOT registered as
// handlers — only REQUEST opcodes need handler registration.
//
// # Handler registration map
//
// Who handles which opcodes:
//
//	ClusterOp  REQUEST        →  master→slave    → RegisterMasterHandlers()  [30 opcodes]
//	ClusterOp  REQUEST        →  slave↔slave     → SetXshardHandlers()       [3 opcodes]
//	CommandOp  (6 opcodes)    →  peer→master→slave → SetPeerHandlers()       [6 opcodes]
//
// The remaining CommandOp codes (HELLO, GET_PEER_LIST_REQUEST,
// GET_ROOT_BLOCK_HEADER_LIST_REQUEST, PING_P2P, PONG_P2P, NEW_ROOT_BLOCK,
// GET_ROOT_BLOCK_LIST_REQUEST, GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST)
// are handled by the Python Master's Peer class (simple_network.py:362-388)
// for inter-cluster P2P communication.  They never reach the Go slave.
//
//	Master (Peer) handles:  HELLO, GET_PEER_LIST, GET_ROOT_BLOCK_*, PING_P2P, NEW_ROOT_BLOCK
//	Master forwards to Slaves:  NEW_MINOR_BLOCK_HEADER_LIST, NEW_TRANSACTION_LIST, NEW_BLOCK_MINOR
//	                                 + 3 GET_MINOR_BLOCK_* RPC requests
package cluster

import "errors"

// =============================================================================
// ClusterOp codes (master-slave communication, cluster_peer_id == 0)
// Wire values: 0x81 - 0xC4 (128 + 1 .. 128 + 68)
// =============================================================================
const (
	// --- Master → Slave (inbound requests) ---
	OP_PING                             = 0x81 // ClusterOp.PING = 1 + 128
	OP_PONG                             = 0x82 // ClusterOp.PONG = 2 + 128
	OP_CONNECT_TO_SLAVES_REQUEST        = 0x83 // 3 + 128
	OP_CONNECT_TO_SLAVES_RESPONSE       = 0x84 // 4 + 128
	OP_ADD_ROOT_BLOCK_REQUEST           = 0x85 // 5 + 128
	OP_ADD_ROOT_BLOCK_RESPONSE          = 0x86 // 6 + 128
	OP_GET_ECO_INFO_LIST_REQUEST        = 0x87 // 7 + 128
	OP_GET_ECO_INFO_LIST_RESPONSE       = 0x88 // 8 + 128
	OP_GET_NEXT_BLOCK_TO_MINE_REQUEST   = 0x89 // 9 + 128
	OP_GET_NEXT_BLOCK_TO_MINE_RESPONSE  = 0x8A // 10 + 128
	OP_GET_UNCONFIRMED_HEADERS_REQUEST  = 0x8B // 11 + 128
	OP_GET_UNCONFIRMED_HEADERS_RESPONSE = 0x8C // 12 + 128
	OP_GET_ACCOUNT_DATA_REQUEST         = 0x8D // 13 + 128
	OP_GET_ACCOUNT_DATA_RESPONSE        = 0x8E // 14 + 128
	OP_ADD_TRANSACTION_REQUEST          = 0x8F // 15 + 128
	OP_ADD_TRANSACTION_RESPONSE         = 0x90 // 16 + 128

	// --- Slave → Master (outbound requests) ---
	OP_ADD_MINOR_BLOCK_HEADER_REQUEST  = 0x91 // 17 + 128
	OP_ADD_MINOR_BLOCK_HEADER_RESPONSE = 0x92 // 18 + 128

	// --- Slave ↔ Slave (xshard, direct TCP) ---
	OP_ADD_XSHARD_TX_LIST_REQUEST  = 0x93 // 19 + 128
	OP_ADD_XSHARD_TX_LIST_RESPONSE = 0x94 // 20 + 128

	// --- Master → Slave ---
	OP_SYNC_MINOR_BLOCK_LIST_REQUEST           = 0x95 // 21 + 128
	OP_SYNC_MINOR_BLOCK_LIST_RESPONSE          = 0x96 // 22 + 128
	OP_ADD_MINOR_BLOCK_REQUEST                 = 0x97 // 23 + 128
	OP_ADD_MINOR_BLOCK_RESPONSE                = 0x98 // 24 + 128
	OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST  = 0x99 // 25 + 128
	OP_CREATE_CLUSTER_PEER_CONNECTION_RESPONSE = 0x9A // 26 + 128
	OP_DESTROY_CLUSTER_PEER_CONNECTION_COMMAND = 0x9B // 27 + 128 (NON-RPC)

	// 28 is skipped (no opcode)

	OP_GET_MINOR_BLOCK_REQUEST  = 0x9D // 29 + 128
	OP_GET_MINOR_BLOCK_RESPONSE = 0x9E // 30 + 128
	OP_GET_TRANSACTION_REQUEST  = 0x9F // 31 + 128
	OP_GET_TRANSACTION_RESPONSE = 0xA0 // 32 + 128

	// --- Slave ↔ Slave (xshard) ---
	OP_BATCH_ADD_XSHARD_TX_LIST_REQUEST  = 0xA1 // 33 + 128
	OP_BATCH_ADD_XSHARD_TX_LIST_RESPONSE = 0xA2 // 34 + 128

	// --- Master → Slave ---
	OP_EXECUTE_TRANSACTION_REQUEST              = 0xA3 // 35 + 128
	OP_EXECUTE_TRANSACTION_RESPONSE             = 0xA4 // 36 + 128
	OP_GET_TRANSACTION_RECEIPT_REQUEST          = 0xA5 // 37 + 128
	OP_GET_TRANSACTION_RECEIPT_RESPONSE         = 0xA6 // 38 + 128
	OP_MINE_REQUEST                             = 0xA7 // 39 + 128
	OP_MINE_RESPONSE                            = 0xA8 // 40 + 128
	OP_GEN_TX_REQUEST                           = 0xA9 // 41 + 128
	OP_GEN_TX_RESPONSE                          = 0xAA // 42 + 128
	OP_GET_TRANSACTION_LIST_BY_ADDRESS_REQUEST  = 0xAB // 43 + 128
	OP_GET_TRANSACTION_LIST_BY_ADDRESS_RESPONSE = 0xAC // 44 + 128
	OP_GET_LOG_REQUEST                          = 0xAD // 45 + 128
	OP_GET_LOG_RESPONSE                         = 0xAE // 46 + 128
	OP_ESTIMATE_GAS_REQUEST                     = 0xAF // 47 + 128
	OP_ESTIMATE_GAS_RESPONSE                    = 0xB0 // 48 + 128
	OP_GET_STORAGE_REQUEST                      = 0xB1 // 49 + 128
	OP_GET_STORAGE_RESPONSE                     = 0xB2 // 50 + 128
	OP_GET_CODE_REQUEST                         = 0xB3 // 51 + 128
	OP_GET_CODE_RESPONSE                        = 0xB4 // 52 + 128
	OP_GAS_PRICE_REQUEST                        = 0xB5 // 53 + 128
	OP_GAS_PRICE_RESPONSE                       = 0xB6 // 54 + 128
	OP_GET_WORK_REQUEST                         = 0xB7 // 55 + 128
	OP_GET_WORK_RESPONSE                        = 0xB8 // 56 + 128
	OP_SUBMIT_WORK_REQUEST                      = 0xB9 // 57 + 128
	OP_SUBMIT_WORK_RESPONSE                     = 0xBA // 58 + 128

	// --- Slave → Master (outbound) ---
	OP_ADD_MINOR_BLOCK_HEADER_LIST_REQUEST  = 0xBB // 59 + 128
	OP_ADD_MINOR_BLOCK_HEADER_LIST_RESPONSE = 0xBC // 60 + 128

	// --- Master → Slave ---
	OP_CHECK_MINOR_BLOCK_REQUEST      = 0xBD // 61 + 128
	OP_CHECK_MINOR_BLOCK_RESPONSE     = 0xBE // 62 + 128
	OP_GET_ALL_TRANSACTIONS_REQUEST   = 0xBF // 63 + 128
	OP_GET_ALL_TRANSACTIONS_RESPONSE  = 0xC0 // 64 + 128
	OP_GET_ROOT_CHAIN_STAKES_REQUEST  = 0xC1 // 65 + 128
	OP_GET_ROOT_CHAIN_STAKES_RESPONSE = 0xC2 // 66 + 128
	OP_GET_TOTAL_BALANCE_REQUEST      = 0xC3 // 67 + 128
	OP_GET_TOTAL_BALANCE_RESPONSE     = 0xC4 // 68 + 128
)

// =============================================================================
// CommandOp codes (peer-shard P2P, cluster_peer_id != 0)
// Wire values: 0x00 - 0x14
//
// OWNERSHIP:
//
//	Master-only (handled by Python Master Peer, simple_network.py):
//	  HELLO, GET_PEER_LIST, GET_ROOT_BLOCK_*, PING_P2P, PONG_P2P, NEW_ROOT_BLOCK
//	Master forwards → Slave (handled by Go Slave's PeerConn):
//	  NEW_MINOR_BLOCK_HEADER_LIST, NEW_TRANSACTION_LIST, NEW_BLOCK_MINOR
//	  GET_MINOR_BLOCK_LIST_REQUEST, GET_MINOR_BLOCK_HEADER_LIST_REQUEST,
//	  GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST
//
// =============================================================================
const (
	// ── Master-only (never forwarded to Slave) ──
	OP_HELLO = 0x00 // CommandOp.HELLO

	// ── Master → Slave (forwarded, NON-RPC) ──
	OP_NEW_MINOR_BLOCK_HEADER_LIST = 0x01 // CommandOp.NEW_MINOR_BLOCK_HEADER_LIST (NON-RPC)
	OP_NEW_TRANSACTION_LIST        = 0x02 // CommandOp.NEW_TRANSACTION_LIST (NON-RPC)

	// ── Master-only ──
	OP_GET_PEER_LIST_REQUEST               = 0x03 // CommandOp.GET_PEER_LIST_REQUEST
	OP_GET_PEER_LIST_RESPONSE              = 0x04 // CommandOp.GET_PEER_LIST_RESPONSE
	OP_GET_ROOT_BLOCK_HEADER_LIST_REQUEST  = 0x05
	OP_GET_ROOT_BLOCK_HEADER_LIST_RESPONSE = 0x06
	OP_GET_ROOT_BLOCK_LIST_REQUEST         = 0x07
	OP_GET_ROOT_BLOCK_LIST_RESPONSE        = 0x08

	// ── Master → Slave (forwarded, RPC) ──
	OP_GET_MINOR_BLOCK_LIST_REQUEST         = 0x09
	OP_GET_MINOR_BLOCK_LIST_RESPONSE        = 0x0A
	OP_GET_MINOR_BLOCK_HEADER_LIST_REQUEST  = 0x0B
	OP_GET_MINOR_BLOCK_HEADER_LIST_RESPONSE = 0x0C

	// ── Master → Slave (forwarded, NON-RPC) ──
	OP_NEW_BLOCK_MINOR = 0x0D // CommandOp.NEW_BLOCK_MINOR (NON-RPC)

	// ── Master-only ──
	OP_PING_P2P                                      = 0x0E // CommandOp.PING
	OP_PONG_P2P                                      = 0x0F // CommandOp.PONG
	OP_GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST  = 0x10
	OP_GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_RESPONSE = 0x11
	OP_NEW_ROOT_BLOCK                                = 0x12 // NON-RPC

	// ── Master → Slave (forwarded, RPC) ──
	OP_GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST  = 0x13
	OP_GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_RESPONSE = 0x14
)

// Errors
var (
	ErrConnectionClosed = errors.New("connection closed")
	ErrNotImplemented   = errors.New("not implemented")
)

// ClusterError is a cluster protocol error.
type ClusterError struct {
	msg string
}

func (e *ClusterError) Error() string {
	return e.msg
}
