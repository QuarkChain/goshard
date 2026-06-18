// Copyright 2026-2027, QuarkChain.
package cluster

import (
	"context"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// SlaveRPC is the typed, business-level adapter for cluster communication.
// It wraps Slave (protocol-level connection manager) and mirrors Python's
// SlaveServer class in cluster/slave.py.
//
// Two directions:
//
//	INBOUND  —  handlers:  Master/Slave → this Slave  (Mode 1, 2)
//	OUTBOUND —  send methods:  this Slave → Master/other Slaves  (Mode 1, 2)
//
// The Shard layer should use SlaveRPC exclusively; never touch Slave directly.
//
// Quick start:
//
//	s, _ := NewSlave(&Config{
//	    MasterAddr:  "127.0.0.1:38291",
//	    OwnBranches: []uint32{0, 1},
//	    ListenAddr:  "0.0.0.0:38292",
//	})
//	defer s.Close()
//
//	rpc := NewSlaveRPC(s)
//	rpc.RegisterHandlers() // inbound handlers (all modes)
//	go rpc.Serve()         // blocks until fatal error
//
//	// Shard calls outbound methods:
//	rpc.SendMinorBlockHeaderToMaster(ctx, params)
//	rpc.BroadcastXshardTxList(ctx, txList)
type SlaveRPC struct {
	slave *Slave
	log   log.Logger
}

func NewSlaveRPC(slave *Slave) *SlaveRPC {
	return &SlaveRPC{
		slave: slave,
		log:   log.New("module", "slave-rpc"),
	}
}

func (s *SlaveRPC) Slave() *Slave { return s.slave }

// =========================================================================
// Side A — INBOUND: Handler registration
// =========================================================================
//
// What Master (or another Slave) sends to THIS slave.
//
// Python equivalents:
//   MASTER_OP_RPC_MAP     —  master → slave (mode 1)
//   MASTER_OP_NONRPC_MAP  —  master → slave, fire-and-forget (mode 1)
//   SLAVE_OP_RPC_MAP      —  other slave → this slave (mode 2)
//
// # Mode mapping
//
//	Mode 1  Master → Slave        →  handles OP_PING, OP_ADD_ROOT_BLOCK_REQUEST, …
//	Mode 2  Slave → Slave         →  handles OP_PING (xshard), OP_ADD_XSHARD_TX_LIST_REQUEST
//	Mode 3  Peer → Master → Slave →  NOT registered here; per-PeerConn via RegisterPeerHandler()

// RegisterHandlers registers all INBOUND handlers (master commands + xshard + peer).
// Call once before Serve().
func (s *SlaveRPC) RegisterHandlers() {
	// —— Master → Slave (MASTER_OP_RPC_MAP + MASTER_OP_NONRPC_MAP) ——
	s.slave.RegisterMasterHandlers(map[byte]MasterHandler{
		OP_PING:                      s.handlePing,
		OP_CONNECT_TO_SLAVES_REQUEST: s.handleConnectToSlaves,
		OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST:  s.handleCreateClusterPeerConnection,
		OP_DESTROY_CLUSTER_PEER_CONNECTION_COMMAND: s.handleDestroyClusterPeerConnection,

		OP_MINE_REQUEST:   s.stub("MINE_REQUEST"),
		OP_GEN_TX_REQUEST: s.stub("GEN_TX_REQUEST"),

		OP_ADD_ROOT_BLOCK_REQUEST:          s.stub("ADD_ROOT_BLOCK_REQUEST"),
		OP_ADD_MINOR_BLOCK_REQUEST:         s.stub("ADD_MINOR_BLOCK_REQUEST"),
		OP_SYNC_MINOR_BLOCK_LIST_REQUEST:   s.stub("SYNC_MINOR_BLOCK_LIST_REQUEST"),
		OP_CHECK_MINOR_BLOCK_REQUEST:       s.stub("CHECK_MINOR_BLOCK_REQUEST"),
		OP_GET_UNCONFIRMED_HEADERS_REQUEST: s.stub("GET_UNCONFIRMED_HEADERS_REQUEST"),

		OP_GET_ECO_INFO_LIST_REQUEST:               s.stub("GET_ECO_INFO_LIST_REQUEST"),
		OP_GET_NEXT_BLOCK_TO_MINE_REQUEST:          s.stub("GET_NEXT_BLOCK_TO_MINE_REQUEST"),
		OP_GET_ACCOUNT_DATA_REQUEST:                s.stub("GET_ACCOUNT_DATA_REQUEST"),
		OP_ADD_TRANSACTION_REQUEST:                 s.stub("ADD_TRANSACTION_REQUEST"),
		OP_EXECUTE_TRANSACTION_REQUEST:             s.stub("EXECUTE_TRANSACTION_REQUEST"),
		OP_GET_TRANSACTION_RECEIPT_REQUEST:         s.stub("GET_TRANSACTION_RECEIPT_REQUEST"),
		OP_GET_MINOR_BLOCK_REQUEST:                 s.stub("GET_MINOR_BLOCK_REQUEST"),
		OP_GET_TRANSACTION_REQUEST:                 s.stub("GET_TRANSACTION_REQUEST"),
		OP_GET_TRANSACTION_LIST_BY_ADDRESS_REQUEST: s.stub("GET_TRANSACTION_LIST_BY_ADDRESS_REQUEST"),
		OP_GET_ALL_TRANSACTIONS_REQUEST:            s.stub("GET_ALL_TRANSACTIONS_REQUEST"),
		OP_GET_LOG_REQUEST:                         s.stub("GET_LOG_REQUEST"),
		OP_ESTIMATE_GAS_REQUEST:                    s.stub("ESTIMATE_GAS_REQUEST"),
		OP_GET_STORAGE_REQUEST:                     s.stub("GET_STORAGE_REQUEST"),
		OP_GET_CODE_REQUEST:                        s.stub("GET_CODE_REQUEST"),
		OP_GAS_PRICE_REQUEST:                       s.stub("GAS_PRICE_REQUEST"),
		OP_GET_WORK_REQUEST:                        s.stub("GET_WORK_REQUEST"),
		OP_SUBMIT_WORK_REQUEST:                     s.stub("SUBMIT_WORK_REQUEST"),
		OP_GET_ROOT_CHAIN_STAKES_REQUEST:           s.stub("GET_ROOT_CHAIN_STAKES_REQUEST"),
		OP_GET_TOTAL_BALANCE_REQUEST:               s.stub("GET_TOTAL_BALANCE_REQUEST"),
	})

	// —— Other Slave → This Slave (SLAVE_OP_RPC_MAP, mode 2) ——
	s.RegisterXshardHandlers()

	// —— Peer → Master → Slave (CommandOp, mode 3) ——
	s.RegisterPeerHandlers()
}

// RegisterPeerHandlers registers the peer P2P command handlers (mode 3).
//
// These handle CommandOp messages that originate from external P2P peers,
// are forwarded by Master through the cluster_peer_id multiplexing mechanism,
// and arrive on PeerConn (not MasterConn).
//
// Python equivalents: OP_NONRPC_MAP + OP_RPC_MAP in cluster/shard.py
//
//	CommandOp.NEW_MINOR_BLOCK_HEADER_LIST            → stub
//	CommandOp.NEW_TRANSACTION_LIST                   → stub
//	CommandOp.NEW_BLOCK_MINOR                        → stub
//	CommandOp.GET_MINOR_BLOCK_LIST_REQUEST           → stub
//	CommandOp.GET_MINOR_BLOCK_HEADER_LIST_REQUEST    → stub
//	CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST → stub
func (s *SlaveRPC) RegisterPeerHandlers() {
	s.slave.SetPeerHandlers(map[byte]MasterHandler{
		OP_NEW_MINOR_BLOCK_HEADER_LIST:                   s.stub("NEW_MINOR_BLOCK_HEADER_LIST"),
		OP_NEW_TRANSACTION_LIST:                          s.stub("NEW_TRANSACTION_LIST"),
		OP_NEW_BLOCK_MINOR:                               s.stub("NEW_BLOCK_MINOR"),
		OP_GET_MINOR_BLOCK_LIST_REQUEST:                  s.stub("GET_MINOR_BLOCK_LIST_REQUEST"),
		OP_GET_MINOR_BLOCK_HEADER_LIST_REQUEST:           s.stub("GET_MINOR_BLOCK_HEADER_LIST_REQUEST"),
		OP_GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST: s.stub("GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST"),
	})
}

// RegisterXshardHandlers registers only the SLAVE_OP_RPC_MAP handlers (mode 2).
// Called automatically by RegisterHandlers().  Exported separately for tests
// that only need xshard communication without a full master connection.
func (s *SlaveRPC) RegisterXshardHandlers() {
	s.slave.SetXshardHandlers(map[byte]MasterHandler{
		OP_PING:                             s.handleXshardPing,
		OP_ADD_XSHARD_TX_LIST_REQUEST:       s.stub("ADD_XSHARD_TX_LIST_REQUEST"),
		OP_BATCH_ADD_XSHARD_TX_LIST_REQUEST: s.stub("BATCH_ADD_XSHARD_TX_LIST_REQUEST"),
	})
}

// stub returns a handler that logs "not implemented" and returns ErrNotImplemented.
func (s *SlaveRPC) stub(name string) MasterHandler {
	return func(frame *Frame) ([]byte, error) {
		s.log.Debug("cluster RPC not implemented", "op", name, "opcode", frame.Opcode)
		return nil, ErrNotImplemented
	}
}

// ── Master → Slave handlers ───────────────────────────────────────────

func (s *SlaveRPC) handlePing(frame *Frame) ([]byte, error) {
	var req PingRequest
	if err := serialize.Deserialize(serialize.NewByteBuffer(frame.Payload), &req); err != nil {
		s.log.Error("failed to deserialize PingRequest", "err", err)
		return nil, err
	}
	s.log.Debug("received PING from master", "id", string(req.ID), "shards", len(req.FullShardIDList))
	// TODO: when Shard exists, call create_shards(ping.root_tip)
	return serialize.SerializeToBytes(&PongResponse{
		ID:              req.ID,
		FullShardIDList: req.FullShardIDList,
	})
}

func (s *SlaveRPC) handleConnectToSlaves(frame *Frame) ([]byte, error) {
	s.log.Debug("received CONNECT_TO_SLAVES_REQUEST")
	return s.slave.ConnectToSlaves(frame.Payload)
}

func (s *SlaveRPC) handleCreateClusterPeerConnection(frame *Frame) ([]byte, error) {
	s.log.Debug("received CREATE_CLUSTER_PEER_CONNECTION_REQUEST", "id", frame.Meta.ClusterPeerID)
	return s.slave.HandleCreateClusterPeerConnection(frame)
}

func (s *SlaveRPC) handleDestroyClusterPeerConnection(frame *Frame) ([]byte, error) {
	s.log.Debug("received DESTROY_CLUSTER_PEER_CONNECTION_COMMAND", "id", frame.Meta.ClusterPeerID)
	return s.slave.HandleDestroyClusterPeerConnection(frame)
}

// ── Other Slave → This Slave handlers (mode 2, xshard TCP) ────────────

// handleXshardPing handles OP_PING from another slave on a direct xshard
// connection.  Used for mutual identification after TCP connect.
func (s *SlaveRPC) handleXshardPing(frame *Frame) ([]byte, error) {
	var req PingRequest
	if err := serialize.Deserialize(serialize.NewByteBuffer(frame.Payload), &req); err != nil {
		s.log.Error("failed to deserialize xshard PingRequest", "err", err)
		return nil, err
	}
	s.log.Debug("received xshard PING", "remote_id", string(req.ID), "remote_shards", req.FullShardIDList)
	// TODO: index connection in pool by fullShardID (Python: SlaveConnectionManager._add_slave_connection)
	return serialize.SerializeToBytes(&PongResponse{
		ID:              req.ID,
		FullShardIDList: req.FullShardIDList,
	})
}

// =========================================================================
// Side B — OUTBOUND: Typed send methods (Shard → SlaveRPC → wire)
// =========================================================================
//
// These are the methods that Shard calls.  Python equivalents (SlaveServer):
//
//	Python                                           Go                        Direction
//	send_minor_block_header_to_master(…)             SendMinorBlockHeaderToMaster    → Master (mode 1)
//	broadcast_xshard_tx_list(…)                      BroadcastXshardTxList          → other Slave (mode 2)
//	batch_broadcast_xshard_tx_list(…)                BatchBroadcastXshardTxList     → other Slave (mode 2)
//	send_minor_block_header_list_to_master(…)        SendMinorBlockHeaderListToMaster → Master (mode 1)
//
// In Python, broadcast_xshard_tx_list (mode 2) and send_minor_block_header_to_master
// (mode 1) are always called together when a block is produced.  They are separate
// operations: cross-shard tx data goes directly Slave→Slave; master gets notified
// about the block header (not the xshard tx content).

// ====================================
// Mode 1: This Slave → Master
// ====================================

// SendMinorBlockHeaderToMaster notifies master that a minor block was appended.
//
// Python: SlaveServer.send_minor_block_header_to_master().
// Sent via master RPC (MasterConn.SendRPC), not xshard.
func (s *SlaveRPC) SendMinorBlockHeaderToMaster(ctx context.Context) error {
	return ErrNotImplemented // TODO: implement when MinorBlockHeader type is ported
}

// SendMinorBlockHeaderListToMaster notifies master about a batch of minor blocks.
//
// Python: SlaveServer.send_minor_block_header_list_to_master().
// Sent via master RPC (MasterConn.SendRPC), not xshard.
func (s *SlaveRPC) SendMinorBlockHeaderListToMaster(ctx context.Context) error {
	return ErrNotImplemented // TODO: implement when MinorBlockHeader type is ported
}

// ====================================
// Mode 2: This Slave → Other Slave
// ====================================

// BroadcastXshardTxList sends cross-shard transaction deposits to the slaves
// that own the target shards.
//
// Python: SlaveServer.broadcast_xshard_tx_list().
//
// This is mode 2 (Slave↔Slave direct TCP).  Master is NOT involved — xshard
// data goes directly through XshardConn.  Master only learns about the
// resulting block later via SendMinorBlockHeaderToMaster (a separate call).
//
// Internal flow (matching Python):
//  1. Group transactions by target fullShardID
//  2. Local shards  →  ShardState.add_cross_shard_tx_list_by_minor_block_hash()
//  3. Remote shards →  XshardConn via xshardPool.SendRPC()
//
// TODO: implement when CrossShardTransactionDeposit, MinorBlockHeader types ported.
func (s *SlaveRPC) BroadcastXshardTxList(ctx context.Context) error {
	return ErrNotImplemented
}

// BatchBroadcastXshardTxList sends batches of cross-shard transactions from
// multiple blocks to the target shards' slaves.
//
// Python: SlaveServer.batch_broadcast_xshard_tx_list().
// Same mode 2 as BroadcastXshardTxList, but optimized for multiple blocks.
//
// TODO: implement when business types are ported.
func (s *SlaveRPC) BatchBroadcastXshardTxList(ctx context.Context) error {
	return ErrNotImplemented
}

// =========================================================================
// Lifecycle
// =========================================================================

// Serve starts the slave and blocks until a fatal error occurs.
func (s *SlaveRPC) Serve() error { return s.slave.Serve() }

// Close shuts down all connections.
func (s *SlaveRPC) Close() { s.slave.Close() }
