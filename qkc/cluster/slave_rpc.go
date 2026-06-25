// Copyright 2026-2027, QuarkChain.
package cluster

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// SlaveRPC is the single entry point for all cluster communication.
// It encapsulates *Slave entirely — business code never touches the raw
// protocol layer.
//
// Quick start:
//
//	rpc, err := NewSlaveRPC(&Config{
//	    MasterAddr:  "127.0.0.1:38291",
//	    OwnBranches: []uint32{0, 1},
//	    ListenAddr:  "0.0.0.0:38292",
//	})
//	if err != nil { ... }
//	defer rpc.Close()
//
//	rpc.RegisterHandlers() // inbound handlers (all modes)
//	rpc.Serve()            // blocks until fatal error
type SlaveRPC struct {
	slave *Slave
	log   log.Logger
}

// NewSlaveRPC creates, connects, and initialises the underlying Slave.
func NewSlaveRPC(cfg *Config) (*SlaveRPC, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config must not be nil")
	}
	slave, err := NewSlave(cfg)
	if err != nil {
		return nil, err
	}
	return &SlaveRPC{
		slave: slave,
		log:   slave.log.New("module", "slave-rpc"),
	}, nil
}

// =========================================================================
// INBOUND handler registration — three groups matching Python's opcode maps
// =========================================================================
//
//	Python                    Go method                  Direction
//	MASTER_OP_RPC_MAP         RegisterMasterHandlers     Master → Slave (Mode 1)
//	MASTER_OP_NONRPC_MAP      (same)                     Master → Slave, no response
//	SLAVE_OP_RPC_MAP          SetXshardHandlers           Other Slave → This Slave (Mode 2)
//	OP_NONRPC_MAP + OP_RPC_MAP  SetPeerHandlers           Peer → Master → Slave (Mode 3)
//
// RESPONSE opcodes (OP_PONG, OP_CONNECT_TO_SLAVES_RESPONSE, …) are NOT
// registered — MasterConn.Handle() auto-generates them as request+1.
// See protocol.go for the full opcode reference.

// RegisterHandlers registers all INBOUND handlers for all three modes.
// Call once before Serve().
func (s *SlaveRPC) RegisterHandlers() {

	// =====================================================================
	// Group 1: MASTER_OP_RPC_MAP + MASTER_OP_NONRPC_MAP
	//   Mode 1, Master → Slave (cluster_peer_id == 0)
	//   Arrives on MasterConn, dispatched by MasterConn.Handle().
	//   Python: cluster/slave.py  lines 715-805
	// =====================================================================

	s.slave.RegisterMasterHandlers(map[byte]MasterHandler{

		// -- Operational (implemented) --
		OP_PING:                      s.handlePing,
		OP_CONNECT_TO_SLAVES_REQUEST: s.handleConnectToSlaves,
		OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST:  s.handleCreateClusterPeerConnection,
		OP_DESTROY_CLUSTER_PEER_CONNECTION_COMMAND: s.handleDestroyClusterPeerConnection, // NON-RPC

		// -- Mining / test (stubs) --
		OP_MINE_REQUEST:   s.stub(),
		OP_GEN_TX_REQUEST: s.stub(),

		// -- Blockchain updates (stubs) --
		OP_ADD_ROOT_BLOCK_REQUEST:          s.stub(),
		OP_ADD_MINOR_BLOCK_REQUEST:         s.stub(),
		OP_SYNC_MINOR_BLOCK_LIST_REQUEST:   s.stub(),
		OP_CHECK_MINOR_BLOCK_REQUEST:       s.stub(),
		OP_GET_UNCONFIRMED_HEADERS_REQUEST: s.stub(),

		// -- Blockchain queries (stubs) --
		OP_GET_ECO_INFO_LIST_REQUEST:               s.stub(),
		OP_GET_NEXT_BLOCK_TO_MINE_REQUEST:          s.stub(),
		OP_GET_ACCOUNT_DATA_REQUEST:                s.stub(),
		OP_ADD_TRANSACTION_REQUEST:                 s.stub(),
		OP_EXECUTE_TRANSACTION_REQUEST:             s.stub(),
		OP_GET_TRANSACTION_RECEIPT_REQUEST:         s.stub(),
		OP_GET_MINOR_BLOCK_REQUEST:                 s.stub(),
		OP_GET_TRANSACTION_REQUEST:                 s.stub(),
		OP_GET_TRANSACTION_LIST_BY_ADDRESS_REQUEST: s.stub(),
		OP_GET_ALL_TRANSACTIONS_REQUEST:            s.stub(),
		OP_GET_LOG_REQUEST:                         s.stub(),
		OP_ESTIMATE_GAS_REQUEST:                    s.stub(),
		OP_GET_STORAGE_REQUEST:                     s.stub(),
		OP_GET_CODE_REQUEST:                        s.stub(),
		OP_GAS_PRICE_REQUEST:                       s.stub(),
		OP_GET_WORK_REQUEST:                        s.stub(),
		OP_SUBMIT_WORK_REQUEST:                     s.stub(),
		OP_GET_ROOT_CHAIN_STAKES_REQUEST:           s.stub(),
		OP_GET_TOTAL_BALANCE_REQUEST:               s.stub(),
	})

	// =====================================================================
	// Group 2: SLAVE_OP_RPC_MAP
	//   Mode 2, Other Slave → This Slave (direct xshard TCP)
	//   Arrives on XshardConn, dispatched by XshardConn.readLoop().
	//   Applied to every outbound and inbound XshardConn by
	//   ConnectToSlaves() / startXshardServer().
	//   Python: cluster/slave.py  lines 929-941
	// =====================================================================

	s.slave.SetXshardHandlers(map[byte]MasterHandler{
		OP_PING:                             s.handleXshardPing, // real — mutual identification
		OP_ADD_XSHARD_TX_LIST_REQUEST:       s.stub(),           // stub — needs CrossShardTransactionList
		OP_BATCH_ADD_XSHARD_TX_LIST_REQUEST: s.stub(),
	})

	// =====================================================================
	// Group 3: OP_NONRPC_MAP + OP_RPC_MAP (peer P2P commands)
	//   Mode 3, External Peer → Master → Dispatcher → PeerConn → Shard
	//   Arrives on MasterConn with cluster_peer_id != 0, dispatched by
	//   Dispatcher.Dispatch() → PeerConn.HandleFrame().
	//   Applied to every PeerConn created by HandleCreateClusterPeerConnection().
	//   Python: cluster/shard.py  lines 329-349
	// =====================================================================

	s.slave.SetPeerHandlers(map[byte]MasterHandler{
		// NON-RPC (fire-and-forget)
		OP_NEW_MINOR_BLOCK_HEADER_LIST: s.stub(), // peer notifying us about a new minor block header
		OP_NEW_TRANSACTION_LIST:        s.stub(), // peer broadcasting transactions
		OP_NEW_BLOCK_MINOR:             s.stub(), // peer announcing a new minor block
		// RPC (request → response)
		OP_GET_MINOR_BLOCK_LIST_REQUEST:                  s.stub(),
		OP_GET_MINOR_BLOCK_HEADER_LIST_REQUEST:           s.stub(),
		OP_GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST: s.stub(),
	})
}

// RegisterPeerHandler sets (or overrides) a handler for a peer-shard P2P
// CommandOp.  It applies to all existing PeerConns on the given branch,
// and persists so future PeerConns created by HandleCreateClusterPeerConnection
// also get it.
//
// Shard layer calls this once per branch at initialisation time to replace
// the stub handlers with real implementations.
func (s *SlaveRPC) RegisterPeerHandler(branch uint32, opcode byte, handler MasterHandler) error {
	return s.slave.RegisterPeerHandler(branch, opcode, handler)
}

// stub returns a handler that logs "not implemented" and returns ErrNotImplemented.
func (s *SlaveRPC) stub() MasterHandler {
	return func(frame *Frame) ([]byte, error) {
		s.log.Debug("cluster RPC not implemented", "opcode", frame.Opcode)
		return nil, ErrNotImplemented
	}
}

// ── Group 1 handler implementations: Master → Slave (Mode 1) ────────────
//
// These handle frames received on MasterConn with cluster_peer_id == 0.
// See RegisterHandlers() Group 1 for the full list.

func (s *SlaveRPC) handlePing(frame *Frame) ([]byte, error) {
	var req PingRequest
	if err := serialize.Deserialize(serialize.NewByteBuffer(frame.Payload), &req); err != nil {
		s.log.Error("failed to deserialize PingRequest", "err", err)
		return nil, err
	}
	s.log.Debug("received PING from master", "id", string(req.ID), "shards", len(req.FullShardIDList))
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

// ── Group 2 handler implementations: Other Slave → This Slave (Mode 2) ──
//
// These handle frames received on XshardConn (direct TCP to another slave).
// See RegisterHandlers() Group 2 for the full list.
//
// Group 3 handlers (Mode 3, Peer → Master → Slave) are NOT declared here.
// They are registered via SetPeerHandlers in RegisterHandlers(), applied to
// every PeerConn by HandleCreateClusterPeerConnection(), and dispatched by
// Dispatcher.Dispatch() → PeerConn.HandleFrame().

func (s *SlaveRPC) handleXshardPing(frame *Frame) ([]byte, error) {
	var req PingRequest
	if err := serialize.Deserialize(serialize.NewByteBuffer(frame.Payload), &req); err != nil {
		s.log.Error("failed to deserialize xshard PingRequest", "err", err)
		return nil, err
	}
	s.log.Debug("received xshard PING", "remote_id", string(req.ID), "remote_shards", req.FullShardIDList)
	return serialize.SerializeToBytes(&PongResponse{
		ID:              req.ID,
		FullShardIDList: req.FullShardIDList,
	})
}

// =========================================================================
// OUTBOUND — typed send methods (Shard calls these)
// =========================================================================
//
//	Python                                    Go                        Direction
//	send_minor_block_header_to_master(…)      SendMinorBlockHeaderToMaster    → Master (Mode 1)
//	broadcast_xshard_tx_list(…)               BroadcastXshardTxList          → Other Slave (Mode 2)
//	batch_broadcast_xshard_tx_list(…)         BatchBroadcastXshardTxList     → Other Slave (Mode 2)
//	send_minor_block_header_list_to_master(…) SendMinorBlockHeaderListToMaster → Master (Mode 1)
//
// broadcast_xshard_tx_list (Mode 2) and send_minor_block_header_to_master
// (Mode 1) are called together when a block is produced.  They are separate:
// xshard data goes directly Slave→Slave; master only learns about the block
// header.

// ====================================
// Mode 1: This Slave → Master
// ====================================

func (s *SlaveRPC) SendMinorBlockHeaderToMaster(ctx context.Context) error {
	return ErrNotImplemented // TODO: implement when MinorBlockHeader type is ported
}

func (s *SlaveRPC) SendMinorBlockHeaderListToMaster(ctx context.Context) error {
	return ErrNotImplemented // TODO: implement when MinorBlockHeader type is ported
}

// ====================================
// Mode 2: This Slave → Other Slave
// ====================================

func (s *SlaveRPC) BroadcastXshardTxList(ctx context.Context) error {
	return ErrNotImplemented // TODO: implement when CrossShardTransactionDeposit type is ported
}

func (s *SlaveRPC) BatchBroadcastXshardTxList(ctx context.Context) error {
	return ErrNotImplemented
}

// =========================================================================
// Lifecycle
// =========================================================================

func (s *SlaveRPC) Serve() error { return s.slave.Serve() }
func (s *SlaveRPC) Close()       { s.slave.Close() }
