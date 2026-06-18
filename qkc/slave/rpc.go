// Copyright 2026-2027, QuarkChain.
//
// Package slave provides the business-level adapter for cluster communication.
// It wraps the low-level cluster protocol (qkc/cluster) with typed, business-oriented
// methods, so the rest of the codebase never calls SendRPCToMaster directly.
package slave

import (
	"context"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster"
)

// SlaveRPC wraps cluster.Slave to provide typed business methods for
// master-slave and peer-shard communication.
//
// This is the Go equivalent of Python's SlaveServer class, specifically
// the methods it exposes to Shard for cluster communication.
type SlaveRPC struct {
	slave *cluster.Slave
	log   log.Logger
}

// NewSlaveRPC creates a new SlaveRPC wrapping the given cluster.Slave.
func NewSlaveRPC(slave *cluster.Slave) *SlaveRPC {
	return &SlaveRPC{
		slave: slave,
		log:   log.New("module", "slave-rpc"),
	}
}

// Slave returns the underlying cluster.Slave.
func (s *SlaveRPC) Slave() *cluster.Slave {
	return s.slave
}

// RegisterHandlers registers all Master cluster RPC handlers.
// This is the Go equivalent of Python's CLUSTER_OP_RPC_MAP / CLUSTER_OP_NONRPC_MAP,
// which maps opcodes to handler methods in MasterConnection.
func (s *SlaveRPC) RegisterHandlers() {
	handlers := map[byte]cluster.MasterHandler{
		cluster.OP_PING:                                    s.handlePing,
		cluster.OP_CONNECT_TO_SLAVES_REQUEST:               s.handleConnectToSlaves,
		cluster.OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST:  s.handleCreateClusterPeerConnection,
		cluster.OP_DESTROY_CLUSTER_PEER_CONNECTION_COMMAND: s.handleDestroyClusterPeerConnection,

		// TODO: others handler
		// cluster.OP_ADD_ROOT_BLOCK_REQUEST:           s.handleAddRootBlock,
		// cluster.OP_GET_ECO_INFO_LIST_REQUEST:        s.handleGetEcoInfoList,
		// cluster.OP_GET_NEXT_BLOCK_TO_MINE_REQUEST:   s.handleGetNextBlockToMine,
		// ... 68
	}
	s.slave.RegisterMasterHandlers(handlers)
}

// ── Default handlers ─────────────────────────────────────────────────────

func (s *SlaveRPC) handlePing(frame *cluster.Frame) ([]byte, error) {
	s.log.Debug("received PING from master")
	return []byte("PONG"), nil
}

func (s *SlaveRPC) handleConnectToSlaves(frame *cluster.Frame) ([]byte, error) {
	s.log.Debug("received CONNECT_TO_SLAVES_REQUEST")
	// TODO: frame.Payload
	return []byte("OK"), nil
}

func (s *SlaveRPC) handleCreateClusterPeerConnection(frame *cluster.Frame) ([]byte, error) {
	s.log.Debug("received CREATE_CLUSTER_PEER_CONNECTION_REQUEST",
		"cluster_peer_id", frame.Meta.ClusterPeerID)
	return s.slave.HandleCreateClusterPeerConnection(frame)
}

func (s *SlaveRPC) handleDestroyClusterPeerConnection(frame *cluster.Frame) ([]byte, error) {
	s.log.Debug("received DESTROY_CLUSTER_PEER_CONNECTION_COMMAND",
		"cluster_peer_id", frame.Meta.ClusterPeerID)
	return s.slave.HandleDestroyClusterPeerConnection(frame)
}

// ── Business-level send methods (Slave → Master) ─────────────────────────

func (s *SlaveRPC) SendMinorBlockHeaderToMaster(
	ctx context.Context,
	// minorBlockHeader *types.MinorBlockHeader,
	// txCount uint32,
	// xShardTxCount uint32,
	// coinbaseAmountMap map[string]*big.Int,
	// shardStats *types.ShardStats,
) error {
	return nil
}

// BroadcastXshardTxList broadcasts cross-shard transactions to target shards.
// This is the Go equivalent of Python's:
//
//	SlaveServer.broadcast_xshard_tx_list()
//
// It routes transactions:
//   - Local shards → direct handling
//   - Remote shards → via XshardConn (direct TCP, not through master)
//
// TODO:
func (s *SlaveRPC) BroadcastXshardTxList(
// block *types.MinorBlock,
// xshardTxList []*types.CrossShardTransactionDeposit,
// prevRootHeight uint32,
) error {
	return nil
}

// ── Lifecycle ────────────────────────────────────────────────────────────

// Serve starts the slave and blocks until a fatal error occurs.
func (s *SlaveRPC) Serve() error {
	return s.slave.Serve()
}

// Close shuts down the slave and all connections.
func (s *SlaveRPC) Close() {
	s.slave.Close()
}
