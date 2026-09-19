// Copyright 2026-2027, QuarkChain.

package core

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// ConnManager propagates executed minor blocks to other shards, master and peers.
// The slave's transport and connection registries remain outside the coordinator.
type ConnManager interface {
	SendMinorBlockHeaderToMaster(block *types.MinorBlock, xShardTxCount uint32) error
	BroadcastXShardTxList(payload XShardBroadcast) error
	BroadcastNewTip(minorHeaders []*types.MinorBlockHeader, rootHeader *types.RootBlockHeader, branch uint32) error
}

// InsertOptions matches the execution layer's candidate import controls.
type InsertOptions struct {
	IsCheckDB   bool
	ForceInsert bool
}

// XShardCursor is the execution layer's view of incoming cross-shard
// deposits. Root ancestry and eligibility remain owned by ShardCoordinator.
type XShardCursor interface {
	GetNextTx() (*types.CrossShardTransactionDeposit, error)
	GetCursorInfo() *types.XShardTxCursorInfo
}

// Processor applies a minor block to its parent state and returns the
// deterministic outputs that must be validated and persisted with the block.
// The concrete block processor is supplied by a separate PR.
type Processor interface {
	Process(block *types.MinorBlock, statedb *state.StateDB, cursor XShardCursor, cfg vm.Config) (*ProcessResult, error)
}

// ProcessResult contains the values computed by Processor.Process.
type ProcessResult struct {
	Receipts       types.Receipts
	Logs           []*coretypes.Log
	GasUsed        uint64
	XShardGasUsed  uint64
	XShardCursor   *types.XShardTxCursorInfo
	CoinbaseAmount *qkccommon.TokenBalances
	OutgoingXShard []*types.CrossShardTransactionDeposit
}

// MinorBlockValidator checks context-free block commitments and compares them
// with the outputs returned by Processor. Root-chain policy remains in
// ShardCoordinator.
type MinorBlockValidator interface {
	ValidateBlock(block *types.MinorBlock) error
	ValidateState(block *types.MinorBlock, statedb *state.StateDB, result *ProcessResult) error
}

// MinorChain owns local minor block execution, persistence and canonical indexes.
// Its implementation is delivered separately from the Milestone 1 coordinator.
type MinorChain interface {
	CurrentBlock() *types.MinorBlock
	GetBlock(hash common.Hash) *types.MinorBlock
	GetBlockByNumber(number uint64) *types.MinorBlock
	InsertBlockWithXShardInput(block *types.MinorBlock, cursor XShardCursor, options InsertOptions) ([]*types.CrossShardTransactionDeposit, error)
	SetCanonicalHead(hash common.Hash) error
	Stop()
}
