// Copyright 2026-2027, QuarkChain.

package core

import (
	"github.com/ethereum/go-ethereum/common"
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

// XShardDepositCursor is the execution layer's view of incoming cross-shard
// deposits. Root ancestry and eligibility remain owned by ShardCoordinator.
type XShardDepositCursor interface {
	GetNextTx() (*types.CrossShardTransactionDeposit, error)
	GetCursorInfo() *types.XShardTxCursorInfo
}

// MinorChain owns local minor block execution, persistence and canonical indexes.
// Its implementation is delivered separately from the Milestone 1 coordinator.
type MinorChain interface {
	CurrentBlock() *types.MinorBlock
	GetBlock(hash common.Hash) *types.MinorBlock
	GetBlockByNumber(number uint64) *types.MinorBlock
	InsertBlockWithXShardInput(block *types.MinorBlock, cursor XShardDepositCursor, options InsertOptions) ([]*types.CrossShardTransactionDeposit, error)
	SetCanonicalHead(hash common.Hash) error
	Stop()
}
