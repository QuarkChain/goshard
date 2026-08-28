// Copyright 2026-2027, QuarkChain.

// Package core executes QuarkChain minor blocks: the Go counterpart of
// pyquarkchain's quarkchain/evm/messages.py and the execution half of
// quarkchain/cluster/shard_state.py.
//
// # Supported profile
//
// Both sides of the native-token fork are implemented: a shard's in-shard and
// cross-shard execution, native tokens as gas with the refund rate the general
// native token manager sets, and all five QuarkChain precompiles.
//
// One boundary is left, and it is a representation rather than a rule. An
// account holding more non-zero token balances than the account leaf's list
// encoding takes moves to a secure trie in pyquarkchain; nothing here
// implements that, so such a write is refused with ErrUnsupportedNativeToken
// and abandons the block rather than committing a state no consumer can read.
package core

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	qkcstate "github.com/ethereum/go-ethereum/qkc/state"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// ExecutionContext is everything outside the state root that decides the result
// of executing a block: the configuration, the two lookups the execution layer
// cannot do for itself, and the root tip the node validated against.
type ExecutionContext struct {
	QKCConfig   *config.QuarkChainConfig
	ShardConfig *config.ShardConfig

	// ValidationRootTip is the node's root tip at the moment it validated the
	// block, which is what __validate_tx reads for "has the target shard been
	// created" and for the neighbour test (shard_state.py:487). It is not a
	// field of the minor block and cannot be recovered from one, so a replay
	// has to supply the root block that actually confirmed this minor block —
	// never the current root head.
	ValidationRootTip *types.RootBlockHeader

	// MinorHeaderByHash walks the shard's own chain backwards: 256 headers for
	// BLOCKHASH, and a POSW window for the disallow map.
	MinorHeaderByHash func(common.Hash) (*types.MinorBlockHeader, error)

	// MinorBlockMetaByHash resolves the parent block's meta, which is where the
	// cross-shard cursor resumes from. Looking it up rather than taking the
	// cursor as a parameter keeps the caller from supplying one that belongs to
	// a different block.
	MinorBlockMetaByHash func(common.Hash) (*types.MinorBlockMeta, error)
}

// Branch is the shard this context executes for.
func (ctx *ExecutionContext) Branch() account.Branch {
	return account.NewBranch(ctx.ShardConfig.GetFullShardId())
}

// FullShardID is the shard's id.
func (ctx *ExecutionContext) FullShardID() uint32 {
	return ctx.ShardConfig.GetFullShardId()
}

// XShardSource is the root-chain and remote-shard data the cursor traversal
// reads. It belongs to the chain layer; the execution layer takes it as a seam
// so a block can be executed against fixtures.
type XShardSource interface {
	// RootBlockByHeight resolves a height along the chain ending at tip. The
	// traversal is fork-aware, so a flat height index is not enough.
	RootBlockByHeight(tip common.Hash, height uint64) (*types.RootBlock, error)

	// RootHeaderByHash resolves a root header by hash. The traversal needs it
	// to turn a remote minor header's hash_prev_root_block into a height
	// (shard_state.py:172), a query RootBlockByHeight cannot answer.
	RootHeaderByHash(hash common.Hash) (*types.RootBlockHeader, error)

	// DepositsByMinorBlockHash returns the cross-shard deposits a remote minor
	// block sent, keyed by that block's hash. found reports whether the list
	// exists at all: pyquarkchain distinguishes an absent list from an empty
	// one and checks for each in a different place, so a nil slice cannot
	// stand in for both.
	DepositsByMinorBlockHash(hash common.Hash) (deposits []*types.CrossShardTransactionDeposit, found bool, err error)
}

// ProcessResult is the complete consensus output of executing one block.
// Callers read it rather than inspecting a half-finished state.
type ProcessResult struct {
	State     *qkcstate.EvmState
	StateRoot common.Hash

	// Receipts and XShardDepositReceipts stay apart: the receipt root is the
	// two concatenated in this order (shard_state.py:957).
	Receipts              types.Receipts
	XShardDepositReceipts types.Receipts
	ReceiptRoot           common.Hash

	GasUsed              uint64
	XShardReceiveGasUsed uint64
	Cursor               types.XShardTxCursorInfo
	CoinbaseAmountMap    map[uint64]*big.Int
	Bloom                types.Bloom

	// ProducedDeposits is what this block sends to other shards, and
	// ConsumedDeposits what it received — the latter is what the chain layer
	// indexes to answer receipt queries for cross-shard transactions.
	ProducedDeposits []*types.CrossShardTransactionDeposit
	ConsumedDeposits []*types.CrossShardTransactionDeposit
}
