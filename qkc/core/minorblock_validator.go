// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/trie"
)

// MinorBlockValidator checks commitments contained in a minor block and
// compares them with deterministic execution outputs. Shard and root-chain
// policy belongs to ShardCoordinator.
type MinorBlockValidator struct{}

// NewMinorBlockValidator creates a context-free minor block validator.
func NewMinorBlockValidator() *MinorBlockValidator {
	return new(MinorBlockValidator)
}

// ValidateBlock validates context-free block commitments and intrinsic limits.
// It intentionally does not query chain state. MinorBlockChain checks known
// blocks, parent and state availability, and parent hash/height continuity.
// ShardCoordinator checks shard, root-chain, configuration, and consensus rules,
// including difficulty, PoW seal, and PoSW-adjusted difficulty and seal.
func (*MinorBlockValidator) ValidateBlock(block *types.MinorBlock) error {
	if block == nil {
		return ErrUnknownBlock
	}
	if block.Version() != 0 {
		return fmt.Errorf("minor block version %d is unsupported", block.Version())
	}
	if block.NumberU64() == 0 {
		return fmt.Errorf("genesis minor block cannot be imported")
	}
	if block.MetaHash() != block.GetMetaData().Hash() {
		return fmt.Errorf("have %s want %s: invalid minor block meta root", block.GetMetaData().Hash(), block.MetaHash())
	}
	if block.GetXShardGasLimit().Cmp(block.GasLimit()) >= 0 {
		return fmt.Errorf("x-shard gas limit %s must be less than block gas limit %s", block.GetXShardGasLimit(), block.GasLimit())
	}
	if block.GasUsed().Cmp(block.GasLimit()) > 0 {
		return fmt.Errorf("gas used %s exceeds block gas limit %s", block.GasUsed(), block.GasLimit())
	}
	// The x-shard limit is soft: pyquarkchain executes the next eligible
	// deposit before checking it. Cross-shard gas may exceed that reservation,
	// but it must remain part of the block's total gas usage.
	if block.CrossShardGasUsed().Cmp(block.GasUsed()) > 0 {
		return fmt.Errorf("x-shard gas used %s exceeds total gas used %s", block.CrossShardGasUsed(), block.GasUsed())
	}
	if txRoot := types.CalculateMerkleRoot(block.Transactions()); txRoot != block.TxHash() {
		return fmt.Errorf("have %s want %s: invalid transaction root", txRoot, block.TxHash())
	}
	return nil
}

// ValidateState compares execution outputs with the commitments carried by a
// minor block. Empty accounts are deleted when deriving the state root, matching
// the state commit semantics used by minor block import.
func (*MinorBlockValidator) ValidateState(block *types.MinorBlock, statedb *state.StateDB, result *ProcessResult) error {
	if block == nil || statedb == nil || result == nil {
		return ErrInvalidExecutionResult
	}
	if !sameXShardCursor(result.XShardCursor, block.Meta().XShardTxCursorInfo) {
		return fmt.Errorf("processed x-shard cursor does not match minor block")
	}
	if root := statedb.IntermediateRoot(true); root != block.Root() {
		return fmt.Errorf("have %s want %s: %w", root, block.Root(), ErrStateRootMismatch)
	}
	if receiptRoot := types.DeriveSha(result.Receipts, trie.NewListHasher()); receiptRoot != block.ReceiptHash() {
		return fmt.Errorf("have %s want %s: %w", receiptRoot, block.ReceiptHash(), ErrReceiptRootMismatch)
	}
	if bloom := types.CreateBloom(result.Receipts); bloom != block.Bloom() {
		return fmt.Errorf("have %x want %x: invalid receipt bloom", bloom, block.Bloom())
	}
	if new(big.Int).SetUint64(result.GasUsed).Cmp(block.GasUsed()) != 0 {
		return fmt.Errorf("have %d want %s: %w", result.GasUsed, block.GasUsed(), ErrGasUsedMismatch)
	}
	if new(big.Int).SetUint64(result.XShardGasUsed).Cmp(block.CrossShardGasUsed()) != 0 {
		return fmt.Errorf("have %d want %s: %w", result.XShardGasUsed, block.CrossShardGasUsed(), ErrXShardGasUsedMismatch)
	}
	actualCoinbase := result.CoinbaseAmount.GetBalanceMap()
	expectedCoinbase := block.CoinbaseAmount().GetBalanceMap()
	if len(actualCoinbase) != len(expectedCoinbase) {
		return ErrCoinbaseAmountMismatch
	}
	for tokenID, actualBalance := range actualCoinbase {
		expectedBalance, ok := expectedCoinbase[tokenID]
		if !ok || actualBalance.Cmp(expectedBalance) != 0 {
			return ErrCoinbaseAmountMismatch
		}
	}
	return nil
}

func sameXShardCursor(left, right *types.XShardTxCursorInfo) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
