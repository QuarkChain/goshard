// Copyright 2026-2027, QuarkChain.

package core

import (
	"bytes"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/trie"
)

// BasicMinorBlockValidator checks commitments contained in a minor block and
// compares them with deterministic execution outputs. Shard and root-chain
// policy belongs to ShardCoordinator.
type BasicMinorBlockValidator struct{}

// NewBasicMinorBlockValidator creates a context-free minor block validator.
func NewBasicMinorBlockValidator() *BasicMinorBlockValidator {
	return new(BasicMinorBlockValidator)
}

// ValidateBlock checks commitments and gas bounds that do not require parent,
// shard, root-chain, or consensus-engine context.
func (*BasicMinorBlockValidator) ValidateBlock(block *types.MinorBlock) error {
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
func (*BasicMinorBlockValidator) ValidateState(block *types.MinorBlock, statedb *state.StateDB, result *ProcessResult) error {
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
	if result.CoinbaseAmount == nil {
		return ErrInvalidExecutionResult
	}
	actualCoinbase, err := result.CoinbaseAmount.SerializeToBytes()
	if err != nil {
		return fmt.Errorf("serialize processed coinbase amount: %w", err)
	}
	expectedCoinbase, err := block.CoinbaseAmount().SerializeToBytes()
	if err != nil {
		return fmt.Errorf("serialize minor block coinbase amount: %w", err)
	}
	if !bytes.Equal(actualCoinbase, expectedCoinbase) {
		return fmt.Errorf("have %x want %x: %w", actualCoinbase, expectedCoinbase, ErrCoinbaseAmountMismatch)
	}
	return nil
}

func sameXShardCursor(left, right *types.XShardTxCursorInfo) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
