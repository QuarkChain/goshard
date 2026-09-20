// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// InsertBlockWithXShardInput imports one candidate block without changing the
// canonical head. ShardCoordinator owns the decision to select the candidate.
func (c *MinorBlockChain) InsertBlockWithXShardInput(block *types.MinorBlock, cursor *XShardTxCursor, options InsertOptions) ([]*types.CrossShardTransactionDeposit, error) {
	if block == nil {
		return nil, ErrUnknownBlock
	}
	if !c.chainmu.TryLock() {
		return nil, ErrChainStopped
	}
	defer c.chainmu.Unlock()
	var executionCursor XShardDepositCursor
	if cursor != nil {
		executionCursor = cursor
	}
	return c.insertBlock(block, executionCursor, options)
}

// insertBlock assumes chainmu is held.
func (c *MinorBlockChain) insertBlock(block *types.MinorBlock, cursor XShardDepositCursor, options InsertOptions) ([]*types.CrossShardTransactionDeposit, error) {
	forceInsert := options.ForceInsert || options.IsCheckDB
	if c.HasBlockAndState(block.Hash()) && !forceInsert {
		return nil, nil
	}
	if err := c.validator.ValidateBlock(block); err != nil {
		return nil, err
	}
	statedb, result, err := c.processBlock(block, cursor)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, ErrInvalidExecutionResult
	}
	if err := c.validator.ValidateState(block, statedb, result); err != nil {
		return nil, err
	}
	if !options.IsCheckDB {
		if err := c.commitBlock(block, statedb, result); err != nil {
			return nil, err
		}
	}
	return result.OutgoingXShard, nil
}

// processBlock opens the parent state and delegates all execution semantics to
// the injected Processor.
func (c *MinorBlockChain) processBlock(block *types.MinorBlock, cursor XShardDepositCursor) (*state.StateDB, *ProcessResult, error) {
	if c.processor == nil {
		return nil, nil, ErrExecutorUnavailable
	}
	if block == nil {
		return nil, nil, ErrUnknownBlock
	}
	parent := c.GetBlock(block.ParentHash())
	if parent == nil {
		return nil, nil, ErrUnknownParent
	}
	if block.NumberU64() != parent.NumberU64()+1 {
		return nil, nil, ErrNonContiguousBlock
	}
	statedb, err := c.StateAt(parent.Root())
	if err != nil {
		return nil, nil, fmt.Errorf("open parent state %s: %w", parent.Root(), err)
	}
	result, err := c.processor.Process(block, statedb, cursor, c.vmConfig)
	return statedb, result, err
}

// commitBlock persists candidate execution state, the block, and its receipts.
// It deliberately does not update canonical indexes or head markers.
func (c *MinorBlockChain) commitBlock(block *types.MinorBlock, statedb *state.StateDB, result *ProcessResult) error {
	if block == nil || statedb == nil || result == nil {
		return ErrInvalidExecutionResult
	}
	root, err := statedb.Commit(block.NumberU64(), true, false)
	if err != nil {
		return fmt.Errorf("commit minor state: %w", err)
	}
	if root != block.Root() {
		return fmt.Errorf("header %s committed %s: %w", block.Root(), root, ErrStateRootMismatch)
	}
	if root != coretypes.EmptyRootHash {
		if err := c.triedb.Commit(root, false); err != nil {
			return fmt.Errorf("flush minor state: %w", err)
		}
	}
	batch := c.db.NewBatch()
	defer batch.Close()
	rawdb.WriteMinorBlock(batch, block)
	rawdb.WriteQKCReceipts(batch, block.Hash(), result.Receipts)
	if err := batch.Write(); err != nil {
		return fmt.Errorf("write minor block results: %w", err)
	}
	return nil
}
