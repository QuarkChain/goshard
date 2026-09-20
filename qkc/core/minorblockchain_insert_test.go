// Copyright 2026-2027, QuarkChain.

package core

import (
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/holiman/uint256"
)

type importTestProcessor struct {
	calls    int
	cursor   XShardDepositCursor
	resultFn func(*types.MinorBlock, *state.StateDB) *ProcessResult
	err      error
}

func (processor *importTestProcessor) Process(block *types.MinorBlock, statedb *state.StateDB, cursor XShardDepositCursor, _ vm.Config) (*ProcessResult, error) {
	processor.calls++
	processor.cursor = cursor
	if processor.err != nil {
		return nil, processor.err
	}
	if processor.resultFn == nil {
		return nil, nil
	}
	return processor.resultFn(block, statedb), nil
}

type importTestValidator struct {
	blockCalls int
	stateCalls int
	blockErr   error
	stateErr   error
}

func (validator *importTestValidator) ValidateBlock(*types.MinorBlock) error {
	validator.blockCalls++
	return validator.blockErr
}

func (validator *importTestValidator) ValidateState(*types.MinorBlock, *state.StateDB, *ProcessResult) error {
	validator.stateCalls++
	return validator.stateErr
}

func TestMinorBlockChainInsertBlockPersistsCandidateWithoutChangingHead(t *testing.T) {
	outgoing := []*types.CrossShardTransactionDeposit{{TxHash: common.HexToHash("0x1234")}}
	processor := &importTestProcessor{
		resultFn: func(block *types.MinorBlock, _ *state.StateDB) *ProcessResult {
			result := validImportTestResult(block)
			result.OutgoingXShard = outgoing
			return result
		},
	}
	chain, db, genesis := newImportTestChain(t, processor, NewBasicMinorBlockValidator())
	block := newImportTestBlock(t, genesis, 1)

	got, err := chain.InsertBlockWithXShardInput(block, nil, InsertOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != outgoing[0] {
		t.Fatalf("outgoing deposits = %#v, want %#v", got, outgoing)
	}
	if processor.calls != 1 {
		t.Fatalf("processor calls = %d, want 1", processor.calls)
	}
	if processor.cursor != nil {
		t.Fatalf("nil cursor reached processor as %#v", processor.cursor)
	}
	if stored := chain.GetBlock(block.Hash()); stored == nil || stored.Hash() != block.Hash() {
		t.Fatal("candidate block was not persisted")
	}
	if !rawdb.HasQKCReceipts(db, block.Hash()) {
		t.Fatal("candidate receipts were not persisted")
	}
	if canonical := chain.GetBlockByNumber(1); canonical != nil {
		t.Fatalf("candidate became canonical: %s", canonical.Hash())
	}
	if current := chain.CurrentBlock(); current == nil || current.Hash() != genesis.Hash() {
		t.Fatalf("current block = %v, want genesis %s", current, genesis.Hash())
	}
	height, hash := chain.Head()
	if height != 0 || hash != genesis.Hash() {
		t.Fatalf("head = %d/%s, want 0/%s", height, hash, genesis.Hash())
	}
}

func TestMinorBlockChainInsertOptions(t *testing.T) {
	processor := &importTestProcessor{resultFn: func(block *types.MinorBlock, _ *state.StateDB) *ProcessResult {
		return validImportTestResult(block)
	}}
	chain, db, genesis := newImportTestChain(t, processor, NewBasicMinorBlockValidator())
	block := newImportTestBlock(t, genesis, 1)

	if _, err := chain.InsertBlockWithXShardInput(block, nil, InsertOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := chain.InsertBlockWithXShardInput(block, nil, InsertOptions{}); err != nil {
		t.Fatal(err)
	}
	if processor.calls != 1 {
		t.Fatalf("known block processor calls = %d, want 1", processor.calls)
	}
	if _, err := chain.InsertBlockWithXShardInput(block, nil, InsertOptions{ForceInsert: true}); err != nil {
		t.Fatal(err)
	}
	if processor.calls != 2 {
		t.Fatalf("forced processor calls = %d, want 2", processor.calls)
	}

	checkBlock := newImportTestBlock(t, genesis, 2)
	if _, err := chain.InsertBlockWithXShardInput(checkBlock, nil, InsertOptions{IsCheckDB: true}); err != nil {
		t.Fatal(err)
	}
	if processor.calls != 3 {
		t.Fatalf("check-only processor calls = %d, want 3", processor.calls)
	}
	if chain.HasBlock(checkBlock.Hash()) || rawdb.HasQKCReceipts(db, checkBlock.Hash()) {
		t.Fatal("check-only import persisted block results")
	}
}

func TestMinorBlockChainInsertFailureStages(t *testing.T) {
	errValidateBlock := errors.New("validate block")
	errProcess := errors.New("process")
	errValidateState := errors.New("validate state")
	tests := []struct {
		name           string
		processorError error
		blockError     error
		stateError     error
		nilResult      bool
		wantError      error
		wantProcess    int
		wantState      int
	}{
		{name: "block validation", blockError: errValidateBlock, wantError: errValidateBlock},
		{name: "execution", processorError: errProcess, wantError: errProcess, wantProcess: 1},
		{name: "nil result", nilResult: true, wantError: ErrInvalidExecutionResult, wantProcess: 1},
		{name: "state validation", stateError: errValidateState, wantError: errValidateState, wantProcess: 1, wantState: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			processor := &importTestProcessor{err: test.processorError}
			if !test.nilResult {
				processor.resultFn = func(block *types.MinorBlock, _ *state.StateDB) *ProcessResult {
					return validImportTestResult(block)
				}
			}
			validator := &importTestValidator{blockErr: test.blockError, stateErr: test.stateError}
			chain, db, genesis := newImportTestChain(t, processor, validator)
			block := newImportTestBlock(t, genesis, 1)

			_, err := chain.InsertBlockWithXShardInput(block, nil, InsertOptions{})
			if !errors.Is(err, test.wantError) {
				t.Fatalf("insert error = %v, want %v", err, test.wantError)
			}
			if processor.calls != test.wantProcess {
				t.Fatalf("processor calls = %d, want %d", processor.calls, test.wantProcess)
			}
			if validator.stateCalls != test.wantState {
				t.Fatalf("state validator calls = %d, want %d", validator.stateCalls, test.wantState)
			}
			if chain.HasBlock(block.Hash()) || rawdb.HasQKCReceipts(db, block.Hash()) {
				t.Fatal("failed import persisted block results")
			}
		})
	}
}

func TestMinorBlockChainInsertRejectsInvalidParent(t *testing.T) {
	processor := &importTestProcessor{resultFn: func(block *types.MinorBlock, _ *state.StateDB) *ProcessResult {
		return validImportTestResult(block)
	}}
	chain, _, genesis := newImportTestChain(t, processor, new(importTestValidator))

	unknownParent := newImportTestBlock(t, nil, 1)
	if _, err := chain.InsertBlockWithXShardInput(unknownParent, nil, InsertOptions{}); !errors.Is(err, ErrUnknownParent) {
		t.Fatalf("unknown-parent error = %v, want %v", err, ErrUnknownParent)
	}
	wrongNumber := newImportTestBlock(t, genesis, 2)
	header := wrongNumber.Header()
	header.Number++
	wrongNumber = types.NewMinorBlockWithHeader(header, wrongNumber.Meta())
	if _, err := chain.InsertBlockWithXShardInput(wrongNumber, nil, InsertOptions{}); !errors.Is(err, ErrNonContiguousBlock) {
		t.Fatalf("wrong-number error = %v, want %v", err, ErrNonContiguousBlock)
	}
	if processor.calls != 0 {
		t.Fatalf("invalid parents reached processor %d times", processor.calls)
	}
}

func TestMinorBlockChainInsertPersistsState(t *testing.T) {
	address := common.HexToAddress("0x1234")
	balance := uint256.NewInt(7)
	wantState := newValidatorState(t)
	wantState.SetBalance(address, balance, tracing.BalanceChangeUnspecified)

	processor := &importTestProcessor{resultFn: func(block *types.MinorBlock, statedb *state.StateDB) *ProcessResult {
		statedb.SetBalance(address, balance, tracing.BalanceChangeUnspecified)
		return validImportTestResult(block)
	}}
	chain, _, genesis := newImportTestChain(t, processor, NewBasicMinorBlockValidator())
	block := newImportTestBlockWithState(t, genesis, 1, wantState)

	if _, err := chain.InsertBlockWithXShardInput(block, nil, InsertOptions{}); err != nil {
		t.Fatal(err)
	}
	if !chain.HasState(block.Root()) {
		t.Fatalf("state root %s was not persisted", block.Root())
	}
	stored, err := chain.StateAt(block.Root())
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.GetBalance(address); got.Cmp(balance) != 0 {
		t.Fatalf("stored balance = %s, want %s", got, balance)
	}
}

func TestMinorBlockChainInsertReleasesFailedBatch(t *testing.T) {
	processor := &importTestProcessor{resultFn: func(block *types.MinorBlock, _ *state.StateDB) *ProcessResult {
		return validImportTestResult(block)
	}}
	chain, db, genesis := newImportTestChain(t, processor, NewBasicMinorBlockValidator())
	block := newImportTestBlock(t, genesis, 1)
	db.failWrites = true

	_, err := chain.InsertBlockWithXShardInput(block, nil, InsertOptions{})
	if !errors.Is(err, errMinorChainTestBatch) {
		t.Fatalf("insert error = %v, want %v", err, errMinorChainTestBatch)
	}
	if db.openBatches != 0 {
		t.Fatalf("failed import left %d batches open", db.openBatches)
	}
	if chain.HasBlock(block.Hash()) || rawdb.HasQKCReceipts(db, block.Hash()) {
		t.Fatal("failed batch persisted block results")
	}
	if current := chain.CurrentBlock(); current == nil || current.Hash() != genesis.Hash() {
		t.Fatal("failed batch changed the canonical head")
	}
}

func newImportTestChain(t *testing.T, processor Processor, validator MinorBlockValidator) (*MinorBlockChain, *batchTrackingDatabase, *types.MinorBlock) {
	t.Helper()
	db := &batchTrackingDatabase{Database: rawdb.NewMemoryDatabase()}
	genesis := storageTestBlock(nil, 0, coretypes.EmptyRootHash)
	writeStorageTestBlock(db, genesis, true)
	chain, err := NewMinorBlockChain(db, storageTestShardConfig(), processor, validator, vm.Config{})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		chain.Stop()
		db.Close()
	})
	return chain, db, genesis
}

func newImportTestBlock(t *testing.T, parent *types.MinorBlock, nonce uint64) *types.MinorBlock {
	t.Helper()
	return newImportTestBlockWithState(t, parent, nonce, newValidatorState(t))
}

func newImportTestBlockWithState(t *testing.T, parent *types.MinorBlock, nonce uint64, statedb *state.StateDB) *types.MinorBlock {
	t.Helper()
	block := newValidatorBlock(t, statedb, nil)
	header := block.Header()
	header.Nonce = nonce
	if parent != nil {
		header.ParentHash = parent.Hash()
		header.Number = parent.NumberU64() + 1
	}
	return types.NewMinorBlockWithHeader(header, block.Meta())
}

func validImportTestResult(block *types.MinorBlock) *ProcessResult {
	return &ProcessResult{
		XShardCursor:   block.Meta().XShardTxCursorInfo,
		CoinbaseAmount: block.CoinbaseAmount(),
	}
}
