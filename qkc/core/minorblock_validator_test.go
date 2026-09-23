// Copyright 2026-2027, QuarkChain.

package core

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
)

func TestBasicMinorBlockValidatorValidateBlock(t *testing.T) {
	validator := NewMinorBlockValidator()
	block := newValidatorBlock(t, newValidatorState(t), nil)
	if err := validator.ValidateBlock(block); err != nil {
		t.Fatalf("valid block rejected: %v", err)
	}
	if err := validator.ValidateBlock(nil); !errors.Is(err, ErrUnknownBlock) {
		t.Fatalf("nil block error = %v, want %v", err, ErrUnknownBlock)
	}

	tests := []struct {
		name   string
		mutate func(*types.MinorBlockHeader, *types.MinorBlockMeta)
	}{
		{
			name: "unsupported version",
			mutate: func(header *types.MinorBlockHeader, _ *types.MinorBlockMeta) {
				header.Version = 1
			},
		},
		{
			name: "genesis import",
			mutate: func(header *types.MinorBlockHeader, _ *types.MinorBlockMeta) {
				header.Number = 0
			},
		},
		{
			name: "meta root",
			mutate: func(header *types.MinorBlockHeader, _ *types.MinorBlockMeta) {
				header.MetaHash = common.HexToHash("0x01")
			},
		},
		{
			name: "x-shard reservation",
			mutate: func(header *types.MinorBlockHeader, meta *types.MinorBlockMeta) {
				meta.XShardGasLimit.Value.Set(header.GasLimit.Value)
				header.MetaHash = meta.Hash()
			},
		},
		{
			name: "total gas overflow",
			mutate: func(header *types.MinorBlockHeader, meta *types.MinorBlockMeta) {
				meta.GasUsed.Value = new(big.Int).Add(header.GasLimit.Value, big.NewInt(1))
				header.MetaHash = meta.Hash()
			},
		},
		{
			name: "x-shard gas above total",
			mutate: func(header *types.MinorBlockHeader, meta *types.MinorBlockMeta) {
				meta.GasUsed.Value.SetUint64(1)
				meta.CrossShardGasUsed.Value.SetUint64(2)
				header.MetaHash = meta.Hash()
			},
		},
		{
			name: "transaction root",
			mutate: func(header *types.MinorBlockHeader, meta *types.MinorBlockMeta) {
				meta.TxHash = common.HexToHash("0x02")
				header.MetaHash = meta.Hash()
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header, meta := block.Header(), block.Meta()
			test.mutate(header, meta)
			if err := validator.ValidateBlock(types.NewMinorBlockWithHeader(header, meta)); err == nil {
				t.Fatal("invalid block accepted")
			}
		})
	}
}

func TestBasicMinorBlockValidatorAllowsSoftXShardGasLimit(t *testing.T) {
	validator := NewMinorBlockValidator()
	block := newValidatorBlock(t, newValidatorState(t), nil)
	header, meta := block.Header(), block.Meta()
	meta.XShardGasLimit.Value.SetUint64(1)
	meta.GasUsed.Value.SetUint64(2)
	meta.CrossShardGasUsed.Value.SetUint64(2)
	header.MetaHash = meta.Hash()

	if err := validator.ValidateBlock(types.NewMinorBlockWithHeader(header, meta)); err != nil {
		t.Fatalf("soft x-shard gas limit rejected: %v", err)
	}
}

func TestBasicMinorBlockValidatorValidateState(t *testing.T) {
	validator := NewMinorBlockValidator()
	statedb := newValidatorState(t)
	block := newValidatorBlock(t, statedb, nil)
	result := newValidatorResult(block)
	if err := validator.ValidateState(block, statedb, result); err != nil {
		t.Fatalf("valid execution result rejected: %v", err)
	}

	if err := validator.ValidateState(nil, statedb, result); !errors.Is(err, ErrInvalidExecutionResult) {
		t.Fatalf("nil block error = %v, want %v", err, ErrInvalidExecutionResult)
	}
	if err := validator.ValidateState(block, nil, result); !errors.Is(err, ErrInvalidExecutionResult) {
		t.Fatalf("nil state error = %v, want %v", err, ErrInvalidExecutionResult)
	}
	if err := validator.ValidateState(block, statedb, nil); !errors.Is(err, ErrInvalidExecutionResult) {
		t.Fatalf("nil result error = %v, want %v", err, ErrInvalidExecutionResult)
	}

	t.Run("cursor", func(t *testing.T) {
		got := newValidatorResult(block)
		got.XShardCursor = &types.XShardTxCursorInfo{RootBlockHeight: 2}
		if err := validator.ValidateState(block, newValidatorState(t), got); err == nil {
			t.Fatal("mismatched cursor accepted")
		}
	})
	t.Run("state root", func(t *testing.T) {
		header, meta := block.Header(), block.Meta()
		meta.Root = common.HexToHash("0x03")
		header.MetaHash = meta.Hash()
		err := validator.ValidateState(types.NewMinorBlockWithHeader(header, meta), newValidatorState(t), newValidatorResult(block))
		if !errors.Is(err, ErrStateRootMismatch) {
			t.Fatalf("state root error = %v, want %v", err, ErrStateRootMismatch)
		}
	})
	t.Run("receipt root", func(t *testing.T) {
		header, meta := block.Header(), block.Meta()
		meta.ReceiptHash = common.HexToHash("0x04")
		header.MetaHash = meta.Hash()
		err := validator.ValidateState(types.NewMinorBlockWithHeader(header, meta), newValidatorState(t), newValidatorResult(block))
		if !errors.Is(err, ErrReceiptRootMismatch) {
			t.Fatalf("receipt root error = %v, want %v", err, ErrReceiptRootMismatch)
		}
	})
	t.Run("receipt bloom", func(t *testing.T) {
		header, meta := block.Header(), block.Meta()
		header.Bloom[0] = 1
		if err := validator.ValidateState(types.NewMinorBlockWithHeader(header, meta), newValidatorState(t), newValidatorResult(block)); err == nil {
			t.Fatal("mismatched bloom accepted")
		}
	})
	t.Run("gas used", func(t *testing.T) {
		header, meta := block.Header(), block.Meta()
		meta.GasUsed.Value.SetUint64(1)
		header.MetaHash = meta.Hash()
		err := validator.ValidateState(types.NewMinorBlockWithHeader(header, meta), newValidatorState(t), newValidatorResult(block))
		if !errors.Is(err, ErrGasUsedMismatch) {
			t.Fatalf("gas error = %v, want %v", err, ErrGasUsedMismatch)
		}
	})
	t.Run("x-shard gas used", func(t *testing.T) {
		header, meta := block.Header(), block.Meta()
		meta.CrossShardGasUsed.Value.SetUint64(1)
		header.MetaHash = meta.Hash()
		err := validator.ValidateState(types.NewMinorBlockWithHeader(header, meta), newValidatorState(t), newValidatorResult(block))
		if !errors.Is(err, ErrXShardGasUsedMismatch) {
			t.Fatalf("x-shard gas error = %v, want %v", err, ErrXShardGasUsedMismatch)
		}
	})
	t.Run("missing coinbase", func(t *testing.T) {
		got := newValidatorResult(block)
		got.CoinbaseAmount = nil
		if err := validator.ValidateState(block, newValidatorState(t), got); !errors.Is(err, ErrInvalidExecutionResult) {
			t.Fatalf("missing coinbase error = %v, want %v", err, ErrInvalidExecutionResult)
		}
	})
	t.Run("coinbase amount", func(t *testing.T) {
		got := newValidatorResult(block)
		got.CoinbaseAmount.SetValue(uint256.NewInt(1), qkccommon.DefaultTokenID)
		if err := validator.ValidateState(block, newValidatorState(t), got); !errors.Is(err, ErrCoinbaseAmountMismatch) {
			t.Fatalf("coinbase error = %v, want %v", err, ErrCoinbaseAmountMismatch)
		}
	})
}

func TestBasicMinorBlockValidatorValidatesReceiptCommitments(t *testing.T) {
	receipt := types.NewReceipt(false, 21_000)
	receipt.Logs = []*coretypes.Log{{
		Address: common.HexToAddress("0x1234"),
		Topics:  []common.Hash{common.HexToHash("0x5678")},
	}}
	receipts := types.Receipts{receipt}
	statedb := newValidatorState(t)
	block := newValidatorBlock(t, statedb, receipts)
	result := newValidatorResult(block)
	result.Receipts = receipts

	if err := NewMinorBlockValidator().ValidateState(block, statedb, result); err != nil {
		t.Fatalf("matching receipt commitments rejected: %v", err)
	}
}

func TestBasicMinorBlockValidatorDeletesEmptyAccountsForStateRoot(t *testing.T) {
	validator := NewMinorBlockValidator()
	wantState := newValidatorState(t)
	block := newValidatorBlock(t, wantState, nil)
	actualState := newValidatorState(t)
	actualState.CreateAccount(common.HexToAddress("0x1234"))

	if err := validator.ValidateState(block, actualState, newValidatorResult(block)); err != nil {
		t.Fatalf("empty-account state rejected: %v", err)
	}
}

func newValidatorState(t *testing.T) *state.StateDB {
	t.Helper()
	statedb, err := state.New(coretypes.EmptyRootHash, state.NewDatabaseForTesting())
	if err != nil {
		t.Fatal(err)
	}
	return statedb
}

func newValidatorBlock(t *testing.T, statedb *state.StateDB, receipts types.Receipts) *types.MinorBlock {
	t.Helper()
	meta := &types.MinorBlockMeta{
		TxHash:             types.CalculateMerkleRoot(types.Transactions(nil)),
		Root:               statedb.IntermediateRoot(true),
		ReceiptHash:        types.DeriveSha(receipts, trie.NewListHasher()),
		GasUsed:            &serialize.Uint256{Value: new(big.Int)},
		CrossShardGasUsed:  &serialize.Uint256{Value: new(big.Int)},
		XShardTxCursorInfo: &types.XShardTxCursorInfo{RootBlockHeight: 1},
		XShardGasLimit:     &serialize.Uint256{Value: big.NewInt(100_000)},
	}
	header := &types.MinorBlockHeader{
		Branch:         account.NewBranch(1),
		Number:         1,
		CoinbaseAmount: qkccommon.NewEmptyTokenBalances(),
		GasLimit:       &serialize.Uint256{Value: big.NewInt(1_000_000)},
		MetaHash:       meta.Hash(),
		Difficulty:     big.NewInt(1),
		Bloom:          types.CreateBloom(receipts),
	}
	return types.NewMinorBlockWithHeader(header, meta)
}

func newValidatorResult(block *types.MinorBlock) *ProcessResult {
	return &ProcessResult{
		Receipts:       nil,
		XShardCursor:   block.Meta().XShardTxCursorInfo,
		CoinbaseAmount: block.CoinbaseAmount(),
	}
}
