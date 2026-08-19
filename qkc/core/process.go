// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	qkcstate "github.com/ethereum/go-ethereum/qkc/state"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// Process is run_block (shard_state.py:783): execute one minor block against
// its parent's state and report every consensus result it produced.
//
// It runs on an isolated copy of the parent state, opened at the parent's root,
// so a rejected block leaves the caller's state untouched.
//
// The cross-shard half runs first, before any transaction. That ordering is
// consensus, not an implementation detail (shard_state.py:1326), and whatever
// the cross-shard half leaves unspent of its allowance is handed back to the
// in-shard gas limit.
func Process(ctx *ExecutionContext, block *types.MinorBlock, parentRoot common.Hash, db ethdb.Database, tdb *triedb.Database, src XShardSource) (*ProcessResult, error) {
	parentHeader, err := ctx.MinorHeaderByHash(block.Header.ParentHash)
	if err != nil {
		return nil, fmt.Errorf("parent header %s: %w", block.Header.ParentHash, err)
	}
	if parentHeader == nil {
		return nil, fmt.Errorf("parent header %s not found", block.Header.ParentHash)
	}

	state, err := qkcstate.New(parentRoot, db, tdb)
	if err != nil {
		return nil, err
	}
	state.SetConfig(ctx.QKCConfig, ctx.ShardConfig)

	coinbase := block.Header.Coinbase.Recipient
	disallow, err := SenderDisallowMap(ctx, parentHeader, &coinbase)
	if err != nil {
		return nil, err
	}
	state.SetSenderDisallowMap(disallow)

	headers, err := prevHeaders(ctx, parentHeader)
	if err != nil {
		return nil, err
	}
	state.SetPrevHeaders(headers)

	state.SetGasLimit(depositUint256(block.Header.GasLimit).Uint64())
	state.SetBlockNumber(block.Header.Number)
	state.SetBlockCoinbase(coinbase)
	state.SetBlockDifficulty(block.Header.Difficulty)
	state.SetTimestamp(block.Header.Time)
	// The shard key is left at zero rather than set to this shard's.
	// __create_evm_state never assigns one (shard_state.py:353), and it is
	// restamped per transaction and per post-EVM deposit; the only code that
	// reads it before then is the pre-EVM cross-shard credit, which is supposed
	// to see the default. See RunOneXShardTx.

	consumed, cursor, err := RunCrossShardTxWithCursor(ctx, state, block, src)
	if err != nil {
		return nil, err
	}
	state.SetXShardTxCursor(&cursor)

	// Cross-shard receiving has its own allowance carved out of the block's gas
	// limit. Whatever it did not use goes back to the transactions.
	xShardGasLimit := depositUint256(block.Meta.XShardGasLimit).Uint64()
	if used := state.GasUsed(); used < xShardGasLimit {
		state.SetGasLimit(state.GasLimit() - (xShardGasLimit - used))
	}

	for i, tx := range block.Transactions {
		if _, err := ctx.QKCConfig.GetFullShardIdByFullShardKey(tx.FromFullShardKey()); err != nil {
			return nil, fmt.Errorf("transaction %d: %w", i, err)
		}
		if _, err := ValidateTxForBlock(ctx, state, tx, xShardGasLimit); err != nil {
			return nil, fmt.Errorf("transaction %d (%s): %w", i, tx.Hash(), err)
		}
		if _, _, err := ApplyTransaction(ctx, state, tx, tx.Hash(), i); err != nil {
			return nil, fmt.Errorf("transaction %d (%s): %w", i, tx.Hash(), err)
		}
	}

	// The block reward, paid after the transactions so the coinbase already
	// holds the fees.
	pureCoinbase := CoinbaseAmountMap(ctx, block.Header.Number)
	for tokenID, amount := range pureCoinbase {
		state.DeltaTokenBalance(coinbase, tokenID, amount)
	}

	state.AddBlockHeader(block.Header)

	receipts := append(types.Receipts{}, state.Receipts()...)
	depositReceipts := append(types.Receipts{}, state.XShardDepositReceipts()...)
	bloom := state.Bloom()
	gasUsed, xShardGasUsed := state.GasUsed(), state.XShardReceiveGasUsed()
	produced := append([]*types.CrossShardTransactionDeposit{}, state.XShardList()...)

	root, err := state.Commit(block.Header.Number)
	if err != nil {
		return nil, err
	}

	return &ProcessResult{
		State:                 state,
		StateRoot:             root,
		Receipts:              receipts,
		XShardDepositReceipts: depositReceipts,
		ReceiptRoot:           receiptRoot(receipts, depositReceipts),
		GasUsed:               gasUsed,
		XShardReceiveGasUsed:  xShardGasUsed,
		Cursor:                cursor,
		CoinbaseAmountMap:     addBlockFees(pureCoinbase, state.BlockFeeTokens()),
		Bloom:                 bloom,
		ProducedDeposits:      produced,
		ConsumedDeposits:      consumed,
	}, nil
}

// ExecuteAndValidate runs the block and publishes the result only if all seven
// consensus outputs match what the block claims.
//
// The atomicity it promises is about what the caller can install as canonical
// state, not about the database: the trie is content-addressed, so a rejected
// block may leave unreachable nodes behind for pruning to collect. What it does
// guarantee is that a mismatched result never reaches the caller.
func ExecuteAndValidate(ctx *ExecutionContext, block *types.MinorBlock, parentRoot common.Hash, db ethdb.Database, tdb *triedb.Database, src XShardSource) (*ProcessResult, error) {
	result, err := Process(ctx, block, parentRoot, db, tdb, src)
	if err != nil {
		return nil, err
	}
	if err := ValidateBlockResult(block, result); err != nil {
		return nil, err
	}
	return result, nil
}

// ValidateBlockResult is add_block's result comparison (shard_state.py:940-994):
// the seven values a minor block commits to.
//
// It is not validate_block. The structural checks that run before execution —
// height, parent, difficulty, gas limits, transaction count, merkle root,
// timestamp, meta hash — belong to the chain layer and are not done here.
func ValidateBlockResult(block *types.MinorBlock, result *ProcessResult) error {
	if result.Cursor != block.Meta.XShardTxCursor {
		return fmt.Errorf("%w: computed %+v, header %+v", ErrXShardCursorMismatch, result.Cursor, block.Meta.XShardTxCursor)
	}
	if result.StateRoot != block.Meta.Root {
		return fmt.Errorf("%w: computed %s, header %s", ErrStateRootMismatch, result.StateRoot, block.Meta.Root)
	}
	if result.ReceiptRoot != block.Meta.ReceiptHash {
		return fmt.Errorf("%w: computed %s, header %s", ErrReceiptRootMismatch, result.ReceiptRoot, block.Meta.ReceiptHash)
	}
	if want := depositUint256(block.Meta.GasUsed).Uint64(); result.GasUsed != want {
		return fmt.Errorf("%w: computed %d, header %d", ErrGasUsedMismatch, result.GasUsed, want)
	}
	if want := depositUint256(block.Meta.CrossShardGasUsed).Uint64(); result.XShardReceiveGasUsed != want {
		return fmt.Errorf("%w: computed %d, header %d", ErrXShardGasUsedMismatch, result.XShardReceiveGasUsed, want)
	}
	if err := compareCoinbase(result.CoinbaseAmountMap, block.Header.CoinbaseAmount); err != nil {
		return err
	}
	if result.Bloom != block.Header.Bloom {
		return ErrBloomMismatch
	}
	return nil
}

// CoinbaseAmountMap is get_coinbase_amount_map (shard_state.py:1074): the block
// reward, decayed by epoch and reduced to the shard's share. Only the genesis
// token is ever minted.
//
// This is half of what a block's coinbase field holds; the other half is the
// fees the transactions paid, added by addBlockFees.
func CoinbaseAmountMap(ctx *ExecutionContext, height uint64) map[uint64]*big.Int {
	amount := decayByEpoch(ctx, ctx.ShardConfig.CoinbaseAmount, height)
	rate := ctx.QKCConfig.LocalFeeRate
	if rate != nil {
		amount = new(big.Int).Div(new(big.Int).Mul(amount, rate.Num()), rate.Denom())
	}
	return map[uint64]*big.Int{ctx.QKCConfig.GetDefaultChainTokenID(): amount}
}

// decayByEpoch is __decay_by_epoch (shard_state.py:2084): the reward is
// multiplied by the decay factor once per elapsed epoch, truncating each time
// only at the end.
func decayByEpoch(ctx *ExecutionContext, value *big.Int, height uint64) *big.Int {
	if value == nil {
		return new(big.Int)
	}
	interval := ctx.ShardConfig.EpochInterval
	if interval == 0 {
		return new(big.Int).Set(value)
	}
	epoch := height / interval
	factor := ctx.QKCConfig.BlockRewardDecayFactor
	if factor == nil || epoch == 0 {
		return new(big.Int).Set(value)
	}
	num := new(big.Int).Exp(factor.Num(), new(big.Int).SetUint64(epoch), nil)
	den := new(big.Int).Exp(factor.Denom(), new(big.Int).SetUint64(epoch), nil)
	out := new(big.Int).Mul(value, num)
	return out.Div(out, den)
}

// addBlockFees is the second half of the coinbase comparison
// (shard_state.py:983): the reward plus every fee the block collected.
func addBlockFees(reward map[uint64]*big.Int, fees map[uint64]*uint256.Int) map[uint64]*big.Int {
	out := make(map[uint64]*big.Int, len(reward)+len(fees))
	for token, amount := range reward {
		out[token] = new(big.Int).Set(amount)
	}
	for token, amount := range fees {
		if existing, ok := out[token]; ok {
			out[token] = existing.Add(existing, amount.ToBig())
		} else {
			out[token] = amount.ToBig()
		}
	}
	return out
}

// receiptRoot is mk_receipt_sha over the two lists concatenated, transactions
// before deposits (shard_state.py:957).
func receiptRoot(receipts, depositReceipts types.Receipts) common.Hash {
	all := make(types.Receipts, 0, len(receipts)+len(depositReceipts))
	all = append(all, receipts...)
	all = append(all, depositReceipts...)
	return types.DeriveSha(all)
}

// compareCoinbase compares the computed map against the block's, entry for
// entry — including entries worth zero.
//
// A zero amount and an absent entry are *not* the same thing here, however much
// they look alike: add_block compares the two balance maps as plain dicts
// (shard_state.py:983), while the serializer that produced the header dropped
// every zero (TokenBalanceMap, core.py:574). So a block whose computed map
// carries a zero entry is refused by pyquarkchain, because the map it is
// compared against came off the wire without one. Tolerating that here would
// make this fork accept a block every reference node rejects.
func compareCoinbase(computed map[uint64]*big.Int, declared *qkccommon.TokenBalances) error {
	declaredMap := map[uint64]*uint256.Int{}
	if declared != nil {
		declaredMap = declared.GetBalanceMap()
	}
	if len(computed) != len(declaredMap) {
		return fmt.Errorf("%w: computed %d tokens, header %d", ErrCoinbaseAmountMismatch, len(computed), len(declaredMap))
	}
	for token, got := range computed {
		if got == nil {
			got = new(big.Int)
		}
		amount, ok := declaredMap[token]
		if !ok {
			return fmt.Errorf("%w: token %d computed %s, header has no entry", ErrCoinbaseAmountMismatch, token, got)
		}
		if got.Cmp(amount.ToBig()) != 0 {
			return fmt.Errorf("%w: token %d computed %s, header %s", ErrCoinbaseAmountMismatch, token, got, amount)
		}
	}
	return nil
}
