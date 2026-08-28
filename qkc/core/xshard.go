// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	qkcstate "github.com/ethereum/go-ethereum/qkc/state"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// RunOneXShardTx is __run_one_xshard_tx (shard_state.py:1581). It is two
// mutually exclusive paths, not one with a branch inside: before the EVM was
// switched on a deposit was settled without executing anything, and reading the
// later path as the only one would replay every block before 2019-09-27 wrong.
//
// checkIsFromRootChain selects how the surcharge is decided, and comes from the
// root block's height against XSHARD_GAS_DDOS_FIX_ROOT_HEIGHT.
func RunOneXShardTx(ctx *ExecutionContext, state *qkcstate.EvmState, deposit *types.CrossShardTransactionDeposit, checkIsFromRootChain bool, index int) error {
	// A deposit from the root chain arrives free; one from another shard has
	// already been charged the surcharge at its source, and pays it here.
	var gasUsedStart uint64
	if checkIsFromRootChain {
		if !deposit.IsFromRootChain {
			gasUsedStart = GTXXShardCost
		}
	} else if depositUint256(deposit.GasPrice).Sign() != 0 {
		gasUsedStart = GTXXShardCost
	}

	if state.Timestamp() < ctx.QKCConfig.EnableEvmTimeStamp {
		// Pre-EVM. The value goes straight to the recipient — there is no
		// "credit the sender first" step, because nothing can fail. No code
		// runs, no proof-of-staked-work check is made, no contract is created,
		// and no deposit receipt is produced. The fee is a flat one, unrelated
		// to what was actually done.
		//
		// The shard key is deliberately *not* restamped here, and pyquarkchain
		// marks the omission with a FIXME (shard_state.py:1594). It is still
		// consensus: an account this credit brings into existence is created
		// with whatever key the state carries, which through the whole
		// cross-shard phase is the default zero. Setting the recipient's key
		// "correctly" writes a different leaf and a different state root.
		state.DeltaTokenBalance(deposit.To.Recipient, deposit.TransferTokenID, depositUint256(deposit.Value).ToBig())
		state.AddGasUsed(gasUsedStart)

		gasPrice := depositUint256(deposit.GasPrice).ToBig()
		fee := localFee(state, gasPrice, GTXXShardCost)
		creditFee(state, deposit.GasTokenID, fee)
	} else {
		if _, _, err := ApplyXShardDeposit(ctx, state, deposit, gasUsedStart, index); err != nil {
			return err
		}
	}

	if state.GasUsed() > state.GasLimit() {
		return fmt.Errorf("%w: cross-shard receive used %d of %d", ErrBlockGasLimitReached, state.GasUsed(), state.GasLimit())
	}
	return nil
}

// RunCrossShardTxWithCursor is __run_cross_shard_tx_with_cursor
// (shard_state.py:1616): consume deposits from where the parent block stopped
// until the block's cross-shard allowance runs out, and report where to resume.
func RunCrossShardTxWithCursor(ctx *ExecutionContext, state *qkcstate.EvmState, block *types.MinorBlock, src XShardSource) ([]*types.CrossShardTransactionDeposit, types.XShardTxCursorInfo, error) {
	cursor, err := newXShardCursor(ctx, src, block.Header)
	if err != nil {
		return nil, types.XShardTxCursorInfo{}, err
	}
	xShardGasLimit := depositUint256(block.Meta.XShardGasLimit).Uint64()

	var consumed []*types.CrossShardTransactionDeposit
	for index := 0; ; index++ {
		deposit, err := cursor.next()
		if err != nil {
			return nil, types.XShardTxCursorInfo{}, err
		}
		if deposit == nil {
			break
		}
		consumed = append(consumed, deposit)

		ddosFixed := cursor.rblock.Header.Number >= uint32(ctx.QKCConfig.XShardGasDDOSFixRootHeight)
		if err := RunOneXShardTx(ctx, state, deposit, ddosFixed, index); err != nil {
			return nil, types.XShardTxCursorInfo{}, err
		}
		// A soft limit: the deposit that crosses it still runs in full, and the
		// next block picks up exactly where this one stopped.
		if state.GasUsed() >= xShardGasLimit {
			break
		}
	}
	state.SetXShardReceiveGasUsed(state.GasUsed())
	return consumed, cursor.info(), nil
}

// xShardCursor is XshardTxCursor (shard_state.py:66). Its position is a triple
// with three distinct meanings, and conflating any two of them consumes the
// wrong deposits:
//
//	(x, 0, 0)          end of stream
//	(x, 0, z), z > 0   the root block's own coinbase payout, which always exists
//	(x, y, z), y > 0   deposit z of the y'th minor block confirmed by root x
//
// In particular, minor block index 0 is not "the coinbase slot": deposit index
// 0 within it is the end marker, so treating the whole slot as coinbase would
// swallow the end of the stream.
type xShardCursor struct {
	ctx *ExecutionContext
	src XShardSource

	// maxRootHeader bounds the traversal: the root block this minor block
	// declares as its own, and the tip the height lookups are resolved along.
	maxRootHeader *types.RootBlockHeader

	// rblock is the root block being consumed, or nil at end of stream.
	rblock       *types.RootBlock
	mblockIndex  uint64
	depositIndex uint64
	xtxList      []*types.CrossShardTransactionDeposit

	branch         account.Branch
	genesisTokenID uint64
}

func newXShardCursor(ctx *ExecutionContext, src XShardSource, header *types.MinorBlockHeader) (*xShardCursor, error) {
	parentMeta, err := ctx.MinorBlockMetaByHash(header.ParentHash)
	if err != nil {
		return nil, fmt.Errorf("cross-shard cursor: parent meta %s: %w", header.ParentHash, err)
	}
	maxRootHeader, err := src.RootHeaderByHash(header.PrevRootBlockHash)
	if err != nil {
		return nil, fmt.Errorf("cross-shard cursor: root header %s: %w", header.PrevRootBlockHash, err)
	}

	if parentMeta == nil {
		return nil, fmt.Errorf("cross-shard cursor: parent meta %s not found", header.ParentHash)
	}
	if maxRootHeader == nil {
		return nil, fmt.Errorf("cross-shard cursor: root block %s not found", header.PrevRootBlockHash)
	}

	info := parentMeta.XShardTxCursor
	c := &xShardCursor{
		ctx:            ctx,
		src:            src,
		maxRootHeader:  maxRootHeader,
		mblockIndex:    info.MinorBlockIndex,
		depositIndex:   info.XShardDepositIndex,
		branch:         ctx.Branch(),
		genesisTokenID: ctx.QKCConfig.GetDefaultChainTokenID(),
	}

	// Resume: resolve the root block the parent stopped in. Only a height past
	// the bound is the end of the stream — get_root_block_header_by_height
	// returns nothing exactly there (shard_db_operator.py:297) and walks the
	// chain otherwise. Missing data below the bound is a broken chain, and
	// treating it as the end would quietly skip deposits and produce a block
	// that a node with the data rejects.
	rblock, err := src.RootBlockByHeight(header.PrevRootBlockHash, info.RootBlockHeight)
	if err != nil {
		return nil, fmt.Errorf("cross-shard cursor: root block at height %d: %w", info.RootBlockHeight, err)
	}
	if rblock == nil {
		if info.RootBlockHeight <= uint64(maxRootHeader.Number) {
			return nil, fmt.Errorf("%w: root block at height %d is missing below the tip %d",
				ErrMissingRootBlock, info.RootBlockHeight, maxRootHeader.Number)
		}
		return c, nil
	}
	c.rblock = rblock
	if c.mblockIndex != 0 {
		if c.mblockIndex > uint64(len(rblock.MinorBlockHeaders)) {
			return nil, fmt.Errorf("cross-shard cursor: minor block index %d beyond root block %d",
				c.mblockIndex, rblock.Header.Number)
		}
		deposits, found, err := src.DepositsByMinorBlockHash(rblock.MinorBlockHeaders[c.mblockIndex-1].Hash())
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("cross-shard cursor: deposits missing for minor block %d of root %d",
				c.mblockIndex-1, rblock.Header.Number)
		}
		c.xtxList = deposits
	}
	return c, nil
}

// current is __get_current_tx: the deposit the cursor now points at, or nil
// when the current slot is exhausted.
func (c *xShardCursor) current() (*types.CrossShardTransactionDeposit, error) {
	if c.mblockIndex == 0 {
		if c.depositIndex != 1 && c.depositIndex != 2 {
			return nil, fmt.Errorf("cross-shard cursor: deposit index %d in the root coinbase slot", c.depositIndex)
		}
		if c.depositIndex != 1 {
			return nil, nil
		}
		return c.rootCoinbaseDeposit(), nil
	}
	if c.depositIndex < uint64(len(c.xtxList)) {
		return c.xtxList[c.depositIndex], nil
	}
	return nil, nil
}

// rootCoinbaseDeposit is the root block's payout to whichever shard its
// coinbase address belongs to. It exists for every root block, and is zero for
// every other shard — which is not the same as absent, because consuming it is
// what advances the cursor.
func (c *xShardCursor) rootCoinbaseDeposit() *types.CrossShardTransactionDeposit {
	coinbase := c.rblock.Header.Coinbase
	amount := new(big.Int)
	if c.branch.IsInBranch(coinbase.FullShardKey) && c.rblock.Header.CoinbaseAmount != nil {
		if balance := c.rblock.Header.CoinbaseAmount.GetTokenBalance(c.genesisTokenID); balance != nil {
			amount = balance.ToBig()
		}
	}
	return &types.CrossShardTransactionDeposit{
		// The root block's hash stands in for a transaction hash: there is no
		// transaction behind this deposit.
		TxHash:          c.rblock.Header.Hash(),
		From:            coinbase,
		To:              coinbase,
		Value:           &serialize.Uint256{Value: amount},
		GasPrice:        &serialize.Uint256{Value: new(big.Int)},
		GasTokenID:      c.genesisTokenID,
		TransferTokenID: c.genesisTokenID,
		GasRemained:     &serialize.Uint256{Value: new(big.Int)},
		IsFromRootChain: true,
		RefundRate:      defaultRefundRate,
	}
}

// next is get_next_tx: advance one position and return what is there, or nil at
// end of stream.
func (c *xShardCursor) next() (*types.CrossShardTransactionDeposit, error) {
	if c.rblock == nil {
		return nil, nil
	}

	c.depositIndex++
	if deposit, err := c.current(); err != nil || deposit != nil {
		return deposit, err
	}

	// Done with this slot; walk the root block's minor headers.
	c.mblockIndex++
	c.depositIndex = 0

	for c.mblockIndex <= uint64(len(c.rblock.MinorBlockHeaders)) {
		header := c.rblock.MinorBlockHeaders[c.mblockIndex-1]

		// Only neighbours can send here, and a shard does not send to itself.
		if !ctxIsNeighborAt(c.ctx, header.Branch, uint64(c.rblock.Header.Number)) ||
			header.Branch.Value == c.branch.Value {
			c.mblockIndex++
			continue
		}

		// A shard may only send to shards that already existed when it built
		// the block. Answering that takes two hops: the remote header names a
		// root block by hash, and it is that block's height that is compared.
		prevRoot, err := c.src.RootHeaderByHash(header.PrevRootBlockHash)
		if err != nil {
			return nil, fmt.Errorf("cross-shard cursor: root header %s: %w", header.PrevRootBlockHash, err)
		}
		if prevRoot == nil {
			return nil, fmt.Errorf("cross-shard cursor: root header %s not found", header.PrevRootBlockHash)
		}
		if uint64(prevRoot.Number) <= uint64(c.ctx.QKCConfig.GetGenesisRootHeight(c.ctx.FullShardID())) {
			// Such a block must not have left a deposit list at all. An empty
			// list would mean it sent nothing; no list means it could not send.
			if _, found, err := c.src.DepositsByMinorBlockHash(header.Hash()); err != nil {
				return nil, err
			} else if found {
				return nil, fmt.Errorf("cross-shard cursor: minor block %s predates this shard but has a deposit list",
					header.Hash())
			}
			c.mblockIndex++
			continue
		}

		deposits, found, err := c.src.DepositsByMinorBlockHash(header.Hash())
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("cross-shard cursor: deposits missing for minor block %s", header.Hash())
		}
		c.xtxList = deposits

		if deposit, err := c.current(); err != nil || deposit != nil {
			return deposit, err
		}
		c.mblockIndex++
	}

	// On to the next root block, whose coinbase deposit always exists. Past the
	// tip that is the end of the stream; below it, a missing block is a broken
	// chain rather than an end.
	height := uint64(c.rblock.Header.Number) + 1
	next, err := c.src.RootBlockByHeight(c.maxRootHeader.Hash(), height)
	if err != nil {
		return nil, err
	}
	if next == nil {
		if height <= uint64(c.maxRootHeader.Number) {
			return nil, fmt.Errorf("%w: root block at height %d is missing below the tip %d",
				ErrMissingRootBlock, height, c.maxRootHeader.Number)
		}
		c.rblock, c.mblockIndex, c.depositIndex = nil, 0, 0
		return nil, nil
	}
	c.rblock = next
	c.mblockIndex, c.depositIndex = 0, 1
	return c.current()
}

// info is get_cursor_info: where the next block resumes. Past the last root
// block the height is one beyond the bound, which is the end marker.
func (c *xShardCursor) info() types.XShardTxCursorInfo {
	height := uint64(c.maxRootHeader.Number) + 1
	if c.rblock != nil {
		height = uint64(c.rblock.Header.Number)
	}
	return types.XShardTxCursorInfo{
		RootBlockHeight:    height,
		MinorBlockIndex:    c.mblockIndex,
		XShardDepositIndex: c.depositIndex,
	}
}

func ctxIsNeighborAt(ctx *ExecutionContext, remote account.Branch, rootHeight uint64) bool {
	return ctx.isNeighbor(remote, rootHeight)
}
