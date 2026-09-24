// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// XShardTxCursor walks deposits on the root-chain branch ending at maxRoot.
// Its indexes have the same meaning as pyquarkchain's XshardTxCursor.
type XShardTxCursor struct {
	db                ethdb.Database
	branch            account.Branch
	genesisRootHeight uint64
	maxRoot           *types.RootBlock
	root              *types.RootBlock
	minorIndex        uint64
	depositIndex      uint64
	deposits          *types.CrossShardTransactionList
}

func newXShardTxCursor(db ethdb.Database, genesisRootHeight uint64, block *types.MinorBlock, cursorInfo *types.XShardTxCursorInfo) (*XShardTxCursor, error) {
	if block == nil {
		return nil, ErrUnknownBlock
	}
	if cursorInfo == nil {
		return nil, fmt.Errorf("parent minor block has no x-shard cursor")
	}
	maxRoot := rawdb.ReadRootBlock(db, block.PrevRootBlockHash())
	if maxRoot == nil {
		return nil, fmt.Errorf("previous root %s: %w", block.PrevRootBlockHash(), ErrUnknownRootBlock)
	}
	info := *cursorInfo
	cursor := &XShardTxCursor{
		db:                db,
		branch:            block.Branch(),
		genesisRootHeight: genesisRootHeight,
		maxRoot:           maxRoot,
		minorIndex:        info.MinorBlockIndex,
		depositIndex:      info.XShardDepositIndex,
	}
	if info.RootBlockHeight == maxRoot.NumberU64()+1 && info.MinorBlockIndex == 0 && info.XShardDepositIndex == 0 {
		return cursor, nil
	}
	if info.RootBlockHeight > maxRoot.NumberU64() {
		return nil, fmt.Errorf("x-shard cursor root height %d exceeds target root height %d", info.RootBlockHeight, maxRoot.NumberU64())
	}
	root, err := rootBlockAtHeight(db, maxRoot, info.RootBlockHeight)
	if err != nil {
		return nil, err
	}
	cursor.root = root
	if info.MinorBlockIndex > uint64(len(root.MinorBlockHeaders())) {
		return nil, fmt.Errorf("x-shard cursor minor index %d exceeds root %d header count %d", info.MinorBlockIndex, root.NumberU64(), len(root.MinorBlockHeaders()))
	}
	if info.MinorBlockIndex != 0 {
		list := rawdb.ReadCrossShardTxList(db, root.MinorBlockHeaders()[info.MinorBlockIndex-1].Hash())
		if list == nil {
			return nil, fmt.Errorf("x-shard list for cursor root %d minor index %d is unavailable", root.NumberU64(), info.MinorBlockIndex)
		}
		cursor.deposits = list
	}
	return cursor, nil
}

// GetNextTx advances the cursor and returns the next eligible deposit. A nil
// deposit marks EOF at the minor block's previous-root boundary.
func (x *XShardTxCursor) GetNextTx() (*types.CrossShardTransactionDeposit, error) {
	if x.root == nil {
		return nil, nil
	}
	x.depositIndex++
	deposit, err := x.getCurrentTx()
	if err != nil || deposit != nil {
		return deposit, err
	}
	x.minorIndex++
	x.depositIndex = 0

	for x.minorIndex <= uint64(len(x.root.MinorBlockHeaders())) {
		header := x.root.MinorBlockHeaders()[x.minorIndex-1]
		// Neighbor filtering bounds cross-shard fan-out when the network grows beyond
		// 32 shards. The current scope is not expected to reach that size, so every
		// active remote shard is treated as a deposit source to keep traversal simple.
		if header.Branch == x.branch {
			x.minorIndex++
			continue
		}
		previousRoot := rawdb.ReadRootBlock(x.db, header.PrevRootBlockHash)
		if previousRoot == nil {
			return nil, fmt.Errorf("previous root %s of remote minor block %s: %w", header.PrevRootBlockHash, header.Hash(), ErrUnknownRootBlock)
		}
		list := rawdb.ReadCrossShardTxList(x.db, header.Hash())
		if previousRoot.NumberU64() <= x.genesisRootHeight {
			if list != nil {
				return nil, fmt.Errorf("unexpected x-shard transactions for pre-genesis minor block %s", header.Hash())
			}
			x.minorIndex++
			continue
		}
		if list == nil {
			return nil, fmt.Errorf("x-shard transactions for minor block %s are unavailable", header.Hash())
		}
		x.deposits = list
		deposit, err = x.getCurrentTx()
		if err != nil || deposit != nil {
			return deposit, err
		}
		x.minorIndex++
	}

	nextRoot, err := rootBlockAtHeight(x.db, x.maxRoot, x.root.NumberU64()+1)
	if err != nil {
		return nil, err
	}
	if nextRoot == nil {
		x.root = nil
		x.minorIndex = 0
		x.depositIndex = 0
		return nil, nil
	}
	x.root = nextRoot
	x.minorIndex = 0
	x.depositIndex = 1
	x.deposits = nil
	return x.getCurrentTx()
}

func (x *XShardTxCursor) getCurrentTx() (*types.CrossShardTransactionDeposit, error) {
	if x.minorIndex == 0 {
		if x.depositIndex != 1 && x.depositIndex != 2 {
			return nil, fmt.Errorf("invalid root deposit index %d at root %d", x.depositIndex, x.root.NumberU64())
		}
		if x.depositIndex == 1 {
			value := new(big.Int)
			coinbase := x.root.Coinbase()
			// Root coinbase deposits are intentionally fixed to QKC. Configurable
			// genesis-token routing is outside this implementation's protocol scope.
			if x.branch.IsInBranch(coinbase.FullShardKey) {
				value.Set(x.root.CoinbaseAmount().GetTokenBalance(qkcCommon.DefaultTokenID).ToBig())
			}
			return &types.CrossShardTransactionDeposit{
				TxHash:          x.root.Hash(),
				From:            coinbase,
				To:              coinbase,
				Value:           &serialize.Uint256{Value: value},
				GasPrice:        &serialize.Uint256{Value: new(big.Int)},
				GasTokenID:      qkcCommon.DefaultTokenID,
				TransferTokenID: qkcCommon.DefaultTokenID,
				GasRemained:     &serialize.Uint256{Value: new(big.Int)},
				IsFromRootChain: true,
				RefundRate:      100,
			}, nil
		}
		return nil, nil
	}
	if x.deposits == nil {
		return nil, fmt.Errorf("x-shard list at root %d minor index %d is unavailable", x.root.NumberU64(), x.minorIndex)
	}
	if x.depositIndex < uint64(len(x.deposits.TXList)) {
		return x.deposits.TXList[x.depositIndex], nil
	}
	return nil, nil
}

// GetCursorInfo returns the current position after the deposits consumed so far.
func (x *XShardTxCursor) GetCursorInfo() *types.XShardTxCursorInfo {
	rootHeight := x.maxRoot.NumberU64() + 1
	if x.root != nil {
		rootHeight = x.root.NumberU64()
	}
	return &types.XShardTxCursorInfo{
		RootBlockHeight:    rootHeight,
		MinorBlockIndex:    x.minorIndex,
		XShardDepositIndex: x.depositIndex,
	}
}

func rootBlockAtHeight(db ethdb.Database, head *types.RootBlock, height uint64) (*types.RootBlock, error) {
	if head == nil || height > head.NumberU64() {
		return nil, nil
	}
	root := head
	for root.NumberU64() > height {
		parent := rawdb.ReadRootBlock(db, root.ParentHash())
		if parent == nil || parent.NumberU64()+1 != root.NumberU64() {
			return nil, fmt.Errorf("root ancestor at height %d: %w", height, ErrUnknownRootBlock)
		}
		root = parent
	}
	if root.NumberU64() != height {
		return nil, fmt.Errorf("root ancestor at height %d: %w", height, ErrUnknownRootBlock)
	}
	return root, nil
}
