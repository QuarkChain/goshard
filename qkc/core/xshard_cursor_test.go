// Copyright 2026-2027, QuarkChain.

package core

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/holiman/uint256"
)

func TestXShardCursorSingleRootAndResume(t *testing.T) {
	c, chain, _, genesis := coordinatorFixture(t)
	root1 := testRootBlock(genesis, 1, nil)
	remote := testMinorBlock(chain.current, 1, root1.Hash(), &types.XShardTxCursorInfo{}).Header()
	remote.Branch = account.NewBranch(1<<16 | 1)
	root2 := testRootBlock(root1, 2, types.MinorBlockHeaders{remote})
	header := root2.Header()
	header.CoinbaseAmount = qkccommon.NewTokenBalancesWithMap(map[uint64]*uint256.Int{qkccommon.DefaultTokenID: uint256.NewInt(17)})
	root2 = types.NewRootBlock(header, root2.MinorBlockHeaders(), nil)
	if _, err := c.AddRootBlock(root2); err != nil {
		t.Fatal(err)
	}
	block := testMinorBlock(chain.current, 1, root2.Hash(), &types.XShardTxCursorInfo{})
	parentCursor := &types.XShardTxCursorInfo{RootBlockHeight: 2}
	cursor, err := newXShardTxCursor(c.db, 0, block, parentCursor)
	if err != nil {
		t.Fatal(err)
	}
	coinbase, err := cursor.GetNextTx()
	if err != nil || coinbase == nil || !coinbase.IsFromRootChain || coinbase.TxHash != root2.Hash() || coinbase.Value.Value.Cmp(big.NewInt(17)) != 0 {
		t.Fatalf("root coinbase: %+v, %v", coinbase, err)
	}
	if tx, err := cursor.GetNextTx(); tx != nil || !errors.Is(err, ErrUnknownRootBlock) {
		t.Fatalf("missing remote previous root must fail: %+v, %v", tx, err)
	}
	if info := cursor.GetCursorInfo(); info.RootBlockHeight != 2 {
		t.Fatalf("missing input advanced to EOF: %+v", info)
	}
	rawdb.WriteRootBlock(c.db, root1)
	if err := c.AddXShardTxList(remote.Hash(), []*types.CrossShardTransactionDeposit{testDeposit(0x41)}); err != nil {
		t.Fatal(err)
	}
	// Failed execution retries from the original parent cursor.
	cursor, err = newXShardTxCursor(c.db, 0, block, parentCursor)
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := cursor.GetNextTx(); err != nil || tx == nil || tx.TxHash != root2.Hash() {
		t.Fatalf("retry root coinbase: %+v, %v", tx, err)
	}
	deposit, err := cursor.GetNextTx()
	if err != nil || deposit == nil || deposit.TxHash != common.HexToHash("0x41") {
		t.Fatalf("remote deposit: %+v, %v", deposit, err)
	}
	resumed, err := newXShardTxCursor(c.db, 0, block, cursor.GetCursorInfo())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if tx, err := resumed.GetNextTx(); err != nil || tx != nil {
			t.Fatalf("consumed deposit replayed or unstable EOF: %+v, %v", tx, err)
		}
	}
	if info := resumed.GetCursorInfo(); *info != (types.XShardTxCursorInfo{RootBlockHeight: 3}) {
		t.Fatalf("EOF cursor: %+v", info)
	}
}
