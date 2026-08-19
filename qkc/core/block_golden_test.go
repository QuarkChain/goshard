// Copyright 2026-2027, QuarkChain.

package core

import (
	"encoding/json"
	"math/big"
	"os"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	qkcstate "github.com/ethereum/go-ethereum/qkc/state"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/triedb"
)

const blockGoldenPath = "../testdata/exec_golden/block_level.json"

type goldenCursor struct {
	RootBlockHeight    uint64 `json:"root_block_height"`
	MinorBlockIndex    uint64 `json:"minor_block_index"`
	XShardDepositIndex uint64 `json:"xshard_deposit_index"`
}

type goldenBlockResult struct {
	PostStateRoot         string                   `json:"post_state_root"`
	ReceiptRoot           string                   `json:"receipt_root"`
	GasUsed               string                   `json:"gas_used"`
	XShardReceiveGasUsed  string                   `json:"xshard_receive_gas_used"`
	Cursor                goldenCursor             `json:"cursor"`
	CoinbaseAmountMap     map[string]string        `json:"coinbase_amount_map"`
	Bloom                 string                   `json:"bloom"`
	Receipts              []goldenReceipt          `json:"receipts"`
	XShardDepositReceipts []goldenReceipt          `json:"xshard_deposit_receipts"`
	ProducedDeposits      []goldenDeposit          `json:"produced_deposits"`
	ConsumedDeposits      []goldenDeposit          `json:"consumed_deposits"`
	Accounts              map[string]goldenAccount `json:"accounts"`
	Rejected              bool                     `json:"rejected"`
	ErrorType             string                   `json:"error_type"`
	Error                 string                   `json:"error"`
}

type goldenBlockStep struct {
	Comment string            `json:"comment"`
	Expect  string            `json:"expect"`
	Block   string            `json:"block"`
	Result  goldenBlockResult `json:"result"`
}

type goldenRootBlock struct {
	Height uint64 `json:"height"`
	Hash   string `json:"hash"`
	Block  string `json:"block"`
}

type goldenXShardList struct {
	MinorBlockHash string          `json:"minor_block_hash"`
	FullShardID    uint32          `json:"full_shard_id"`
	Deposits       []goldenDeposit `json:"deposits"`
}

type goldenBlockCase struct {
	Name         string                      `json:"name"`
	Comment      string                      `json:"comment"`
	Network      string                      `json:"network"`
	FullShardID  uint32                      `json:"full_shard_id"`
	GenesisAlloc map[string]goldenAllocation `json:"genesis_alloc"`
	Genesis      struct {
		StateRoot string `json:"state_root"`
		Block     string `json:"block"`
	} `json:"genesis"`
	RootBlocks  []goldenRootBlock  `json:"root_blocks"`
	XShardLists []goldenXShardList `json:"xshard_lists"`
	Blocks      []goldenBlockStep  `json:"blocks"`
}

func loadBlockGolden(t *testing.T) []goldenBlockCase {
	t.Helper()
	raw, err := os.ReadFile(blockGoldenPath)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var file struct {
		Cases []goldenBlockCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if len(file.Cases) == 0 {
		t.Fatal("golden has no cases")
	}
	return file.Cases
}

// goldenChain is the chain layer a case describes: the root blocks the shard
// saw, the deposit lists its neighbours sent, and the shard's own headers as
// they are executed. It serves both the XShardSource and the two header
// lookups, which is exactly the split ShardState holds in one object.
type goldenChain struct {
	roots       map[common.Hash]*types.RootBlock
	deposits    map[common.Hash][]*types.CrossShardTransactionDeposit
	headers     map[common.Hash]*types.MinorBlockHeader
	metas       map[common.Hash]*types.MinorBlockMeta
	depositsFor map[common.Hash]bool
}

func (c *goldenChain) RootBlockByHeight(tip common.Hash, height uint64) (*types.RootBlock, error) {
	// Walk back from the tip rather than indexing by height: the traversal is
	// fork-aware, and answering from a flat index would hide a wrong tip.
	for block, ok := c.roots[tip]; ok; block, ok = c.roots[block.Header.ParentHash] {
		if uint64(block.Header.Number) == height {
			return block, nil
		}
		if block.Header.Number == 0 {
			break
		}
	}
	return nil, nil
}

func (c *goldenChain) RootHeaderByHash(hash common.Hash) (*types.RootBlockHeader, error) {
	if block, ok := c.roots[hash]; ok {
		return block.Header, nil
	}
	return nil, nil
}

func (c *goldenChain) DepositsByMinorBlockHash(hash common.Hash) ([]*types.CrossShardTransactionDeposit, bool, error) {
	deposits, ok := c.deposits[hash]
	return deposits, ok, nil
}

func (c *goldenChain) headerByHash(hash common.Hash) (*types.MinorBlockHeader, error) {
	return c.headers[hash], nil
}

func (c *goldenChain) metaByHash(hash common.Hash) (*types.MinorBlockMeta, error) {
	return c.metas[hash], nil
}

func (c *goldenChain) addMinorBlock(block *types.MinorBlock) {
	c.headers[block.Hash()] = block.Header
	c.metas[block.Hash()] = block.Meta
}

func decodeMinorBlock(t *testing.T, encoded string) *types.MinorBlock {
	t.Helper()
	var block types.MinorBlock
	if err := serialize.DeserializeFromBytes(common.FromHex(encoded), &block); err != nil {
		t.Fatalf("decode minor block: %v", err)
	}
	return &block
}

func decodeRootBlock(t *testing.T, encoded string) *types.RootBlock {
	t.Helper()
	var block types.RootBlock
	if err := serialize.DeserializeFromBytes(common.FromHex(encoded), &block); err != nil {
		t.Fatalf("decode root block: %v", err)
	}
	return &block
}

// newBlockCaseChain rebuilds the shard's starting point: the allocation
// committed to the genesis state root the vector names, and the chain data the
// blocks will be executed against.
func newBlockCaseChain(t *testing.T, tc *goldenBlockCase) (*goldenChain, *ExecutionContext, ethdb.Database, *triedb.Database, common.Hash) {
	t.Helper()
	cluster := network(t, tc.Network)
	shard := cluster.Quarkchain.GetShardConfigByFullShardID(tc.FullShardID)
	if shard == nil {
		t.Fatalf("network %s has no shard %#x", tc.Network, tc.FullShardID)
	}

	db := rawdb.NewMemoryDatabase()
	tdb := qkcstate.NewDatabase(db)
	state, err := qkcstate.New(coretypes.EmptyRootHash, db, tdb)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	state.SetConfig(cluster.Quarkchain, shard)
	applyGoldenAlloc(t, state, tc.GenesisAlloc)
	genesisRoot, err := state.Commit(0)
	if err != nil {
		t.Fatalf("commit allocation: %v", err)
	}
	if want := common.HexToHash(tc.Genesis.StateRoot); genesisRoot != want {
		t.Fatalf("genesis state root = %s, want %s: the allocation is the shard's "+
			"GENESIS.ALLOC and nothing else should have been applied", genesisRoot, want)
	}

	chain := &goldenChain{
		roots:    map[common.Hash]*types.RootBlock{},
		deposits: map[common.Hash][]*types.CrossShardTransactionDeposit{},
		headers:  map[common.Hash]*types.MinorBlockHeader{},
		metas:    map[common.Hash]*types.MinorBlockMeta{},
	}
	for _, rootSpec := range tc.RootBlocks {
		root := decodeRootBlock(t, rootSpec.Block)
		if got := root.Hash(); got != common.HexToHash(rootSpec.Hash) {
			t.Fatalf("root block %d hash = %s, want %s", rootSpec.Height, got, rootSpec.Hash)
		}
		chain.roots[root.Hash()] = root
	}
	for _, list := range tc.XShardLists {
		deposits := make([]*types.CrossShardTransactionDeposit, len(list.Deposits))
		for i := range list.Deposits {
			deposits[i] = buildDeposit(t, &list.Deposits[i])
		}
		chain.deposits[common.HexToHash(list.MinorBlockHash)] = deposits
	}
	chain.addMinorBlock(decodeMinorBlock(t, tc.Genesis.Block))

	ctx := &ExecutionContext{
		QKCConfig:            cluster.Quarkchain,
		ShardConfig:          shard,
		MinorHeaderByHash:    chain.headerByHash,
		MinorBlockMetaByHash: chain.metaByHash,
	}
	return chain, ctx, db, tdb, genesisRoot
}

// TestBlockGolden replays every block-level vector: a shard built from its
// genesis allocation, then each block in turn against the state its parent
// left, comparing all seven values a block commits to and the deposits the
// block consumed and produced.
func TestBlockGolden(t *testing.T) {
	for _, tc := range loadBlockGolden(t) {
		t.Run(tc.Name, func(t *testing.T) {
			chain, ctx, db, tdb, parentRoot := newBlockCaseChain(t, &tc)

			for i, step := range tc.Blocks {
				block := decodeMinorBlock(t, step.Block)
				root, ok := chain.roots[block.Header.PrevRootBlockHash]
				if !ok {
					t.Fatalf("block %d references root block %s, which the case does not carry",
						i, block.Header.PrevRootBlockHash)
				}
				// The root tip a node validates against is the one it had when
				// it accepted the block, which for these vectors is the root
				// block the block itself references.
				ctx.ValidationRootTip = root.Header

				result, err := ExecuteAndValidate(ctx, block, parentRoot, db, tdb, chain)
				if step.Expect == "rejected" {
					if err == nil {
						t.Fatalf("block %d: expected a rejection, it ran", i)
					}
					state, stateErr := qkcstate.New(parentRoot, db, tdb)
					if stateErr != nil {
						t.Fatalf("reopen parent state: %v", stateErr)
					}
					if got := state.Root(); got != parentRoot {
						t.Errorf("block %d: parent state moved to %s, was %s", i, got, parentRoot)
					}
					continue
				}
				if err != nil {
					t.Fatalf("block %d: %v\ncase: %s", i, err, step.Comment)
				}

				checkBlockResult(t, i, &step.Result, result)
				checkGoldenAccounts(t, result.State, step.Result.Accounts)

				chain.addMinorBlock(block)
				parentRoot = result.StateRoot
			}
		})
	}
}

func checkBlockResult(t *testing.T, index int, want *goldenBlockResult, got *ProcessResult) {
	t.Helper()
	if root := common.HexToHash(want.PostStateRoot); got.StateRoot != root {
		t.Errorf("block %d: state root = %s, want %s", index, got.StateRoot, root)
	}
	if root := common.HexToHash(want.ReceiptRoot); got.ReceiptRoot != root {
		t.Errorf("block %d: receipt root = %s, want %s", index, got.ReceiptRoot, root)
	}
	if got, want := strconv.FormatUint(got.GasUsed, 10), want.GasUsed; got != want {
		t.Errorf("block %d: gas used = %s, want %s", index, got, want)
	}
	if got, want := strconv.FormatUint(got.XShardReceiveGasUsed, 10), want.XShardReceiveGasUsed; got != want {
		t.Errorf("block %d: cross-shard receive gas used = %s, want %s", index, got, want)
	}
	cursor := types.XShardTxCursorInfo{
		RootBlockHeight:    want.Cursor.RootBlockHeight,
		MinorBlockIndex:    want.Cursor.MinorBlockIndex,
		XShardDepositIndex: want.Cursor.XShardDepositIndex,
	}
	if got.Cursor != cursor {
		t.Errorf("block %d: cursor = %+v, want %+v", index, got.Cursor, cursor)
	}
	if bloom := "0x" + common.Bytes2Hex(got.Bloom.Bytes()); bloom != want.Bloom {
		t.Errorf("block %d: bloom mismatch", index)
	}
	checkCoinbaseMap(t, index, got.CoinbaseAmountMap, want.CoinbaseAmountMap)
	checkReceipts(t, "receipt", got.Receipts, want.Receipts)
	checkReceipts(t, "deposit receipt", got.XShardDepositReceipts, want.XShardDepositReceipts)
	checkDeposits(t, got.ProducedDeposits, want.ProducedDeposits)
	checkDeposits(t, got.ConsumedDeposits, want.ConsumedDeposits)
}

func checkCoinbaseMap(t *testing.T, index int, got map[uint64]*big.Int, want map[string]string) {
	t.Helper()
	nonZero := 0
	for _, amount := range got {
		if amount.Sign() != 0 {
			nonZero++
		}
	}
	if nonZero != len(want) {
		t.Errorf("block %d: coinbase holds %d non-zero tokens, want %d", index, nonZero, len(want))
	}
	for token, amount := range want {
		id, err := strconv.ParseUint(token, 10, 64)
		if err != nil {
			t.Fatalf("token id %q: %v", token, err)
		}
		have := got[id]
		if have == nil {
			have = new(big.Int)
		}
		if have.String() != amount {
			t.Errorf("block %d: coinbase token %s = %s, want %s", index, token, have, amount)
		}
	}
}
