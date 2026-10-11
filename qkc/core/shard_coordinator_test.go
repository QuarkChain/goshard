// Copyright 2026-2027, QuarkChain.

package core

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	ethcore "github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

const minorBlockInputKey = "qkc-demo/minor-block/1"

type minorBlockFixture struct {
	GenesisData       string   `json:"genesis_data"`
	BlockData         string   `json:"block_data"`
	GenesisHash       string   `json:"genesis_hash"`
	BlockHash         string   `json:"block_hash"`
	MetaHash          string   `json:"meta_hash"`
	TransactionHashes []string `json:"transaction_hashes"`
	PreviousRootHash  string   `json:"previous_root_hash"`
}

type testRootChainView map[common.Hash]struct{}

func (roots testRootChainView) HasRootBlock(hash common.Hash) bool {
	_, ok := roots[hash]
	return ok
}

func TestShardCoordinatorImportsMinorBlockFromDatabase(t *testing.T) {
	fixture := loadMinorBlockFixture(t)
	genesisData := decodeHexFixture(t, fixture.GenesisData)
	blockData := decodeHexFixture(t, fixture.BlockData)
	genesis := decodeMinorBlock(t, genesisData)
	if genesis.Hash() != common.HexToHash(fixture.GenesisHash) {
		t.Fatalf("genesis hash = %s, want %s", genesis.Hash(), fixture.GenesisHash)
	}
	targetDB, chain := newCandidateTestChain(t, genesis)
	headBefore := chain.CurrentBlock().Hash()

	inputDB := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { inputDB.Close() })
	if err := inputDB.Put([]byte(minorBlockInputKey), blockData); err != nil {
		t.Fatal(err)
	}
	encoded, err := inputDB.Get([]byte(minorBlockInputKey))
	if err != nil {
		t.Fatal(err)
	}
	block := decodeMinorBlock(t, encoded)
	assertFixtureHashes(t, fixture, block)

	adapter, err := NewGethBlockChainAdapter(chain)
	if err != nil {
		t.Fatal(err)
	}
	roots := testRootChainView{common.HexToHash(fixture.PreviousRootHash): {}}
	coordinator, err := NewShardCoordinator(block.Branch(), roots, adapter, coretypes.NewQKCSigner(3, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.AddMinorBlock(block); err != nil {
		t.Fatal(err)
	}

	stored := chain.GetBlockByHash(block.Hash())
	if stored == nil {
		t.Fatal("inserted candidate was not readable by hash")
	}
	storedData, err := serialize.SerializeToBytes(stored)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(storedData, blockData) {
		t.Fatalf("stored candidate encoding differs from input:\n have %x\n want %x", storedData, blockData)
	}
	if headAfter := chain.CurrentBlock().Hash(); headAfter != headBefore {
		t.Fatalf("canonical head changed from %s to %s", headBefore, headAfter)
	}
	if chain.HasBlockAndState(block.Hash(), block.NumberU64()) {
		t.Fatal("candidate unexpectedly has executable state")
	}
	if receipts := rawdb.ReadRawReceipts(targetDB, block.Hash(), block.NumberU64()); receipts != nil {
		t.Fatalf("candidate unexpectedly has receipts: %d", len(receipts))
	}
	for _, tx := range block.Transactions() {
		if number := rawdb.ReadTxLookupEntry(targetDB, tx.Hash()); number != nil {
			t.Fatalf("candidate transaction %s has lookup entry at block %d", tx.Hash(), *number)
		}
	}
	if canonical := rawdb.ReadCanonicalHash(targetDB, block.NumberU64()); canonical == block.Hash() {
		t.Fatalf("candidate %s was written as canonical", block.Hash())
	}
	if err := coordinator.AddMinorBlock(block); err != nil {
		t.Fatalf("exact duplicate was not idempotent: %v", err)
	}
	conflictingBody := *block.Body()
	conflictingBody.TrackingData = append(conflictingBody.TrackingData, 0xff)
	conflicting := block.WithBody(conflictingBody)
	if err := coordinator.AddMinorBlock(conflicting); err == nil {
		t.Fatal("candidate with conflicting tracking data was accepted as an exact duplicate")
	}
	storedData, err = serialize.SerializeToBytes(chain.GetBlockByHash(block.Hash()))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(storedData, blockData) {
		t.Fatal("conflicting duplicate changed the stored candidate")
	}
}

func TestShardCoordinatorRejectsUnknownParentWithoutWrite(t *testing.T) {
	fixture := loadMinorBlockFixture(t)
	genesis := decodeMinorBlock(t, decodeHexFixture(t, fixture.GenesisData))
	targetDB, chain := newCandidateTestChain(t, genesis)
	block := decodeMinorBlock(t, decodeHexFixture(t, fixture.BlockData))
	header := block.Header()
	header.ParentHash = common.HexToHash("0xdeadbeef")
	invalid := block.WithSeal(header)

	adapter, err := NewGethBlockChainAdapter(chain)
	if err != nil {
		t.Fatal(err)
	}
	roots := testRootChainView{invalid.PrevRootBlockHash(): {}}
	coordinator, err := NewShardCoordinator(invalid.Branch(), roots, adapter, coretypes.NewQKCSigner(3, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.AddMinorBlock(invalid); err == nil {
		t.Fatal("candidate with unknown parent was accepted")
	}
	if rawdb.HasHeader(targetDB, invalid.Hash(), invalid.NumberU64()) || rawdb.HasBody(targetDB, invalid.Hash(), invalid.NumberU64()) {
		t.Fatal("rejected candidate was written to the database")
	}
	if err := coordinator.AddMinorBlock(new(coretypes.Block)); err == nil {
		t.Fatal("candidate without a header was accepted")
	}
}

func TestShardCoordinatorRejectsInvalidSignatureWithoutWrite(t *testing.T) {
	fixture := loadMinorBlockFixture(t)
	genesis := decodeMinorBlock(t, decodeHexFixture(t, fixture.GenesisData))
	targetDB, chain := newCandidateTestChain(t, genesis)
	invalid := decodeMinorBlock(t, decodeHexFixture(t, fixture.BlockData))

	invalid.Transactions()[0].SetVRS(big.NewInt(27), new(big.Int), new(big.Int))
	header := invalid.Header()
	header.TxHash = coretypes.CalculateMerkleRoot(invalid.Transactions())
	meta, err := header.ToMinorBlockMeta()
	if err != nil {
		t.Fatal(err)
	}
	header.MetaHash = meta.Hash()
	invalid = invalid.WithSeal(header)
	if err := invalid.Validate(); err != nil {
		t.Fatalf("signature test block is structurally invalid: %v", err)
	}

	adapter, err := NewGethBlockChainAdapter(chain)
	if err != nil {
		t.Fatal(err)
	}
	roots := testRootChainView{invalid.PrevRootBlockHash(): {}}
	coordinator, err := NewShardCoordinator(invalid.Branch(), roots, adapter, coretypes.NewQKCSigner(3, 1))
	if err != nil {
		t.Fatal(err)
	}
	err = coordinator.AddMinorBlock(invalid)
	if err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("candidate with invalid signature returned %v", err)
	}
	if rawdb.HasHeader(targetDB, invalid.Hash(), invalid.NumberU64()) || rawdb.HasBody(targetDB, invalid.Hash(), invalid.NumberU64()) {
		t.Fatal("candidate with invalid signature was written to the database")
	}
}

func TestCommitQKCGenesisRejectsInvalidHeader(t *testing.T) {
	fixture := loadMinorBlockFixture(t)
	genesis := decodeMinorBlock(t, decodeHexFixture(t, fixture.GenesisData))

	tests := map[string]func(*coretypes.Header){
		"overflow number": func(header *coretypes.Header) {
			header.Number = new(big.Int).Lsh(big.NewInt(1), 64)
		},
		"missing QKC marker": func(header *coretypes.Header) {
			header.Branch.Value = 0
			header.CoinbaseAmount = nil
			header.XShardTxCursorInfo = nil
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			header := genesis.Header()
			mutate(header)
			invalid := genesis.WithSeal(header)
			db := rawdb.NewMemoryDatabase()
			t.Cleanup(func() { db.Close() })
			if err := ethcore.CommitQKCGenesisBlock(db, invalid, params.AllEthashProtocolChanges); err == nil {
				t.Fatal("invalid QKC genesis was accepted")
			}
		})
	}
}

func loadMinorBlockFixture(t *testing.T) minorBlockFixture {
	t.Helper()
	data, err := os.ReadFile("testdata/minor_block_import.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture minorBlockFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func decodeHexFixture(t *testing.T, value string) []byte {
	t.Helper()
	data, err := hex.DecodeString(strings.TrimPrefix(value, "0x"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func decodeMinorBlock(t *testing.T, encoded []byte) *coretypes.Block {
	t.Helper()
	var block coretypes.Block
	if err := serialize.DeserializeFromBytes(encoded, &block); err != nil {
		t.Fatal(err)
	}
	if err := block.Validate(); err != nil {
		t.Fatal(err)
	}
	return &block
}

func assertFixtureHashes(t *testing.T, fixture minorBlockFixture, block *coretypes.Block) {
	t.Helper()
	if block.Hash() != common.HexToHash(fixture.BlockHash) {
		t.Fatalf("block hash = %s, want %s", block.Hash(), fixture.BlockHash)
	}
	if block.MetaHash() != common.HexToHash(fixture.MetaHash) {
		t.Fatalf("meta hash = %s, want %s", block.MetaHash(), fixture.MetaHash)
	}
	if len(block.Transactions()) != len(fixture.TransactionHashes) {
		t.Fatalf("transaction count = %d, want %d", len(block.Transactions()), len(fixture.TransactionHashes))
	}
	for index, tx := range block.Transactions() {
		if tx.Hash() != common.HexToHash(fixture.TransactionHashes[index]) {
			t.Fatalf("transaction %d hash = %s, want %s", index, tx.Hash(), fixture.TransactionHashes[index])
		}
	}
}

func newCandidateTestChain(t *testing.T, genesis *coretypes.Block) (ethdb.Database, *ethcore.BlockChain) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { db.Close() })
	if err := ethcore.CommitQKCGenesisBlock(db, genesis, params.AllEthashProtocolChanges); err != nil {
		t.Fatal(err)
	}
	chain, err := ethcore.NewBlockChain(db, nil, ethash.NewFaker(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(chain.Stop)
	return db, chain
}
