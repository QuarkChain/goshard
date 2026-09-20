// Copyright 2026-2027, QuarkChain.

package core

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/types"
)

var errMinorChainTestBatch = errors.New("minor chain test batch write failed")

type storageTestProcessor struct{}

func (storageTestProcessor) Process(*types.MinorBlock, *state.StateDB, XShardCursor, vm.Config) (*ProcessResult, error) {
	return nil, errors.New("storage test processor must not be called")
}

type batchTrackingDatabase struct {
	ethdb.Database
	openBatches int
	failWrites  bool
}

func (db *batchTrackingDatabase) NewBatch() ethdb.Batch {
	db.openBatches++
	return &trackedMinorBatch{Batch: db.Database.NewBatch(), db: db}
}

type trackedMinorBatch struct {
	ethdb.Batch
	db *batchTrackingDatabase
}

func (batch *trackedMinorBatch) Write() error {
	if batch.db.failWrites {
		return errMinorChainTestBatch
	}
	return batch.Batch.Write()
}

func (batch *trackedMinorBatch) Close() {
	batch.db.openBatches--
	batch.Batch.Close()
}

func TestNewMinorBlockChainRejectsMissingDependencies(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { db.Close() })
	config := storageTestShardConfig()
	processor := storageTestProcessor{}
	validator := NewMinorBlockValidator()

	if _, err := NewMinorBlockChain(nil, config, processor, validator, vm.Config{}); !errors.Is(err, ErrDatabaseUnavailable) {
		t.Fatalf("nil database error = %v, want %v", err, ErrDatabaseUnavailable)
	}
	if _, err := NewMinorBlockChain(db, nil, processor, validator, vm.Config{}); !errors.Is(err, ErrShardConfigUnavailable) {
		t.Fatalf("nil shard config error = %v, want %v", err, ErrShardConfigUnavailable)
	}
	if _, err := NewMinorBlockChain(db, config, nil, validator, vm.Config{}); !errors.Is(err, ErrExecutorUnavailable) {
		t.Fatalf("nil processor error = %v, want %v", err, ErrExecutorUnavailable)
	}
	if _, err := NewMinorBlockChain(db, config, processor, nil, vm.Config{}); !errors.Is(err, ErrValidatorUnavailable) {
		t.Fatalf("nil validator error = %v, want %v", err, ErrValidatorUnavailable)
	}
	if _, err := NewMinorBlockChain(db, config, processor, validator, vm.Config{}); !errors.Is(err, ErrNoGenesis) {
		t.Fatalf("missing genesis error = %v, want %v", err, ErrNoGenesis)
	}
}

func TestNewMinorBlockChainInitializesGenesisHead(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { db.Close() })
	genesis := storageTestBlock(nil, 0, coretypes.EmptyRootHash)
	writeStorageTestBlock(db, genesis, true)

	chain, err := NewMinorBlockChain(db, storageTestShardConfig(), storageTestProcessor{}, NewMinorBlockValidator(), vm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(chain.Stop)

	if got := chain.GenesisHash(); got != genesis.Hash() {
		t.Fatalf("genesis hash = %s, want %s", got, genesis.Hash())
	}
	height, hash := chain.Head()
	if height != 0 || hash != genesis.Hash() {
		t.Fatalf("head = %d/%s, want 0/%s", height, hash, genesis.Hash())
	}
	if current := chain.CurrentBlock(); current == nil || current.Hash() != genesis.Hash() {
		t.Fatalf("current block = %v, want genesis %s", current, genesis.Hash())
	}
	if rawdb.ReadHeadBlockHash(db) != genesis.Hash() || rawdb.ReadHeadHeaderHash(db) != genesis.Hash() {
		t.Fatal("persistent head markers do not point to genesis")
	}
	if block := chain.GetBlock(genesis.Hash()); block == nil || block.Hash() != genesis.Hash() {
		t.Fatal("genesis is not available by hash")
	}
	if block := chain.GetBlockByNumber(0); block == nil || block.Hash() != genesis.Hash() {
		t.Fatal("genesis is not available by number")
	}
	if !chain.HasBlock(genesis.Hash()) || !chain.HasBlockAndState(genesis.Hash()) {
		t.Fatal("genesis block or state is unavailable")
	}
	if !chain.HasState(genesis.Root()) {
		t.Fatal("genesis state is unavailable")
	}
	if _, err := chain.StateAt(genesis.Root()); err != nil {
		t.Fatalf("open genesis state: %v", err)
	}
	if got := chain.EthChainID(); got != storageTestShardConfig().EthChainID {
		t.Fatalf("eth chain ID = %d, want %d", got, storageTestShardConfig().EthChainID)
	}

	chain.Stop()
}

func TestNewMinorBlockChainRestoresPersistedHead(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { db.Close() })
	genesis := storageTestBlock(nil, 0, coretypes.EmptyRootHash)
	head := storageTestBlock(genesis, 1, coretypes.EmptyRootHash)
	writeStorageTestBlock(db, genesis, true)
	writeStorageTestBlock(db, head, true)
	rawdb.WriteHeadHeaderHash(db, head.Hash())
	rawdb.WriteHeadBlockHash(db, head.Hash())

	chain, err := NewMinorBlockChain(db, storageTestShardConfig(), storageTestProcessor{}, NewMinorBlockValidator(), vm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(chain.Stop)
	height, hash := chain.Head()
	if height != 1 || hash != head.Hash() {
		t.Fatalf("restored head = %d/%s, want 1/%s", height, hash, head.Hash())
	}
	if chain.GenesisHash() != genesis.Hash() {
		t.Fatalf("restored genesis = %s, want %s", chain.GenesisHash(), genesis.Hash())
	}
}

func TestNewMinorBlockChainRejectsUnavailableHead(t *testing.T) {
	tests := []struct {
		name      string
		head      *types.MinorBlock
		wantError error
	}{
		{
			name:      "missing block",
			wantError: ErrNoCurrentBlock,
		},
		{
			name:      "missing state",
			head:      storageTestBlock(nil, 1, common.HexToHash("0x1234")),
			wantError: ErrStateUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			t.Cleanup(func() { db.Close() })
			genesis := storageTestBlock(nil, 0, coretypes.EmptyRootHash)
			writeStorageTestBlock(db, genesis, true)
			headHash := common.HexToHash("0x5678")
			if test.head != nil {
				rawdb.WriteMinorBlock(db, test.head)
				headHash = test.head.Hash()
			}
			rawdb.WriteHeadHeaderHash(db, headHash)
			rawdb.WriteHeadBlockHash(db, headHash)

			_, err := NewMinorBlockChain(db, storageTestShardConfig(), storageTestProcessor{}, NewMinorBlockValidator(), vm.Config{})
			if !errors.Is(err, test.wantError) {
				t.Fatalf("constructor error = %v, want %v", err, test.wantError)
			}
			if rawdb.ReadHeadBlockHash(db) != headHash || rawdb.ReadHeadHeaderHash(db) != headHash {
				t.Fatal("failed recovery rewrote persistent head markers")
			}
		})
	}
}

func TestNewMinorBlockChainRejectsMissingGenesisState(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { db.Close() })
	genesis := storageTestBlock(nil, 0, common.HexToHash("0x1234"))
	writeStorageTestBlock(db, genesis, true)

	_, err := NewMinorBlockChain(db, storageTestShardConfig(), storageTestProcessor{}, NewMinorBlockValidator(), vm.Config{})
	if !errors.Is(err, ErrStateUnavailable) {
		t.Fatalf("constructor error = %v, want %v", err, ErrStateUnavailable)
	}
	if rawdb.ReadHeadBlockHash(db) != (common.Hash{}) || rawdb.ReadHeadHeaderHash(db) != (common.Hash{}) {
		t.Fatal("missing genesis state published head markers")
	}
}

func TestNewMinorBlockChainReleasesFailedGenesisBatch(t *testing.T) {
	db := &batchTrackingDatabase{Database: rawdb.NewMemoryDatabase()}
	t.Cleanup(func() { db.Close() })
	genesis := storageTestBlock(nil, 0, coretypes.EmptyRootHash)
	writeStorageTestBlock(db, genesis, true)
	db.failWrites = true

	_, err := NewMinorBlockChain(db, storageTestShardConfig(), storageTestProcessor{}, NewMinorBlockValidator(), vm.Config{})
	if !errors.Is(err, errMinorChainTestBatch) {
		t.Fatalf("constructor error = %v, want %v", err, errMinorChainTestBatch)
	}
	if db.openBatches != 0 {
		t.Fatalf("constructor left %d batches open", db.openBatches)
	}
	if rawdb.ReadHeadBlockHash(db) != (common.Hash{}) || rawdb.ReadHeadHeaderHash(db) != (common.Hash{}) {
		t.Fatal("failed genesis batch published head markers")
	}
}

func writeStorageTestBlock(db ethdb.KeyValueWriter, block *types.MinorBlock, canonical bool) {
	rawdb.WriteMinorBlock(db, block)
	if canonical {
		rawdb.WriteMinorCanonicalHash(db, block.Hash(), block.NumberU64())
	}
}

func storageTestBlock(parent *types.MinorBlock, number uint64, root common.Hash) *types.MinorBlock {
	meta := &types.MinorBlockMeta{
		Root:               root,
		GasUsed:            &serialize.Uint256{Value: new(big.Int)},
		CrossShardGasUsed:  &serialize.Uint256{Value: new(big.Int)},
		XShardTxCursorInfo: new(types.XShardTxCursorInfo),
		XShardGasLimit:     &serialize.Uint256{Value: big.NewInt(6_000_000)},
	}
	header := &types.MinorBlockHeader{
		Branch:         account.NewBranch(1),
		Number:         number,
		CoinbaseAmount: qkccommon.NewEmptyTokenBalances(),
		GasLimit:       &serialize.Uint256{Value: big.NewInt(12_000_000)},
		MetaHash:       meta.Hash(),
		Difficulty:     big.NewInt(1),
	}
	if parent != nil {
		header.ParentHash = parent.Hash()
	}
	return types.NewMinorBlockWithHeader(header, meta)
}

func storageTestShardConfig() *config.ShardConfig {
	chainConfig := config.NewChainConfig()
	chainConfig.ConsensusConfig = config.NewPOWConfig()
	shardConfig := config.NewShardConfig(chainConfig)
	rootConfig := config.NewRootConfig()
	rootConfig.ConsensusConfig = config.NewPOWConfig()
	shardConfig.SetRootConfig(rootConfig)
	shardConfig.ShardSize = 1
	shardConfig.ShardID = 0
	return shardConfig
}
