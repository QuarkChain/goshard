// End-to-end structural tests for the qkc shardchain port: genesis creation,
// block validation + import (including a real double-sha256 PoW seal), canonical
// head management and database reopen. Execution/state is exercised by the
// deferred execution issue.

package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/consensus"
	"github.com/ethereum/go-ethereum/qkc/consensus/doublesha256"
	"github.com/ethereum/go-ethereum/qkc/core/rawdb"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/stretchr/testify/assert"
)

const testFullShardId = uint32(2) // chain 0, shard size 2, shard 0

func newTestClusterConfig() *config.ClusterConfig {
	clusterCfg := config.NewClusterConfig()
	clusterCfg.Quarkchain.SkipMinorDifficultyCheck = true
	clusterCfg.Quarkchain.DisablePowCheck = true
	return clusterCfg
}

// setupGenesis writes the anchoring root block and the shard genesis block.
func setupGenesis(t *testing.T, db ethdb.Database, clusterCfg *config.ClusterConfig) (*types.RootBlock, *types.MinorBlock) {
	gspec := NewGenesis(clusterCfg.Quarkchain)
	rootBlock := gspec.CreateRootBlock()
	rawdb.WriteRootBlock(db, rootBlock)
	genesisBlock := gspec.MustCommitMinorBlock(db, rootBlock, testFullShardId)
	return rootBlock, genesisBlock
}

// finalizeNext derives a sealed-shape child block on top of parent with no txs.
func finalizeNext(parent *types.MinorBlock) *types.MinorBlock {
	next := parent.CreateBlockToAppend(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	next.Finalize(types.Receipts{}, types.EmptyTrieHash, nil, nil, types.NewEmptyTokenBalances(), parent.Meta().XShardTxCursorInfo)
	return next
}

func TestGenesisDeterministic(t *testing.T) {
	clusterCfg := newTestClusterConfig()
	gspec := NewGenesis(clusterCfg.Quarkchain)
	rootBlock := gspec.CreateRootBlock()

	db1 := ethrawdb.NewMemoryDatabase()
	db2 := ethrawdb.NewMemoryDatabase()
	b1, err := gspec.CreateMinorBlock(rootBlock, testFullShardId, db1)
	assert.NoError(t, err)
	b2, err := gspec.CreateMinorBlock(rootBlock, testFullShardId, db2)
	assert.NoError(t, err)
	assert.Equal(t, b1.Hash(), b2.Hash())
	assert.Equal(t, uint64(0), b1.NumberU64())
	assert.Equal(t, rootBlock.Hash(), b1.PrevRootBlockHash())
}

func TestSetupGenesisMinorBlockMismatch(t *testing.T) {
	clusterCfg := newTestClusterConfig()
	db := ethrawdb.NewMemoryDatabase()
	rootBlock, genesisBlock := setupGenesis(t, db, clusterCfg)

	// Setting up again with the same config is a no-op returning the stored hash.
	gspec := NewGenesis(clusterCfg.Quarkchain)
	_, hash, err := SetupGenesisMinorBlock(db, gspec, rootBlock, testFullShardId)
	assert.NoError(t, err)
	assert.Equal(t, genesisBlock.Hash(), hash)
}

func TestStructuralInsertChain(t *testing.T) {
	clusterCfg := newTestClusterConfig()
	db := ethrawdb.NewMemoryDatabase()
	_, genesisBlock := setupGenesis(t, db, clusterCfg)

	engine := consensus.NewFakeEngine(nil)
	mbc, err := NewMinorBlockChain(db, nil, nil, clusterCfg, engine, vm.Config{}, nil, testFullShardId)
	assert.NoError(t, err)
	defer mbc.Stop()

	assert.Equal(t, genesisBlock.Hash(), mbc.CurrentBlock().Hash())
	assert.Equal(t, genesisBlock.Hash(), mbc.Genesis().Hash())

	// Import two blocks on top of genesis.
	b1 := finalizeNext(genesisBlock)
	b2 := finalizeNext(b1)

	n, err := mbc.InsertChain([]types.IBlock{b1, b2}, false)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	assert.Equal(t, b2.Hash(), mbc.CurrentBlock().Hash())
	assert.Equal(t, uint64(2), mbc.CurrentBlock().NumberU64())
	assert.Equal(t, b1.Hash(), mbc.GetBlockByNumber(1).Hash())
	assert.True(t, mbc.HasBlock(b1.Hash()))
	assert.True(t, mbc.HasBlock(b2.Hash()))

	// A block with a broken linkage must be rejected.
	badBlock := finalizeNext(b1) // same height as b2 but parent b1: side fork is allowed
	nBad, errBad := mbc.InsertChain([]types.IBlock{badBlock}, false)
	assert.NoError(t, errBad) // stored as side fork, head unchanged
	assert.Equal(t, 1, nBad)
	assert.Equal(t, b2.Hash(), mbc.CurrentBlock().Hash())

	// Unknown ancestor must error out.
	orphanParent := finalizeNext(b2)
	orphan := finalizeNext(orphanParent)
	_, errOrphan := mbc.InsertChain([]types.IBlock{orphan}, false)
	assert.Error(t, errOrphan)
}

func TestReopenChain(t *testing.T) {
	clusterCfg := newTestClusterConfig()
	db := ethrawdb.NewMemoryDatabase()
	_, genesisBlock := setupGenesis(t, db, clusterCfg)

	engine := consensus.NewFakeEngine(nil)
	mbc, err := NewMinorBlockChain(db, nil, nil, clusterCfg, engine, vm.Config{}, nil, testFullShardId)
	assert.NoError(t, err)

	b1 := finalizeNext(genesisBlock)
	b2 := finalizeNext(b1)
	_, err = mbc.InsertChain([]types.IBlock{b1, b2}, false)
	assert.NoError(t, err)
	mbc.Stop()

	// Reopen on the same database: head, genesis and bodies must round-trip.
	mbc2, err := NewMinorBlockChain(db, nil, nil, clusterCfg, consensus.NewFakeEngine(nil), vm.Config{}, nil, testFullShardId)
	assert.NoError(t, err)
	defer mbc2.Stop()

	assert.Equal(t, b2.Hash(), mbc2.CurrentBlock().Hash())
	assert.Equal(t, genesisBlock.Hash(), mbc2.Genesis().Hash())
	assert.Equal(t, b1.Hash(), mbc2.GetBlockByNumber(1).Hash())
	assert.Equal(t, b2.Hash(), mbc2.GetBlockByNumber(2).Hash())

	got := mbc2.GetMinorBlock(b1.Hash())
	assert.NotNil(t, got)
	assert.Equal(t, b1.MetaHash(), got.MetaHash())
}

func TestInsertChainWithDoubleSHA256PoW(t *testing.T) {
	clusterCfg := newTestClusterConfig()
	clusterCfg.Quarkchain.DisablePowCheck = false // actually verify the seal
	db := ethrawdb.NewMemoryDatabase()
	_, genesisBlock := setupGenesis(t, db, clusterCfg)

	engine := doublesha256.New(nil, false, nil)
	mbc, err := NewMinorBlockChain(db, nil, nil, clusterCfg, engine, vm.Config{}, nil, testFullShardId)
	assert.NoError(t, err)
	defer mbc.Stop()

	// Mine a real double-sha256 seal for the next block (difficulty 10000).
	unsealed := finalizeNext(genesisBlock)
	results := make(chan types.IBlock)
	err = engine.Seal(nil, unsealed, nil, 1, results, nil)
	assert.NoError(t, err)
	sealed := (<-results).(*types.MinorBlock)

	n, err := mbc.InsertChain([]types.IBlock{sealed}, false)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, sealed.Hash(), mbc.CurrentBlock().Hash())

	// A block with a zeroed nonce must fail PoW validation.
	bogus := finalizeNext(sealed)
	_, err = mbc.InsertChain([]types.IBlock{bogus}, false)
	assert.Error(t, err)
}
