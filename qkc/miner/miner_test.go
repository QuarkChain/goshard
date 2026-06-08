package miner

import (
	"math/big"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/consensus"
	"github.com/ethereum/go-ethereum/qkc/consensus/doublesha256"
	qkccore "github.com/ethereum/go-ethereum/qkc/core"
	qkcrawdb "github.com/ethereum/go-ethereum/qkc/core/rawdb"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/stretchr/testify/assert"
)

const testFullShardId = uint32(2)

// minerTestAPI implements MinerAPI over a real MinorBlockChain, mirroring how a
// shard runner wires the miner.
type minerTestAPI struct {
	chain  *qkccore.MinorBlockChain
	engine consensus.Engine
}

func (a *minerTestAPI) GetDefaultCoinbaseAddress() account.Address {
	return account.CreatEmptyAddress(testFullShardId)
}

func (a *minerTestAPI) CreateBlockToMine(addr *account.Address) (types.IBlock, *big.Int, uint64, error) {
	parent := a.chain.CurrentBlock()
	createTime := uint64(time.Now().Unix())
	if createTime <= parent.Time() {
		createTime = parent.Time() + 1
	}
	diff, err := a.engine.CalcDifficulty(a.chain, createTime, parent)
	if err != nil {
		return nil, nil, 0, err
	}
	next := parent.CreateBlockToAppend(&createTime, diff, addr, nil, nil, nil, nil, nil, nil)
	next.Finalize(types.Receipts{}, types.EmptyTrieHash, nil, nil,
		types.NewEmptyTokenBalances(), parent.Meta().XShardTxCursorInfo)
	return next, diff, 1, nil
}

func (a *minerTestAPI) InsertMinedBlock(block types.IBlock) error {
	_, err := a.chain.InsertChain([]types.IBlock{block}, false)
	return err
}

func (a *minerTestAPI) IsSyncing() bool { return false }
func (a *minerTestAPI) GetTip() uint64  { return a.chain.CurrentBlock().NumberU64() }

func TestMinerProducesBlocks(t *testing.T) {
	clusterCfg := config.NewClusterConfig()
	clusterCfg.Quarkchain.SkipMinorDifficultyCheck = true // diff is deterministic; skip recompute race

	db := ethrawdb.NewMemoryDatabase()
	gspec := qkccore.NewGenesis(clusterCfg.Quarkchain)
	rootBlock := gspec.CreateRootBlock()
	qkcrawdb.WriteRootBlock(db, rootBlock)
	gspec.MustCommitMinorBlock(db, rootBlock, testFullShardId)

	diffCalc := &consensus.EthDifficultyCalculator{AdjustmentCutoff: 7, AdjustmentFactor: 512, MinimumDifficulty: big.NewInt(10000)}
	engine := doublesha256.New(diffCalc, false, nil) // real PoW, instant at genesis difficulty 10000
	chain, err := qkccore.NewMinorBlockChain(db, nil, nil, clusterCfg, engine, vm.Config{}, nil, testFullShardId)
	assert.NoError(t, err)
	defer chain.Stop()

	api := &minerTestAPI{chain: chain, engine: engine}
	m := New(api, engine)
	defer m.Stop()

	// Drive continuation the way a shard does: chain-head event -> HandleNewTip.
	const target = 3
	headCh := make(chan qkccore.MinorChainHeadEvent, target+1)
	sub := chain.SubscribeChainHeadEvent(headCh)
	defer sub.Unsubscribe()

	m.SetMining(true)

	deadline := time.After(20 * time.Second)
	for produced := 0; produced < target; {
		select {
		case ev := <-headCh:
			produced++
			assert.Equal(t, uint64(produced), ev.Block.NumberU64())
			if produced < target {
				m.HandleNewTip()
			}
		case <-deadline:
			t.Fatalf("timed out after %d blocks", produced)
		}
	}

	assert.Equal(t, uint64(target), chain.CurrentBlock().NumberU64())
}
