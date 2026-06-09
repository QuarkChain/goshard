package simulate

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/consensus"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/stretchr/testify/assert"
)

// TestSeal checks that the simulated engine produces a sealed block (paced by
// the block interval; 0 here for an instant test) and that its seal always
// verifies.
func TestSeal(t *testing.T) {
	assert := assert.New(t)
	diffCalc := &consensus.EthDifficultyCalculator{AdjustmentCutoff: 7, AdjustmentFactor: 512, MinimumDifficulty: big.NewInt(10000)}

	header := &types.RootBlockHeader{Number: 1, Difficulty: big.NewInt(10000)}
	block := types.NewRootBlockWithHeader(header)
	e := New(diffCalc, false, nil, 0 /* blockInterval: instant */)

	resultsCh := make(chan types.IBlock)
	err := e.Seal(nil, block, nil, 1, resultsCh, nil)
	assert.NoError(err, "should have no problem sealing the block")
	sealed := <-resultsCh

	// The seal carries the engine's random digest.
	assert.NotEqual(types.EmptyHash, sealed.IHeader().GetMixDigest(), "mix digest should be set")
	// Simulated seal verification is unconditional.
	assert.NoError(e.VerifySeal(nil, sealed.IHeader(), big.NewInt(0)))
	assert.Equal(config.PoWSimulate, e.Name())
}
