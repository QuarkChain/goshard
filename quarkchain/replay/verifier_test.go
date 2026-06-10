// Copyright 2026-2027, QuarkChain.

package replay

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	qkcstate "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestFullShardIDFromKey(t *testing.T) {
	config := testClusterConfig(t)
	got, err := FullShardIDFromKey(config, 0x0007c953)
	if err != nil {
		t.Fatal(err)
	}
	if got != 0x00070001 {
		t.Fatalf("wrong full shard id: got %#x want 0x00070001", got)
	}
}

func TestVerifierEmptyBlockCoinbase(t *testing.T) {
	config := testClusterConfig(t)
	miner := common.HexToAddress("0x1111111111111111111111111111111111111111")
	expected, err := qkcstate.BuildQuarkChainStateRoot(map[common.Address]qkcstate.QuarkChainAccount{
		miner: {
			TokenBalances: map[uint64]*big.Int{35760: big.NewInt(100)},
			FullShardKey:  0,
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewVerifier(config, 0x00070001)
	if err != nil {
		t.Fatal(err)
	}
	result, err := verifier.VerifyBlock(&MinorBlockInput{
		FullShardID:       0x00070001,
		Height:            1,
		Hash:              common.HexToHash("0x01"),
		ExpectedStateRoot: expected,
		Coinbase: QKCAddress{
			Recipient:    miner,
			FullShardKey: 0x00070001,
		},
		CoinbaseAmountMap: []TokenBalance{{TokenID: 35760, Balance: big.NewInt(100)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.GotStateRoot != expected {
		t.Fatalf("wrong state root: got %s want %s", result.GotStateRoot, expected)
	}
}

func TestVerifierGenesisRoot(t *testing.T) {
	config := testClusterConfig(t)
	verifier, err := NewVerifier(config, 0x00070001)
	if err != nil {
		t.Fatal(err)
	}
	result, err := verifier.VerifyBlock(&MinorBlockInput{
		FullShardID:       0x00070001,
		Height:            0,
		Hash:              common.HexToHash("0x01"),
		ExpectedStateRoot: types.EmptyRootHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.GotStateRoot != types.EmptyRootHash {
		t.Fatalf("wrong genesis root: got %s want %s", result.GotStateRoot, types.EmptyRootHash)
	}
}

func TestVerifierStopsOnTransactions(t *testing.T) {
	config := testClusterConfig(t)
	verifier, err := NewVerifier(config, 0x00070001)
	if err != nil {
		t.Fatal(err)
	}
	_, err = verifier.VerifyBlock(&MinorBlockInput{
		FullShardID:       0x00070001,
		Height:            1,
		Hash:              common.HexToHash("0x01"),
		ExpectedStateRoot: types.EmptyRootHash,
		Transactions:      []TransactionInput{{Hash: common.HexToHash("0x02")}},
	})
	if _, ok := err.(*UnsupportedBlockError); !ok {
		t.Fatalf("expected UnsupportedBlockError, got %T %v", err, err)
	}
}

func testClusterConfig(t *testing.T) *qkcstate.QuarkChainClusterGenesisConfig {
	t.Helper()
	config, err := qkcstate.ParseQuarkChainClusterGenesisConfig([]byte(`{
		"GENESIS_DIR": null,
		"QUARKCHAIN": {
			"GENESIS_TOKEN": "QKC",
			"BASE_ETH_CHAIN_ID": 100000,
			"CHAINS": [{
				"CHAIN_ID": 7,
				"SHARD_SIZE": 1,
				"GENESIS": {"ALLOC": {}}
			}]
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	return config
}
