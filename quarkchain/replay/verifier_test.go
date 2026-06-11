// Copyright 2026-2027, QuarkChain.

package replay

import (
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	qkcstate "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
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

func TestVerifierAppliesMinimalTransfer(t *testing.T) {
	config := testClusterConfig(t)
	qkcID, err := qkcstate.QuarkChainTokenIDEncode("QKC")
	if err != nil {
		t.Fatal(err)
	}
	key := mustTestKey(t)
	from := crypto.PubkeyToAddress(key.PublicKey)
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	miner := common.HexToAddress("0x3333333333333333333333333333333333333333")
	tx := signedTransferInput(t, key, to, qkcID)

	expectedAccounts := map[common.Address]qkcstate.QuarkChainAccount{
		from: {
			Nonce:         1,
			TokenBalances: map[uint64]*big.Int{qkcID: big.NewInt(57900)},
			FullShardKey:  tx.FromFullShardKey,
		},
		to: {
			TokenBalances: map[uint64]*big.Int{qkcID: big.NewInt(100)},
			FullShardKey:  tx.ToFullShardKey,
		},
		miner: {
			TokenBalances: map[uint64]*big.Int{qkcID: big.NewInt(21012)},
			FullShardKey:  0,
		},
	}
	expected, err := qkcstate.BuildQuarkChainStateRoot(expectedAccounts, nil)
	if err != nil {
		t.Fatal(err)
	}

	verifier, err := NewVerifier(config, 0x00070001)
	if err != nil {
		t.Fatal(err)
	}
	verifier.accounts[from] = qkcstate.QuarkChainAccount{
		TokenBalances: map[uint64]*big.Int{qkcID: big.NewInt(100000)},
		FullShardKey:  tx.FromFullShardKey,
	}
	verifier.accounts[miner] = qkcstate.QuarkChainAccount{
		TokenBalances: map[uint64]*big.Int{qkcID: big.NewInt(5)},
		FullShardKey:  0,
	}
	result, err := verifier.VerifyBlock(&MinorBlockInput{
		FullShardID:       0x00070001,
		Height:            1,
		Hash:              common.HexToHash("0x01"),
		ExpectedStateRoot: expected,
		GasUsed:           qkcTransferIntrinsicGas,
		Coinbase: QKCAddress{
			Recipient:    miner,
			FullShardKey: 0x00070001,
		},
		CoinbaseAmountMap: []TokenBalance{{TokenID: qkcID, Balance: big.NewInt(21007)}},
		Transactions:      []TransactionInput{tx},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.GotStateRoot != expected {
		t.Fatalf("wrong state root: got %s want %s", result.GotStateRoot, expected)
	}
	if got := verifier.accounts[miner].FullShardKey; got != 0 {
		t.Fatalf("miner full shard key changed: got %#x want 0", got)
	}
}

func TestVerifierResetsStateFullShardKeyPerBlock(t *testing.T) {
	config := testClusterConfig(t)
	qkcID, err := qkcstate.QuarkChainTokenIDEncode("QKC")
	if err != nil {
		t.Fatal(err)
	}
	key := mustTestKey(t)
	from := crypto.PubkeyToAddress(key.PublicKey)
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	miner := common.HexToAddress("0x3333333333333333333333333333333333333333")
	nextMiner := common.HexToAddress("0x4444444444444444444444444444444444444444")
	tx := signedTransferInput(t, key, to, qkcID)

	afterTransfer := map[common.Address]qkcstate.QuarkChainAccount{
		from: {
			Nonce:         1,
			TokenBalances: map[uint64]*big.Int{qkcID: big.NewInt(57900)},
			FullShardKey:  tx.FromFullShardKey,
		},
		to: {
			TokenBalances: map[uint64]*big.Int{qkcID: big.NewInt(100)},
			FullShardKey:  tx.ToFullShardKey,
		},
		miner: {
			TokenBalances: map[uint64]*big.Int{qkcID: big.NewInt(21012)},
			FullShardKey:  0,
		},
	}
	firstRoot, err := qkcstate.BuildQuarkChainStateRoot(afterTransfer, nil)
	if err != nil {
		t.Fatal(err)
	}

	verifier, err := NewVerifier(config, 0x00070001)
	if err != nil {
		t.Fatal(err)
	}
	verifier.accounts[from] = qkcstate.QuarkChainAccount{
		TokenBalances: map[uint64]*big.Int{qkcID: big.NewInt(100000)},
		FullShardKey:  tx.FromFullShardKey,
	}
	verifier.accounts[miner] = qkcstate.QuarkChainAccount{
		TokenBalances: map[uint64]*big.Int{qkcID: big.NewInt(5)},
		FullShardKey:  0,
	}
	if _, err := verifier.VerifyBlock(&MinorBlockInput{
		FullShardID:       0x00070001,
		Height:            1,
		Hash:              common.HexToHash("0x01"),
		ExpectedStateRoot: firstRoot,
		GasUsed:           qkcTransferIntrinsicGas,
		Coinbase: QKCAddress{
			Recipient:    miner,
			FullShardKey: 0x00070001,
		},
		CoinbaseAmountMap: []TokenBalance{{TokenID: qkcID, Balance: big.NewInt(21007)}},
		Transactions:      []TransactionInput{tx},
	}); err != nil {
		t.Fatal(err)
	}

	afterTransfer[nextMiner] = qkcstate.QuarkChainAccount{
		TokenBalances: map[uint64]*big.Int{qkcID: big.NewInt(50)},
		FullShardKey:  0,
	}
	secondRoot, err := qkcstate.BuildQuarkChainStateRoot(afterTransfer, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifyBlock(&MinorBlockInput{
		FullShardID:       0x00070001,
		Height:            2,
		Hash:              common.HexToHash("0x02"),
		ExpectedStateRoot: secondRoot,
		Coinbase: QKCAddress{
			Recipient:    nextMiner,
			FullShardKey: 0x00070001,
		},
		CoinbaseAmountMap: []TokenBalance{{TokenID: qkcID, Balance: big.NewInt(50)}},
	}); err != nil {
		t.Fatal(err)
	}
	if got := verifier.accounts[nextMiner].FullShardKey; got != 0 {
		t.Fatalf("new miner full shard key leaked from previous block: got %#x want 0", got)
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

func mustTestKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := crypto.HexToECDSA("4c0883a6910395b4cbd770039445eae8c9af548b5e907c2d6a01d9d3c7baf6ab")
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func signedTransferInput(t *testing.T, key *ecdsa.PrivateKey, to common.Address, tokenID uint64) TransactionInput {
	t.Helper()
	tx := &EVMTransaction{
		Nonce:            0,
		GasPrice:         big.NewInt(2),
		Gas:              30000,
		To:               &to,
		Value:            big.NewInt(100),
		NetworkID:        1,
		FromFullShardKey: 0x0007c953,
		ToFullShardKey:   0x0007d31c,
		GasTokenID:       tokenID,
		TransferTokenID:  tokenID,
		Version:          TransactionVersionTyped,
	}
	signTestTransaction(t, tx, key)
	from := crypto.PubkeyToAddress(key.PublicKey)
	return TransactionInput{
		Hash:             common.HexToHash("0x02"),
		From:             from,
		RecoveredSender:  from,
		To:               &to,
		Nonce:            tx.Nonce,
		Value:            new(big.Int).Set(tx.Value),
		GasPrice:         new(big.Int).Set(tx.GasPrice),
		Gas:              tx.Gas,
		NetworkID:        tx.NetworkID,
		FromFullShardKey: tx.FromFullShardKey,
		ToFullShardKey:   tx.ToFullShardKey,
		GasTokenID:       tx.GasTokenID,
		TransferTokenID:  tx.TransferTokenID,
		Version:          tx.Version,
		V:                new(big.Int).Set(tx.V),
		R:                new(big.Int).Set(tx.R),
		S:                new(big.Int).Set(tx.S),
		EVMTransaction:   tx,
	}
}

func testClusterConfig(t *testing.T) *qkcstate.QuarkChainClusterGenesisConfig {
	t.Helper()
	config, err := qkcstate.ParseQuarkChainClusterGenesisConfig([]byte(`{
		"GENESIS_DIR": null,
		"QUARKCHAIN": {
			"GENESIS_TOKEN": "QKC",
			"BASE_ETH_CHAIN_ID": 100000,
			"NETWORK_ID": 1,
			"REWARD_TAX_RATE": 0.5,
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
