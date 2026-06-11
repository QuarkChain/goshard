// Copyright 2026-2027, QuarkChain.

package replay

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	qkcstate "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/crypto"
)

const (
	sampleQKCTxHash   = "0x094914dcab2ff5ebeb8b38794aebf108ddcc79427b4754585922a7f0f0e2c65f"
	sampleRawEVMRLP   = "0xf87e808502540be400827530941f231b489a2d5a1eb374d363d3ac851c25db8626880de0b6b3a76400008001840007c953840007d31c828bb0828bb0011ca00d02aff00e5e71b0c8275fc1f3eed8b43af7c1aa7b6e4f0bf91feefd8a4442d4a051150b6494c81ceb3ac036ca22c6f2ee9babd226cc2e19e872c9d590832231e6"
	sampleRawTypedTx  = "0x0000000080f87e808502540be400827530941f231b489a2d5a1eb374d363d3ac851c25db8626880de0b6b3a76400008001840007c953840007d31c828bb0828bb0011ca00d02aff00e5e71b0c8275fc1f3eed8b43af7c1aa7b6e4f0bf91feefd8a4442d4a051150b6494c81ceb3ac036ca22c6f2ee9babd226cc2e19e872c9d590832231e6"
	sampleToAddress   = "0x1f231b489a2d5a1eb374d363d3ac851c25db8626"
	sampleFromAddress = "0xc4fba3740f95d25b2196c9437fdb005359296d36"
)

func TestParseTypedTransaction(t *testing.T) {
	tx, err := ParseTypedTransaction(common.FromHex(sampleRawTypedTx))
	if err != nil {
		t.Fatal(err)
	}
	if tx.TxType != TypedTransactionEVM {
		t.Fatalf("wrong tx type: got %d want %d", tx.TxType, TypedTransactionEVM)
	}
	if tx.Hash != common.HexToHash(sampleQKCTxHash) {
		t.Fatalf("wrong tx hash: got %s want %s", tx.Hash, sampleQKCTxHash)
	}
	if !tx.EVM.MatchesRLP(tx.SerializedEVMRLP) {
		t.Fatal("evm tx does not round-trip to the serialized rlp payload")
	}

	evm := tx.EVM
	if evm.Nonce != 0 {
		t.Fatalf("wrong nonce: got %d", evm.Nonce)
	}
	if evm.GasPrice.Cmp(big.NewInt(10_000_000_000)) != 0 {
		t.Fatalf("wrong gas price: got %s", evm.GasPrice)
	}
	if evm.Gas != 30000 {
		t.Fatalf("wrong gas: got %d", evm.Gas)
	}
	if evm.To == nil || *evm.To != common.HexToAddress(sampleToAddress) {
		t.Fatalf("wrong to: got %v", evm.To)
	}
	if evm.Value.Cmp(big.NewInt(1_000_000_000_000_000_000)) != 0 {
		t.Fatalf("wrong value: got %s", evm.Value)
	}
	if evm.NetworkID != 1 {
		t.Fatalf("wrong network id: got %d", evm.NetworkID)
	}
	if evm.FromFullShardKey != 0x0007c953 {
		t.Fatalf("wrong from full shard key: got %#x", evm.FromFullShardKey)
	}
	if evm.ToFullShardKey != 0x0007d31c {
		t.Fatalf("wrong to full shard key: got %#x", evm.ToFullShardKey)
	}
	if evm.GasTokenID != 0x8bb0 {
		t.Fatalf("wrong gas token id: got %#x", evm.GasTokenID)
	}
	if evm.TransferTokenID != 0x8bb0 {
		t.Fatalf("wrong transfer token id: got %#x", evm.TransferTokenID)
	}
	if evm.Version != 1 {
		t.Fatalf("wrong version: got %d", evm.Version)
	}
	if evm.V.Uint64() != 28 {
		t.Fatalf("wrong v: got %s", evm.V)
	}
	sender, err := evm.RecoverSender()
	if err != nil {
		t.Fatal(err)
	}
	if sender != common.HexToAddress(sampleFromAddress) {
		t.Fatalf("wrong recovered sender: got %s want %s", sender, sampleFromAddress)
	}
}

func TestParseMinorBlockInputJSONParsesTransactionEnvelope(t *testing.T) {
	block, err := ParseMinorBlockInputJSON([]byte(sampleBlockJSON(sampleRawEVMRLP)))
	if err != nil {
		t.Fatal(err)
	}
	if len(block.Transactions) != 1 {
		t.Fatalf("wrong tx count: got %d", len(block.Transactions))
	}
	tx := block.Transactions[0]
	if tx.TypedTransaction == nil {
		t.Fatal("typed transaction was not parsed")
	}
	if tx.EVMTransaction == nil {
		t.Fatal("evm transaction was not parsed")
	}
	if tx.Hash != common.HexToHash(sampleQKCTxHash) {
		t.Fatalf("wrong hash: got %s", tx.Hash)
	}
	if tx.NetworkID != 1 {
		t.Fatalf("wrong network id: got %d", tx.NetworkID)
	}
	if tx.Version != 1 {
		t.Fatalf("wrong version: got %d", tx.Version)
	}
	if tx.To == nil || *tx.To != common.HexToAddress(sampleToAddress) {
		t.Fatalf("wrong to: got %v", tx.To)
	}
	if tx.V.Uint64() != 28 {
		t.Fatalf("wrong v: got %s", tx.V)
	}
	if tx.RecoveredSender != common.HexToAddress(sampleFromAddress) {
		t.Fatalf("wrong recovered sender: got %s", tx.RecoveredSender)
	}
}

func TestParseMinorBlockInputJSONRejectsMismatchedRawEVMRLP(t *testing.T) {
	badRLP := sampleRawEVMRLP[:len(sampleRawEVMRLP)-2] + "00"
	_, err := ParseMinorBlockInputJSON([]byte(sampleBlockJSON(badRLP)))
	if err == nil {
		t.Fatal("expected rawEvmRlp mismatch")
	}
	if !strings.Contains(err.Error(), "rawEvmRlp mismatch") {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestParseMinorBlockInputJSONRejectsMismatchedFrom(t *testing.T) {
	input := strings.Replace(sampleBlockJSON(sampleRawEVMRLP), sampleFromAddress, "0x1111111111111111111111111111111111111111", 1)
	_, err := ParseMinorBlockInputJSON([]byte(input))
	if err == nil {
		t.Fatal("expected sender mismatch")
	}
	if !strings.Contains(err.Error(), "sender mismatch") {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestParseTypedTransactionRejectsLengthMismatch(t *testing.T) {
	raw := common.FromHex(sampleRawTypedTx)
	raw[4]--
	_, err := ParseTypedTransaction(raw)
	if err == nil {
		t.Fatal("expected payload length mismatch")
	}
	if !strings.Contains(err.Error(), "payload length mismatch") {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestRecoverLegacyTransactionSender(t *testing.T) {
	key, err := crypto.HexToECDSA("4c0883a6910395b4cbd770039445eae8c9af548b5e907c2d6a01d9d3c7baf6ab")
	if err != nil {
		t.Fatal(err)
	}
	to := common.HexToAddress(sampleToAddress)
	tx := &EVMTransaction{
		Nonce:            7,
		GasPrice:         big.NewInt(100),
		Gas:              21000,
		To:               &to,
		Value:            big.NewInt(42),
		NetworkID:        1,
		FromFullShardKey: 0x0007c953,
		ToFullShardKey:   0x0007d31c,
		GasTokenID:       0x8bb0,
		TransferTokenID:  0x8bb0,
		Version:          TransactionVersionLegacy,
	}
	signTestTransaction(t, tx, key)
	sender, err := tx.RecoverSender()
	if err != nil {
		t.Fatal(err)
	}
	if sender != crypto.PubkeyToAddress(key.PublicKey) {
		t.Fatalf("wrong sender: got %s want %s", sender, crypto.PubkeyToAddress(key.PublicKey))
	}
}

func TestValidateEIP155Transaction(t *testing.T) {
	key, err := crypto.HexToECDSA("4c0883a6910395b4cbd770039445eae8c9af548b5e907c2d6a01d9d3c7baf6ab")
	if err != nil {
		t.Fatal(err)
	}
	to := common.HexToAddress(sampleToAddress)
	qkcID, err := qkcstate.QuarkChainTokenIDEncode("QKC")
	if err != nil {
		t.Fatal(err)
	}
	tx := &EVMTransaction{
		Nonce:            1,
		GasPrice:         big.NewInt(1),
		Gas:              21000,
		To:               &to,
		Value:            big.NewInt(2),
		NetworkID:        100008,
		FromFullShardKey: 0x00070000,
		ToFullShardKey:   0x00070000,
		GasTokenID:       qkcID,
		TransferTokenID:  qkcID,
		Version:          TransactionVersionEIP155,
	}
	signTestTransaction(t, tx, key)
	input := &TransactionInput{
		From:             crypto.PubkeyToAddress(key.PublicKey),
		FromFullShardKey: tx.FromFullShardKey,
		EVMTransaction:   tx,
	}
	block := &MinorBlockInput{FullShardID: 0x00070001, Timestamp: 2000}
	config := testSigningClusterConfig(t, 1000)
	if err := ValidateHistoricalTransaction(config, block, input); err != nil {
		t.Fatal(err)
	}
	block.Timestamp = 999
	err = ValidateHistoricalTransaction(config, block, input)
	if err == nil {
		t.Fatal("expected timestamp gate failure")
	}
	if !strings.Contains(err.Error(), "EIP155 signer is not enabled") {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestValidateHistoricalTransactionRejectsWrongShard(t *testing.T) {
	block, err := ParseMinorBlockInputJSON([]byte(sampleBlockJSON(sampleRawEVMRLP)))
	if err != nil {
		t.Fatal(err)
	}
	config := testSigningClusterConfig(t, 1000)
	block.FullShardID = 0x00060001
	err = ValidateHistoricalTransaction(config, block, &block.Transactions[0])
	if err == nil {
		t.Fatal("expected sender shard mismatch")
	}
	if !strings.Contains(err.Error(), "sender shard mismatch") {
		t.Fatalf("wrong error: %v", err)
	}
}

func signTestTransaction(t *testing.T, tx *EVMTransaction, key *ecdsa.PrivateKey) {
	t.Helper()
	hash, err := tx.SigningHash()
	if err != nil {
		t.Fatal(err)
	}
	sig, err := crypto.Sign(hash[:], key)
	if err != nil {
		t.Fatal(err)
	}
	tx.R = new(big.Int).SetBytes(sig[:32])
	tx.S = new(big.Int).SetBytes(sig[32:64])
	if tx.Version == TransactionVersionEIP155 {
		tx.V = new(big.Int).SetUint64(uint64(sig[64]) + uint64(tx.NetworkID)*2 + 35)
	} else {
		tx.V = new(big.Int).SetUint64(uint64(sig[64]) + 27)
	}
}

func testSigningClusterConfig(t *testing.T, eip155Timestamp uint64) *qkcstate.QuarkChainClusterGenesisConfig {
	t.Helper()
	config, err := qkcstate.ParseQuarkChainClusterGenesisConfig([]byte(fmt.Sprintf(`{
		"GENESIS_DIR": null,
		"QUARKCHAIN": {
			"GENESIS_TOKEN": "QKC",
			"BASE_ETH_CHAIN_ID": 100000,
			"NETWORK_ID": 1,
			"ENABLE_EIP155_SIGNER_TIMESTAMP": %d,
			"CHAINS": [{
				"CHAIN_ID": 7,
				"ETH_CHAIN_ID": 100008,
				"SHARD_SIZE": 1,
				"DEFAULT_CHAIN_TOKEN": "QKC",
				"GENESIS": {"ALLOC": {}}
			}]
		}
	}`, eip155Timestamp)))
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func sampleBlockJSON(rawEVMRLP string) string {
	return fmt.Sprintf(`{
		"fullShardId": "0x70001",
		"height": "0xbf",
		"timestamp": "0x5d000000",
		"hash": "0x10776e6f4dc8a60c41452ab67c886f9d7338d2907066db2ae906cbda8c9c551e",
		"miner": "0x7c498812b681763bad1aa3513e9a949d436f2c2d00070001",
		"coinbase": [{"balance": "0x2d1ab14717b89000", "tokenId": "0x8bb0"}],
		"hashEvmStateRoot": "0xda3c748070027f97120ca5a18a6762026c9bf96b619a946ee87b3725a4ba8a94",
		"hashEvmReceiptRoot": "0x2914dc6c652eea1aca9c2cbc7b6306c21ab2ab47a560ead2235e9e5c32240962",
		"gasUsed": "0x5208",
		"crossShardReceiveGasUsed": "0x0",
		"transactions": [{
			"hash": %q,
			"from": %q,
			"to": %q,
			"nonce": "0x0",
			"value": "0xde0b6b3a7640000",
			"gasPrice": "0x2540be400",
			"gas": "0x7530",
			"fromFullShardKey": "0x0007c953",
			"toFullShardKey": "0x0007d31c",
			"gasTokenId": "0x8bb0",
			"transferTokenId": "0x8bb0",
			"version": "0x1",
			"networkId": "0x1",
			"rawEvmRlp": %q,
			"rawTypedTransaction": %q
		}],
		"xshardReceiveDepositHashes": [],
		"xshardReceiveDeposits": []
	}`, sampleQKCTxHash, sampleFromAddress, sampleToAddress, rawEVMRLP, sampleRawTypedTx)
}
