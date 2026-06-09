// Copyright 2026-2027, QuarkChain.

package state

import (
	"bytes"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestQuarkChainTokenIDEncode(t *testing.T) {
	tests := []struct {
		token string
		want  uint64
	}{
		{"0", 0},
		{"A", 10},
		{"QKC", 35760},
	}
	for _, tt := range tests {
		got, err := QuarkChainTokenIDEncode(tt.token)
		if err != nil {
			t.Fatal(err)
		}
		if got != tt.want {
			t.Fatalf("wrong token id for %s: got %d want %d", tt.token, got, tt.want)
		}
	}
	if _, err := QuarkChainTokenIDEncode("qkc"); err == nil {
		t.Fatal("expected lowercase token symbol to fail")
	}
}

func TestParseQuarkChainGenesisAddress(t *testing.T) {
	addr, err := ParseQuarkChainGenesisAddress("32c53C6c2B57B2026a51C87aDD0695F5AeEd3f2e000075b2")
	if err != nil {
		t.Fatal(err)
	}
	if addr.Recipient != common.HexToAddress("0x32c53C6c2B57B2026a51C87aDD0695F5AeEd3f2e") {
		t.Fatalf("wrong recipient: got %s", addr.Recipient)
	}
	if addr.FullShardKey != 0x75b2 {
		t.Fatalf("wrong full shard key: got %#x", addr.FullShardKey)
	}
	fullShardID, err := addr.FullShardID(1)
	if err != nil {
		t.Fatal(err)
	}
	if fullShardID != 1 {
		t.Fatalf("wrong full shard id: got %#x", fullShardID)
	}
}

func TestParseQuarkChainGenesisAllocFormats(t *testing.T) {
	const input = `{
		"GENESIS_DIR": null,
		"QUARKCHAIN": {
			"GENESIS_TOKEN": "QKC",
			"BASE_ETH_CHAIN_ID": 100000,
			"CHAINS": [{
				"CHAIN_ID": 0,
				"SHARD_SIZE": 1,
				"GENESIS": {
					"ALLOC": {
						"111111111111111111111111111111111111111100000000": {"QKC": 7},
						"222222222222222222222222222222222222222200000000": {
							"balances": {"QKC": 9, "QETC": 11},
							"code": "0x6000",
							"storage": {"0x01": "0x02"}
						}
					}
				}
			}]
		}
	}`
	config, err := ParseQuarkChainClusterGenesisConfig([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	accountsByShard, err := config.GenesisAccountsByFullShardID()
	if err != nil {
		t.Fatal(err)
	}
	accounts := accountsByShard[1]
	if len(accounts) != 2 {
		t.Fatalf("wrong account count: got %d", len(accounts))
	}
	qkcID, err := QuarkChainTokenIDEncode("QKC")
	if err != nil {
		t.Fatal(err)
	}
	account := accounts[common.HexToAddress("0x2222222222222222222222222222222222222222")]
	if account.Nonce != 1 {
		t.Fatalf("code-bearing genesis account should have nonce 1, got %d", account.Nonce)
	}
	if account.TokenBalances[qkcID].Cmp(big.NewInt(9)) != 0 {
		t.Fatalf("wrong qkc balance: got %s", account.TokenBalances[qkcID])
	}
	if !bytes.Equal(account.Code, []byte{0x60, 0x00}) {
		t.Fatalf("wrong code: got %x", account.Code)
	}
	if account.Storage[common.HexToHash("0x01")] != common.HexToHash("0x02") {
		t.Fatalf("wrong storage value: got %s", account.Storage[common.HexToHash("0x01")])
	}
	roots, err := config.BuildGenesisStateRoots(nil)
	if err != nil {
		t.Fatal(err)
	}
	if roots[1] == (common.Hash{}) || roots[1] == types.EmptyRootHash {
		t.Fatalf("expected non-empty genesis state root, got %s", roots[1])
	}
}

func TestQuarkChainMainnetGenesisStateRoots(t *testing.T) {
	path := filepath.Join(quarkChainTestRepoRoot(t), "quarkchain", "mainnet", "cluster_config_template.json")
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	config, err := ReadQuarkChainClusterGenesisConfig(file)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := config.BuildGenesisStateRoots(nil)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[uint32]common.Hash{
		1:      common.HexToHash("0x699737e3597ea304b7d2e2f4ecbf8ab6348688287c59cec8599cf7a4f7c82153"),
		65537:  common.HexToHash("0x5014e24903eca74b3f357010227aa1c70f089eba88f9edfacee8b660eddb6739"),
		131073: common.HexToHash("0x767dc124493f23fe4294c5394dce509e747beabea3039dfcc2430abd54631a95"),
		196609: common.HexToHash("0x5bb72e83b4dfff0b219384137c3dc512189f4e54ccd5eec01c4764e3bd48069a"),
		262145: common.HexToHash("0xc53143c313d3a9aa38076291dbfa28d7abf0280927ca0b14a1cfa503943f4e56"),
		327681: common.HexToHash("0x2ad203363969a0d85a459738ef556b0d6a81491f6d97240a223ea3d0d4bd109a"),
		393217: common.HexToHash("0xc6e88e3068e38a7655c399ecdcd94a6aaa21a53b60711879e817cd86294be88d"),
		458753: common.HexToHash("0xfa40aaa834a306686782f062da703499246d91748bbdc8b1386e559abccb7744"),
	}
	if len(roots) != len(expected) {
		t.Fatalf("wrong root count: got %d want %d", len(roots), len(expected))
	}
	for fullShardID, want := range expected {
		if got, ok := roots[fullShardID]; !ok {
			t.Fatalf("missing root for full shard %d", fullShardID)
		} else if got != want {
			t.Fatalf("wrong root for full shard %d: got %s want %s", fullShardID, got, want)
		}
	}
}

func quarkChainTestRepoRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to locate quarkchain genesis test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
