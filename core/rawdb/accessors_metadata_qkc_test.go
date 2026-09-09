// Copyright 2026-2027, QuarkChain.

package rawdb

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	qkcconfig "github.com/ethereum/go-ethereum/qkc/config"
)

func TestQKCChainConfigStorage(t *testing.T) {
	db := memorydb.New()
	hash := common.HexToHash("0x1234")
	if cfg := QKCReadChainConfig(db, hash); cfg != nil {
		t.Fatalf("non-existent QKC chain config returned: %v", cfg)
	}

	cfg := qkcconfig.NewQuarkChainConfig()
	cfg.NetworkID = 123
	QKCWriteChainConfig(db, hash, cfg)

	wantKey := append([]byte("qkc_config-"), hash.Bytes()...)
	if key := qkcChainConfigKey(hash); !bytes.Equal(key, wantKey) {
		t.Fatalf("QKC chain config key mismatch: have %x, want %x", key, wantKey)
	}
	stored, err := db.Get(qkcChainConfigKey(hash))
	if err != nil {
		t.Fatal("read stored QKC chain config:", err)
	}
	if !json.Valid(stored) {
		t.Fatalf("stored QKC chain config is invalid JSON: %x", stored)
	}
	if has, _ := db.Has(configKey(hash)); has {
		t.Fatal("QKC chain config was written with the Ethereum config key")
	}

	got := QKCReadChainConfig(db, hash)
	if got == nil {
		t.Fatal("stored QKC chain config not found")
	}
	if got.NetworkID != cfg.NetworkID || got.ChainSize != cfg.ChainSize || got.GenesisToken != cfg.GenesisToken {
		t.Fatalf("QKC chain config mismatch: have %+v, want %+v", got, cfg)
	}
}
