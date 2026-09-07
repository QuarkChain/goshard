// Copyright 2026-2027, QuarkChain.

package rawdb

import (
	"encoding/json"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	qkcconfig "github.com/ethereum/go-ethereum/qkc/config"
)

// QKCReadChainConfig retrieves the QuarkChain consensus settings based on the given genesis hash.
func QKCReadChainConfig(db ethdb.KeyValueReader, hash common.Hash) *qkcconfig.QuarkChainConfig {
	data, _ := db.Get(qkcChainConfigKey(hash))
	if len(data) == 0 {
		return nil
	}
	var config qkcconfig.QuarkChainConfig
	if err := json.Unmarshal(data, &config); err != nil {
		log.Error("Invalid QKC chain config JSON", "hash", hash, "err", err)
		return nil
	}
	return &config
}

// QKCWriteChainConfig writes the QuarkChain consensus settings to the database.
func QKCWriteChainConfig(db ethdb.KeyValueWriter, hash common.Hash, cfg *qkcconfig.QuarkChainConfig) {
	if cfg == nil {
		return
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		log.Crit("Failed to JSON encode QKC chain config", "err", err)
	}
	if err := db.Put(qkcChainConfigKey(hash), data); err != nil {
		log.Crit("Failed to store QKC chain config", "err", err)
	}
}
