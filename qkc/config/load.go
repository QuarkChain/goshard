// Copyright 2026-2027, QuarkChain.

// The slave's load-time entry point onto the reused config types: it adds (1) a
// non-panicking Validate() run before any database is opened, and (2)
// ResolveSlave(), which narrows the full ClusterConfig down to a SlaveContext so
// the SlaveBackend never sees other slaves' data.

package config

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// SlaveContext is the narrowed view a single slave boots from: just its own slave
// entry, the shared chain config, and the db path root. It deliberately does not
// carry the whole ClusterConfig (which includes every other slave's assignments).
type SlaveContext struct {
	ID         string
	Slave      *SlaveConfig
	Quarkchain *QuarkChainConfig
	DBPathRoot string
}

// FullShardIDs returns this slave's owned full shard ids, in config order.
func (c *SlaveContext) FullShardIDs() []uint32 {
	return c.Slave.FullShardList
}

// ShardConfigs returns the resolved shard config for each owned full shard id, in
// config order. Validate guarantees each id resolves, so no entry is nil.
func (c *SlaveContext) ShardConfigs() []*ShardConfig {
	out := make([]*ShardConfig, 0, len(c.Slave.FullShardList))
	for _, id := range c.Slave.FullShardList {
		out = append(out, c.Quarkchain.GetShardConfigByFullShardID(id))
	}
	return out
}

// LoadClusterConfig reads, parses, and validates a pyquarkchain cluster_config.json
// from path. Validation runs before any database is touched, so a bad config fails
// here rather than mid-boot. A panic from the reused initAndValidate path (the
// goquarkchain config validates by panicking) is converted into an error so the
// binary exits cleanly instead of crashing.
func LoadClusterConfig(path string) (*ClusterConfig, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg, err := unmarshalClusterConfig(content)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

func unmarshalClusterConfig(content []byte) (cfg *ClusterConfig, err error) {
	defer func() {
		if r := recover(); r != nil {
			cfg, err = nil, fmt.Errorf("invalid cluster config: %v", r)
		}
	}()
	cfg = new(ClusterConfig)
	if e := json.Unmarshal(content, cfg); e != nil {
		return nil, e
	}
	return cfg, nil
}

// Validate checks the boot-relevant invariants before any database opens:
//   - the config carries a chain config and at least one slave;
//   - no slave still uses the legacy CHAIN_MASK_LIST shard-assignment form;
//   - every owned full shard id resolves to a configured chain/shard;
//   - no slave lists the same full shard id twice (intra-slave only — the same
//     shard on multiple slaves is a valid multi-replica deployment, allowed here
//     and validated at the master layer);
//   - each owned shard's genesis ROOT_HEIGHT equals ROOT.GENESIS.HEIGHT (this
//     issue derives only the genesis root block);
//   - the root genesis hash fields are well-formed hex;
//   - any per-chain ETH_CHAIN_ID equals BASE_ETH_CHAIN_ID + CHAIN_ID + 1, the
//     derivation pyquarkchain forces on load (config.py:534).
func (c *ClusterConfig) Validate() error {
	if c.Quarkchain == nil {
		return fmt.Errorf("missing QUARKCHAIN config")
	}
	if c.Quarkchain.Root == nil || c.Quarkchain.Root.Genesis == nil {
		return fmt.Errorf("missing QUARKCHAIN.ROOT.GENESIS")
	}
	if len(c.SlaveList) == 0 {
		return fmt.Errorf("SLAVE_LIST is empty")
	}

	root := c.Quarkchain.Root.Genesis
	for _, name := range []struct {
		field string
		value string
	}{
		{"HASH_PREV_BLOCK", root.HashPrevBlock},
		{"HASH_MERKLE_ROOT", root.HashMerkleRoot},
	} {
		if err := validateRootHashHex(name.value); err != nil {
			return fmt.Errorf("ROOT.GENESIS.%s: %w", name.field, err)
		}
	}

	for _, chain := range c.Quarkchain.Chains {
		// Computed in uint64: pyquarkchain's arithmetic is unbounded, so the
		// derivation may exceed uint32 and must not silently wrap.
		want := uint64(c.Quarkchain.BaseEthChainID) + uint64(chain.ChainID) + 1
		if chain.EthChainID != 0 && uint64(chain.EthChainID) != want {
			return fmt.Errorf("chain %d ETH_CHAIN_ID %d != BASE_ETH_CHAIN_ID %d + CHAIN_ID + 1 = %d",
				chain.ChainID, chain.EthChainID, c.Quarkchain.BaseEthChainID, want)
		}
	}

	for _, slave := range c.SlaveList {
		if slave == nil {
			continue
		}
		if len(slave.ChainMaskListForBackward) > 0 {
			return fmt.Errorf("slave %q uses legacy CHAIN_MASK_LIST: legacy config not supported, use FULL_SHARD_ID_LIST", slave.ID)
		}
		// Duplicates are rejected only within one slave's list. The same id on two
		// slaves is a valid multi-replica deployment (pyquarkchain master.py:1122);
		// global shard-coverage and replica sanity belong at the master layer.
		seen := make(map[uint32]bool)
		for _, id := range slave.FullShardList {
			shard := c.Quarkchain.GetShardConfigByFullShardID(id)
			if shard == nil {
				return fmt.Errorf("slave %q full shard id 0x%08x is not configured in any chain", slave.ID, id)
			}
			if seen[id] {
				return fmt.Errorf("slave %q has duplicate full shard id 0x%08x in FULL_SHARD_ID_LIST", slave.ID, id)
			}
			seen[id] = true
			if shard.Genesis == nil {
				return fmt.Errorf("full shard id 0x%08x has no GENESIS", id)
			}
			if shard.Genesis.RootHeight != root.Height {
				return fmt.Errorf("full shard id 0x%08x genesis ROOT_HEIGHT %d != ROOT.GENESIS.HEIGHT %d", id, shard.Genesis.RootHeight, root.Height)
			}
		}
	}
	return nil
}

// ResolveSlave narrows the cluster config to the single slave identified by nodeID,
// returning a SlaveContext. An unknown id is reported with the ids the config does
// define, so the operator can correct the launch flag.
func (c *ClusterConfig) ResolveSlave(nodeID string) (*SlaveContext, error) {
	var ids []string
	for _, slave := range c.SlaveList {
		if slave == nil {
			continue
		}
		ids = append(ids, slave.ID)
		if slave.ID == nodeID {
			return &SlaveContext{
				ID:         slave.ID,
				Slave:      slave,
				Quarkchain: c.Quarkchain,
				DBPathRoot: c.DbPathRoot,
			}, nil
		}
	}
	return nil, fmt.Errorf("unknown node id %q (config defines: %s)", nodeID, strings.Join(ids, ", "))
}

// validateRootHashHex accepts an empty string (meaning all-zero) or a hex string
// (optionally 0x-prefixed) decoding to exactly 32 bytes.
func validateRootHashHex(s string) error {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("invalid hex %q: %w", s, err)
	}
	if len(b) != common.HashLength {
		return fmt.Errorf("expected %d bytes, got %d", common.HashLength, len(b))
	}
	return nil
}
