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
	"math"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
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

// Owns reports whether fullShardID is assigned to this slave. A shard that
// resolves in the shared QuarkChainConfig may still belong to another slave, so
// this — not GetShardConfigByFullShardID — is the permission boundary a
// SlaveContext narrows to.
func (c *SlaveContext) Owns(fullShardID uint32) bool {
	for _, id := range c.Slave.FullShardList {
		if id == fullShardID {
			return true
		}
	}
	return false
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
	if err := cfg.deriveEthChainID(); err != nil {
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
//   - no slave carries the legacy CHAIN_MASK_LIST key, and every slave owns at
//     least one shard;
//   - every owned full shard id resolves to a configured chain/shard;
//   - no slave lists the same full shard id twice (intra-slave only — the same
//     shard on multiple slaves is a valid multi-replica deployment, allowed here
//     and validated at the master layer);
//   - the root and owned-shard genesis hash fields are well-formed hex;
//   - each owned shard's genesis ROOT_HEIGHT equals ROOT.GENESIS.HEIGHT (this
//     issue derives only the genesis root block).
//
// ETH_CHAIN_ID is filled in and consistency-checked separately by deriveEthChainID.
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

	if err := validateTokenNames(c.Quarkchain); err != nil {
		return err
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

	for _, slave := range c.SlaveList {
		if slave == nil {
			continue
		}
		// The legacy CHAIN_MASK_LIST form is unsupported. Reject it whenever the key
		// is present, including an empty list: slave_config.go leaves the field nil
		// when the key is absent and a non-nil (possibly empty) slice when present,
		// so len()>0 alone would let CHAIN_MASK_LIST:[] slip through as zero shards.
		if slave.ChainMaskListForBackward != nil {
			return fmt.Errorf("slave %q uses legacy CHAIN_MASK_LIST: legacy config not supported, use FULL_SHARD_ID_LIST", slave.ID)
		}
		if len(slave.FullShardList) == 0 {
			return fmt.Errorf("slave %q owns no shards: FULL_SHARD_ID_LIST is empty", slave.ID)
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
			// Validate the shard genesis hash fields at load, not deferred to shard
			// genesis creation, so a malformed hex value fails while reporting config.
			for _, h := range []struct {
				field string
				value string
			}{
				{"HASH_PREV_MINOR_BLOCK", shard.Genesis.HashPrevMinorBlock},
				{"HASH_MERKLE_ROOT", shard.Genesis.HashMerkleRoot},
			} {
				if err := validateRootHashHex(h.value); err != nil {
					return fmt.Errorf("full shard id 0x%08x GENESIS.%s: %w", id, h.field, err)
				}
			}
			if shard.Genesis.RootHeight != root.Height {
				return fmt.Errorf("full shard id 0x%08x genesis ROOT_HEIGHT %d != ROOT.GENESIS.HEIGHT %d", id, shard.Genesis.RootHeight, root.Height)
			}
		}
	}
	return nil
}

// validateTokenNames checks every token name the config can feed to the encoder —
// GENESIS_TOKEN and every ALLOC balance key of every configured shard — against
// pyquarkchain's [0-9A-Z]{1,12} domain. common.TokenIDEncode panics outside it while
// its callers downstream of load (genesis materialization, GetDefaultChainTokenID)
// report errors, so the domain has to be settled here.
func validateTokenNames(q *QuarkChainConfig) error {
	if err := qkccommon.ValidateTokenName(q.GenesisToken); err != nil {
		return fmt.Errorf("QUARKCHAIN.GENESIS_TOKEN: %w", err)
	}
	for id, shard := range q.shards {
		if shard == nil || shard.Genesis == nil {
			continue
		}
		for addr, alloc := range shard.Genesis.Alloc {
			for token := range alloc.Balances {
				if err := qkccommon.ValidateTokenName(token); err != nil {
					return fmt.Errorf("full shard id 0x%08x GENESIS.ALLOC %s: %w", id, addr.ToHex(), err)
				}
			}
		}
	}
	return nil
}

// deriveEthChainID fills in each chain's ETH_CHAIN_ID the way pyquarkchain does on
// load: ETH_CHAIN_ID = BASE_ETH_CHAIN_ID + CHAIN_ID + 1 (config.py:534). pyquarkchain
// overwrites any configured value with this derivation; goshard is stricter and first
// rejects a present-but-inconsistent value so a hand-edited typo is reported rather
// than silently overwritten. A zero value is treated as absent (a real id is
// BASE_ETH_CHAIN_ID + 1 or greater, so it is never zero). The derivation is applied to
// the chain configs and to the shard configs copied from them (NewShardConfig deep-
// copies each chain config before this runs), so both carry the resolved id.
func (c *ClusterConfig) deriveEthChainID() error {
	if c.Quarkchain == nil {
		return nil // Validate reports the missing config.
	}
	derive := func(cc *ChainConfig) error {
		// Computed in uint64: pyquarkchain's arithmetic is unbounded, so the
		// derivation may exceed uint32 and must not silently wrap.
		want := uint64(c.Quarkchain.BaseEthChainID) + uint64(cc.ChainID) + 1
		if cc.EthChainID != 0 && uint64(cc.EthChainID) != want {
			return fmt.Errorf("chain %d ETH_CHAIN_ID %d != BASE_ETH_CHAIN_ID %d + CHAIN_ID + 1 = %d",
				cc.ChainID, cc.EthChainID, c.Quarkchain.BaseEthChainID, want)
		}
		if want > math.MaxUint32 {
			return fmt.Errorf("chain %d derived ETH_CHAIN_ID %d exceeds uint32", cc.ChainID, want)
		}
		cc.EthChainID = uint32(want)
		return nil
	}
	for _, chain := range c.Quarkchain.Chains {
		if err := derive(chain); err != nil {
			return err
		}
	}
	for _, shard := range c.Quarkchain.shards {
		if err := derive(shard.ChainConfig); err != nil {
			return err
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
