// Copyright 2026-2027, QuarkChain.

package shard

import (
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/qkc/config"
)

const (
	fixtureMainnet = "../config/singularity/mainnet.json"
	fixtureDevnet  = "../config/singularity/devnet.json"
)

// firstShardID is S0's first owned shard in both singularity networks:
// chain 0, shard size 1, shard 0.
const firstShardID = uint32(0x00000001)

func loadFixture(t *testing.T, path string) *config.ClusterConfig {
	t.Helper()
	cfg, err := config.LoadClusterConfig(path)
	if err != nil {
		t.Fatalf("LoadClusterConfig(%s): %v", path, err)
	}
	return cfg
}

// TestNewGenesisDescriptor asserts the descriptor built from each real network
// config: the shard genesis fields and the parsed ALLOC are carried to the seam
// intact, and the EVM chain id is BASE_ETH_CHAIN_ID + CHAIN_ID + 1 under
// Petersburg-only rules.
func TestNewGenesisDescriptor(t *testing.T) {
	for _, tc := range []struct {
		name       string
		path       string
		ethChainID uint64
	}{
		{"mainnet", fixtureMainnet, 100001},
		{"devnet", fixtureDevnet, 110001},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadFixture(t, tc.path)
			shardCfg := cfg.Quarkchain.GetShardConfigByFullShardID(firstShardID)

			g, err := NewGenesis(cfg.Quarkchain, shardCfg)
			if err != nil {
				t.Fatalf("NewGenesis: %v", err)
			}
			if g.FullShardID != firstShardID {
				t.Errorf("FullShardID = 0x%08x, want 0x%08x", g.FullShardID, firstShardID)
			}
			sg := shardCfg.Genesis
			if g.Timestamp != sg.Timestamp || g.Difficulty != sg.Difficulty ||
				g.GasLimit != sg.GasLimit || g.Nonce != sg.Nonce {
				t.Errorf("descriptor fields = %d/%d/%d/%d, want %d/%d/%d/%d",
					g.Timestamp, g.Difficulty, g.GasLimit, g.Nonce,
					sg.Timestamp, sg.Difficulty, sg.GasLimit, sg.Nonce)
			}
			if len(g.Alloc) == 0 || !reflect.DeepEqual(g.Alloc, sg.Alloc) {
				t.Errorf("Alloc not carried intact: %d entries, config has %d", len(g.Alloc), len(sg.Alloc))
			}

			cc := g.ChainConfig
			if cc.ChainID.Cmp(new(big.Int).SetUint64(tc.ethChainID)) != 0 {
				t.Errorf("ChainID = %v, want %d", cc.ChainID, tc.ethChainID)
			}
			if cc.PetersburgBlock == nil || cc.PetersburgBlock.Sign() != 0 {
				t.Errorf("PetersburgBlock = %v, want 0", cc.PetersburgBlock)
			}
			if cc.IstanbulBlock != nil {
				t.Errorf("IstanbulBlock = %v, want nil (Petersburg-only rules)", cc.IstanbulBlock)
			}
		})
	}
}

// TestNewGenesisRejectsInconsistentEthChainID: a per-chain ETH_CHAIN_ID that
// disagrees with the forced pyquarkchain derivation is rejected.
func TestNewGenesisRejectsInconsistentEthChainID(t *testing.T) {
	cfg := loadFixture(t, fixtureMainnet)
	shardCfg := cfg.Quarkchain.GetShardConfigByFullShardID(firstShardID)
	shardCfg.EthChainID = 42
	if _, err := NewGenesis(cfg.Quarkchain, shardCfg); err == nil ||
		!strings.Contains(err.Error(), "ETH_CHAIN_ID") {
		t.Fatalf("NewGenesis err = %v, want ETH_CHAIN_ID mismatch", err)
	}
}

// TestNewGenesisEthChainIDNoOverflow: the derivation is computed in uint64 —
// pyquarkchain's arithmetic is unbounded, so BASE_ETH_CHAIN_ID = MaxUint32
// derives 4294967296, not a wrapped 0 (which would read as "absent" and put a
// wrong replay-protection chain id into the EVM rule set).
func TestNewGenesisEthChainIDNoOverflow(t *testing.T) {
	cfg := loadFixture(t, fixtureMainnet)
	cfg.Quarkchain.BaseEthChainID = math.MaxUint32
	shardCfg := cfg.Quarkchain.GetShardConfigByFullShardID(firstShardID)

	g, err := NewGenesis(cfg.Quarkchain, shardCfg)
	if err != nil {
		t.Fatalf("NewGenesis: %v", err)
	}
	if want := new(big.Int).SetUint64(1 << 32); g.ChainConfig.ChainID.Cmp(want) != 0 {
		t.Errorf("ChainID = %v, want %v", g.ChainConfig.ChainID, want)
	}
}

// TestGenesisFingerprint: the fingerprint is deterministic and sensitive to every
// descriptor field Reconcile must catch a change in.
func TestGenesisFingerprint(t *testing.T) {
	cfg := loadFixture(t, fixtureMainnet)
	shardCfg := cfg.Quarkchain.GetShardConfigByFullShardID(firstShardID)
	g1, err := NewGenesis(cfg.Quarkchain, shardCfg)
	if err != nil {
		t.Fatalf("NewGenesis: %v", err)
	}
	g2, _ := NewGenesis(cfg.Quarkchain, shardCfg)
	if g1.Fingerprint() != g2.Fingerprint() {
		t.Fatal("fingerprint not deterministic")
	}

	base := g1.Fingerprint()
	fresh := loadFixture(t, fixtureMainnet) // independent copy to mutate
	mutCfg := fresh.Quarkchain.GetShardConfigByFullShardID(firstShardID)
	mutCfg.Genesis.Timestamp++
	mut, _ := NewGenesis(fresh.Quarkchain, mutCfg)
	if mut.Fingerprint() == base {
		t.Error("fingerprint unchanged after Timestamp change")
	}

	mutCfg.Genesis.Timestamp--
	for _, alloc := range mutCfg.Genesis.Alloc {
		for token := range alloc.Balances {
			alloc.Balances[token] = new(big.Int).Add(alloc.Balances[token], big.NewInt(1))
			break
		}
		break
	}
	mut, _ = NewGenesis(fresh.Quarkchain, mutCfg)
	if mut.Fingerprint() == base {
		t.Error("fingerprint unchanged after ALLOC balance change")
	}

	// The compiled-in fork schedule is part of the identity: a rule-set change
	// (a code change, not a config one) must not be silently accepted on reopen.
	forked, _ := NewGenesis(cfg.Quarkchain, shardCfg)
	forked.ChainConfig.IstanbulBlock = big.NewInt(0)
	if forked.Fingerprint() == base {
		t.Error("fingerprint unchanged after fork-schedule change")
	}
}
