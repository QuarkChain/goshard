// Copyright 2026-2027, QuarkChain.

package config

import (
	"os"
	"strings"
	"testing"
)

const (
	fixtureMainnet = "./singularity/mainnet.json"
	fixtureDevnet  = "./singularity/devnet.json"
	templatePath   = "./testdata/cluster_config_template.json"
)

// network describes the boot-relevant values of one real network config. Both
// singularity networks have 8 chains and 4 slaves S0..S3 each owning 2 shards;
// they differ in network id, consensus, db path, and root difficulty.
type network struct {
	name        string
	path        string
	dbRoot      string
	networkID   uint32
	s0Consensus string // consensus of S0's shards
	chain6      string // consensus of chain 6 (qkchash on mainnet, simulate on devnet)
	rootDiff    uint64
}

var networks = []network{
	{"mainnet", fixtureMainnet, "./qkc-data/mainnet", 1, PoWEthash, PoWQkchash, 10000000000000},
	{"devnet", fixtureDevnet, "./qkc-data/devnet", 255, PoWSimulate, PoWSimulate, 100000},
}

// TestLoadClusterConfigFixture asserts the boot-relevant values parsed from each
// real network config: 8 chains, 4 slaves each owning 2 shards. S0's first id is
// written "0x1" in both configs, so leading-zero-tolerant hex must parse it to 1.
func TestLoadClusterConfigFixture(t *testing.T) {
	for _, nw := range networks {
		t.Run(nw.name, func(t *testing.T) {
			cfg, err := LoadClusterConfig(nw.path)
			if err != nil {
				t.Fatalf("LoadClusterConfig: %v", err)
			}
			if cfg.DbPathRoot != nw.dbRoot {
				t.Errorf("DB_PATH_ROOT = %q, want %q", cfg.DbPathRoot, nw.dbRoot)
			}
			if cfg.Quarkchain.NetworkID != nw.networkID {
				t.Errorf("NETWORK_ID = %d, want %d", cfg.Quarkchain.NetworkID, nw.networkID)
			}
			if len(cfg.SlaveList) != 4 {
				t.Fatalf("SLAVE_LIST len = %d, want 4", len(cfg.SlaveList))
			}

			s0 := cfg.SlaveList[0]
			if s0.ID != "S0" {
				t.Errorf("slave ID = %q, want S0", s0.ID)
			}
			wantIDs := []uint32{0x00000001, 0x00040001}
			if len(s0.FullShardList) != len(wantIDs) {
				t.Fatalf("FULL_SHARD_ID_LIST = %v, want %v", s0.FullShardList, wantIDs)
			}
			for i, id := range wantIDs {
				if s0.FullShardList[i] != id {
					t.Errorf("FULL_SHARD_ID_LIST[%d] = 0x%08x, want 0x%08x", i, s0.FullShardList[i], id)
				}
				// full shard id math: (chain_id<<16) | shard_size | shard_id.
				shard := cfg.Quarkchain.GetShardConfigByFullShardID(id)
				if shard == nil {
					t.Fatalf("shard 0x%08x not configured", id)
				}
				if got := shard.GetFullShardId(); got != id {
					t.Errorf("GetFullShardId = 0x%08x, want 0x%08x", got, id)
				}
				if shard.ConsensusType != nw.s0Consensus {
					t.Errorf("shard 0x%08x consensus = %q, want %q", id, shard.ConsensusType, nw.s0Consensus)
				}
			}
			if c6 := cfg.Quarkchain.GetShardConfigByFullShardID(0x00060001); c6 == nil || c6.ConsensusType != nw.chain6 {
				t.Errorf("shard 0x00060001 consensus = %v, want %q", c6, nw.chain6)
			}

			root := cfg.Quarkchain.Root.Genesis
			if root.Difficulty != nw.rootDiff {
				t.Errorf("ROOT.GENESIS.DIFFICULTY = %d, want %d", root.Difficulty, nw.rootDiff)
			}
			if root.Timestamp != 1556639999 {
				t.Errorf("ROOT.GENESIS.TIMESTAMP = %d, want 1556639999", root.Timestamp)
			}
		})
	}
}

func TestResolveSlave(t *testing.T) {
	for _, nw := range networks {
		t.Run(nw.name, func(t *testing.T) {
			cfg, err := LoadClusterConfig(nw.path)
			if err != nil {
				t.Fatalf("LoadClusterConfig: %v", err)
			}

			sc, err := cfg.ResolveSlave("S0")
			if err != nil {
				t.Fatalf("ResolveSlave(S0): %v", err)
			}
			if sc.ID != "S0" || sc.DBPathRoot != nw.dbRoot {
				t.Errorf("SlaveContext = %+v, want ID=S0 DBPathRoot=%q", sc, nw.dbRoot)
			}
			shards := sc.ShardConfigs()
			if len(shards) != 2 {
				t.Fatalf("ShardConfigs len = %d, want 2", len(shards))
			}
			if shards[0].GetFullShardId() != 0x00000001 || shards[1].GetFullShardId() != 0x00040001 {
				t.Errorf("ShardConfigs order wrong: %x, %x", shards[0].GetFullShardId(), shards[1].GetFullShardId())
			}

			_, err = cfg.ResolveSlave("S9")
			if err == nil {
				t.Fatal("ResolveSlave(S9): expected error, got nil")
			}
			if !strings.Contains(err.Error(), `unknown node id "S9"`) || !strings.Contains(err.Error(), "S3") {
				t.Errorf("ResolveSlave(S9) error = %q, want mention of S9 and the configured ids (incl S3)", err)
			}
		})
	}
}

// TestWebSocketPortParsing covers the optional single WEBSOCKET_JSON_RPC_PORT field
// (pyquarkchain's form, adapted in slave_config.go): devnet's S0 omits it (null) and
// S1 sets 38591.
func TestWebSocketPortParsing(t *testing.T) {
	cfg, err := LoadClusterConfig(fixtureDevnet)
	if err != nil {
		t.Fatalf("LoadClusterConfig: %v", err)
	}
	if cfg.SlaveList[0].WSPort != nil {
		t.Errorf("S0 WEBSOCKET_JSON_RPC_PORT = %v, want nil", *cfg.SlaveList[0].WSPort)
	}
	if got := cfg.SlaveList[1].WSPort; got == nil || *got != 38591 {
		t.Errorf("S1 WEBSOCKET_JSON_RPC_PORT = %v, want 38591", got)
	}
}

func TestValidateRejectsUnknownShard(t *testing.T) {
	cfg := mustLoad(t)
	cfg.SlaveList[0].FullShardList = []uint32{0x00990099}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("Validate err = %v, want 'not configured'", err)
	}
}

func TestValidateRejectsDuplicateShard(t *testing.T) {
	cfg := mustLoad(t)
	cfg.SlaveList[0].FullShardList = []uint32{0x00000001, 0x00000001}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("Validate err = %v, want 'duplicate'", err)
	}
}

// TestValidateAllowsCrossSlaveReplica asserts that the same full shard id appearing
// on two different slaves is accepted: pyquarkchain treats this as a valid
// multi-replica deployment (master.py:1122), so the duplicate check is intra-slave
// only. Global shard-coverage/replica validation belongs at the master layer.
func TestValidateAllowsCrossSlaveReplica(t *testing.T) {
	cfg := mustLoad(t)
	// S1 now replicates S0's shards: a valid multi-replica config.
	cfg.SlaveList[1].FullShardList = append([]uint32(nil), cfg.SlaveList[0].FullShardList...)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate rejected a valid multi-replica config: %v", err)
	}
}

func TestValidateRejectsBadRootHash(t *testing.T) {
	cfg := mustLoad(t)
	cfg.Quarkchain.Root.Genesis.HashPrevBlock = "nothex!!"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "HASH_PREV_BLOCK") {
		t.Fatalf("Validate err = %v, want 'HASH_PREV_BLOCK'", err)
	}
}

func TestValidateRejectsGenesisRootHeightMismatch(t *testing.T) {
	cfg := mustLoad(t)
	cfg.Quarkchain.GetShardConfigByFullShardID(0x00000001).Genesis.RootHeight = 5
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "ROOT_HEIGHT") {
		t.Fatalf("Validate err = %v, want 'ROOT_HEIGHT'", err)
	}
}

// TestLoadRejectsChainMaskList parses the larger real template (which still uses
// the legacy CHAIN_MASK_LIST assignment form): it must parse without panicking,
// but be rejected with an explicit legacy-config error.
func TestLoadRejectsChainMaskList(t *testing.T) {
	content, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	// Parse stage tolerates the legacy form, nulls, and extra fields.
	if _, err := unmarshalClusterConfig(content); err != nil {
		t.Fatalf("parse template: %v", err)
	}
	// Validate stage rejects it.
	if _, err := LoadClusterConfig(templatePath); err == nil ||
		!strings.Contains(err.Error(), "legacy config not supported") {
		t.Fatalf("LoadClusterConfig(template) err = %v, want 'legacy config not supported'", err)
	}
}

func mustLoad(t *testing.T) *ClusterConfig {
	t.Helper()
	cfg, err := LoadClusterConfig(fixtureMainnet)
	if err != nil {
		t.Fatalf("LoadClusterConfig: %v", err)
	}
	return cfg
}
