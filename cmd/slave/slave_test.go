// Copyright 2026-2027, QuarkChain.

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/genesis"
)

var fixtures = []struct {
	name        string
	path        string
	consensus   string
	genesisHash string
}{
	{"mainnet", "../../qkc/config/singularity/mainnet.json", "POW_ETHASH", "0x4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51"},
	{"devnet", "../../qkc/config/singularity/devnet.json", "POW_SIMULATE", "0x5ad443efb7cf5246a3d1bbc1734bd02bf3a5d83bedeccfcfe707d0ebee03780d"},
}

func TestConfigSummaryOutput(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			cfg, err := config.LoadClusterConfig(f.path)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			sc, err := cfg.ResolveSlave("S0")
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}

			var buf bytes.Buffer
			printConfigSummary(&buf, sc)
			out := buf.String()
			for _, want := range []string{"slave S0", "0x00000001", "0x00040001", f.consensus, "owns 2 shard(s)"} {
				if !strings.Contains(out, want) {
					t.Errorf("config summary missing %q:\n%s", want, out)
				}
			}
		})
	}
}

// loadFixtureWithTempDBRoot loads a fixture with its DB_PATH_ROOT redirected into
// t.TempDir(), so booting from it never writes into the repo tree.
func loadFixtureWithTempDBRoot(t *testing.T, path string) *config.ClusterConfig {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	doc["DB_PATH_ROOT"], _ = json.Marshal(t.TempDir())
	rewritten, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	tmpPath := filepath.Join(t.TempDir(), "cluster_config.json")
	if err := os.WriteFile(tmpPath, rewritten, 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	cfg, err := config.LoadClusterConfig(tmpPath)
	if err != nil {
		t.Fatalf("load rewritten fixture: %v", err)
	}
	return cfg
}

// TestBootSlave drives the default action's boot pipeline end to end from each
// real network config: resolve S0, derive the root genesis, start both owned
// shards, stop, and boot again from the same datadir.
func TestBootSlave(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			cfg := loadFixtureWithTempDBRoot(t, f.path)

			backend, err := bootSlave(cfg, "S0")
			if err != nil {
				t.Fatalf("bootSlave: %v", err)
			}
			if got := len(backend.Shards()); got != 2 {
				t.Errorf("booted %d shards, want 2", got)
			}
			if err := backend.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}

			backend, err = bootSlave(cfg, "S0")
			if err != nil {
				t.Fatalf("bootSlave(reopen): %v", err)
			}
			if err := backend.Stop(); err != nil {
				t.Fatalf("Stop(reopen): %v", err)
			}
		})
	}
}

func TestBootSlaveRejectsBadNodeID(t *testing.T) {
	cfg := loadFixtureWithTempDBRoot(t, fixtures[0].path)
	if _, err := bootSlave(cfg, ""); err == nil || !strings.Contains(err.Error(), "--node_id is required") {
		t.Errorf("bootSlave(\"\") err = %v, want required-flag error", err)
	}
	if _, err := bootSlave(cfg, "S9"); err == nil || !strings.Contains(err.Error(), `unknown node id "S9"`) {
		t.Errorf("bootSlave(S9) err = %v, want unknown node id", err)
	}
}

func TestGenesisOutput(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			cfg, err := config.LoadClusterConfig(f.path)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			header, err := genesis.RootBlock(cfg.Quarkchain)
			if err != nil {
				t.Fatalf("genesis: %v", err)
			}

			var buf bytes.Buffer
			printRootGenesis(&buf, header)
			if out := buf.String(); !strings.Contains(out, f.genesisHash) {
				t.Errorf("genesis output missing pinned %s hash:\n%s", f.name, out)
			}
		})
	}
}
