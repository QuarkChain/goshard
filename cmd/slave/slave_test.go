// Copyright 2026-2027, QuarkChain.

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/genesis"
	"github.com/urfave/cli/v2"
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

// writeFixtureWithDBRoot copies a fixture with its DB_PATH_ROOT redirected to
// dbRoot and returns the rewritten config's path.
func writeFixtureWithDBRoot(t *testing.T, path, dbRoot string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	doc["DB_PATH_ROOT"], _ = json.Marshal(dbRoot)
	rewritten, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	tmpPath := filepath.Join(t.TempDir(), "cluster_config.json")
	if err := os.WriteFile(tmpPath, rewritten, 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return tmpPath
}

// writeFixtureWithTempDBRoot is writeFixtureWithDBRoot into a fresh t.TempDir(),
// so booting from it never writes into the repo tree.
func writeFixtureWithTempDBRoot(t *testing.T, path string) string {
	t.Helper()
	return writeFixtureWithDBRoot(t, path, t.TempDir())
}

func loadFixtureWithTempDBRoot(t *testing.T, path string) *config.ClusterConfig {
	t.Helper()
	cfg, err := config.LoadClusterConfig(writeFixtureWithTempDBRoot(t, path))
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

// TestNoNetworkedDebugFlags pins the no-network-I/O boundary of the CLI: no
// registered debug flag may be able to open a socket, and every allowlisted
// debug flag must still exist upstream so a geth merge that renames or drops
// one fails loudly instead of silently shrinking the slave's flag surface.
func TestNoNetworkedDebugFlags(t *testing.T) {
	app := newApp()
	registered := map[string]bool{}
	collect := func(fs []cli.Flag) {
		for _, f := range fs {
			for _, name := range f.Names() {
				registered[name] = true
			}
		}
	}
	collect(app.Flags)
	for _, cmd := range app.Commands {
		collect(cmd.Flags)
	}

	networked := []string{
		"pprof", "pprof.addr", "pprof.port",
		"pyroscope", "pyroscope.server", "pyroscope.username", "pyroscope.password", "pyroscope.tags",
	}
	for _, name := range networked {
		if registered[name] {
			t.Errorf("networked debug flag --%s is registered", name)
		}
	}
	for name := range debugFlagAllowlist {
		if !registered[name] {
			t.Errorf("allowlisted debug flag --%s missing from the app (renamed upstream?)", name)
		}
	}

	app = newApp()
	app.Writer, app.ErrWriter = io.Discard, io.Discard
	if err := app.Run([]string{"slave", "--pprof"}); err == nil || !strings.Contains(err.Error(), "pprof") {
		t.Errorf("--pprof accepted, err = %v, want unknown-flag error", err)
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
