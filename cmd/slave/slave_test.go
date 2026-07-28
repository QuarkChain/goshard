// Copyright 2026-2027, QuarkChain.

package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/qkc"
	"github.com/ethereum/go-ethereum/qkc/config"
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

func TestGenesisOutput(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			cfg, err := config.LoadClusterConfig(f.path)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			header, err := qkc.CreateRootBlock(cfg.Quarkchain)
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
