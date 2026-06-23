// Copyright 2026-2027, QuarkChain.

package main

import (
	"bytes"
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
