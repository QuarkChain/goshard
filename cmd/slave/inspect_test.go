// Copyright 2026-2027, QuarkChain.

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/qkc/config"
)

// initDataDir boots S0 from a fixture into dbRoot and stops it, leaving behind
// initialized shard chaindbs to inspect.
func initDataDir(t *testing.T, fixturePath, dbRoot string) {
	t.Helper()
	cfg, err := config.LoadClusterConfig(writeFixtureWithDBRoot(t, fixturePath, dbRoot))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	backend, err := bootSlave(cfg, "S0")
	if err != nil {
		t.Fatalf("bootSlave: %v", err)
	}
	if err := backend.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TODO(#1): initialize the real QKC shard chain and assert its canonical minor
// genesis/head once inspectShardDB stops reading the temporary GenesisMeta.
func TestInspectDataDir(t *testing.T) {
	dbRoot := t.TempDir()
	initDataDir(t, fixtures[0].path, dbRoot)

	var buf bytes.Buffer
	if err := inspectDataDir(&buf, dbRoot); err != nil {
		t.Fatalf("inspectDataDir: %v\noutput:\n%s", err, buf.String())
	}
	out := buf.String()
	for _, want := range []string{
		"shard 0x00000001 (",
		"shard 0x00040001 (",
		"meta version:          1",
		"chain genesis:         0x",
		"root genesis:          0x4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51",
		"xshard cursor:         root=0 minor=0 deposit=0",
		"head block:            none recorded",
		"2 shard(s) inspected, 0 failed",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q:\n%s", want, out)
		}
	}
}

// TestInspectReportsBrokenShardAndContinues plants a shard-named directory that
// is not a chaindb next to two healthy shards: the broken one is reported, the
// healthy ones still print, and the joined error is non-nil.
func TestInspectReportsBrokenShardAndContinues(t *testing.T) {
	dbRoot := t.TempDir()
	initDataDir(t, fixtures[0].path, dbRoot)
	if err := os.Mkdir(filepath.Join(dbRoot, "shard-0xdeadbeef"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Neither a stray file with a shard name nor an unrelated directory is a
	// shard chaindb; both must be skipped silently.
	if err := os.WriteFile(filepath.Join(dbRoot, "shard-0x000000ff"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dbRoot, "not-a-shard"), 0o755); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	err := inspectDataDir(&buf, dbRoot)
	if err == nil || !strings.Contains(err.Error(), "0xdeadbeef") {
		t.Fatalf("inspectDataDir err = %v, want failure naming 0xdeadbeef", err)
	}
	out := buf.String()
	for _, want := range []string{
		"shard 0x00000001 (",
		"shard 0x00040001 (",
		"shard 0xdeadbeef:",
		"3 shard(s) inspected, 1 failed",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "0x000000ff") {
		t.Errorf("stray file inspected as a shard:\n%s", out)
	}
}

func TestInspectRejectsUnusableDatadir(t *testing.T) {
	var buf bytes.Buffer
	if err := inspectDataDir(&buf, t.TempDir()); err == nil || !strings.Contains(err.Error(), "no shard chaindbs") {
		t.Errorf("empty datadir err = %v, want no-shard-chaindbs error", err)
	}
	if err := inspectDataDir(&buf, filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("missing datadir did not error")
	}
}
