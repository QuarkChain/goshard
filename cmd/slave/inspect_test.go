// Copyright 2026-2027, QuarkChain.

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/internal/reexec"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/shard"
	"github.com/ethereum/go-ethereum/qkc/types"
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

// TODO(#1): add a case where the rule set changes without changing block 0, once
// the real chain executes above genesis.
// TestRunGenesisMismatchExitsLoudly initializes a datadir from the mainnet
// config, then reruns the slave against the same datadir with the devnet
// config: the run must exit 1 and say which genesis is stored, which one the
// config derives, and which db holds the stored one.
func TestRunGenesisMismatchExitsLoudly(t *testing.T) {
	dbRoot := t.TempDir()
	initDataDir(t, fixtures[0].path, dbRoot)
	devnetCfg := writeFixtureWithDBRoot(t, fixtures[1].path, dbRoot)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, reexec.Self(), "--cluster_config", devnetCfg, "--node_id", "S0")
	cmd.Args[0] = "slave-test"
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("slave exited with %v, want exit 1; output:\n%s", err, out)
	}
	for _, want := range []string{
		"stored genesis 0x",
		"does not match config genesis 0x",
		"cluster config changed since initialization",
		filepath.Join(dbRoot, "shard-0x00000001"),
	} {
		if !strings.Contains(string(out), want) {
			t.Errorf("mismatch report missing %q:\n%s", want, out)
		}
	}
}

// TestInspectDataDir pins the report of a datadir initialized from mainnet
// against qkc/testdata/minor_genesis_golden.json: what inspect prints for chain
// 0's shard is pyquarkchain's own create_minor_block() output, not a value
// derived a second way.
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
		"genesis block:         0x04493a3c06261af970ca4fc33caa585fbcef11cdb73bb1e3be2a9f6b828a7a0f",
		"height:                0",
		"state root:            0x699737e3597ea304b7d2e2f4ecbf8ab6348688287c59cec8599cf7a4f7c82153",
		"coinbase:              0x000000000000000000000000000000000000000000000001",
		"coinbase amount:       token 35760 = 3250000000000000000",
		"evm_gas_limit:         12000000",
		"evm_xshard_gas_limit:  6000000",
		"hash_prev_root_block:  0x4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51",
		"xshard cursor:         root=0 minor=0 deposit=0",
		"head block:            none recorded",
		"2 shard(s) inspected, 0 failed",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q:\n%s", want, out)
		}
	}
}

// rewriteGenesisBlock reopens an initialized shard chaindb writable and replaces its
// stored genesis block, standing in for a database that was corrupted underneath the
// slave.
func rewriteGenesisBlock(t *testing.T, dbPath string, mutate func(*types.MinorBlock)) {
	t.Helper()
	kv, err := pebble.New(dbPath, 16, 16, "qkc/test/", false)
	if err != nil {
		t.Fatalf("open %s: %v", dbPath, err)
	}
	defer kv.Close()
	block, err := shard.ReadGenesisBlock(kv)
	if err != nil || block == nil {
		t.Fatalf("read genesis block: %v (block %v)", err, block)
	}
	mutate(block)
	if err := shard.WriteGenesisBlock(kv, block); err != nil {
		t.Fatalf("write genesis block: %v", err)
	}
}

// TestInspectRejectsInconsistentGenesis is the reason inspect validates before it
// prints. A minor block's hash is its header's hash alone, so a database whose meta
// was replaced still holds the original, authentic-looking block hash — and inspect,
// being config-free, has no config-derived encoding to compare against. Each mutation
// here leaves the header untouched, so only recomputing what the header commits to
// can catch it.
func TestInspectRejectsInconsistentGenesis(t *testing.T) {
	for _, tc := range []struct {
		name   string
		want   string
		mutate func(*types.MinorBlock)
	}{
		{
			name:   "substituted state root",
			want:   "stored genesis meta hashes to",
			mutate: func(b *types.MinorBlock) { b.Meta.Root = common.HexToHash("0xdeadbeef") },
		},
		{
			name:   "rewound xshard cursor",
			want:   "stored genesis meta hashes to",
			mutate: func(b *types.MinorBlock) { b.Meta.XShardTxCursor.MinorBlockIndex = 7 },
		},
		{
			name:   "transactions in the genesis body",
			want:   "the genesis body is empty",
			mutate: func(b *types.MinorBlock) { b.TrackingData = []byte{0x01} },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbRoot := t.TempDir()
			initDataDir(t, fixtures[0].path, dbRoot)
			dbPath := filepath.Join(dbRoot, "shard-0x00000001")
			rewriteGenesisBlock(t, dbPath, tc.mutate)

			var buf bytes.Buffer
			err := inspectDataDir(&buf, dbRoot)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("inspectDataDir err = %v, want %q", err, tc.want)
			}
			if !strings.Contains(err.Error(), "corrupt chaindb") {
				t.Errorf("error does not name the cause: %v", err)
			}
			out := buf.String()
			// No field of the mutated block may be presented as that shard's genesis:
			// its report opens with the error line, not with the "shard 0x… (path):"
			// header that introduces a field block. The healthy shard still prints.
			if strings.Contains(out, "shard 0x00000001 ("+dbPath) {
				t.Errorf("printed fields of a block that does not hold together:\n%s", out)
			}
			for _, want := range []string{
				"shard 0x00000001: ",
				"shard 0x00040001 (",
				"2 shard(s) inspected, 1 failed",
			} {
				if !strings.Contains(out, want) {
					t.Errorf("inspect output missing %q:\n%s", want, out)
				}
			}
		})
	}
}

// TestInspectRejectsHeadWithoutGenesis plants a head pointer in a chaindb that has no
// genesis block. The slave writes the genesis block last, so a missing block means an
// interrupted bootstrap — but only on a database holding nothing else. Reporting this
// one as safely re-initializable would be wrong twice over: the state is unreachable
// for this lifecycle, and the directory may not be a shard chaindb at all.
func TestInspectRejectsHeadWithoutGenesis(t *testing.T) {
	dbRoot := t.TempDir()
	dbPath := filepath.Join(dbRoot, "shard-0x00000001")
	kv, err := pebble.New(dbPath, 16, 16, "qkc/test/", false)
	if err != nil {
		t.Fatal(err)
	}
	rawdb.WriteHeadBlockHash(kv, common.HexToHash("0xabc"))
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	err = inspectDataDir(&buf, dbRoot)
	if err == nil || !strings.Contains(err.Error(), "with no genesis block under it") {
		t.Fatalf("inspectDataDir err = %v, want head-without-genesis failure", err)
	}
	if strings.Contains(buf.String(), "next boot re-initializes") {
		t.Errorf("claimed a safe re-initialization for an impossible state:\n%s", buf.String())
	}
}

// TestInspectReportsWriteFailure pins that a report which never reached the reader
// fails the command instead of being summarized as a success.
func TestInspectReportsWriteFailure(t *testing.T) {
	dbRoot := t.TempDir()
	initDataDir(t, fixtures[0].path, dbRoot)
	if err := inspectDataDir(errWriter{}, dbRoot); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("inspectDataDir err = %v, want the write error", err)
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// TestInspectReportsMisplacedChaindb renames an initialized shard's chaindb to
// another shard's directory name. The stored block names its own shard through
// its branch, so inspect must report the directory as misplaced rather than
// present the block as that shard's genesis.
func TestInspectReportsMisplacedChaindb(t *testing.T) {
	dbRoot := t.TempDir()
	initDataDir(t, fixtures[0].path, dbRoot)
	if err := os.Rename(filepath.Join(dbRoot, "shard-0x00040001"), filepath.Join(dbRoot, "shard-0x00080001")); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	err := inspectDataDir(&buf, dbRoot)
	if err == nil || !strings.Contains(err.Error(), "misplaced chaindb") {
		t.Fatalf("inspectDataDir err = %v, want misplaced chaindb", err)
	}
	for _, want := range []string{
		"stored genesis belongs to shard 0x00040001",
		"the directory name says 0x00080001",
		"2 shard(s) inspected, 1 failed",
	} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("inspect output missing %q:\n%s", want, buf.String())
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
