// Copyright 2026-2027, QuarkChain.

package shard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/genesis"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// bootEnv resolves everything shard.New needs from a fixture: S0's context and
// the derived root genesis header.
func bootEnv(t *testing.T, path string) (*config.SlaveContext, *types.RootBlockHeader) {
	t.Helper()
	cfg := loadFixture(t, path)
	ctx, err := cfg.ResolveSlave("S0")
	if err != nil {
		t.Fatalf("ResolveSlave: %v", err)
	}
	root, err := genesis.RootBlock(cfg.Quarkchain)
	if err != nil {
		t.Fatalf("RootBlock: %v", err)
	}
	return ctx, root
}

// TODO(#1): replace the stub fingerprint and GenesisRecord assertions with the
// real QKC minor genesis/head and native reopen compatibility checks.
// TestShardNewAndReopen constructs a single shard from each real network config,
// stops it, and verifies that the same database reopens cleanly.
func TestShardNewAndReopen(t *testing.T) {
	for _, path := range []string{fixtureMainnet, fixtureDevnet} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			ctx, root := bootEnv(t, path)
			datadir := t.TempDir()
			branch := account.NewBranch(firstShardID)

			s, err := New(ctx, branch, root, datadir, Options{})
			if err != nil {
				t.Fatalf("shard.New: %v", err)
			}

			// A directory per shard under the datadir.
			dbPath := filepath.Join(datadir, fmt.Sprintf("shard-0x%08x", firstShardID))
			if fi, err := os.Stat(dbPath); err != nil || !fi.IsDir() {
				t.Errorf("shard db dir %s: %v", dbPath, err)
			}

			// The stub chain reports head height 0 at the descriptor's identity.
			descriptor, err := NewGenesis(ctx.Quarkchain, ctx.Quarkchain.GetShardConfigByFullShardID(firstShardID))
			if err != nil {
				t.Fatalf("NewGenesis: %v", err)
			}
			height, head := s.Chain().Head()
			if height != 0 {
				t.Errorf("head height = %d, want 0", height)
			}
			if want := descriptor.Fingerprint(); head != want || s.Chain().GenesisHash() != want {
				t.Errorf("head/genesis hash = %s/%s, want %s", head, s.Chain().GenesisHash(), want)
			}

			// The genesis record links the shard to the root genesis.
			rec, err := ReadGenesisRecord(s.DB())
			if err != nil || rec == nil {
				t.Fatalf("ReadGenesisRecord = %v, %v", rec, err)
			}
			rootHash := root.Hash()
			if rec.FullShardID != firstShardID || rec.RootGenesisHash != rootHash ||
				rec.HashPrevRootBlock != rootHash ||
				rec.XShardCursor != (XShardCursor{RootBlockHeight: uint64(root.Number)}) ||
				rec.ChainGenesisHash != descriptor.Fingerprint() {
				t.Errorf("stored record %+v inconsistent with config derivation", rec)
			}

			if err := s.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}

			// Reopen the same directory: Reconcile passes on the exact match.
			s, err = New(ctx, branch, root, datadir, Options{})
			if err != nil {
				t.Fatalf("shard.New(reopen): %v", err)
			}
			if err := s.Stop(); err != nil {
				t.Fatalf("Stop(reopen): %v", err)
			}
		})
	}
}

// TestShardMemDB: an empty datadir is pyquarkchain's mem-db mode (use_mem_db,
// cluster_config.py:247) — the shard runs on an ephemeral in-memory database
// and writes nothing to disk.
func TestShardMemDB(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	branch := account.NewBranch(firstShardID)

	s, err := New(ctx, branch, root, "", Options{})
	if err != nil {
		t.Fatalf("shard.New(mem): %v", err)
	}
	// No shard directory appears in the working directory.
	if _, err := os.Stat(fmt.Sprintf("shard-0x%08x", firstShardID)); !os.IsNotExist(err) {
		t.Errorf("shard-0x%08x was created in the working directory (stat err = %v)", firstShardID, err)
	}
	if height, head := s.Chain().Head(); height != 0 || head != s.Chain().GenesisHash() {
		t.Errorf("head = %d/%s, want 0 at the genesis hash", height, head)
	}
	if rec, err := ReadGenesisRecord(s.DB()); err != nil || rec == nil {
		t.Fatalf("ReadGenesisRecord = %v, %v", rec, err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestShardReopenGenesisMismatch: changing the shard genesis in the config between
// runs fails the reopen loudly, naming both genesis hashes and the db path.
func TestShardReopenGenesisMismatch(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	datadir := t.TempDir()
	branch := account.NewBranch(firstShardID)

	s, err := New(ctx, branch, root, datadir, Options{})
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	ctx.Quarkchain.GetShardConfigByFullShardID(firstShardID).Genesis.Timestamp++
	_, err = New(ctx, branch, root, datadir, Options{})
	if err == nil || !strings.Contains(err.Error(), "does not match config genesis") ||
		!strings.Contains(err.Error(), "cluster config changed since initialization") ||
		!strings.Contains(err.Error(), datadir) {
		t.Fatalf("reopen err = %v, want loud genesis mismatch naming the db path", err)
	}
}

// TestShardReopenRootGenesisMismatch: a root genesis change (which leaves the
// shard's own descriptor untouched) is also caught on reopen.
func TestShardReopenRootGenesisMismatch(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	datadir := t.TempDir()
	branch := account.NewBranch(firstShardID)

	s, err := New(ctx, branch, root, datadir, Options{})
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	ctx.Quarkchain.Root.Genesis.Difficulty++
	root2, err := genesis.RootBlock(ctx.Quarkchain)
	if err != nil {
		t.Fatalf("RootBlock: %v", err)
	}
	_, err = New(ctx, branch, root2, datadir, Options{})
	if err == nil || !strings.Contains(err.Error(), "cluster config changed since initialization") {
		t.Fatalf("reopen err = %v, want loud record mismatch", err)
	}
}

func TestShardNewRejectsUnknownShard(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	_, err := New(ctx, account.NewBranch(0x00990099), root, t.TempDir(), Options{})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("shard.New(unknown) err = %v, want 'not configured'", err)
	}
}

// failingChainService fails every chain construction, standing in for a real
// chain implementation that errors during boot.
type failingChainService struct{}

func (failingChainService) New(ethdb.Database, *Genesis) (ShardChain, error) {
	return nil, errors.New("injected chain failure")
}

// TestShardFailedFirstBootLeavesNoRecord: a boot that fails at chain
// construction commits no genesis record, so the retry takes the fresh path
// instead of reporting a never-validated record as existing.
func TestShardFailedFirstBootLeavesNoRecord(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	datadir := t.TempDir()
	branch := account.NewBranch(firstShardID)

	if _, err := New(ctx, branch, root, datadir, Options{Chain: failingChainService{}}); err == nil ||
		!strings.Contains(err.Error(), "injected chain failure") {
		t.Fatalf("New(failing chain) err = %v, want the injected failure", err)
	}

	// The half-initialized db carries no record.
	kv, err := pebble.New(filepath.Join(datadir, fmt.Sprintf("shard-0x%08x", firstShardID)),
		dbCacheMB, dbHandles, "", false)
	if err != nil {
		t.Fatalf("reopen chaindb: %v", err)
	}
	rec, err := ReadGenesisRecord(rawdb.NewDatabase(kv))
	kv.Close()
	if err != nil || rec != nil {
		t.Fatalf("record after failed boot = %+v, %v, want none", rec, err)
	}

	// The retry succeeds and commits the record.
	s, err := New(ctx, branch, root, datadir, Options{})
	if err != nil {
		t.Fatalf("shard.New(retry): %v", err)
	}
	if rec, err := ReadGenesisRecord(s.DB()); err != nil || rec == nil {
		t.Fatalf("ReadGenesisRecord after retry = %v, %v", rec, err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestShardStopIdempotent(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	s, err := New(ctx, account.NewBranch(firstShardID), root, t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop(again): %v", err)
	}
}

func TestDBDirNameRoundTrip(t *testing.T) {
	for _, id := range []uint32{0, 1, 0x00040001, 0xffffffff} {
		name := DBDirName(id)
		if got, ok := ParseDBDirName(name); !ok || got != id {
			t.Errorf("ParseDBDirName(%q) = (0x%08x, %v), want (0x%08x, true)", name, got, ok, id)
		}
	}
	for _, name := range []string{
		"shard-0x1",         // not zero-padded
		"shard-0x000000012", // too long
		"shard-00000001",    // missing 0x
		"shard-0xzzzzzzzz",  // not hex
		"chaindata",
	} {
		if id, ok := ParseDBDirName(name); ok {
			t.Errorf("ParseDBDirName(%q) = (0x%08x, true), want rejection", name, id)
		}
	}
}
