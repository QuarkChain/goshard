// Copyright 2026-2027, QuarkChain.

package shard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

// TestShardNewAndReopen is the milestone demo: construct a single shard from each
// real network config into t.TempDir(), assert the stub chain reports head height
// 0 at the genesis descriptor's identity and the metadata record is stored, then
// stop and reopen from the same directory — Reconcile passes.
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

			// The metadata record links the shard to the root genesis.
			meta, err := ReadGenesisMeta(s.DB())
			if err != nil || meta == nil {
				t.Fatalf("ReadGenesisMeta = %v, %v", meta, err)
			}
			rootHash := root.Hash()
			if meta.FullShardID != firstShardID || meta.RootGenesisHash != rootHash ||
				meta.HashPrevRootBlock != rootHash ||
				meta.XShardCursor != (XShardCursor{RootBlockHeight: uint64(root.Number)}) ||
				meta.ChainGenesisHash != descriptor.Fingerprint() {
				t.Errorf("stored metadata %+v inconsistent with config derivation", meta)
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
	if meta, err := ReadGenesisMeta(s.DB()); err != nil || meta == nil {
		t.Fatalf("ReadGenesisMeta = %v, %v", meta, err)
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
		t.Fatalf("reopen err = %v, want loud metadata mismatch", err)
	}
}

func TestShardNewRejectsUnknownShard(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	_, err := New(ctx, account.NewBranch(0x00990099), root, t.TempDir(), Options{})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("shard.New(unknown) err = %v, want 'not configured'", err)
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
