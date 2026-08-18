// Copyright 2026-2027, QuarkChain.

package slave

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/qkc"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/shard"
	"github.com/ethereum/go-ethereum/qkc/types"
)

const (
	fixtureMainnet = "../config/singularity/mainnet.json"
	fixtureDevnet  = "../config/singularity/devnet.json"
)

// bootEnv resolves S0 from a fixture with its db path root redirected into
// t.TempDir(), plus the derived root genesis header.
func bootEnv(t *testing.T, path string) (*config.SlaveContext, *types.RootBlockHeader) {
	t.Helper()
	cfg, err := config.LoadClusterConfig(path)
	if err != nil {
		t.Fatalf("LoadClusterConfig(%s): %v", path, err)
	}
	ctx, err := cfg.ResolveSlave("S0")
	if err != nil {
		t.Fatalf("ResolveSlave: %v", err)
	}
	ctx.DBPathRoot = t.TempDir()
	root, err := qkc.CreateRootBlock(cfg.Quarkchain)
	if err != nil {
		t.Fatalf("CreateRootBlock: %v", err)
	}
	return ctx, root
}

// TestSlaveBootAndReopen boots S0 from each real network config, checks its shard
// registry, stops it, and verifies that the same databases reopen cleanly.
//
// TODO: inject the real chain service here once it exists and assert its
// canonical genesis/head plus blocking shutdown of its background work.
func TestSlaveBootAndReopen(t *testing.T) {
	for _, path := range []string{fixtureMainnet, fixtureDevnet} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			ctx, root := bootEnv(t, path)

			b, err := New(ctx, root, shard.Options{})
			if err != nil {
				t.Fatalf("slave.New: %v", err)
			}
			if b.ID != "S0" {
				t.Errorf("ID = %q, want S0", b.ID)
			}

			shards := b.Shards()
			if len(shards) != len(ctx.FullShardIDs()) {
				t.Fatalf("booted %d shards, config assigns %d", len(shards), len(ctx.FullShardIDs()))
			}
			for i, id := range ctx.FullShardIDs() {
				branch := account.NewBranch(id)
				s := b.Shard(branch)
				if s == nil || s != shards[i] {
					t.Fatalf("shard 0x%08x: registry lookup and boot order disagree", id)
				}
				if height, _ := s.Chain().Head(); height != 0 {
					t.Errorf("shard 0x%08x head height = %d, want 0", id, height)
				}
			}
			if b.Shard(account.NewBranch(0x00990099)) != nil {
				t.Error("lookup of an unowned branch returned a shard")
			}

			if err := b.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}

			b, err = New(ctx, root, shard.Options{})
			if err != nil {
				t.Fatalf("slave.New(reopen): %v", err)
			}
			if err := b.Stop(); err != nil {
				t.Fatalf("Stop(reopen): %v", err)
			}
		})
	}
}

// failingChainService fails chain construction after failAfter successful builds,
// injected through the Options.Chain seam to exercise the boot rollback.
type failingChainService struct {
	failAfter int
	built     int
}

func (s *failingChainService) New(db ethdb.Database, genesis *types.MinorBlock, chainConfig *params.ChainConfig) (shard.ShardChain, error) {
	if s.built >= s.failAfter {
		return nil, errors.New("injected chain failure")
	}
	s.built++
	return shard.StubChainService{}.New(db, genesis, chainConfig)
}

// TestSlaveBootRollback: when a later shard fails to boot, the shards already
// started are stopped and their databases closed (a still-open pebble instance
// would hold its directory lock and make the reboot below fail), and the datadir
// remains reopenable.
func TestSlaveBootRollback(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	if len(ctx.FullShardIDs()) < 2 {
		t.Fatal("fixture must assign S0 at least two shards to exercise rollback")
	}

	_, err := New(ctx, root, shard.Options{Chain: &failingChainService{failAfter: 1}})
	if err == nil || !strings.Contains(err.Error(), "injected chain failure") ||
		!strings.Contains(err.Error(), "slave S0") {
		t.Fatalf("slave.New err = %v, want injected chain failure attributed to slave S0", err)
	}

	b, err := New(ctx, root, shard.Options{})
	if err != nil {
		t.Fatalf("slave.New after rollback: %v", err)
	}
	if err := b.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestSlaveStopIdempotent(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	b, err := New(ctx, root, shard.Options{})
	if err != nil {
		t.Fatalf("slave.New: %v", err)
	}
	if err := b.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := b.Stop(); err != nil {
		t.Fatalf("Stop(again): %v", err)
	}
}
