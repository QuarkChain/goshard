// Copyright 2026-2027, QuarkChain.

package shard

import (
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/qkc"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/types"
)

const (
	fixtureMainnet = "../config/singularity/mainnet.json"
	fixtureDevnet  = "../config/singularity/devnet.json"
)

// firstShardID is S0's first owned shard in both singularity networks:
// chain 0, shard size 1, shard 0. secondShardID is the other shard S0 owns.
const (
	firstShardID  = uint32(0x00000001)
	secondShardID = uint32(0x00040001)
)

func loadFixture(t *testing.T, path string) *config.ClusterConfig {
	t.Helper()
	cfg, err := config.LoadClusterConfig(path)
	if err != nil {
		t.Fatalf("LoadClusterConfig(%s): %v", path, err)
	}
	return cfg
}

// bootEnv resolves everything shard.New needs from a fixture: S0's context and
// the derived root genesis header.
func bootEnv(t *testing.T, path string) (*config.SlaveContext, *types.RootBlockHeader) {
	t.Helper()
	cfg := loadFixture(t, path)
	ctx, err := cfg.ResolveSlave("S0")
	if err != nil {
		t.Fatalf("ResolveSlave: %v", err)
	}
	root, err := qkc.CreateRootBlock(cfg.Quarkchain)
	if err != nil {
		t.Fatalf("CreateRootBlock: %v", err)
	}
	return ctx, root
}

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

			// The chain stands at the genesis block the config derives.
			genesis, err := qkc.CreateMinorBlock(ctx.Quarkchain, firstShardID, root, rawdb.NewMemoryDatabase())
			if err != nil {
				t.Fatalf("CreateMinorBlock: %v", err)
			}
			height, head := s.Chain().Head()
			if height != 0 {
				t.Errorf("head height = %d, want 0", height)
			}
			if want := genesis.Hash(); head != want || s.Chain().GenesisHash() != want {
				t.Errorf("head/genesis hash = %s/%s, want %s", head, s.Chain().GenesisHash(), want)
			}

			// The genesis state is in the shard's own database, not just the
			// throwaway one the block was derived against.
			if !rawdb.HasLegacyTrieNode(s.DB(), genesis.Meta.Root) {
				t.Errorf("genesis state root %s is missing from the shard db", genesis.Meta.Root)
			}

			// The genesis block is stored, and carries the shard's root linkage
			// and initial cross-shard cursor.
			stored, err := ReadGenesisBlock(s.DB())
			if err != nil || stored == nil {
				t.Fatalf("ReadGenesisBlock = %v, %v", stored, err)
			}
			if stored.Hash() != genesis.Hash() {
				t.Errorf("stored genesis %s, want %s", stored.Hash(), genesis.Hash())
			}
			if stored.Header.Branch.GetFullShardID() != firstShardID ||
				stored.Header.PrevRootBlockHash != root.Hash() ||
				stored.Meta.XShardTxCursor != (types.XShardTxCursorInfo{RootBlockHeight: uint64(root.Number)}) {
				t.Errorf("stored genesis %+v inconsistent with config derivation", stored.Header)
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
	if block, err := ReadGenesisBlock(s.DB()); err != nil || block == nil {
		t.Fatalf("ReadGenesisBlock = %v, %v", block, err)
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
// shard's own genesis untouched) is also caught on reopen.
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

	diff := ctx.Quarkchain.Root.Genesis.Difficulty
	diff.Add(diff, big.NewInt(1))
	root2, err := qkc.CreateRootBlock(ctx.Quarkchain)
	if err != nil {
		t.Fatalf("CreateRootBlock: %v", err)
	}
	_, err = New(ctx, branch, root2, datadir, Options{})
	if err == nil || !strings.Contains(err.Error(), "cluster config changed since initialization") {
		t.Fatalf("reopen err = %v, want loud genesis mismatch", err)
	}
}

// TestShardReopenMissingGenesisState: a datadir that kept the genesis block but
// lost the state under it must not boot. The stored block is only an identity —
// it passes the reconcile unchanged — so nothing but an explicit check stands
// between a corrupt datadir and a chain that reports "existing genesis validated"
// and then fails at its first state access.
func TestShardReopenMissingGenesisState(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	datadir := t.TempDir()
	branch := account.NewBranch(firstShardID)

	s, err := New(ctx, branch, root, datadir, Options{})
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}
	genesis, err := qkc.CreateMinorBlock(ctx.Quarkchain, firstShardID, root, rawdb.NewMemoryDatabase())
	if err != nil {
		t.Fatalf("CreateMinorBlock: %v", err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Drop the genesis state while leaving the stored genesis block intact.
	withDB(t, datadir, func(db ethdb.Database) {
		rawdb.DeleteLegacyTrieNode(db, genesis.Meta.Root)
	})

	_, err = New(ctx, branch, root, datadir, Options{})
	if err == nil || !strings.Contains(err.Error(), "its state is missing") ||
		!strings.Contains(err.Error(), "corrupt chaindb") ||
		!strings.Contains(err.Error(), datadir) {
		t.Fatalf("reopen err = %v, want a loud missing-state failure naming the db path", err)
	}
	// The failed boot did not quietly re-materialize what was lost: a corrupt
	// datadir stays corrupt until an operator looks at it.
	withDB(t, datadir, func(db ethdb.Database) {
		if rawdb.HasLegacyTrieNode(db, genesis.Meta.Root) {
			t.Error("the failed reopen rewrote the genesis state instead of reporting corruption")
		}
	})
}

func TestShardNewRejectsUnknownShard(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	_, err := New(ctx, account.NewBranch(0x00990099), root, t.TempDir(), Options{})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("shard.New(unknown) err = %v, want 'not configured'", err)
	}
}

// TestShardNewRejectsForeignShard: 0x00010001 is configured in the cluster but
// owned by S1, so S0's context must refuse it — and refuse it before touching
// the datadir.
func TestShardNewRejectsForeignShard(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	datadir := t.TempDir()
	const foreignShardID = uint32(0x00010001)

	_, err := New(ctx, account.NewBranch(foreignShardID), root, datadir, Options{})
	if err == nil || !strings.Contains(err.Error(), "not assigned to slave \"S0\"") {
		t.Fatalf("shard.New(foreign) err = %v, want a rejected assignment naming S0", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("0x%08x", firstShardID)) {
		t.Errorf("err = %q, want the owned ids listed", err)
	}
	if _, err := os.Stat(filepath.Join(datadir, DBDirName(foreignShardID))); !os.IsNotExist(err) {
		t.Errorf("stat foreign shard db dir = %v, want it never created", err)
	}
}

// failingChainService fails every chain construction, standing in for a real
// chain implementation that errors during boot.
type failingChainService struct{}

func (failingChainService) New(ethdb.Database, *types.MinorBlock, *params.ChainConfig) (ShardChain, error) {
	return nil, errors.New("injected chain failure")
}

// TestShardFailedFirstBootLeavesNoGenesis: a boot that fails at chain
// construction stores no genesis block, so the retry takes the fresh path
// instead of reporting a never-validated genesis as existing.
func TestShardFailedFirstBootLeavesNoGenesis(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	datadir := t.TempDir()
	branch := account.NewBranch(firstShardID)

	if _, err := New(ctx, branch, root, datadir, Options{Chain: failingChainService{}}); err == nil ||
		!strings.Contains(err.Error(), "injected chain failure") {
		t.Fatalf("New(failing chain) err = %v, want the injected failure", err)
	}

	// The half-initialized db carries no genesis.
	kv, err := pebble.New(filepath.Join(datadir, fmt.Sprintf("shard-0x%08x", firstShardID)),
		dbCacheMB, dbHandles, "", false)
	if err != nil {
		t.Fatalf("reopen chaindb: %v", err)
	}
	block, err := ReadGenesisBlock(rawdb.NewDatabase(kv))
	kv.Close()
	if err != nil || block != nil {
		t.Fatalf("genesis after failed boot = %+v, %v, want none", block, err)
	}

	// The retry succeeds and stores the genesis.
	s, err := New(ctx, branch, root, datadir, Options{})
	if err != nil {
		t.Fatalf("shard.New(retry): %v", err)
	}
	if block, err := ReadGenesisBlock(s.DB()); err != nil || block == nil {
		t.Fatalf("ReadGenesisBlock after retry = %v, %v", block, err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// chainAtHeight stands at a chosen height, standing in for a reopened datadir
// whose chain already holds imported blocks — what the stub, permanently at
// genesis, cannot express.
type chainAtHeight struct{ height uint64 }

func (c chainAtHeight) New(_ ethdb.Database, genesis *types.MinorBlock, _ *params.ChainConfig) (ShardChain, error) {
	return &heightChain{genesis: genesis.Hash(), height: c.height}, nil
}

type heightChain struct {
	genesis common.Hash
	height  uint64
}

func (c *heightChain) GenesisHash() common.Hash { return c.genesis }
func (c *heightChain) Head() (uint64, common.Hash) {
	return c.height, common.BigToHash(new(big.Int).SetUint64(c.height))
}
func (c *heightChain) Stop() {}

// TestShardReopenIncompatibleForkSchedule: a datadir standing above genesis must
// not adopt a fork schedule its blocks were never executed under. The head the
// compatibility check runs at comes from the chain, so the same datadir is
// rejected above genesis and accepted at genesis.
func TestShardReopenIncompatibleForkSchedule(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	datadir := t.TempDir()
	branch := account.NewBranch(firstShardID)

	s, err := New(ctx, branch, root, datadir, Options{})
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}
	genesisHash := s.Chain().GenesisHash()
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Stand in for a datadir initialized by a build that scheduled a fork the
	// current config no longer carries.
	withDB(t, datadir, func(db ethdb.Database) {
		stored := rawdb.ReadChainConfig(db, genesisHash)
		if stored == nil {
			t.Fatal("first boot stored no chain config")
		}
		stored.IstanbulBlock = big.NewInt(100)
		rawdb.WriteChainConfig(db, genesisHash, stored)
	})

	// Above that fork, the config from the cluster file is a break with the
	// executed history.
	if _, err := New(ctx, branch, root, datadir, Options{Chain: chainAtHeight{height: 200}}); err == nil ||
		!strings.Contains(err.Error(), "incompatible chain config") ||
		!strings.Contains(err.Error(), datadir) {
		t.Fatalf("reopen at head 200 err = %v, want a loud incompatibility naming the db path", err)
	}
	withDB(t, datadir, func(db ethdb.Database) {
		if stored := rawdb.ReadChainConfig(db, genesisHash); stored == nil || stored.IstanbulBlock == nil {
			t.Errorf("stored chain config = %v, want the rejected change left unwritten", stored)
		}
	})

	// The same datadir at genesis has executed nothing under the old rules, so
	// the change is adopted instead — geth's rule, and the reason the head must
	// be the real one.
	s, err = New(ctx, branch, root, datadir, Options{Chain: chainAtHeight{height: 0}})
	if err != nil {
		t.Fatalf("shard.New(head 0): %v", err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop(head 0): %v", err)
	}
	withDB(t, datadir, func(db ethdb.Database) {
		if stored := rawdb.ReadChainConfig(db, genesisHash); stored == nil || stored.IstanbulBlock != nil {
			t.Errorf("stored chain config = %v, want the config rules adopted at genesis", stored)
		}
	})
}

// withDB opens firstShardID's chaindb outside of a running shard, so a test can
// inspect or doctor what a boot left behind.
func withDB(t *testing.T, datadir string, fn func(ethdb.Database)) {
	t.Helper()
	kv, err := pebble.New(filepath.Join(datadir, DBDirName(firstShardID)), dbCacheMB, dbHandles, "", false)
	if err != nil {
		t.Fatalf("reopen chaindb: %v", err)
	}
	defer kv.Close()
	fn(rawdb.NewDatabase(kv))
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
		"shard-0xABCD0001",  // uppercase: DBDirName never emits it
		"chaindata",
	} {
		if id, ok := ParseDBDirName(name); ok {
			t.Errorf("ParseDBDirName(%q) = (0x%08x, true), want rejection", name, id)
		}
	}
}
