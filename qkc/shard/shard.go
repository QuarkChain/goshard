// Copyright 2026-2027, QuarkChain.

// Package shard hosts one QuarkChain shard inside the slave process: an isolated
// per-shard chaindb, the genesis descriptor derived from config, the temporary
// genesis metadata record, and the chain behind the ShardChain seam. It performs
// no network I/O.
package shard

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// Modest fixed sizing for the skeleton's per-shard pebble instance; real tuning
// belongs to the chain task.
const (
	dbCacheMB = 16
	dbHandles = 16
)

// DBDirName returns the chaindb directory name one shard uses under the datadir.
func DBDirName(fullShardID uint32) string {
	return fmt.Sprintf("shard-0x%08x", fullShardID)
}

// ParseDBDirName reports the full shard id a chaindb directory name encodes, or
// ok=false when the name is not a canonical shard chaindb directory name.
func ParseDBDirName(name string) (fullShardID uint32, ok bool) {
	hexPart, found := strings.CutPrefix(name, "shard-0x")
	if !found || len(hexPart) != 8 {
		return 0, false
	}
	id, err := strconv.ParseUint(hexPart, 16, 32)
	if err != nil {
		return 0, false
	}
	return uint32(id), true
}

// Shard is one shard chain hosted by the slave: its Branch (the registry key), its
// resolved config, an isolated chaindb, and the chain behind the ShardChain seam.
type Shard struct {
	Branch account.Branch
	cfg    *config.ShardConfig
	db     ethdb.Database
	chain  ShardChain

	stopOnce sync.Once
	stopErr  error
}

// New constructs one shard: it opens an isolated pebble chaindb under
// {datadir}/shard-0x{full_shard_id}/ — or an ephemeral in-memory database when
// datadir is empty, pyquarkchain's mem-db mode (use_mem_db, cluster_config.py:247)
// — writes or reconciles the genesis metadata, and constructs the chain through
// the ShardChain seam. On any failure the database is closed before returning,
// so the datadir stays reopenable.
func New(ctx *config.SlaveContext, branch account.Branch, rootGenesis *types.RootBlockHeader, datadir string, opts Options) (*Shard, error) {
	fullShardID := branch.GetFullShardID()
	shardCfg := ctx.Quarkchain.GetShardConfigByFullShardID(fullShardID)
	if shardCfg == nil {
		return nil, fmt.Errorf("shard 0x%08x is not configured in any chain", fullShardID)
	}
	genesis, err := NewGenesis(ctx.Quarkchain, shardCfg)
	if err != nil {
		return nil, fmt.Errorf("shard 0x%08x: %w", fullShardID, err)
	}

	var db ethdb.Database
	dbPath := "in-memory"
	if datadir == "" {
		db = rawdb.NewMemoryDatabase()
	} else {
		// A directory per shard (not pyquarkchain's shard-{id}.db file): the
		// directory form is what geth's rawdb expects.
		dbPath = filepath.Join(datadir, DBDirName(fullShardID))
		kv, err := pebble.New(dbPath, dbCacheMB, dbHandles, fmt.Sprintf("qkc/shard/0x%08x/", fullShardID), false)
		if err != nil {
			return nil, fmt.Errorf("shard 0x%08x: open chaindb %s: %w", fullShardID, dbPath, err)
		}
		db = rawdb.NewDatabase(kv)
	}

	rootHash := rootGenesis.Hash()
	expected := &GenesisMeta{
		Version:           genesisMetaVersion,
		FullShardID:       fullShardID,
		RootGenesisHash:   rootHash,
		HashPrevRootBlock: rootHash,
		// The cross-shard cursor starts at (root_height, 0, 0), matching
		// pyquarkchain (quarkchain/genesis.py:92).
		XShardCursor:     XShardCursor{RootBlockHeight: uint64(rootGenesis.Number)},
		ChainGenesisHash: genesis.Fingerprint(),
	}
	existed, err := ReconcileGenesisMeta(db, expected, dbPath)
	if err != nil {
		db.Close()
		return nil, err
	}

	chain, err := opts.chainService().New(db, genesis)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("shard 0x%08x: construct chain: %w", fullShardID, err)
	}
	// The seam's genesis must be the one the metadata records. The stub derives
	// both from the descriptor, so this only fires when a chain implementation
	// disagrees with the record — the moment the metadata scaffold must go.
	if got := chain.GenesisHash(); got != expected.ChainGenesisHash {
		chain.Stop()
		db.Close()
		return nil, fmt.Errorf("shard 0x%08x: chain genesis %s does not match recorded genesis %s (db %s)",
			fullShardID, got, expected.ChainGenesisHash, dbPath)
	}

	if existed {
		log.Info("existing genesis validated", "shard", fmt.Sprintf("0x%08x", fullShardID), "genesis", expected.ChainGenesisHash)
	} else {
		// Committed only after the chain stands at this genesis: a boot that
		// failed earlier left no record, so the retry re-runs the fresh path
		// instead of reporting a never-validated record as existing.
		if err := WriteGenesisMeta(db, expected); err != nil {
			chain.Stop()
			db.Close()
			return nil, fmt.Errorf("shard 0x%08x: write genesis metadata (db %s): %w", fullShardID, dbPath, err)
		}
		log.Info("genesis metadata written", "shard", fmt.Sprintf("0x%08x", fullShardID), "genesis", expected.ChainGenesisHash)
	}

	return &Shard{Branch: branch, cfg: shardCfg, db: db, chain: chain}, nil
}

// Chain returns the shard chain behind the seam.
func (s *Shard) Chain() ShardChain { return s.chain }

// Config returns the shard's resolved config.
func (s *Shard) Config() *config.ShardConfig { return s.cfg }

// DB returns the shard's chaindb.
func (s *Shard) DB() ethdb.Database { return s.db }

// Stop shuts the shard down: the chain first, then the database. It is idempotent
// and blocks until both are stopped and closed.
func (s *Shard) Stop() error {
	s.stopOnce.Do(func() {
		s.chain.Stop()
		s.stopErr = s.db.Close()
	})
	return s.stopErr
}
