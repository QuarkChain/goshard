// Copyright 2026-2027, QuarkChain.

// Package shard hosts one QuarkChain shard inside the slave process: an isolated
// per-shard chaindb, the stored genesis block, the chain behind the ShardChain
// seam, and its virtual cluster-peer connections (PeerConns). Peer frames tunnel
// through the master connection rather than a real socket.
//
// The genesis block itself is derived in qkc (qkc.CreateMinorBlock), next to the
// root genesis, as pyquarkchain's GenesisManager does. This package only decides
// where it is stored and whether a reopened database still agrees with it.
package shard

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/qkc"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/conn"
	"github.com/ethereum/go-ethereum/qkc/slaveconn"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/qkc/wire"
)

// Modest fixed sizing for the skeleton's per-shard pebble instance.
//
// TODO: size these against the cluster's real shard count and the process cache
// budget once the chain task owns the database — a slave hosting many shards
// multiplies both numbers, and 16 is a placeholder, not a measurement.
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
	// DBDirName emits lowercase hex only; reject the non-canonical spellings
	// ParseUint would still accept (uppercase digits).
	if !found || len(hexPart) != 8 || hexPart != strings.ToLower(hexPart) {
		return 0, false
	}
	id, err := strconv.ParseUint(hexPart, 16, 32)
	if err != nil {
		return 0, false
	}
	return uint32(id), true
}

// formatShardIDs renders full shard ids the way every other message here spells
// them, for the operator reading a rejected assignment.
func formatShardIDs(ids []uint32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("0x%08x", id)
	}
	return strings.Join(parts, ", ")
}

// Shard is one shard chain hosted by the slave: its Branch (the registry key), its
// resolved config, an isolated chaindb, and the chain behind the ShardChain seam.
type Shard struct {
	Branch account.Branch
	cfg    *config.ShardConfig
	db     ethdb.Database
	chain  ShardChain

	// peers maps cluster_peer_id to the shard's virtual peer connection (py:
	// shard.peers). It is owned here so a shard's peers die with the shard.
	peersMu sync.Mutex
	peers   map[uint64]*slaveconn.PeerConn

	// sender is the outbound hook the business layer uses to reach the cluster
	// (master, x-shard). It is injected at construction; the concrete
	// communicator may not exist until after the comm is built.
	sender Sender

	stopped  atomic.Bool
	stopOnce sync.Once
	stopErr  error
}

// New constructs one shard: it opens an isolated pebble chaindb under
// {datadir}/shard-0x{full_shard_id}/ — or an ephemeral in-memory database when
// datadir is empty, pyquarkchain's mem-db mode (use_mem_db, cluster_config.py:247)
// — commits or reconciles the shard's genesis, and constructs the chain through
// the ShardChain seam. On any failure the database is closed before returning,
// so the datadir stays reopenable.
func New(ctx *config.SlaveContext, branch account.Branch, rootGenesis *types.RootBlockHeader, datadir string, opts Options, sender Sender) (*Shard, error) {
	fullShardID := branch.GetFullShardID()
	// A shard configured somewhere in the cluster can still belong to another
	// slave. Refuse it here, before any database is opened, so a wrong or hostile
	// instruction cannot create or reopen a chaindb outside this slave's
	// assignment.
	if !ctx.Owns(fullShardID) {
		return nil, fmt.Errorf("shard 0x%08x is not assigned to slave %q (owns %s)",
			fullShardID, ctx.ID, formatShardIDs(ctx.FullShardIDs()))
	}
	// Non-nil for every owned id: Validate resolves each entry of the slave's
	// FULL_SHARD_ID_LIST before the config is accepted. A nil here would still be
	// caught by ShardChainConfig below, before any database is opened.
	shardCfg := ctx.Quarkchain.GetShardConfigByFullShardID(fullShardID)
	chainConfig, err := qkc.ShardChainConfig(ctx.Quarkchain, shardCfg)
	if err != nil {
		return nil, fmt.Errorf("shard 0x%08x: %w", fullShardID, err)
	}
	// Derive the genesis before the database is opened: this is the identity a
	// reopened datadir is checked against, and a mismatching config must not leave
	// its state behind. CreateMinorBlock persists nothing; CommitGenesisState below
	// writes the state, and only on the fresh path.
	genesis, err := qkc.CreateMinorBlock(ctx.Quarkchain, fullShardID, rootGenesis)
	if err != nil {
		return nil, err
	}

	db, dbPath, err := openChainDB(fullShardID, datadir)
	if err != nil {
		return nil, err
	}
	setup := genesisSetup{
		block:       genesis,
		chainConfig: chainConfig,
		commit: func(target ethdb.Database) error {
			stateRoot, err := qkc.CommitGenesisState(ctx.Quarkchain, fullShardID, target)
			if err != nil {
				return err
			}
			// The block was derived from a hash of the same allocation. If the
			// flushed root disagrees, the chain would open on a genesis whose state
			// is not the one below it.
			if stateRoot != genesis.Root() {
				return fmt.Errorf("committed genesis state %s does not match the derived genesis root %s", stateRoot, genesis.Root())
			}
			return nil
		},
	}
	chain, existed, err := initializeChain(db, dbPath, setup, opts.chainService())
	if err != nil {
		db.Close()
		return nil, err
	}

	if existed {
		log.Info("existing genesis validated", "shard", fmt.Sprintf("0x%08x", fullShardID), "genesis", genesis.Hash())
	} else {
		log.Info("genesis committed", "shard", fmt.Sprintf("0x%08x", fullShardID), "genesis", genesis.Hash())
	}

	return &Shard{
		Branch: branch,
		cfg:    shardCfg,
		db:     db,
		chain:  chain,
		peers:  make(map[uint64]*slaveconn.PeerConn),
		sender: sender,
	}, nil
}

func openChainDB(fullShardID uint32, datadir string) (ethdb.Database, string, error) {
	if datadir == "" {
		return rawdb.NewMemoryDatabase(), "in-memory", nil
	}
	// A directory per shard (not pyquarkchain's shard-{id}.db file): the
	// directory form is what geth's rawdb expects.
	dbPath := filepath.Join(datadir, DBDirName(fullShardID))
	kv, err := pebble.New(dbPath, dbCacheMB, dbHandles, fmt.Sprintf("qkc/shard/0x%08x/", fullShardID), false)
	if err != nil {
		return nil, dbPath, fmt.Errorf("shard 0x%08x: open chaindb %s: %w", fullShardID, dbPath, err)
	}
	return rawdb.NewDatabase(kv), dbPath, nil
}

// genesisSetup is everything needed to stand a shard at its genesis: the block,
// the EVM rule set, and the commit that materializes the genesis state into the
// shard's own database.
type genesisSetup struct {
	block       *types.MinorBlock
	chainConfig *params.ChainConfig
	commit      func(ethdb.Database) error
}

// initializeChain reconciles the stored genesis against the config-derived one,
// commits the genesis state on a fresh database, constructs the chain, and stores
// the genesis block only once the chain is standing. It stops a constructed chain
// on failure; the caller retains ownership of the db.
func initializeChain(db ethdb.Database, dbPath string, g genesisSetup, service ChainService) (ShardChain, bool, error) {
	fullShardID := g.block.Header().Branch.GetFullShardID()
	existed, err := ReconcileGenesisBlock(db, g.block, dbPath)
	if err != nil {
		return nil, false, err
	}
	if !existed {
		// Materialize the genesis state into this database before the chain opens
		// on it.
		if err := g.commit(db); err != nil {
			return nil, false, fmt.Errorf("shard 0x%08x: commit genesis state (db %s): %w", fullShardID, dbPath, err)
		}
	} else if err := qkc.CheckGenesisState(db, g.block.Root()); err != nil {
		// On reopen the state is already there — unless the datadir lost it. The
		// stored genesis is an identity and says nothing about the trie under it, so
		// the two are checked separately. Re-materializing here would repair a
		// corrupt database silently and hide whatever else it dropped; geth answers
		// a missing head state the same way, by refusing to open the chain.
		return nil, false, fmt.Errorf("shard 0x%08x: genesis is stored but its state is missing (db %s): %w — corrupt chaindb",
			fullShardID, dbPath, err)
	}
	chain, err := service.New(db, g.block, g.chainConfig)
	if err != nil {
		return nil, false, fmt.Errorf("shard 0x%08x: construct chain: %w", fullShardID, err)
	}
	// The chain must stand at the genesis it was handed.
	if got := chain.GenesisHash(); got != g.block.Hash() {
		chain.Stop()
		return nil, false, fmt.Errorf("shard 0x%08x: chain genesis %s does not match the derived genesis %s (db %s)",
			fullShardID, got, g.block.Hash(), dbPath)
	}
	// The rule set is reconciled apart from the genesis identity, as in geth. The
	// head comes from the chain rather than a constant: a reopened datadir may
	// already stand above genesis, and the fork schedule its blocks were executed
	// under must not be replaced silently.
	//
	// TODO: geth runs this check before the chain is constructed, against the head
	// header persisted in the database (core/genesis.go), and answers an
	// incompatibility by rewinding to the fork point instead of refusing to boot.
	// Neither is reachable yet — minor blocks have no persisted head, and the seam
	// exposes no rewind. Move the check ahead of service.New, and take the head
	// height and timestamp from the stored header, once the chain owns block
	// storage — the same task that retires genesisBlockKey.
	head, _ := chain.Head()
	if err := ReconcileChainConfig(db, g.block, g.chainConfig, head, existed, dbPath); err != nil {
		chain.Stop()
		return nil, false, err
	}
	if existed {
		return chain, true, nil
	}
	// Stored only after the chain stands at this genesis: a boot that failed
	// earlier left nothing behind, so the retry re-runs the fresh path.
	if err := WriteGenesisBlock(db, g.block); err != nil {
		chain.Stop()
		return nil, false, fmt.Errorf("shard 0x%08x: write genesis block (db %s): %w", fullShardID, dbPath, err)
	}
	return chain, false, nil
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
		s.stopped.Store(true)
		s.ClosePeers()
		s.chain.Stop()
		s.stopErr = s.db.Close()
	})
	return s.stopErr
}

// ── Virtual cluster peers ─────────────────────────────────────────────────────

// ErrStopped is returned by AddPeer when the shard has already been stopped: the
// peer registry is (about to be) drained, so no new PeerConn may be created.
var ErrStopped = errors.New("shard stopped")

var _ slaveconn.PeerHandler = (*Shard)(nil)

// Peer returns the shard's virtual peer connection for clusterPeerID, or nil.
func (s *Shard) Peer(clusterPeerID uint64) *slaveconn.PeerConn {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()
	return s.peers[clusterPeerID]
}

// AddPeer builds, starts, and registers a virtual peer connection for
// clusterPeerID against master connection mc, reporting created=false on a
// duplicate. The stopped check inside peersMu totally orders registration against
// ClosePeers's drain: a PeerConn is either registered before the drain and closed
// by it, or refused after it.
func (s *Shard) AddPeer(clusterPeerID uint64, mc *slaveconn.MasterConn) (created bool, err error) {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()
	if s.stopped.Load() {
		return false, ErrStopped
	}
	if _, exists := s.peers[clusterPeerID]; exists {
		return false, nil
	}

	pc, err := slaveconn.NewPeerConn(clusterPeerID, s.Branch.GetFullShardID(), mc, s, log.Root())
	if err != nil {
		return false, err
	}
	pc.Start()
	s.peers[clusterPeerID] = pc
	return true, nil
}

// RemovePeer deregisters and closes the virtual peer connection, or is a no-op
// for an unknown id. Close happens outside peersMu.
func (s *Shard) RemovePeer(clusterPeerID uint64) {
	s.peersMu.Lock()
	pc, ok := s.peers[clusterPeerID]
	if ok {
		delete(s.peers, clusterPeerID)
	}
	s.peersMu.Unlock()
	if ok {
		pc.Close()
	}
}

// ClosePeers removes and closes every registered virtual peer connection. The
// snapshot-and-drain happens under peersMu, which AddPeer's stopped gate also
// takes, so registration and shutdown are totally ordered. Close happens outside
// peersMu.
func (s *Shard) ClosePeers() {
	s.peersMu.Lock()
	all := make([]*slaveconn.PeerConn, 0, len(s.peers))
	for _, pc := range s.peers {
		all = append(all, pc)
	}
	s.peers = make(map[uint64]*slaveconn.PeerConn)
	s.peersMu.Unlock()
	for _, pc := range all {
		pc.Close()
	}
}

// ── PeerHandler: inbound peer traffic ────────────────────────────────────────
//
// These remain stubs until the shard chain implementation lands. They return
// ErrHandlerNotImplemented rather than faking success.

func (s *Shard) NewMinorBlockHeaderList(*wire.NewMinorBlockHeaderListCommand) error {
	return fmt.Errorf("NewMinorBlockHeaderList: %w", conn.ErrHandlerNotImplemented)
}

func (s *Shard) NewTransactionList(*wire.NewTransactionListCommand) error {
	return fmt.Errorf("NewTransactionList: %w", conn.ErrHandlerNotImplemented)
}

func (s *Shard) NewBlockMinor(*wire.NewBlockMinorCommand) error {
	return fmt.Errorf("NewBlockMinor: %w", conn.ErrHandlerNotImplemented)
}

func (s *Shard) GetMinorBlockHeaderList(*wire.GetMinorBlockHeaderListRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
	return nil, fmt.Errorf("GetMinorBlockHeaderList: %w", conn.ErrHandlerNotImplemented)
}

func (s *Shard) GetMinorBlockList(*wire.GetMinorBlockListRequest) (*wire.GetMinorBlockListResponse, error) {
	return nil, fmt.Errorf("GetMinorBlockList: %w", conn.ErrHandlerNotImplemented)
}

func (s *Shard) GetMinorBlockHeaderListWithSkip(*wire.GetMinorBlockHeaderListWithSkipRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
	return nil, fmt.Errorf("GetMinorBlockHeaderListWithSkip: %w", conn.ErrHandlerNotImplemented)
}
