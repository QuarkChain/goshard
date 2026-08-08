// Copyright 2026-2027, QuarkChain.

package shard

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/qkc"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// testGenesisBlock derives chain 0's genesis block from one of the real network
// configs. The two networks derive different blocks, which is what the mismatch
// cases below rely on.
func testGenesisBlock(t *testing.T, path string) *types.MinorBlock {
	t.Helper()
	return testGenesisBlockOf(t, path, firstShardID)
}

func testGenesisBlockOf(t *testing.T, path string, fullShardID uint32) *types.MinorBlock {
	t.Helper()
	cfg := loadFixture(t, path)
	root, err := qkc.CreateRootBlock(cfg.Quarkchain)
	if err != nil {
		t.Fatalf("CreateRootBlock: %v", err)
	}
	block, err := qkc.CreateMinorBlock(cfg.Quarkchain, fullShardID, root)
	if err != nil {
		t.Fatalf("CreateMinorBlock: %v", err)
	}
	return block
}

// TestGenesisBlockRoundTrip: the stored block decodes back to the same block —
// header, meta and empty body included.
func TestGenesisBlockRoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	got, err := ReadGenesisBlock(db)
	if err != nil || got != nil {
		t.Fatalf("ReadGenesisBlock(empty) = %v, %v, want nil, nil", got, err)
	}

	want := testGenesisBlock(t, fixtureMainnet)
	if err := WriteGenesisBlock(db, want); err != nil {
		t.Fatalf("WriteGenesisBlock: %v", err)
	}
	got, err = ReadGenesisBlock(db)
	if err != nil {
		t.Fatalf("ReadGenesisBlock: %v", err)
	}
	if got.Hash() != want.Hash() {
		t.Errorf("round-trip hash mismatch: got %s, want %s", got.Hash(), want.Hash())
	}
	if got.Meta().Hash() != want.Meta().Hash() {
		t.Errorf("round-trip meta mismatch: got %s, want %s", got.Meta().Hash(), want.Meta().Hash())
	}
	if got.Root() != want.Root() {
		t.Errorf("round-trip state root = %s, want %s", got.Root(), want.Root())
	}
	if got.Header().Branch.GetFullShardID() != firstShardID {
		t.Errorf("round-trip branch = 0x%08x, want 0x%08x", got.Header().Branch.GetFullShardID(), firstShardID)
	}
	if len(got.Transactions()) != 0 || len(got.TrackingData()) != 0 {
		t.Errorf("round-trip body = %d txs / %d tracking bytes, want empty", len(got.Transactions()), len(got.TrackingData()))
	}
}

func TestReconcileGenesisBlock(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	expected := testGenesisBlock(t, fixtureMainnet)

	// Fresh db: reported as fresh, and nothing is written — storing the block is
	// the caller's job once the chain stands at it.
	if existed, err := ReconcileGenesisBlock(db, expected, "/tmp/x"); err != nil || existed {
		t.Fatalf("Reconcile(fresh) = %v, %v, want false, nil", existed, err)
	}
	if block, err := ReadGenesisBlock(db); err != nil || block != nil {
		t.Fatalf("Reconcile(fresh) wrote a block: %+v, %v", block, err)
	}

	// Reopen with the identical genesis after the caller stored it: passes.
	if err := WriteGenesisBlock(db, expected); err != nil {
		t.Fatalf("WriteGenesisBlock: %v", err)
	}
	if existed, err := ReconcileGenesisBlock(db, expected, "/tmp/x"); err != nil || !existed {
		t.Fatalf("Reconcile(reopen) = %v, %v, want true, nil", existed, err)
	}

	// A different genesis: a typed mismatch carrying both identities, whose
	// message names both hashes and the db.
	changed := testGenesisBlock(t, fixtureDevnet)
	_, err := ReconcileGenesisBlock(db, changed, "/tmp/x")
	var mismatch *GenesisMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Reconcile(changed genesis) err = %v, want *GenesisMismatchError", err)
	}
	if mismatch.Stored != expected.Hash() || mismatch.New != changed.Hash() {
		t.Errorf("mismatch carries %s/%s, want %s/%s",
			mismatch.Stored, mismatch.New, expected.Hash(), changed.Hash())
	}
	if !strings.Contains(err.Error(), "does not match config genesis") ||
		!strings.Contains(err.Error(), "/tmp/x") ||
		!strings.Contains(err.Error(), "cluster config changed since initialization") {
		t.Errorf("Reconcile(changed genesis) message = %q, want the loud operator-facing form", err)
	}

	// A corrupt payload is reported as such, not read as an absent genesis.
	if err := db.Put(genesisBlockKey, []byte{0x01, 0x02}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := ReconcileGenesisBlock(db, expected, "/tmp/x"); err == nil ||
		!strings.Contains(err.Error(), "decode genesis block") {
		t.Fatalf("Reconcile(corrupt) err = %v, want a decode failure", err)
	}
}

// TestReconcileGenesisBlockTamperedMeta: a minor block's hash is its header's hash
// alone, and nothing verifies on read that the stored meta still hashes to the
// header's hash_meta. So a stored block whose state root was replaced under an
// untouched header compares equal on hash — the reconcile must compare the stored
// encoding instead, or it would hand the chain a genesis it never derived.
func TestReconcileGenesisBlockTamperedMeta(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	expected := testGenesisBlock(t, fixtureMainnet)

	meta := *expected.Meta()
	meta.Root = common.HexToHash("0xdead")
	tampered := types.NewMinorBlockWithHeader(expected.Header(), &meta)
	if tampered.Hash() != expected.Hash() {
		t.Fatal("tampering with the meta moved the block hash: this test no longer covers what it claims")
	}
	if err := WriteGenesisBlock(db, tampered); err != nil {
		t.Fatalf("WriteGenesisBlock: %v", err)
	}

	existed, err := ReconcileGenesisBlock(db, expected, "/tmp/x")
	if !existed || err == nil ||
		!strings.Contains(err.Error(), "does not match the meta and body its header commits to") ||
		!strings.Contains(err.Error(), "corrupt chaindb") ||
		!strings.Contains(err.Error(), "/tmp/x") {
		t.Fatalf("Reconcile(tampered meta) = %v, %v, want a loud corruption report", existed, err)
	}
	// Not reported as a config change: the config never produced this block.
	var mismatch *GenesisMismatchError
	if errors.As(err, &mismatch) {
		t.Errorf("Reconcile(tampered meta) reported a config change: %v", err)
	}
}

// TestReconcileGenesisBlockMisplacedChainDB: another shard's genesis in this
// shard's directory is a misplaced datadir, not a cluster config change, and says
// so. Both shards below belong to S0 in the mainnet config.
func TestReconcileGenesisBlockMisplacedChainDB(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := WriteGenesisBlock(db, testGenesisBlockOf(t, fixtureMainnet, secondShardID)); err != nil {
		t.Fatalf("WriteGenesisBlock: %v", err)
	}

	_, err := ReconcileGenesisBlock(db, testGenesisBlock(t, fixtureMainnet), "/tmp/x")
	if err == nil || !strings.Contains(err.Error(), "misplaced chaindb") ||
		!strings.Contains(err.Error(), fmt.Sprintf("belongs to shard 0x%08x", secondShardID)) {
		t.Fatalf("Reconcile(other shard) err = %v, want a misplaced-chaindb report naming both shards", err)
	}
}

// petersburgOnly is the rule set every shard runs today (see qkc.ShardChainConfig).
func petersburgOnly(chainID int64) *params.ChainConfig {
	zero := big.NewInt(0)
	return &params.ChainConfig{
		ChainID:             big.NewInt(chainID),
		HomesteadBlock:      zero,
		EIP150Block:         zero,
		EIP155Block:         zero,
		EIP158Block:         zero,
		ByzantiumBlock:      zero,
		ConstantinopleBlock: zero,
		PetersburgBlock:     zero,
	}
}

// TestReconcileChainConfig: the rule set lives outside the genesis identity and is
// reconciled the way geth does it — stored on first boot, and on reopen a change
// is accepted (nothing has executed under the old rules on a genesis-only db)
// rather than forcing a re-bootstrap.
func TestReconcileChainConfig(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	genesis := testGenesisBlock(t, fixtureMainnet)
	cfg := petersburgOnly(100001)

	if err := ReconcileChainConfig(db, genesis, cfg, 0, false, "/tmp/x"); err != nil {
		t.Fatalf("ReconcileChainConfig(fresh): %v", err)
	}
	stored := rawdb.ReadChainConfig(db, genesis.Hash())
	if stored == nil || stored.ChainID.Cmp(cfg.ChainID) != 0 {
		t.Fatalf("stored chain config = %v, want chain id %v", stored, cfg.ChainID)
	}

	// Reopen with the same rules: accepted, and nothing is rewritten.
	if err := ReconcileChainConfig(db, genesis, cfg, 0, true, "/tmp/x"); err != nil {
		t.Fatalf("ReconcileChainConfig(reopen): %v", err)
	}

	// A later fork appearing in a software upgrade is a compatible change on a
	// genesis-only db: accepted and adopted, not a loud failure.
	upgraded := petersburgOnly(100001)
	upgraded.IstanbulBlock = big.NewInt(100)
	if err := ReconcileChainConfig(db, genesis, upgraded, 0, true, "/tmp/x"); err != nil {
		t.Fatalf("ReconcileChainConfig(added fork): %v", err)
	}
	if stored := rawdb.ReadChainConfig(db, genesis.Hash()); stored == nil || stored.IstanbulBlock == nil {
		t.Errorf("stored chain config = %v, want the upgraded rules adopted", stored)
	}

	// An out-of-order fork schedule is a bug, and is rejected before anything is
	// written.
	broken := petersburgOnly(100001)
	broken.ByzantiumBlock = nil
	if err := ReconcileChainConfig(db, genesis, broken, 0, true, "/tmp/x"); err == nil ||
		!strings.Contains(err.Error(), "invalid chain config") {
		t.Fatalf("ReconcileChainConfig(fork gap) err = %v, want a fork-order rejection", err)
	}
	if stored := rawdb.ReadChainConfig(db, genesis.Hash()); stored == nil || stored.ByzantiumBlock == nil {
		t.Errorf("stored chain config = %v, want the rejected change left unwritten", stored)
	}
}

// TestReconcileChainConfigAboveGenesis: above genesis the rules the stored blocks
// were executed under cannot be replaced silently — the check a genesis-only
// database skips.
func TestReconcileChainConfigAboveGenesis(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	genesis := testGenesisBlock(t, fixtureMainnet)
	scheduled := petersburgOnly(100001)
	scheduled.IstanbulBlock = big.NewInt(100)
	if err := ReconcileChainConfig(db, genesis, scheduled, 0, false, "/tmp/x"); err != nil {
		t.Fatalf("ReconcileChainConfig(fresh): %v", err)
	}

	// A fork the chain has already passed disappearing from the schedule breaks
	// with the executed history: rejected, and nothing is written.
	dropped := petersburgOnly(100001)
	err := ReconcileChainConfig(db, genesis, dropped, 200, true, "/tmp/x")
	if err == nil || !strings.Contains(err.Error(), "incompatible chain config") ||
		!strings.Contains(err.Error(), "/tmp/x") {
		t.Fatalf("ReconcileChainConfig(dropped fork at head 200) err = %v, want a loud incompatibility", err)
	}
	if stored := rawdb.ReadChainConfig(db, genesis.Hash()); stored == nil || stored.IstanbulBlock == nil {
		t.Errorf("stored chain config = %v, want the rejected change left unwritten", stored)
	}

	// A fork scheduled above the head is still ahead of the executed history:
	// accepted and adopted, as on a genesis-only db.
	later := petersburgOnly(100001)
	later.IstanbulBlock = big.NewInt(100)
	later.BerlinBlock = big.NewInt(1000)
	if err := ReconcileChainConfig(db, genesis, later, 200, true, "/tmp/x"); err != nil {
		t.Fatalf("ReconcileChainConfig(future fork at head 200): %v", err)
	}
	if stored := rawdb.ReadChainConfig(db, genesis.Hash()); stored == nil || stored.BerlinBlock == nil {
		t.Errorf("stored chain config = %v, want the compatible change adopted", stored)
	}
}

// TestReconcileChainConfigBlock0ForkAboveGenesis: a fork scheduled at block 0 —
// which is where every shard fork sits — changing under a chain that already has
// history is rejected. Geth accepts this one (its RewindToBlock is 0, and its
// caller would rewind); the slave has no rewind, so it refuses to boot.
func TestReconcileChainConfigBlock0ForkAboveGenesis(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	genesis := testGenesisBlock(t, fixtureMainnet)
	cfg := petersburgOnly(100001)
	if err := ReconcileChainConfig(db, genesis, cfg, 0, false, "/tmp/x"); err != nil {
		t.Fatalf("ReconcileChainConfig(fresh): %v", err)
	}

	added := petersburgOnly(100001)
	added.IstanbulBlock = big.NewInt(0)
	if err := ReconcileChainConfig(db, genesis, added, 200, true, "/tmp/x"); err == nil ||
		!strings.Contains(err.Error(), "incompatible chain config") {
		t.Fatalf("ReconcileChainConfig(block-0 fork at head 200) err = %v, want a loud incompatibility", err)
	}
	if stored := rawdb.ReadChainConfig(db, genesis.Hash()); stored == nil || stored.IstanbulBlock != nil {
		t.Errorf("stored chain config = %v, want the rejected change left unwritten", stored)
	}
}

// TestReconcileChainConfigReadOnly: an unchanged config must not be rewritten,
// the guard geth keeps for handles that cannot take a write (core/genesis.go).
func TestReconcileChainConfigReadOnly(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	genesis := testGenesisBlock(t, fixtureMainnet)
	cfg := petersburgOnly(100001)
	if err := ReconcileChainConfig(db, genesis, cfg, 0, false, "/tmp/x"); err != nil {
		t.Fatalf("ReconcileChainConfig(fresh): %v", err)
	}
	if err := ReconcileChainConfig(readOnlyDB{db}, genesis, cfg, 0, true, "/tmp/x"); err != nil {
		t.Fatalf("ReconcileChainConfig(unchanged, read-only): %v", err)
	}
}

// TestReconcileChainConfigWriteFailure: a rule set that cannot be persisted must
// come back as an ordinary error, so the caller closes the chain and the database
// it opened. Handing the database to rawdb.WriteChainConfig directly loses that:
// the accessor answers a failed Put with log.Crit and exits the process, taking
// down a test binary that reaches it — a regression here does not fail an
// assertion, it kills the run.
func TestReconcileChainConfigWriteFailure(t *testing.T) {
	genesis := testGenesisBlock(t, fixtureMainnet)
	cfg := petersburgOnly(100001)

	t.Run("missing config", func(t *testing.T) {
		db := readOnlyDB{rawdb.NewMemoryDatabase()}
		if err := ReconcileChainConfig(db, genesis, cfg, 0, false, "/tmp/x"); err == nil {
			t.Fatal("ReconcileChainConfig(unwritable) = nil, want a write error")
		}
	})

	t.Run("changed config", func(t *testing.T) {
		db := rawdb.NewMemoryDatabase()
		if err := ReconcileChainConfig(db, genesis, cfg, 0, false, "/tmp/x"); err != nil {
			t.Fatalf("ReconcileChainConfig(fresh): %v", err)
		}
		upgraded := petersburgOnly(100001)
		upgraded.IstanbulBlock = big.NewInt(100)
		if err := ReconcileChainConfig(readOnlyDB{db}, genesis, upgraded, 0, true, "/tmp/x"); err == nil {
			t.Fatal("ReconcileChainConfig(changed, unwritable) = nil, want a write error")
		}
	})
}

// TestReconcileChainConfigMissingWarnsOnlyOnAnExistingGenesis: a fresh database
// reaches the same branch as a datadir that lost its rule set, because the
// genesis commit writes state and nothing else. Only the second is a gap worth
// reporting; warning on every first boot would train the reader to ignore it.
func TestReconcileChainConfigMissingWarnsOnlyOnAnExistingGenesis(t *testing.T) {
	genesis := testGenesisBlock(t, fixtureMainnet)
	cfg := petersburgOnly(100001)

	for _, tc := range []struct {
		name     string
		existed  bool
		wantWarn bool
	}{
		{name: "fresh database", existed: false, wantWarn: false},
		{name: "existing genesis", existed: true, wantWarn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logged bytes.Buffer
			defer log.SetDefault(log.Root())
			log.SetDefault(log.NewLogger(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})))

			db := rawdb.NewMemoryDatabase()
			if err := ReconcileChainConfig(db, genesis, cfg, 0, tc.existed, "/tmp/x"); err != nil {
				t.Fatalf("ReconcileChainConfig: %v", err)
			}
			if stored := rawdb.ReadChainConfig(db, genesis.Hash()); stored == nil {
				t.Fatal("chain config was not written")
			}
			if warned := strings.Contains(logged.String(), "without chain config"); warned != tc.wantWarn {
				t.Errorf("warned = %v, want %v (log: %q)", warned, tc.wantWarn, logged.String())
			}
		})
	}
}

// readOnlyDB fails the writes this package makes — a direct Put, and a batch —
// standing in for a read-only pebble handle or, equally, for a full disk. Batches
// fail where a real one does, in Write.
type readOnlyDB struct{ ethdb.Database }

func (readOnlyDB) Put([]byte, []byte) error { return errors.New("read-only database") }

func (db readOnlyDB) NewBatch() ethdb.Batch { return readOnlyBatch{db.Database.NewBatch()} }

type readOnlyBatch struct{ ethdb.Batch }

func (readOnlyBatch) Write() error { return errors.New("read-only database") }
