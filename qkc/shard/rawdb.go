// Copyright 2026-2027, QuarkChain.

package shard

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// genesisBlockKey is the single QKC-prefixed chaindb key the shard's genesis block
// is stored under, encoded with qkc/serialize.
//
// TODO: this key is scaffolding, though the block it holds is not. The permanent
// home of a minor block is the chain's own block storage — canonical-hash
// mapping, head pointers, written in one batch by the chain's commit, as geth
// does in Genesis.Commit (core/genesis.go). Those accessors belong to the chain;
// until they exist the slave keeps block 0 under this one key so a reopened
// datadir can still be checked. Dropped, not migrated, when the chain owns block
// storage.
var genesisBlockKey = []byte("QKC-genesis-block")

// GenesisMismatchError reports a database that was initialized from a different
// cluster config, mirroring geth's core.GenesisMismatchError (core/genesis.go):
// callers get both identities rather than only a message to match on.
type GenesisMismatchError struct {
	FullShardID uint32
	Stored, New common.Hash
	DBPath      string
}

func (e *GenesisMismatchError) Error() string {
	return fmt.Sprintf("shard 0x%08x: stored genesis %s does not match config genesis %s (db %s) — cluster config changed since initialization",
		e.FullShardID, e.Stored, e.New, e.DBPath)
}

// readGenesisBlockBytes returns the stored genesis block's encoding, or nil if
// none is stored (a fresh chaindb).
func readGenesisBlockBytes(db ethdb.KeyValueReader) ([]byte, error) {
	has, err := db.Has(genesisBlockKey)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, nil
	}
	return db.Get(genesisBlockKey)
}

// ReadGenesisBlock returns the stored genesis block, or nil if none is stored (a
// fresh chaindb).
func ReadGenesisBlock(db ethdb.KeyValueReader) (*types.MinorBlock, error) {
	data, err := readGenesisBlockBytes(db)
	if err != nil || data == nil {
		return nil, err
	}
	block := new(types.MinorBlock)
	if err := serialize.DeserializeFromBytes(data, block); err != nil {
		return nil, fmt.Errorf("decode genesis block: %w", err)
	}
	return block, nil
}

// WriteGenesisBlock stores the shard's genesis block.
func WriteGenesisBlock(db ethdb.KeyValueWriter, block *types.MinorBlock) error {
	data, err := serialize.SerializeToBytes(block)
	if err != nil {
		return fmt.Errorf("encode genesis block: %w", err)
	}
	return db.Put(genesisBlockKey, data)
}

// ReconcileGenesisBlock is the reopen check: it compares the stored genesis block
// against the config-derived one, the comparison geth's SetupGenesisBlock makes
// against canonical block 0. A fresh chaindb (no block) reports existed=false and
// writes nothing — the caller stores the block with WriteGenesisBlock only after
// the chain is standing, so a boot that fails mid-way leaves nothing behind. An
// existing block passes (existed=true) only when it is the same block, and
// hard-errors otherwise, so a cluster config change since initialization is caught
// loudly instead of silently keeping the stored genesis.
//
// The comparison is over the stored encoding, not the block hash. A minor block's
// hash is its header's hash alone (types.MinorBlock.Hash), and nothing on the read
// path verifies that the stored meta still hashes to the header's hash_meta — so
// comparing hashes would accept a stored block whose meta or body had been
// replaced, including its state root. Comparing the encoding covers header, meta
// and body in one check; the block is decoded afterwards only to name the cause —
// another shard's chaindb, a changed config, or a header whose meta and body no
// longer match it.
func ReconcileGenesisBlock(db ethdb.KeyValueStore, expected *types.MinorBlock, dbPath string) (existed bool, err error) {
	fullShardID := expected.Header.Branch.GetFullShardID()
	storedData, err := readGenesisBlockBytes(db)
	if err != nil {
		return false, fmt.Errorf("shard 0x%08x: read genesis block (db %s): %w", fullShardID, dbPath, err)
	}
	if storedData == nil {
		return false, nil
	}
	expectedData, err := serialize.SerializeToBytes(expected)
	if err != nil {
		return false, fmt.Errorf("shard 0x%08x: encode genesis block: %w", fullShardID, err)
	}
	if bytes.Equal(storedData, expectedData) {
		return true, nil
	}

	stored := new(types.MinorBlock)
	if err := serialize.DeserializeFromBytes(storedData, stored); err != nil {
		return true, fmt.Errorf("shard 0x%08x: decode genesis block (db %s): %w", fullShardID, dbPath, err)
	}
	// A chaindb holding another shard's genesis is a misplaced directory, not a
	// config change — name the right cause.
	if storedID := stored.Header.Branch.GetFullShardID(); storedID != fullShardID {
		return true, fmt.Errorf("shard 0x%08x: stored genesis belongs to shard 0x%08x (db %s) — misplaced chaindb",
			fullShardID, storedID, dbPath)
	}
	if got := stored.Hash(); got != expected.Hash() {
		return true, &GenesisMismatchError{
			FullShardID: fullShardID,
			Stored:      got,
			New:         expected.Hash(),
			DBPath:      dbPath,
		}
	}
	// The stored header is the expected one, yet the encoding differs: the meta or
	// body no longer matches what that header commits to. No config change produces
	// this — the header would have moved with it.
	return true, fmt.Errorf("shard 0x%08x: stored genesis %s does not match the meta and body its header commits to (db %s) — corrupt chaindb",
		fullShardID, expected.Hash(), dbPath)
}

// ReconcileChainConfig stores or validates the shard's EVM rule set the way geth's
// SetupGenesisBlock does (core/genesis.go): the rule set is kept out of the
// genesis identity and reconciled on its own, so a compatible rule change is
// accepted instead of forcing a re-bootstrap. It is keyed by the genesis block
// hash, as geth keys it by the genesis hash.
//
// head is the chain's current height. On a genesis-only database (head 0) geth
// accepts any rule set, because nothing has been executed under the old rules
// yet; above genesis a schedule that breaks with the executed history is
// rejected.
//
// existed reports whether the datadir already held the genesis block. It only
// separates the first write of a rule set on a fresh database from a rule set
// missing under a genesis that was already initialized.
//
// Above genesis the rejection is stricter than geth's, deliberately. Geth also
// waves through a conflict whose RewindToBlock is 0, because its caller answers a
// compatibility error by rewinding the chain to that block. Every shard fork is
// scheduled at block 0 (qkc.ShardChainConfig), so here that arm would swallow
// essentially every schedule change on a chain with history, and nothing in the
// slave can rewind. An incompatibility above genesis is fatal instead.
//
// No head timestamp is taken: the shard rule sets are block-numbered, so
// CheckCompatible's time argument is inert, and the ShardChain seam exposes no
// head timestamp to pass. A timestamp-scheduled fork would need both.
func ReconcileChainConfig(db ethdb.Database, genesis *types.MinorBlock, cfg *params.ChainConfig, head uint64, existed bool, dbPath string) error {
	fullShardID := genesis.Header.Branch.GetFullShardID()
	if cfg == nil {
		return fmt.Errorf("shard 0x%08x: shard has no chain config (db %s)", fullShardID, dbPath)
	}
	if err := cfg.CheckConfigForkOrder(); err != nil {
		return fmt.Errorf("shard 0x%08x: invalid chain config (db %s): %w", fullShardID, dbPath, err)
	}
	genesisHash := genesis.Hash()
	stored := rawdb.ReadChainConfig(db, genesisHash)
	if stored == nil {
		// On a fresh database this is simply where the rule set gets written: the
		// genesis commit materializes state only. Under a genesis that was already
		// stored it is instead a gap in an initialized datadir — recoverable, but
		// said out loud.
		//
		// Geth reaches its equivalent branch only in that second case, and answers it
		// by re-running the genesis commit (core/genesis.go), because there a stored
		// genesis with no stored config means a key-value store that was never
		// initialized alongside an existing ancient store. There is no ancient store
		// here, and the genesis block itself was reconciled before this call, so
		// writing the rule set is the whole repair.
		if existed {
			log.Warn("found shard genesis without chain config", "shard", fmt.Sprintf("0x%08x", fullShardID))
		}
		return writeChainConfig(db, genesisHash, cfg, fullShardID, dbPath)
	}
	if compatErr := stored.CheckCompatible(cfg, head, 0); compatErr != nil && head != 0 {
		return fmt.Errorf("shard 0x%08x: incompatible chain config (db %s): %w", fullShardID, dbPath, compatErr)
	}
	// Never rewrite an identical config, as geth does not (core/genesis.go): the
	// rewrite is noise, and it leaves the common reopen — a rule set that has not
	// changed — writeless, so a caller holding a read-only handle gets through it.
	storedData, _ := json.Marshal(stored)
	if newData, _ := json.Marshal(cfg); !bytes.Equal(storedData, newData) {
		return writeChainConfig(db, genesisHash, cfg, fullShardID, dbPath)
	}
	return nil
}

// writeChainConfig stores the rule set through a batch rather than handing the
// database to rawdb.WriteChainConfig directly. That accessor answers a failed Put
// with log.Crit, which exits the process — a full disk or a read-only handle would
// take the slave down mid-boot, past every unwind its caller has. A batch's Put is
// a memory append; the I/O it can fail on happens in Write, which returns an error.
func writeChainConfig(db ethdb.Database, genesisHash common.Hash, cfg *params.ChainConfig, fullShardID uint32, dbPath string) error {
	batch := db.NewBatch()
	rawdb.WriteChainConfig(batch, genesisHash, cfg)
	if err := batch.Write(); err != nil {
		return fmt.Errorf("shard 0x%08x: write chain config (db %s): %w", fullShardID, dbPath, err)
	}
	return nil
}
