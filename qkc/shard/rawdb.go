// Copyright 2026-2027, QuarkChain.

package shard

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// genesisRecordKey is the single QKC-prefixed chaindb key the GenesisRecord is
// stored under, encoded with qkc/serialize.
//
// TODO: temporary — remove once QKC block format (#1) lands.
var genesisRecordKey = []byte("QKC-genesis-record")

// genesisRecordVersion is the format version of the GenesisRecord. Reconcile
// compares it like every other field, so a format change fails loudly instead of
// misreading an old record.
const genesisRecordVersion = 1

// XShardCursor is the cross-shard transaction cursor, mirroring pyquarkchain's
// XshardTxCursorInfo (quarkchain/core.py:623): three uint64 fields.
type XShardCursor struct {
	RootBlockHeight    uint64
	MinorBlockIndex    uint64
	XShardDepositIndex uint64
}

// GenesisRecord captures the QKC-specific genesis facts geth's stock block format
// has no field for: the root-genesis linkage and the initial cross-shard cursor.
//
// TODO: temporary — remove once QKC block format (#1) lands. When #1 merges this
// record is re-implemented, not patched: HashPrevRootBlock and XShardCursor move
// into the genesis block's own header/meta, Reconcile switches to the geth-native
// genesis-hash check (comparing the genesis block itself), and the record is
// deleted, not migrated — the db holds only the genesis block at that point, so a
// clean re-bootstrap suffices.
type GenesisRecord struct {
	Version           uint32
	FullShardID       uint32
	RootGenesisHash   common.Hash // = pyquarkchain hash_prev_root_block
	HashPrevRootBlock common.Hash
	XShardCursor      XShardCursor
	ChainGenesisHash  common.Hash // the chain seam's genesis hash, compared on reopen
}

// ReadGenesisRecord returns the stored genesis record, or nil if the record is
// absent (a fresh chaindb).
//
// TODO: temporary — remove once QKC block format (#1) lands.
func ReadGenesisRecord(db ethdb.KeyValueReader) (*GenesisRecord, error) {
	has, err := db.Has(genesisRecordKey)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, nil
	}
	data, err := db.Get(genesisRecordKey)
	if err != nil {
		return nil, err
	}
	rec := new(GenesisRecord)
	if err := serialize.DeserializeFromBytes(data, rec); err != nil {
		return nil, fmt.Errorf("decode genesis record: %w", err)
	}
	return rec, nil
}

// WriteGenesisRecord stores the genesis record.
//
// TODO: temporary — remove once QKC block format (#1) lands.
func WriteGenesisRecord(db ethdb.KeyValueWriter, rec *GenesisRecord) error {
	data, err := serialize.SerializeToBytes(rec)
	if err != nil {
		return fmt.Errorf("encode genesis record: %w", err)
	}
	return db.Put(genesisRecordKey, data)
}

// ReconcileGenesisRecord is the reopen check: it compares the stored record against
// the config-derived expectation. A fresh chaindb (no record) reports
// existed=false and writes nothing — the caller commits the record with
// WriteGenesisRecord only after the chain is constructed at that genesis, so a
// boot that fails mid-way leaves no record behind. An existing record passes
// (existed=true) only on an exact match and hard-errors otherwise, so a cluster
// config change since initialization is caught loudly instead of silently
// keeping the stored genesis. Once the real chain lands, its own genesis check
// stacks on top of this.
//
// TODO: temporary — remove once QKC block format (#1) lands.
func ReconcileGenesisRecord(db ethdb.KeyValueStore, expected *GenesisRecord, dbPath string) (existed bool, err error) {
	stored, err := ReadGenesisRecord(db)
	if err != nil {
		return false, fmt.Errorf("shard 0x%08x: read genesis record (db %s): %w", expected.FullShardID, dbPath, err)
	}
	if stored == nil {
		return false, nil
	}
	if *stored == *expected {
		return true, nil
	}
	if stored.ChainGenesisHash != expected.ChainGenesisHash {
		return true, fmt.Errorf("shard 0x%08x: stored genesis %s does not match config genesis %s (db %s) — cluster config changed since initialization",
			expected.FullShardID, stored.ChainGenesisHash, expected.ChainGenesisHash, dbPath)
	}
	return true, fmt.Errorf("shard 0x%08x: stored genesis record %+v does not match config-derived record %+v (db %s) — cluster config changed since initialization",
		expected.FullShardID, stored, expected, dbPath)
}
