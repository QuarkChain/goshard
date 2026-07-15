// Copyright 2026-2027, QuarkChain.

package shard

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// genesisMetaKey is the single QKC-prefixed chaindb key the GenesisMeta record is
// stored under, encoded with qkc/serialize.
//
// TODO: temporary — remove once QKC block format (#1) lands.
var genesisMetaKey = []byte("QKC-genesis-meta")

// genesisMetaVersion is the format version of the GenesisMeta record. Reconcile
// compares it like every other field, so a format change fails loudly instead of
// misreading an old record.
const genesisMetaVersion = 1

// XShardCursor is the cross-shard transaction cursor, mirroring pyquarkchain's
// XshardTxCursorInfo (quarkchain/core.py:623): three uint64 fields.
type XShardCursor struct {
	RootBlockHeight    uint64
	MinorBlockIndex    uint64
	XShardDepositIndex uint64
}

// GenesisMeta records the QKC-specific genesis facts geth's stock block format
// has no field for: the root-genesis linkage and the initial cross-shard cursor.
//
// TODO: temporary — remove once QKC block format (#1) lands. When #1 merges this
// record is re-implemented, not patched: HashPrevRootBlock and XShardCursor move
// into the genesis block's own header/meta, Reconcile switches to the geth-native
// genesis-hash check (comparing the genesis block itself), and the record is
// deleted, not migrated — the db holds only the genesis block at that point, so a
// clean re-bootstrap suffices.
type GenesisMeta struct {
	Version           uint32
	FullShardID       uint32
	RootGenesisHash   common.Hash // = pyquarkchain hash_prev_root_block
	HashPrevRootBlock common.Hash
	XShardCursor      XShardCursor
	ChainGenesisHash  common.Hash // the chain seam's genesis hash, compared on reopen
}

// ReadGenesisMeta returns the stored genesis metadata, or nil if the record is
// absent (a fresh chaindb).
//
// TODO: temporary — remove once QKC block format (#1) lands.
func ReadGenesisMeta(db ethdb.KeyValueReader) (*GenesisMeta, error) {
	has, err := db.Has(genesisMetaKey)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, nil
	}
	data, err := db.Get(genesisMetaKey)
	if err != nil {
		return nil, err
	}
	meta := new(GenesisMeta)
	if err := serialize.DeserializeFromBytes(data, meta); err != nil {
		return nil, fmt.Errorf("decode genesis metadata: %w", err)
	}
	return meta, nil
}

// WriteGenesisMeta stores the genesis metadata record.
//
// TODO: temporary — remove once QKC block format (#1) lands.
func WriteGenesisMeta(db ethdb.KeyValueWriter, meta *GenesisMeta) error {
	data, err := serialize.SerializeToBytes(meta)
	if err != nil {
		return fmt.Errorf("encode genesis metadata: %w", err)
	}
	return db.Put(genesisMetaKey, data)
}

// ReconcileGenesisMeta is the reopen check: it compares the stored record against
// the config-derived expectation. A fresh chaindb (no record) reports
// existed=false and writes nothing — the caller commits the record with
// WriteGenesisMeta only after the chain is constructed at that genesis, so a
// boot that fails mid-way leaves no record behind. An existing record passes
// (existed=true) only on an exact match and hard-errors otherwise, so a cluster
// config change since initialization is caught loudly instead of silently
// keeping the stored genesis. Once the real chain lands, its own genesis check
// stacks on top of this.
//
// TODO: temporary — remove once QKC block format (#1) lands.
func ReconcileGenesisMeta(db ethdb.KeyValueStore, expected *GenesisMeta, dbPath string) (existed bool, err error) {
	stored, err := ReadGenesisMeta(db)
	if err != nil {
		return false, fmt.Errorf("shard 0x%08x: read genesis metadata (db %s): %w", expected.FullShardID, dbPath, err)
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
	return true, fmt.Errorf("shard 0x%08x: stored genesis metadata %+v does not match config-derived metadata %+v (db %s) — cluster config changed since initialization",
		expected.FullShardID, stored, expected, dbPath)
}
