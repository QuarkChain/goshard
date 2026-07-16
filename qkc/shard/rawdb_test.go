// Copyright 2026-2027, QuarkChain.

package shard

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

// TODO(#1): delete these GenesisRecord tests with the temporary record and move
// reopen/mismatch coverage to the real QKC genesis setup path.

func testRecord() *GenesisRecord {
	return &GenesisRecord{
		Version:           genesisRecordVersion,
		FullShardID:       0x00040001,
		RootGenesisHash:   common.HexToHash("0x4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51"),
		HashPrevRootBlock: common.HexToHash("0x4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51"),
		XShardCursor:      XShardCursor{RootBlockHeight: 3},
		ChainGenesisHash:  common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
	}
}

func TestGenesisRecordRoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	got, err := ReadGenesisRecord(db)
	if err != nil || got != nil {
		t.Fatalf("ReadGenesisRecord(empty) = %v, %v, want nil, nil", got, err)
	}

	want := testRecord()
	if err := WriteGenesisRecord(db, want); err != nil {
		t.Fatalf("WriteGenesisRecord: %v", err)
	}
	got, err = ReadGenesisRecord(db)
	if err != nil {
		t.Fatalf("ReadGenesisRecord: %v", err)
	}
	if *got != *want {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestReconcileGenesisRecord(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	expected := testRecord()

	// Fresh db: reported as fresh, and nothing is written — committing the
	// record is the caller's job once the chain stands at that genesis.
	if existed, err := ReconcileGenesisRecord(db, expected, "/tmp/x"); err != nil || existed {
		t.Fatalf("Reconcile(fresh) = %v, %v, want false, nil", existed, err)
	}
	if rec, err := ReadGenesisRecord(db); err != nil || rec != nil {
		t.Fatalf("Reconcile(fresh) wrote a record: %+v, %v", rec, err)
	}

	// Reopen with identical expectation after the caller committed: passes.
	if err := WriteGenesisRecord(db, expected); err != nil {
		t.Fatalf("WriteGenesisRecord: %v", err)
	}
	if existed, err := ReconcileGenesisRecord(db, expected, "/tmp/x"); err != nil || !existed {
		t.Fatalf("Reconcile(reopen) = %v, %v, want true, nil", existed, err)
	}

	// Changed shard genesis: the canonical loud error, naming both hashes and the db.
	changed := testRecord()
	changed.ChainGenesisHash = common.HexToHash("0x22")
	_, err := ReconcileGenesisRecord(db, changed, "/tmp/x")
	if err == nil || !strings.Contains(err.Error(), "does not match config genesis") ||
		!strings.Contains(err.Error(), "/tmp/x") ||
		!strings.Contains(err.Error(), "cluster config changed since initialization") {
		t.Fatalf("Reconcile(changed genesis) err = %v, want loud mismatch", err)
	}

	// Changed root linkage with the same chain genesis: still a hard error.
	changed = testRecord()
	changed.RootGenesisHash = common.HexToHash("0x33")
	_, err = ReconcileGenesisRecord(db, changed, "/tmp/x")
	if err == nil || !strings.Contains(err.Error(), "cluster config changed since initialization") {
		t.Fatalf("Reconcile(changed root linkage) err = %v, want loud mismatch", err)
	}
}
