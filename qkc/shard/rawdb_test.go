// Copyright 2026-2027, QuarkChain.

package shard

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

func testMeta() *GenesisMeta {
	return &GenesisMeta{
		Version:           genesisMetaVersion,
		FullShardID:       0x00040001,
		RootGenesisHash:   common.HexToHash("0x4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51"),
		HashPrevRootBlock: common.HexToHash("0x4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51"),
		XShardCursor:      XShardCursor{RootBlockHeight: 3},
		ChainGenesisHash:  common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
	}
}

func TestGenesisMetaRoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	got, err := ReadGenesisMeta(db)
	if err != nil || got != nil {
		t.Fatalf("ReadGenesisMeta(empty) = %v, %v, want nil, nil", got, err)
	}

	want := testMeta()
	if err := WriteGenesisMeta(db, want); err != nil {
		t.Fatalf("WriteGenesisMeta: %v", err)
	}
	got, err = ReadGenesisMeta(db)
	if err != nil {
		t.Fatalf("ReadGenesisMeta: %v", err)
	}
	if *got != *want {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestReconcileGenesisMeta(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	expected := testMeta()

	// Fresh db: the record is written.
	if existed, err := ReconcileGenesisMeta(db, expected, "/tmp/x"); err != nil || existed {
		t.Fatalf("Reconcile(fresh) = %v, %v, want false, nil", existed, err)
	}
	// Reopen with identical expectation: passes.
	if existed, err := ReconcileGenesisMeta(db, expected, "/tmp/x"); err != nil || !existed {
		t.Fatalf("Reconcile(reopen) = %v, %v, want true, nil", existed, err)
	}

	// Changed shard genesis: the canonical loud error, naming both hashes and the db.
	changed := testMeta()
	changed.ChainGenesisHash = common.HexToHash("0x22")
	_, err := ReconcileGenesisMeta(db, changed, "/tmp/x")
	if err == nil || !strings.Contains(err.Error(), "does not match config genesis") ||
		!strings.Contains(err.Error(), "/tmp/x") ||
		!strings.Contains(err.Error(), "cluster config changed since initialization") {
		t.Fatalf("Reconcile(changed genesis) err = %v, want loud mismatch", err)
	}

	// Changed root linkage with the same chain genesis: still a hard error.
	changed = testMeta()
	changed.RootGenesisHash = common.HexToHash("0x33")
	_, err = ReconcileGenesisMeta(db, changed, "/tmp/x")
	if err == nil || !strings.Contains(err.Error(), "cluster config changed since initialization") {
		t.Fatalf("Reconcile(changed root linkage) err = %v, want loud mismatch", err)
	}
}
