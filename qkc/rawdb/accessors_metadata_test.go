// Copyright 2026-2027, QuarkChain.

package rawdb

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

func TestDatabaseVersionStorage(t *testing.T) {
	db := ethdb.NewMemDatabase()
	if got := ReadDatabaseVersion(db); got != 0 {
		t.Fatalf("empty database version = %d, want 0", got)
	}
	WriteDatabaseVersion(db, 7)
	if got := ReadDatabaseVersion(db); got != 7 {
		t.Fatalf("database version = %d, want 7", got)
	}
	if err := db.Put(databaseVerisionKey, []byte{1}); err != nil {
		t.Fatalf("write corrupt database version: %v", err)
	}
	if got := ReadDatabaseVersion(db); got != 0 {
		t.Fatalf("corrupt database version = %d, want 0", got)
	}
}
