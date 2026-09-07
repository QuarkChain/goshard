// Copyright 2026-2027, QuarkChain.

package rawdb

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/rlp"
)

var (
	qkcLimitedSizeBytes = []byte{'\x01', '\x02', '\x03'}
	qkcTx1              = types.NewEvmTransaction(1, account.BytesToIdentityRecipient([]byte{0x11}), big.NewInt(111), 1111, big.NewInt(11111), 0, 1, 1, 0, []byte{0x11, 0x11, 0x11}, 0, 0)
	qkcTx2              = types.NewEvmTransaction(2, account.BytesToIdentityRecipient([]byte{0x22}), big.NewInt(222), 2222, big.NewInt(22222), 0, 1, 1, 0, []byte{0x22, 0x22, 0x22}, 0, 0)
	qkcTx3              = types.NewEvmTransaction(3, account.BytesToIdentityRecipient([]byte{0x33}), big.NewInt(333), 3333, big.NewInt(33333), 0, 1, 1, 0, []byte{0x33, 0x33, 0x33}, 0, 0)
	qkcTxs              = types.Transactions{qkcTx1, qkcTx2, qkcTx3}

	qkcHeader1 = &types.MinorBlockHeader{Number: uint64(41)}
	qkcHeader2 = &types.MinorBlockHeader{Number: uint64(42)}
	qkcHeader3 = &types.MinorBlockHeader{Number: uint64(43)}
	qkcHeaders = types.MinorBlockHeaders{qkcHeader1, qkcHeader2, qkcHeader3}
)

func TestQKCBlockKeys(t *testing.T) {
	hash := common.HexToHash("0x1234")
	if got, want := qkcRootBlockKey(hash), append([]byte("q_rb"), hash.Bytes()...); !bytes.Equal(got, want) {
		t.Fatalf("root block key mismatch: have %x, want %x", got, want)
	}
	if got, want := qkcMinorBlockKey(hash), append([]byte("q_mb"), hash.Bytes()...); !bytes.Equal(got, want) {
		t.Fatalf("minor block key mismatch: have %x, want %x", got, want)
	}
}

// Tests block storage and retrieval operations.
func TestQKCRootBlockStorage(t *testing.T) {
	db := memorydb.New()

	// Create a test block to move around the database and make sure it's really new
	block := types.NewRootBlock(&types.RootBlockHeader{
		Extra:           qkcLimitedSizeBytes,
		ParentHash:      types.EmptyHash,
		MinorHeaderHash: types.EmptyHash,
	}, qkcHeaders, qkcLimitedSizeBytes)

	if entry := ReadRootBlock(db, block.Hash()); entry != nil {
		t.Fatalf("Non existent block returned: %v", entry)
	}
	// Write and verify the block in the database
	WriteRootBlock(db, block)
	if !HasRootBlock(db, block.Hash()) {
		t.Fatal("Stored root block key not found")
	}
	if HasMinorBlock(db, block.Hash()) {
		t.Fatal("Root block was written with the minor block key")
	}
	if entry := ReadRootBlock(db, block.Hash()); entry == nil {
		t.Fatalf("Stored block not found")
	} else if entry.Hash() != block.Hash() {
		t.Fatalf("Retrieved block mismatch: have %v, want %v", entry, block)
	}
	// Delete the block and verify the execution
	DeleteRootBlock(db, block.Hash())
	if entry := ReadRootBlock(db, block.Hash()); entry != nil {
		t.Fatalf("Deleted block returned: %v", entry)
	}
}

// Tests block storage and retrieval operations.
func TestQKCMinorBlockStorage(t *testing.T) {
	db := memorydb.New()

	// Create a test block to move around the database and make sure it's really new
	block := types.NewMinorBlockWithHeader(&types.MinorBlockHeader{
		Extra:      qkcLimitedSizeBytes,
		ParentHash: types.EmptyHash,
	}, &types.MinorBlockMeta{}).WithBody(qkcTxs, qkcLimitedSizeBytes)

	if entry := ReadMinorBlock(db, block.Hash()); entry != nil {
		t.Fatalf("Non existent block returned: %v", entry)
	}
	// Write and verify the block in the database
	WriteMinorBlock(db, block)
	if !HasMinorBlock(db, block.Hash()) {
		t.Fatal("Stored minor block key not found")
	}
	if HasRootBlock(db, block.Hash()) {
		t.Fatal("Minor block was written with the root block key")
	}
	if entry := ReadMinorBlock(db, block.Hash()); entry == nil {
		t.Fatalf("Stored block not found")
	} else if entry.Hash() != block.Hash() {
		t.Fatalf("Retrieved block mismatch: have %v, want %v", entry, block)
	}

	// Delete the block and verify the execution
	DeleteMinorBlock(db, block.Hash())
	if entry := ReadMinorBlock(db, block.Hash()); entry != nil {
		t.Fatalf("Deleted block returned: %v", entry)
	}
}

// Tests that canonical numbers can be mapped to hashes and retrieved.
func TestQKCCanonicalMappingStorage(t *testing.T) {
	db := memorydb.New()

	rootHash := common.Hash{0: 0xff}
	minorHash := common.Hash{0: 0xee}
	number := uint64(314)
	if entry := ReadRootCanonicalHash(db, number); entry != (common.Hash{}) {
		t.Fatalf("Non existent root canonical mapping returned: %v", entry)
	}
	if entry := ReadMinorCanonicalHash(db, number); entry != (common.Hash{}) {
		t.Fatalf("Non existent minor canonical mapping returned: %v", entry)
	}

	WriteRootCanonicalHash(db, rootHash, number)
	WriteMinorCanonicalHash(db, minorHash, number)
	if entry := ReadRootCanonicalHash(db, number); entry != rootHash {
		t.Fatalf("Root canonical mapping mismatch: have %v, want %v", entry, rootHash)
	}
	if entry := ReadMinorCanonicalHash(db, number); entry != minorHash {
		t.Fatalf("Minor canonical mapping mismatch: have %v, want %v", entry, minorHash)
	}
	if has, _ := db.Has(append([]byte("rn"), encodeBlockNumber(number)...)); has {
		t.Fatal("Root canonical mapping was written without the q_ prefix")
	}
	if has, _ := db.Has(append([]byte("mn"), encodeBlockNumber(number)...)); has {
		t.Fatal("Minor canonical mapping was written without the q_ prefix")
	}
	if has, _ := db.Has(append([]byte("q_rn"), encodeBlockNumber(number)...)); !has {
		t.Fatal("Root canonical mapping was not written with the q_ prefix")
	}
	if has, _ := db.Has(append([]byte("q_mn"), encodeBlockNumber(number)...)); !has {
		t.Fatal("Minor canonical mapping was not written with the q_ prefix")
	}

	DeleteRootCanonicalHash(db, number)
	DeleteMinorCanonicalHash(db, number)
	if entry := ReadRootCanonicalHash(db, number); entry != (common.Hash{}) {
		t.Fatalf("Deleted root canonical mapping returned: %v", entry)
	}
	if entry := ReadMinorCanonicalHash(db, number); entry != (common.Hash{}) {
		t.Fatalf("Deleted minor canonical mapping returned: %v", entry)
	}
}

// Tests that head headers and head blocks can be assigned, individually.
func TestQKCHeadStorage(t *testing.T) {
	db := memorydb.New()

	blockHeadHash := common.BytesToHash([]byte{0x44})
	blockFullHash := common.BytesToHash([]byte{0x55})
	blockFastHash := common.BytesToHash([]byte{0x66})

	// Check that no head entries are in a pristine database
	if entry := ReadHeadHeaderHash(db); entry != (common.Hash{}) {
		t.Fatalf("Non head header entry returned: %v", entry)
	}
	if entry := ReadHeadBlockHash(db); entry != (common.Hash{}) {
		t.Fatalf("Non head block entry returned: %v", entry)
	}
	if entry := ReadHeadFastBlockHash(db); entry != (common.Hash{}) {
		t.Fatalf("Non fast head block entry returned: %v", entry)
	}
	// Assign separate entries for the head header and block
	WriteHeadHeaderHash(db, blockHeadHash)
	WriteHeadBlockHash(db, blockFullHash)
	WriteHeadFastBlockHash(db, blockFastHash)

	// Check that both heads are present, and different (i.e. two heads maintained)
	if entry := ReadHeadHeaderHash(db); entry != blockHeadHash {
		t.Fatalf("Head header hash mismatch: have %v, want %v", entry, blockHeadHash)
	}
	if entry := ReadHeadBlockHash(db); entry != blockFullHash {
		t.Fatalf("Head block hash mismatch: have %v, want %v", entry, blockFullHash)
	}
	if entry := ReadHeadFastBlockHash(db); entry != blockFastHash {
		t.Fatalf("Fast head block hash mismatch: have %v, want %v", entry, blockFastHash)
	}
}

// Tests that receipts associated with a single block can be stored and retrieved.
func TestQKCBlockReceiptStorage(t *testing.T) {
	db := memorydb.New()

	receipt1 := &types.Receipt{
		Status:            types.ReceiptStatusFailed,
		CumulativeGasUsed: 1,
		Logs: []*coretypes.Log{
			{
				Address:     common.BytesToAddress([]byte{0x11}),
				Topics:      []common.Hash{common.HexToHash("0x21")},
				Data:        []byte{0x31},
				BlockNumber: 11,
				TxHash:      common.BytesToHash([]byte{0x12}),
				TxIndex:     1,
				BlockHash:   common.BytesToHash([]byte{0x13}),
				Index:       2,
			},
			{Address: common.BytesToAddress([]byte{0x01, 0x11}), Topics: []common.Hash{}, Data: []byte{0x32}},
		},
		TxHash:          common.BytesToHash([]byte{0x11, 0x11}),
		ContractAddress: account.BytesToIdentityRecipient([]byte{0x01, 0x11, 0x11}),
		GasUsed:         111111,
	}
	receipt2 := &types.Receipt{
		Status:            types.ReceiptStatusSuccessful,
		CumulativeGasUsed: 2,
		Logs: []*coretypes.Log{
			{Address: common.BytesToAddress([]byte{0x22}), Topics: []common.Hash{common.HexToHash("0x23")}, Data: []byte{0x33}, BlockNumber: 22, TxIndex: 3, Index: 4},
			{Address: common.BytesToAddress([]byte{0x02, 0x22}), Topics: []common.Hash{}, Data: []byte{0x34}},
		},
		TxHash:          common.BytesToHash([]byte{0x22, 0x22}),
		ContractAddress: account.BytesToIdentityRecipient([]byte{0x02, 0x22, 0x22}),
		GasUsed:         222222,
	}
	receipts := types.Receipts{receipt1, receipt2}

	// Check that no receipt entries are in a pristine database
	hash := common.BytesToHash([]byte{0x03, 0x14})
	if rs := ReadQKCReceipts(db, hash); len(rs) != 0 {
		t.Fatalf("non existent receipts returned: %v", rs)
	}
	// Insert the receipt slice into the database and check presence
	WriteQKCReceipts(db, hash, receipts)
	wantEncoding, err := rlp.EncodeToBytes(receipts)
	if err != nil {
		t.Fatal("encode receipts:", err)
	}
	stored, err := db.Get(qkcBlockReceiptsKey(hash))
	if err != nil {
		t.Fatal("read stored receipts:", err)
	}
	if !bytes.Equal(stored, wantEncoding) {
		t.Fatalf("stored receipt encoding mismatch: have %x, want %x", stored, wantEncoding)
	}
	if rs := ReadQKCReceipts(db, hash); len(rs) == 0 {
		t.Fatalf("no receipts returned")
	} else {
		for i := range receipts {
			got, err := rlp.EncodeToBytes(rs[i])
			if err != nil {
				t.Fatalf("encode receipt %d: %v", i, err)
			}
			want, err := rlp.EncodeToBytes(receipts[i])
			if err != nil {
				t.Fatalf("encode expected receipt %d: %v", i, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("receipt %d consensus fields mismatch: have %x, want %x", i, got, want)
			}
			if rs[i].TxHash != (common.Hash{}) || rs[i].GasUsed != 0 {
				t.Fatalf("receipt %d retained derived fields: %+v", i, rs[i])
			}
			for j, log := range rs[i].Logs {
				if log.BlockNumber != 0 || log.TxHash != (common.Hash{}) || log.TxIndex != 0 ||
					log.BlockHash != (common.Hash{}) || log.BlockTimestamp != 0 || log.Index != 0 || log.Removed {
					t.Fatalf("receipt %d log %d retained derived fields: %+v", i, j, log)
				}
			}
		}
	}
	// Delete the receipt slice and check purge
	DeleteQKCReceipts(db, hash)
	if rs := ReadQKCReceipts(db, hash); len(rs) != 0 {
		t.Fatalf("deleted receipts returned: %v", rs)
	}
}
