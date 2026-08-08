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
	limitedSizeByes = []byte{'\x01', '\x02', '\x03'}
	tx1             = types.NewEvmTransaction(1, account.BytesToIdentityRecipient([]byte{0x11}), big.NewInt(111), 1111, big.NewInt(11111), 0, 1, 1, 0, []byte{0x11, 0x11, 0x11}, 0, 0)
	tx2             = types.NewEvmTransaction(2, account.BytesToIdentityRecipient([]byte{0x22}), big.NewInt(222), 2222, big.NewInt(22222), 0, 1, 1, 0, []byte{0x22, 0x22, 0x22}, 0, 0)
	tx3             = types.NewEvmTransaction(3, account.BytesToIdentityRecipient([]byte{0x33}), big.NewInt(333), 3333, big.NewInt(33333), 0, 1, 1, 0, []byte{0x33, 0x33, 0x33}, 0, 0)
	txs             = types.Transactions{tx1, tx2, tx3}

	header1 = &types.MinorBlockHeader{Number: uint64(41)}
	header2 = &types.MinorBlockHeader{Number: uint64(42)}
	header3 = &types.MinorBlockHeader{Number: uint64(43)}
	headers = types.MinorBlockHeaders{header1, header2, header3}
)

// Tests block header storage and retrieval operations.
func TestMinorBlockHeaderStorage(t *testing.T) {
	db := memorydb.New()

	// Create a test header to move around the database and make sure it's really new
	//todo init header and meta
	header := &types.MinorBlockHeader{Number: uint64(42)}
	if entry := ReadMinorBlockHeader(db, header.Hash()); entry != nil {
		t.Fatalf("Non existent header returned: %v", entry)
	}
	// Write and verify the header in the database
	WriteMinorBlockHeader(db, header)
	if entry := ReadMinorBlockHeader(db, header.Hash()); entry == nil {
		t.Fatalf("Stored header not found")
	} else if entry.Hash() != header.Hash() {
		t.Fatalf("Retrieved header mismatch: have %v, want %v", entry, header)
	}
	// Delete the header and verify the execution
	DeleteMinorBlockHeader(db, header.Hash())
	if entry := ReadMinorBlockHeader(db, header.Hash()); entry != nil {
		t.Fatalf("Deleted header returned: %v", entry)
	}
}

// Tests block header storage and retrieval operations.
func TestRootBlockHeaderStorage(t *testing.T) {
	db := memorydb.New()

	// Create a test header to move around the database and make sure it's really new
	//todo init header and meta
	header := &types.RootBlockHeader{Number: uint32(42)}
	if entry := ReadRootBlockHeader(db, header.Hash()); entry != nil {
		t.Fatalf("Non existent header returned: %v", entry)
	}
	// Write and verify the header in the database
	WriteRootBlockHeader(db, header)
	if entry := ReadRootBlockHeader(db, header.Hash()); entry == nil {
		t.Fatalf("Stored header not found")
	} else if entry.Hash() != header.Hash() {
		t.Fatalf("Retrieved header mismatch: have %v, want %v", entry, header)
	}
	// Delete the header and verify the execution
	DeleteRootBlockHeader(db, header.Hash())
	if entry := ReadRootBlockHeader(db, header.Hash()); entry != nil {
		t.Fatalf("Deleted header returned: %v", entry)
	}
}

// Tests block storage and retrieval operations.
func TestRootBlockStorage(t *testing.T) {
	db := memorydb.New()

	// Create a test block to move around the database and make sure it's really new
	block := types.NewRootBlockWithHeader(&types.RootBlockHeader{
		Extra:           limitedSizeByes,
		ParentHash:      types.EmptyHash,
		MinorHeaderHash: types.EmptyHash,
	}).WithBody(headers, limitedSizeByes)

	if entry := ReadRootBlock(db, block.Hash()); entry != nil {
		t.Fatalf("Non existent block returned: %v", entry)
	}
	if entry := ReadRootBlockHeader(db, block.Hash()); entry != nil {
		t.Fatalf("Non existent header returned: %v", entry)
	}
	// Write and verify the block in the database
	WriteRootBlock(db, block)
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
func TestMinorBlockStorage(t *testing.T) {
	db := memorydb.New()

	// Create a test block to move around the database and make sure it's really new
	block := types.NewMinorBlockWithHeader(&types.MinorBlockHeader{
		Extra:      limitedSizeByes,
		ParentHash: types.EmptyHash,
	}, &types.MinorBlockMeta{}).WithBody(txs, limitedSizeByes)

	if entry := ReadMinorBlock(db, block.Hash()); entry != nil {
		t.Fatalf("Non existent block returned: %v", entry)
	}
	if entry := ReadMinorBlockHeader(db, block.Hash()); entry != nil {
		t.Fatalf("Non existent header returned: %v", entry)
	}
	// Write and verify the block in the database
	WriteMinorBlock(db, block)
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
	if entry := ReadMinorBlockHeader(db, block.Hash()); entry != nil {
		t.Fatalf("Deleted header returned: %v", entry)
	}
}

// Tests that canonical numbers can be mapped to hashes and retrieved.
func TestCanonicalMappingStorage(t *testing.T) {
	db := memorydb.New()

	// Create a test canonical number and assinged hash to move around
	hash, number := common.Hash{0: 0xff}, uint64(314)
	if entry := ReadCanonicalHash(db, ChainType(0), number); entry != (common.Hash{}) {
		t.Fatalf("Non existent canonical mapping returned: %v", entry)
	}
	// Write and verify the TD in the database
	WriteCanonicalHash(db, 0, hash, number)
	if entry := ReadCanonicalHash(db, ChainType(0), number); entry == (common.Hash{}) {
		t.Fatalf("Stored canonical mapping not found")
	} else if entry != hash {
		t.Fatalf("Retrieved canonical mapping mismatch: have %v, want %v", entry, hash)
	}
	if entry := ReadCanonicalHash(db, ChainType(1), number); entry != (common.Hash{}) {
		t.Fatalf("Non existent canonical mapping returned: %v", entry)
	}
	// Delete the TD and verify the execution
	DeleteCanonicalHash(db, ChainType(0), number)
	if entry := ReadCanonicalHash(db, ChainType(0), number); entry != (common.Hash{}) {
		t.Fatalf("Deleted canonical mapping returned: %v", entry)
	}
}

// Tests that head headers and head blocks can be assigned, individually.
func TestHeadStorage(t *testing.T) {
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
func TestBlockReceiptStorage(t *testing.T) {
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
	if rs := ReadReceipts(db, hash); len(rs) != 0 {
		t.Fatalf("non existent receipts returned: %v", rs)
	}
	// Insert the receipt slice into the database and check presence
	WriteReceipts(db, hash, receipts)
	wantEncoding, err := rlp.EncodeToBytes(receipts)
	if err != nil {
		t.Fatal("encode receipts:", err)
	}
	stored, err := db.Get(blockReceiptsKey(hash))
	if err != nil {
		t.Fatal("read stored receipts:", err)
	}
	if !bytes.Equal(stored, wantEncoding) {
		t.Fatalf("stored receipt encoding mismatch: have %x, want %x", stored, wantEncoding)
	}
	if rs := ReadReceipts(db, hash); len(rs) == 0 {
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
	DeleteReceipts(db, hash)
	if rs := ReadReceipts(db, hash); len(rs) != 0 {
		t.Fatalf("deleted receipts returned: %v", rs)
	}
}
