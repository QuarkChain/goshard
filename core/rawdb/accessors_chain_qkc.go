// Copyright 2026-2027, QuarkChain.
package rawdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/rlp"
)

const qkcDBLog = "db-operation"

// ReadRootCanonicalHash retrieves the root block hash assigned to a canonical block number.
func ReadRootCanonicalHash(db ethdb.KeyValueReader, number uint64) common.Hash {
	data, _ := db.Get(qkcRootCanonicalHashKey(number))
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteRootCanonicalHash stores the root block hash assigned to a canonical block number.
func WriteRootCanonicalHash(db ethdb.KeyValueWriter, hash common.Hash, number uint64) {
	if err := db.Put(qkcRootCanonicalHashKey(number), hash.Bytes()); err != nil {
		log.Crit("Failed to store root number to hash mapping", "err", err)
	}
}

// DeleteRootCanonicalHash removes the root number to hash canonical mapping.
func DeleteRootCanonicalHash(db ethdb.KeyValueWriter, number uint64) {
	if err := db.Delete(qkcRootCanonicalHashKey(number)); err != nil {
		log.Crit("Failed to delete root number to hash mapping", "err", err)
	}
}

// ReadMinorCanonicalHash retrieves the minor block hash assigned to a canonical block number.
func ReadMinorCanonicalHash(db ethdb.KeyValueReader, number uint64) common.Hash {
	data, _ := db.Get(qkcMinorCanonicalHashKey(number))
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteMinorCanonicalHash stores the minor block hash assigned to a canonical block number.
func WriteMinorCanonicalHash(db ethdb.KeyValueWriter, hash common.Hash, number uint64) {
	if err := db.Put(qkcMinorCanonicalHashKey(number), hash.Bytes()); err != nil {
		log.Crit("Failed to store minor number to hash mapping", "err", err)
	}
}

// DeleteMinorCanonicalHash removes the minor number to hash canonical mapping.
func DeleteMinorCanonicalHash(db ethdb.KeyValueWriter, number uint64) {
	if err := db.Delete(qkcMinorCanonicalHashKey(number)); err != nil {
		log.Crit("Failed to delete minor number to hash mapping", "err", err)
	}
}

// ReadRootHeadHash retrieves the current canonical root block hash for a shard.
func ReadRootHeadHash(db ethdb.KeyValueReader) common.Hash {
	data, _ := db.Get(qkcRootHeadKey())
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteRootHeadHash stores the current canonical root block hash for a shard.
func WriteRootHeadHash(db ethdb.KeyValueWriter, hash common.Hash) {
	if err := db.Put(qkcRootHeadKey(), hash.Bytes()); err != nil {
		log.Crit("Failed to store root head block hash", "err", err)
	}
}

// HasQKCReceipts verifies the existence of all the transaction receipts belonging
// to a block.
func HasQKCReceipts(db ethdb.KeyValueReader, hash common.Hash) bool {
	if has, err := db.Has(qkcBlockReceiptsKey(hash)); !has || err != nil {
		return false
	}
	return true
}

// ReadQKCReceipts retrieves the consensus-encoded receipts belonging to a block.
func ReadQKCReceipts(db ethdb.KeyValueReader, hash common.Hash) types.Receipts {
	data, _ := db.Get(qkcBlockReceiptsKey(hash))
	if len(data) == 0 {
		return nil
	}
	var receipts types.Receipts
	if err := rlp.DecodeBytes(data, &receipts); err != nil {
		log.Error("Invalid receipt array RLP", "hash", hash, "err", err)
		return nil
	}
	return receipts
}

// WriteQKCReceipts stores the consensus encoding of all receipts belonging to a block.
func WriteQKCReceipts(db ethdb.KeyValueWriter, hash common.Hash, receipts types.Receipts) {
	bytes, err := rlp.EncodeToBytes(receipts)
	if err != nil {
		log.Crit("Failed to encode block receipts", "err", err)
	}
	// Store the flattened receipt slice
	if err := db.Put(qkcBlockReceiptsKey(hash), bytes); err != nil {
		log.Crit("Failed to store block receipts", "err", err)
	}
}

// DeleteQKCReceipts removes all receipt data associated with a block hash.
func DeleteQKCReceipts(db ethdb.KeyValueWriter, hash common.Hash) {
	if err := db.Delete(qkcBlockReceiptsKey(hash)); err != nil {
		log.Crit("Failed to delete block receipts", "err", err)
	}
}

// HasMinorBlock verifies the existence of a minor block corresponding to the hash.
func HasMinorBlock(db ethdb.KeyValueReader, hash common.Hash) bool {
	if has, err := db.Has(qkcMinorBlockKey(hash)); !has || err != nil {
		return false
	}
	return true
}

// ReadMinorBlock retrieves the block body corresponding to the hash.
func ReadMinorBlock(db ethdb.KeyValueReader, hash common.Hash) *types.MinorBlock {
	data, _ := db.Get(qkcMinorBlockKey(hash))
	if len(data) == 0 {
		return nil
	}
	block := new(types.MinorBlock)
	if err := serialize.Deserialize(serialize.NewByteBuffer(data), block); err != nil {
		log.Error("Invalid block body Deserialize", "hash", hash, "err", err)
		return nil
	}
	return block
}

// WriteMinorBlock storea a block body into the database.
func WriteMinorBlock(db ethdb.KeyValueWriter, block *types.MinorBlock) {
	data, err := serialize.SerializeToBytes(block)
	if err != nil {
		log.Crit("Failed to serialize body", "err", err)
	}
	log.Info(qkcDBLog+" Write MinorBlock", "branch", fmt.Sprintf("%x", block.Branch().Value), "height", block.NumberU64(), "hash", block.Hash().TerminalString(), "len(tx)", len(block.Transactions()))
	if err := db.Put(qkcMinorBlockKey(block.Hash()), data); err != nil {
		log.Crit("Failed to store minor block body", "err", err)
	}
}

// DeleteBlock removes all block data associated with a hash.
func DeleteMinorBlock(db ethdb.KeyValueWriter, hash common.Hash) {
	DeleteQKCReceipts(db, hash)
	if err := db.Delete(qkcMinorBlockKey(hash)); err != nil {
		log.Crit("Failed to delete minor block", "err", err)
	}
}

// HasRootBlock verifies the existence of a root block corresponding to the hash.
func HasRootBlock(db ethdb.KeyValueReader, hash common.Hash) bool {
	if has, err := db.Has(qkcRootBlockKey(hash)); !has || err != nil {
		return false
	}
	return true
}

// ReadRootBlock retrieves the block rootBlockBody corresponding to the hash.
func ReadRootBlock(db ethdb.KeyValueReader, hash common.Hash) *types.RootBlock {
	data, _ := db.Get(qkcRootBlockKey(hash))
	if len(data) == 0 {
		return nil
	}
	block := new(types.RootBlock)
	if err := serialize.Deserialize(serialize.NewByteBuffer(data), block); err != nil {
		log.Error("Invalid block rootBlockBody Deserialize", "hash", hash, "err", err)
		return nil
	}
	return block
}

// WriteRootBlock storea a block rootBlockBody into the database.
func WriteRootBlock(db ethdb.KeyValueWriter, block *types.RootBlock) {
	data, err := serialize.SerializeToBytes(block)
	if err != nil {
		log.Crit("Failed to serialize RootBlock", "err", err)
	}
	log.Info(qkcDBLog+" Write RootBlock", "height", block.NumberU64(), "hash", block.Hash())
	if err := db.Put(qkcRootBlockKey(block.Hash()), data); err != nil {
		log.Crit("Failed to store RootBlock", "err", err)
	}
}

// DeleteRootBlock removes all block data associated with a hash.
func DeleteRootBlock(db ethdb.KeyValueWriter, hash common.Hash) {
	if err := db.Delete(qkcRootBlockKey(hash)); err != nil {
		log.Crit("Failed to delete root block", "err", err)
	}
}
