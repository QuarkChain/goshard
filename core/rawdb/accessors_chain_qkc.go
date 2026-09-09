// Copyright 2026-2027, QuarkChain.
package rawdb

import (
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/rlp"
)

const qkcDBLog = "db-operation"

// QKCHashList is the serialized list of cross-shard deposit hashes.
type HashList struct {
	HList []common.Hash `bytesizeofslicelen:"4"`
}

// ReadRootCanonicalHash retrieves the root block hash assigned to a canonical block number.
func ReadRootCanonicalHash(db ethdb.KeyValueReader, number uint64) common.Hash {
	data, _ := db.Get(rootCanonicalHashKey(number))
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteRootCanonicalHash stores the root block hash assigned to a canonical block number.
func WriteRootCanonicalHash(db ethdb.KeyValueWriter, hash common.Hash, number uint64) {
	if err := db.Put(rootCanonicalHashKey(number), hash.Bytes()); err != nil {
		log.Crit("Failed to store root number to hash mapping", "err", err)
	}
}

// DeleteRootCanonicalHash removes the root number to hash canonical mapping.
func DeleteRootCanonicalHash(db ethdb.KeyValueWriter, number uint64) {
	if err := db.Delete(rootCanonicalHashKey(number)); err != nil {
		log.Crit("Failed to delete root number to hash mapping", "err", err)
	}
}

// ReadMinorCanonicalHash retrieves the minor block hash assigned to a canonical block number.
func ReadMinorCanonicalHash(db ethdb.KeyValueReader, number uint64) common.Hash {
	data, _ := db.Get(minorCanonicalHashKey(number))
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteMinorCanonicalHash stores the minor block hash assigned to a canonical block number.
func WriteMinorCanonicalHash(db ethdb.KeyValueWriter, hash common.Hash, number uint64) {
	if err := db.Put(minorCanonicalHashKey(number), hash.Bytes()); err != nil {
		log.Crit("Failed to store minor number to hash mapping", "err", err)
	}
}

// DeleteMinorCanonicalHash removes the minor number to hash canonical mapping.
func DeleteMinorCanonicalHash(db ethdb.KeyValueWriter, number uint64) {
	if err := db.Delete(minorCanonicalHashKey(number)); err != nil {
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

// HasReceipts verifies the existence of all the transaction receipts belonging
// to a block.
func HasQKCReceipts(db ethdb.KeyValueReader, hash common.Hash) bool {
	if has, err := db.Has(qkcBlockReceiptsKey(hash)); !has || err != nil {
		return false
	}
	return true
}

// ReadReceipts retrieves the consensus-encoded receipts belonging to a block.
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

// WriteReceipts stores the consensus encoding of all receipts belonging to a block.
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

// DeleteReceipts removes all receipt data associated with a block hash.
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

// DeleteBlock removes all block data associated with a hash.
func DeleteMinorBlock(db ethdb.KeyValueWriter, hash common.Hash) {
	DeleteQKCReceipts(db, hash)
	if err := db.Delete(qkcMinorBlockKey(hash)); err != nil {
		log.Crit("Failed to delete minor block", "err", err)
	}
}

func WriteTotalTx(db ethdb.KeyValueWriter, hash common.Hash, txCount uint32) {
	data := qkcEncodeUint32(txCount)
	if err := db.Put(qkcTotalTxCountKey(hash), data); err != nil {
		log.Crit("Failed to store total Tx", "err", err)
	}
}

func ReadTotalTx(db ethdb.KeyValueReader, hash common.Hash) *uint32 {
	data, _ := db.Get(qkcTotalTxCountKey(hash))
	if len(data) != 4 {
		return nil
	}
	number := binary.BigEndian.Uint32(data)
	return &number

}

func WriteGenesisBlock(db ethdb.KeyValueWriter, rHash common.Hash, block *types.MinorBlock) {
	data, err := serialize.SerializeToBytes(block)
	if err != nil {
		log.Crit("can not serilalize Minor block")
	}
	key := qkcGenesisKey(rHash)
	if err := db.Put(key, data); err != nil {
		log.Crit("Failed to store genesis", "err", err)
	}
}

func ReadGenesis(db ethdb.KeyValueReader, rHash common.Hash) *types.MinorBlock {
	data, _ := db.Get(qkcGenesisKey(rHash))
	if len(data) == 0 {
		return nil
	}
	res := new(types.MinorBlock)
	if err := serialize.DeserializeFromBytes(data, res); err != nil {
		return nil
	}
	return res
}

func WriteConfirmedCrossShardTxList(db ethdb.KeyValueWriter, rHash common.Hash, list *types.CrossShardTransactionList) {
	data, err := serialize.SerializeToBytes(list)
	if err != nil {
		log.Crit("can not serialize CrossShardTransactionList")
	}
	key := qkcConfirmedXShardKey(rHash)
	if err := db.Put(key, data); err != nil {
		log.Crit("Failed to store header", "err", err)
	}
}

func ReadConfirmedCrossShardTxList(db ethdb.KeyValueReader, rHash common.Hash) *types.CrossShardTransactionList {
	data, _ := db.Get(qkcConfirmedXShardKey(rHash))
	if len(data) == 0 {
		return nil
	}
	list := new(types.CrossShardTransactionList)
	if err := serialize.Deserialize(serialize.NewByteBuffer(data), list); err != nil {
		log.Error("Invalid block header Deserialize", "hash", rHash, "err", err)
		return nil
	}
	return list
}

func WriteCrossShardTxList(db ethdb.KeyValueWriter, rHash common.Hash, list *types.CrossShardTransactionList) {
	data, err := serialize.SerializeToBytes(list)
	if err != nil {
		log.Crit("can not serialize CrossShardTransactionList")
	}
	key := qkcXShardTxListKey(rHash)
	if err := db.Put(key, data); err != nil {
		log.Crit("Failed to store header", "err", err)
	}
}

func ReadCrossShardTxList(db ethdb.KeyValueReader, rHash common.Hash) *types.CrossShardTransactionList {
	data, _ := db.Get(qkcXShardTxListKey(rHash))
	if len(data) == 0 {
		return nil
	}
	list := new(types.CrossShardTransactionList)
	if err := serialize.Deserialize(serialize.NewByteBuffer(data), list); err != nil {
		log.Error("Invalid block header Deserialize", "hash", rHash, "err", err)
		return nil
	}
	return list
}

func WriteLastConfirmedMinorBlockHeaderAtRootBlock(db ethdb.KeyValueWriter, rHash common.Hash, mHash common.Hash) {
	if err := db.Put(qkcLastMinorAtRootKey(rHash), mHash.Bytes()); err != nil {
		log.Crit("failed to store last confirmed  minot block at root block")
	}
}

func ReadLastConfirmedMinorBlockHeaderAtRootBlock(db ethdb.KeyValueReader, rHash common.Hash) common.Hash {
	data, _ := db.Get(qkcLastMinorAtRootKey(rHash))
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

func PutXShardDepositHashList(db ethdb.KeyValueWriter, h common.Hash, hList *HashList) {
	bytes, err := serialize.SerializeToBytes(hList)
	if err != nil {
		log.Crit("can not serialize HashList")
	}
	if err := db.Put(qkcXShardDepositHashListKey(h), bytes); err != nil {
		log.Crit("failed to put xshard deposit hash list err", err)
	}
}

func GetXShardDepositHashList(db ethdb.KeyValueReader, h common.Hash) *HashList {
	data, _ := db.Get(qkcXShardDepositHashListKey(h))
	if len(data) == 0 {
		return nil
	}
	hList := new(HashList)
	if err := serialize.DeserializeFromBytes(data, hList); err != nil {
		log.Error("GetXShardDepositHashList", "DeserializeFromBytes err", err)
		return nil
	}
	return hList
}
