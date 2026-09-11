// Copyright 2026-2027, QuarkChain.

package rawdb

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// ReadBlockContentLookupEntry retrieves the positional metadata associated with a transaction
// hash to allow retrieving the transaction or receipt by hash.
func ReadBlockContentLookupEntry(db ethdb.KeyValueReader, hash common.Hash) (common.Hash, uint32) {
	data, _ := db.Get(txLookupKey(hash))
	if len(data) == 0 {
		return common.Hash{}, 0
	}
	var entry qkcLookupEntry
	if err := serialize.Deserialize(serialize.NewByteBuffer(data), &entry); err != nil {
		log.Error("Invalid transaction lookup entry RLP", "hash", hash, "err", err)
		return common.Hash{}, 0
	}
	return entry.BlockHash, entry.Index
}

// WriteBlockContentLookupEntriesWithCrossShardHashList stores a positional metadata for every transaction from
// a block, enabling hash based transaction and receipt lookups.
func WriteBlockContentLookupEntriesWithCrossShardHashList(db ethdb.KeyValueWriter, block *types.MinorBlock, hList *HashList) {
	blockHash := block.Hash()
	for i, item := range block.Content() {
		entry := qkcLookupEntry{
			BlockHash: blockHash,
			Index:     uint32(i),
		}
		data, err := serialize.SerializeToBytes(entry)
		if err != nil {
			log.Crit("Failed to encode content lookup entry", "err", err)
		}
		if err := db.Put(txLookupKey(item.Hash()), data); err != nil {
			log.Crit("Failed to store content lookup entry", "err", err)
		}
	}
	if hList == nil || len(hList.HList) == 0 {
		return
	}
	for i, h := range hList.HList {
		entry := qkcLookupEntry{
			BlockHash: blockHash,
			Index:     uint32(i) + uint32(len(block.Content())),
		}
		data, err := serialize.SerializeToBytes(entry)
		if err != nil {
			log.Crit("Failed to encode xshard tx lookup entry", "err", err)
		}
		if err := db.Put(txLookupKey(h), data); err != nil {
			log.Crit("Failed to store xshard tx lookup entry")
		}
	}
}

// ReadTransaction retrieves a specific transaction from the database, along with
// its added positional metadata.
func ReadTransaction(db ethdb.KeyValueReader, hash common.Hash) (*types.Transaction, common.Hash, uint32) {
	blockHash, txIndex := ReadBlockContentLookupEntry(db, hash)
	if blockHash == (common.Hash{}) {
		return nil, common.Hash{}, 0
	}
	block := ReadMinorBlock(db, blockHash)
	if block == nil {
		log.Error("Transaction referenced missing", "hash", blockHash, "index", txIndex)
		return nil, common.Hash{}, 0
	}
	if int(txIndex) < len(block.Transactions()) {
		return block.Transactions()[txIndex], blockHash, txIndex
	}
	return nil, blockHash, txIndex //xShardTx
}

// ReadReceipt retrieves a specific transaction receipt from the database, along with
// its added positional metadata.
func ReadReceipt(db ethdb.KeyValueReader, hash common.Hash) (*types.Receipt, common.Hash, uint32) {
	blockHash, receiptIndex := ReadBlockContentLookupEntry(db, hash)
	if blockHash == (common.Hash{}) {
		return nil, common.Hash{}, 0
	}
	receipts := ReadQKCReceipts(db, blockHash)
	if len(receipts) <= int(receiptIndex) {
		log.Error("Receipt refereced missing", "hash", blockHash, "index", receiptIndex)
		return nil, common.Hash{}, 0
	}
	return receipts[receiptIndex], blockHash, receiptIndex
}
