// Copyright 2026-2027, QuarkChain.

package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
)

// The fields below define the QKC-specific low-level database schema.
var (
	qkcPrefix = []byte("qkc_") // qkcPrefix + QKC-specific prefix + key parts -> QKC namespaced key

	rootHashPrefixQKC        = []byte("rn")       // qkcPrefix + rootHashPrefixQKC + num (uint64 big endian) -> root canonical hash
	minorHashPrefixQKC       = []byte("mn")       // qkcPrefix + minorHashPrefixQKC + num (uint64 big endian) -> minor canonical hash
	rootBlockPrefixQKC       = []byte("rb")       // qkcPrefix + rootBlockPrefixQKC + hash -> root block
	minorBlockPrefixQKC      = []byte("mb")       // qkcPrefix + minorBlockPrefixQKC + hash -> minor block
	totalTxCountPrefixQKC    = []byte("txC")      // qkcPrefix + totalTxCountPrefixQKC + hash -> total tx count (uint32 big endian)
	confirmedXShardPrefixQKC = []byte("xr")       // qkcPrefix + confirmedXShardPrefixQKC + hash -> confirmed cross-shard tx list
	xShardListPrefixQKC      = []byte("xSL")      // qkcPrefix + xShardListPrefixQKC + hash -> cross-shard tx list
	xShardHashListPrefixQKC  = []byte("xSHL")     // qkcPrefix + xShardHashListPrefixQKC + hash -> cross-shard deposit hash list
	lastMinorAtRootPrefixQKC = []byte("rLM")      // qkcPrefix + lastMinorAtRootPrefixQKC + root hash -> last confirmed minor block hash
	genesisPrefixQKC         = []byte("genesis")  // qkcPrefix + genesisPrefixQKC + root hash -> genesis minor block
	chainConfigPrefixQKC     = []byte("config-")  // qkcPrefix + chainConfigPrefixQKC + genesis hash -> QKC chain config
	rootHeadKey              = []byte("LastRoot") // qkcPrefix + rootHeadPrefixQKC -> canonical root head hash
)

// qkcLookupEntry is positional metadata for looking up block content by hash.
type qkcLookupEntry struct {
	BlockHash common.Hash
	Index     uint32
}

func qkcEncodeUint32(number uint32) []byte {
	enc := make([]byte, 4)
	binary.BigEndian.PutUint32(enc, number)
	return enc
}

// qkcKey builds a QKC-specific database key by prepending qkcPrefix to the
// record prefix and key parts, keeping QKC additions isolated from geth keys.
func qkcKey(prefix []byte, parts ...[]byte) []byte {
	size := len(qkcPrefix) + len(prefix)
	for _, part := range parts {
		size += len(part)
	}
	key := make([]byte, 0, size)
	key = append(key, qkcPrefix...)
	key = append(key, prefix...)
	for _, part := range parts {
		key = append(key, part...)
	}
	return key
}

func rootCanonicalHashKey(number uint64) []byte {
	return qkcKey(rootHashPrefixQKC, encodeBlockNumber(number))
}

func minorCanonicalHashKey(number uint64) []byte {
	return qkcKey(minorHashPrefixQKC, encodeBlockNumber(number))
}

func qkcRootBlockKey(hash common.Hash) []byte {
	return qkcKey(rootBlockPrefixQKC, hash.Bytes())
}

func qkcMinorBlockKey(hash common.Hash) []byte {
	return qkcKey(minorBlockPrefixQKC, hash.Bytes())
}

func qkcBlockReceiptsKey(hash common.Hash) []byte {
	return qkcKey(blockReceiptsPrefix, hash.Bytes())
}

func qkcTotalTxCountKey(hash common.Hash) []byte {
	return qkcKey(totalTxCountPrefixQKC, hash.Bytes())
}

// qkcConfirmedXShardKey returns the key for deposits executed by a receiving minor block.
func qkcConfirmedXShardKey(hash common.Hash) []byte {
	return qkcKey(confirmedXShardPrefixQKC, hash.Bytes())
}

// qkcXShardTxListKey returns the key for deposits broadcast by a source minor block.
func qkcXShardTxListKey(hash common.Hash) []byte {
	return qkcKey(xShardListPrefixQKC, hash.Bytes())
}

func qkcGenesisKey(hash common.Hash) []byte {
	return qkcKey(genesisPrefixQKC, hash.Bytes())
}

func qkcLastMinorAtRootKey(hash common.Hash) []byte {
	return qkcKey(lastMinorAtRootPrefixQKC, hash.Bytes())
}

func qkcXShardDepositHashListKey(hash common.Hash) []byte {
	return qkcKey(xShardHashListPrefixQKC, hash.Bytes())
}

func qkcChainConfigKey(hash common.Hash) []byte {
	return qkcKey(chainConfigPrefixQKC, hash.Bytes())
}

func qkcRootHeadKey() []byte {
	return qkcKey(rootHeadKey)
}
