// Copyright 2026-2027, QuarkChain.

package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
)

// The fields below define the QKC-specific low-level database schema.
var (
	qkcPrefix                = []byte("q_")
	rootHashPrefixQKC        = []byte("rn")
	minorHashPrefixQKC       = []byte("mn")
	rootBlockPrefixQKC       = []byte("rb")
	minorBlockPrefixQKC      = []byte("mb")
	totalTxCountPrefixQKC    = []byte("txC")
	confirmedXShardPrefixQKC = []byte("xr")
	xShardListPrefixQKC      = []byte("xSL")
	xShardHashListPrefixQKC  = []byte("xSHL")
	lastMinorAtRootPrefixQKC = []byte("rLM")
	genesisPrefixQKC         = []byte("genesis")
	chainConfigPrefixQKC     = []byte("config-")
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
