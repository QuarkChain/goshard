// Copyright 2026-2027, QuarkChain.

package rawdb

import (
	"github.com/ethereum/go-ethereum/common"
)

// The fields below define the QKC-specific low-level database schema.
var (
	qkcPrefix = []byte("qkc_") // qkcPrefix + QKC-specific prefix + key parts -> QKC namespaced key

	rootHashPrefixQKC   = []byte("rn")       // qkcPrefix + rootHashPrefixQKC + num (uint64 big endian) -> root canonical hash
	minorHashPrefixQKC  = []byte("mn")       // qkcPrefix + minorHashPrefixQKC + num (uint64 big endian) -> minor canonical hash
	rootBlockPrefixQKC  = []byte("rb")       // qkcPrefix + rootBlockPrefixQKC + hash -> root block
	minorBlockPrefixQKC = []byte("mb")       // qkcPrefix + minorBlockPrefixQKC + hash -> minor block
	rootHeadKey         = []byte("LastRoot") // qkcPrefix + rootHeadPrefixQKC -> canonical root head hash
)

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

func qkcRootHeadKey() []byte {
	return qkcKey(rootHeadKey)
}

func qkcBlockReceiptsKey(hash common.Hash) []byte {
	return qkcKey(blockReceiptsPrefix, hash.Bytes())
}
