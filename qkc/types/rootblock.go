// Copyright 2026-2027, QuarkChain.

// RootBlockHeader mirrors goquarkchain's core/types.RootBlockHeader, kept minimal:
// only the fields and the Hash/SealHash needed to derive and pin the root genesis
// block. Mining, signing, and RLP helpers are omitted.
//
// Field order and ser tags reproduce pyquarkchain's RootBlockHeader.FIELDS
// (quarkchain/core.py:888) exactly, so qkc/serialize encodes it byte-identically:
//
//	version              uint32
//	height               uint32
//	hash_prev_block       hash256  (32 raw bytes)
//	hash_merkle_root      hash256
//	hash_evm_state_root   hash256
//	coinbase_address      Address  (20-byte recipient + uint32 full_shard_key)
//	coinbase_amount_map   TokenBalanceMap
//	create_time           uint64
//	difficulty            biguint  (1-byte length prefix + big-endian bytes)
//	total_difficulty      biguint
//	nonce                 uint64
//	extra_data            PrependedSizeBytes(2)
//	mixhash               hash256
//	signature             FixedSizeBytes(65)

package types

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
)

// RootBlockHeader is the QuarkChain root block header. Only the subset of behavior
// needed to derive, hash, and seal-hash the root genesis is implemented.
type RootBlockHeader struct {
	Version         uint32
	Number          uint32
	ParentHash      common.Hash
	MinorHeaderHash common.Hash
	Root            common.Hash
	Coinbase        account.Address
	CoinbaseAmount  *TokenBalances
	Time            uint64
	Difficulty      *big.Int
	TotalDifficulty *big.Int
	Nonce           uint64
	Extra           []byte `bytesizeofslicelen:"2"`
	MixDigest       common.Hash
	Signature       [65]byte
}

// Hash returns keccak256 of the full serialized header — the value pyquarkchain
// records as the block hash (RootBlockHeader.get_hash, quarkchain/core.py:938).
func (h *RootBlockHeader) Hash() common.Hash {
	return serHash(*h, nil)
}

// SealHash returns keccak256 of the header serialized without Nonce, MixDigest,
// and Signature — pyquarkchain's get_hash_for_mining (the proof-of-work input).
func (h *RootBlockHeader) SealHash() common.Hash {
	return serHash(*h, map[string]bool{"Nonce": true, "MixDigest": true, "Signature": true})
}
