// Copyright 2026-2027, QuarkChain.

// RootBlockHeader mirrors goquarkchain's core/types.RootBlockHeader, kept minimal:
// only the fields and the Hash/SealHash needed to derive and pin the root genesis
// block, plus the body a shard needs to walk its cross-shard cursor. Mining,
// signing, and RLP helpers are omitted.
// TODO: more content need to be added later.
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
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
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
	CoinbaseAmount  *qkcCommon.TokenBalances
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

// RootBlock is a root block: its header plus the minor block headers it confirms
// (quarkchain/core.py:989). The header list is ordered, and that order is what
// the shards' cross-shard cursor walks, so it is consensus data rather than an
// index: a deposit's position is (root block height, index in this list, index
// within that minor block's deposit list).
type RootBlock struct {
	Header            *RootBlockHeader
	MinorBlockHeaders []*MinorBlockHeader `bytesizeofslicelen:"4"`
	TrackingData      []byte              `bytesizeofslicelen:"2"`
}

// NewRootBlock assembles a root block. The merkle root over the header list is
// not recomputed here: a block read off the wire must keep the root it carries,
// so that a mismatch stays visible to validation.
func NewRootBlock(header *RootBlockHeader, headers []*MinorBlockHeader, trackingData []byte) *RootBlock {
	return &RootBlock{Header: header, MinorBlockHeaders: headers, TrackingData: trackingData}
}

// Hash returns the block hash: the header's hash.
func (b *RootBlock) Hash() common.Hash { return b.Header.Hash() }

// NumberU64 returns the block height.
func (b *RootBlock) NumberU64() uint64 { return uint64(b.Header.Number) }

// MinorHeaderMerkleRoot is the merkle root the header commits to over the minor
// block header list (RootBlock.finalize, quarkchain/core.py:1017).
func (b *RootBlock) MinorHeaderMerkleRoot() common.Hash {
	return CalculateMerkleRoot(b.MinorBlockHeaders)
}
