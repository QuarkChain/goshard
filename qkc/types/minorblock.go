// Copyright 2026-2027, QuarkChain.

// MinorBlockMeta, MinorBlockHeader and MinorBlock mirror goquarkchain's
// core/types, kept to what the shard genesis needs: the fields, their wire order,
// and the hashes. Mining, RLP, copy and accessor helpers are omitted.
//
// Field order and ser tags reproduce pyquarkchain's FIELDS exactly, so
// qkc/serialize encodes them byte-identically:
//
//	MinorBlockMeta (quarkchain/core.py:637)
//	  hash_merkle_root                  hash256
//	  hash_evm_state_root               hash256
//	  hash_evm_receipt_root             hash256
//	  evm_gas_used                      uint256
//	  evm_cross_shard_receive_gas_used  uint256
//	  xshard_tx_cursor_info             XshardTxCursorInfo
//	  evm_xshard_gas_limit              uint256
//
//	MinorBlockHeader (quarkchain/core.py:673)
//	  version                uint32
//	  branch                 Branch   (uint32)
//	  height                 uint64
//	  coinbase_address       Address  (20-byte recipient + uint32 full_shard_key)
//	  coinbase_amount_map    TokenBalanceMap
//	  hash_prev_minor_block  hash256
//	  hash_prev_root_block   hash256
//	  evm_gas_limit          uint256
//	  hash_meta              hash256
//	  create_time            uint64
//	  difficulty             biguint
//	  nonce                  uint64
//	  bloom                  uint2048 (256 raw bytes)
//	  extra_data             PrependedSizeBytes(2)
//	  mixhash                hash256
//
//	MinorBlock (quarkchain/core.py:745)
//	  header, meta, tx_list PrependedSizeList(4), tracking_data PrependedSizeBytes(2)

package types

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// MinorBlockMeta holds the minor block fields the root chain does not carry.
type MinorBlockMeta struct {
	TxHash            common.Hash // = pyquarkchain hash_merkle_root
	Root              common.Hash // EVM state root
	ReceiptHash       common.Hash
	GasUsed           *serialize.Uint256
	CrossShardGasUsed *serialize.Uint256
	XShardTxCursor    XShardTxCursorInfo
	XShardGasLimit    *serialize.Uint256
}

// Hash returns keccak256 of the serialized meta — the value the header commits to
// as hash_meta (quarkchain/genesis.py:111).
func (m *MinorBlockMeta) Hash() common.Hash {
	return serHash(*m, nil)
}

// MinorBlockHeader is the QuarkChain minor (shard) block header.
type MinorBlockHeader struct {
	Version           uint32
	Branch            account.Branch
	Number            uint64 // = pyquarkchain height
	Coinbase          account.Address
	CoinbaseAmount    *qkcCommon.TokenBalances
	ParentHash        common.Hash // = pyquarkchain hash_prev_minor_block
	PrevRootBlockHash common.Hash
	GasLimit          *serialize.Uint256
	MetaHash          common.Hash
	Time              uint64
	Difficulty        *big.Int
	Nonce             uint64
	Bloom             Bloom
	Extra             []byte `bytesizeofslicelen:"2"`
	MixDigest         common.Hash
}

// Hash returns keccak256 of the full serialized header — the value pyquarkchain
// records as the block hash (MinorBlockHeader.get_hash, quarkchain/core.py:733).
func (h *MinorBlockHeader) Hash() common.Hash {
	return serHash(*h, nil)
}

// SealHash returns keccak256 of the header serialized without Nonce and
// MixDigest — pyquarkchain's get_hash_for_mining (the proof-of-work input).
func (h *MinorBlockHeader) SealHash() common.Hash {
	return serHash(*h, map[string]bool{"Nonce": true, "MixDigest": true})
}

// MinorBlock is a minor block: its header, its meta, and its body.
type MinorBlock struct {
	Header       *MinorBlockHeader
	Meta         *MinorBlockMeta
	Transactions []*Transaction `bytesizeofslicelen:"4"`
	TrackingData []byte         `bytesizeofslicelen:"2"`
}

// NewMinorBlock assembles a minor block. The block's identity is its header's
// hash; the body is committed to through the meta's merkle root.
func NewMinorBlock(header *MinorBlockHeader, meta *MinorBlockMeta, txs []*Transaction, trackingData []byte) *MinorBlock {
	return &MinorBlock{Header: header, Meta: meta, Transactions: txs, TrackingData: trackingData}
}

// Hash returns the block hash: the header's hash.
func (b *MinorBlock) Hash() common.Hash { return b.Header.Hash() }

// NumberU64 returns the block height.
func (b *MinorBlock) NumberU64() uint64 { return b.Header.Number }
