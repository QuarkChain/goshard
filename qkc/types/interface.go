// Copyright 2026-2027, QuarkChain.

// QKC block interfaces follow pyquarkchain-compatible type boundaries.

package types

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
)

type IHeader interface {
	Hash() common.Hash
	SealHash() common.Hash
	NumberU64() uint64
	GetVersion() uint32
	GetParentHash() common.Hash
	GetCoinbase() account.Address
	GetTime() uint64
	GetCoinbaseAmount() *qkcCommon.TokenBalances
	GetDifficulty() *big.Int
	GetNonce() uint64
	GetExtra() []byte
	GetMixDigest() common.Hash
}

type IBlock interface {
	Hash() common.Hash
	NumberU64() uint64
	IHeader() IHeader
	Content() []IHashable
	GetTrackingData() []byte
	GetSize() common.StorageSize
	ParentHash() common.Hash
	Coinbase() account.Address
	Time() uint64
	Difficulty() *big.Int
}

type IHashable interface {
	Hash() common.Hash
}

var (
	_ IHeader = (*MinorBlockHeader)(nil)
	_ IHeader = (*RootBlockHeader)(nil)
	_ IBlock  = (*MinorBlock)(nil)
	_ IBlock  = (*RootBlock)(nil)
)
