// Copyright 2026-2027, QuarkChain.

package core

import (
	"github.com/ethereum/go-ethereum/common"
	ethcore "github.com/ethereum/go-ethereum/core"
	coretypes "github.com/ethereum/go-ethereum/core/types"
)

// RootChainView exposes only the root lookup required before candidate import.
type RootChainView interface {
	HasRootBlock(hash common.Hash) bool
}

// BlockChain is the minimal local-chain surface used by the demo coordinator.
type BlockChain interface {
	GetBlock(hash common.Hash) *coretypes.Block
	HasBlock(hash common.Hash) bool
	InsertBlockWithoutExecution(block *coretypes.Block, signer coretypes.Signer) error
}

// GethBlockChainAdapter adapts geth BlockChain method shapes without exposing
// any root-chain information to the local chain.
type GethBlockChainAdapter struct {
	chain *ethcore.BlockChain
}

func NewGethBlockChainAdapter(chain *ethcore.BlockChain) (*GethBlockChainAdapter, error) {
	if chain == nil {
		return nil, ErrMinorChainUnavailable
	}
	return &GethBlockChainAdapter{chain: chain}, nil
}

func (a *GethBlockChainAdapter) GetBlock(hash common.Hash) *coretypes.Block {
	return a.chain.GetBlockByHash(hash)
}

func (a *GethBlockChainAdapter) HasBlock(hash common.Hash) bool {
	number := a.chain.GetBlockNumber(hash)
	return number != nil && a.chain.HasBlock(hash, *number)
}

func (a *GethBlockChainAdapter) InsertBlockWithoutExecution(block *coretypes.Block, signer coretypes.Signer) error {
	return a.chain.InsertBlockWithoutExecution(block, signer)
}
