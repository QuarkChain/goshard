// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/qkc/account"
)

// ShardCoordinator performs the root-aware checks required by the candidate
// import demo. Execution, fork choice, networking, and recovery are deferred.
type ShardCoordinator struct {
	branch account.Branch
	roots  RootChainView
	chain  BlockChain
	signer types.Signer
}

func NewShardCoordinator(branch account.Branch, roots RootChainView, chain BlockChain, signer types.Signer) (*ShardCoordinator, error) {
	if roots == nil {
		return nil, ErrRootChainUnavailable
	}
	if chain == nil {
		return nil, ErrMinorChainUnavailable
	}
	if signer == nil {
		return nil, ErrSignerUnavailable
	}
	return &ShardCoordinator{branch: branch, roots: roots, chain: chain, signer: signer}, nil
}

// AddMinorBlock validates one decoded minor block and stores it as a
// non-canonical candidate without executing its transactions.
func (c *ShardCoordinator) AddMinorBlock(block *types.Block) error {
	if block == nil {
		return ErrUnknownBlock
	}
	header, err := block.ToMinorBlockHeader()
	if err != nil {
		return fmt.Errorf("project minor block header: %w", err)
	}
	if header.Number == 0 {
		return ErrGenesisBlock
	}
	if header.Branch.Value != c.branch.Value {
		return fmt.Errorf("minor block branch %d does not match shard branch %d: %w", header.Branch.Value, c.branch.Value, ErrWrongShard)
	}
	if c.chain.HasBlock(block.Hash()) {
		return c.chain.InsertBlockWithoutExecution(block, c.signer)
	}
	parent := c.chain.GetBlock(header.ParentHash)
	if parent == nil {
		return fmt.Errorf("parent %s: %w", header.ParentHash, ErrUnknownParent)
	}
	if header.Number != parent.NumberU64()+1 || header.Time <= parent.Time() {
		return fmt.Errorf("block %d at time %d does not extend parent %d at time %d: %w",
			header.Number, header.Time, parent.NumberU64(), parent.Time(), ErrNonContiguousBlock)
	}
	if !c.roots.HasRootBlock(header.PrevRootBlockHash) {
		return fmt.Errorf("previous root %s: %w", header.PrevRootBlockHash, ErrUnknownRootBlock)
	}
	return c.chain.InsertBlockWithoutExecution(block, c.signer)
}
