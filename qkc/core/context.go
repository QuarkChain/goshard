// Copyright 2026-2027, QuarkChain.

// Package core applies QuarkChain transactions to geth's native StateDB and
// EVM. Block processing and cross-shard settlement are layered on top of this
// package.
package core

import (
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
)

// ExecutionContext contains the network and shard rules needed to admit and
// execute a transaction. Block-local values are read from the EVM context.
type ExecutionContext struct {
	QKCConfig   *config.QuarkChainConfig
	ShardConfig *config.ShardConfig
}

// Branch returns the shard this context executes.
func (ctx *ExecutionContext) Branch() account.Branch {
	return account.NewBranch(ctx.ShardConfig.GetFullShardId())
}
