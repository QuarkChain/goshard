// Copyright 2026-2027, QuarkChain.

package core

import "errors"

var (
	ErrQuarkChainConfigUnavailable = errors.New("quarkchain config is unavailable")
	ErrShardConfigUnavailable      = errors.New("shard config is unavailable")
	ErrDatabaseUnavailable         = errors.New("database is unavailable")
	ErrMinorChainUnavailable       = errors.New("minor block chain is unavailable")
	ErrConnManagerUnavailable      = errors.New("connection manager is unavailable")
	ErrNoGenesis                   = errors.New("minor block chain genesis is missing")
	ErrUnknownBlock                = errors.New("unknown minor block")
	ErrUnknownParent               = errors.New("unknown minor block parent")
	ErrStateRootMismatch           = errors.New("processed state root does not match minor block")
	ErrReceiptRootMismatch         = errors.New("processed receipt root does not match minor block")
	ErrGasUsedMismatch             = errors.New("processed gas used does not match minor block")
	ErrXShardGasUsedMismatch       = errors.New("processed x-shard gas used does not match minor block")
	ErrInvalidExecutionResult      = errors.New("minor block execution result is invalid")
	ErrCoinbaseAmountMismatch      = errors.New("processed coinbase amount does not match minor block")
	ErrUnknownRootBlock            = errors.New("unknown root block")
	ErrRootChainUninitialized      = errors.New("shard coordinator is not initialized from the root chain")
	ErrRootChainAlreadyInitialized = errors.New("shard coordinator is already initialized from the root chain")
	ErrChainStopped                = errors.New("shard coordinator stopped")
	ErrWrongShard                  = errors.New("minor block branch does not match shard branch")
)
