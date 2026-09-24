// Copyright 2026-2027, QuarkChain.

package core

import "errors"

var (
	ErrQuarkChainConfigUnavailable = errors.New("quarkchain config is unavailable")
	ErrShardConfigUnavailable      = errors.New("shard config is unavailable")
	ErrDatabaseUnavailable         = errors.New("shard coordinator database is unavailable")
	ErrMinorChainUnavailable       = errors.New("minor chain is unavailable")
	ErrConnManagerUnavailable      = errors.New("connection manager is unavailable")
	ErrNoGenesis                   = errors.New("minor chain genesis is missing")
	ErrUnknownBlock                = errors.New("unknown minor block")
	ErrUnknownParent               = errors.New("unknown minor block parent")
	ErrUnknownRootBlock            = errors.New("unknown root block")
	ErrRootChainUninitialized      = errors.New("shard coordinator is not initialized from the root chain")
	ErrRootChainAlreadyInitialized = errors.New("shard coordinator is already initialized from the root chain")
	ErrChainStopped                = errors.New("shard coordinator stopped")
	ErrWrongShard                  = errors.New("minor block branch does not match shard branch")
)
