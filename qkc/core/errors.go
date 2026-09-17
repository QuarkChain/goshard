// Copyright 2026-2027, QuarkChain.

package core

import "errors"

var (
	ErrQuarkChainConfigUnavailable    = errors.New("quarkchain config is unavailable")
	ErrShardConfigUnavailable         = errors.New("shard config is unavailable")
	ErrDatabaseUnavailable            = errors.New("shard coordinator database is unavailable")
	ErrMinorChainUnavailable          = errors.New("minor chain is unavailable")
	ErrConnManagerUnavailable         = errors.New("connection manager is unavailable")
	ErrNoGenesis                      = errors.New("minor chain genesis is missing")
	ErrNoCurrentBlock                 = errors.New("minor chain current block is missing")
	ErrUnknownParent                  = errors.New("unknown minor block parent")
	ErrStateUnavailable               = errors.New("minor block state is unavailable")
	ErrRootChainUninitialized         = errors.New("shard coordinator is not initialized from the root chain")
	ErrRootChainAlreadyInitialized    = errors.New("shard coordinator is already initialized from the root chain")
	ErrRootBeforeGenesis              = errors.New("root block predates shard genesis")
	ErrUnknownRootBlock               = errors.New("unknown root block")
	ErrMinorRootNotCanonical          = errors.New("minor block references a non-canonical root block")
	ErrMinorNotConfirmedDescendant    = errors.New("minor block does not descend from the confirmed minor tip")
	ErrConfirmedMinorBlockUnavailable = errors.New("confirmed minor block is unavailable")
	ErrRemoteXShardTxListUnavailable  = errors.New("remote cross-shard transaction list is unavailable")
	ErrUnexpectedRemoteXShardTxList   = errors.New("unexpected remote cross-shard transaction list")
	ErrRootNotContiguous              = errors.New("root block does not extend its parent")
	ErrChainStopped                   = errors.New("shard coordinator stopped")
)
