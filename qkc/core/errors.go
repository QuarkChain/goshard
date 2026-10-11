// Copyright 2026-2027, QuarkChain.

package core

import "errors"

var (
	ErrRootChainUnavailable  = errors.New("root chain view is unavailable")
	ErrMinorChainUnavailable = errors.New("minor chain is unavailable")
	ErrSignerUnavailable     = errors.New("QKC transaction signer is unavailable")
	ErrUnknownBlock          = errors.New("unknown minor block")
	ErrGenesisBlock          = errors.New("minor genesis cannot be imported as a candidate")
	ErrUnknownParent         = errors.New("unknown minor block parent")
	ErrNonContiguousBlock    = errors.New("minor block does not extend its parent")
	ErrUnknownRootBlock      = errors.New("unknown root block")
	ErrWrongShard            = errors.New("minor block branch does not match shard branch")
)
