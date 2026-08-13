// Copyright 2026-2027, QuarkChain.

package types

// XShardTxCursorInfo is the cross-shard transaction cursor, mirroring
// pyquarkchain's XshardTxCursorInfo (quarkchain/core.py:623): three uint64 fields
// serialized in this order. It is a field of the minor block's meta.
type XShardTxCursorInfo struct {
	RootBlockHeight    uint64
	MinorBlockIndex    uint64
	XShardDepositIndex uint64
}
