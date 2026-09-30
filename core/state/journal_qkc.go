// Copyright 2026-2027, QuarkChain.

package state

import (
	"github.com/ethereum/go-ethereum/common"
)

// qkcFullShardKeyChange undoes SetFullShardKey. full_shard_key is part of
// STATE_DEFAULTS (state.py:45), which State.revert puts back. The keys already
// frozen into qkcAccountCache stay: pyquarkchain keeps those blank accounts cached.
type qkcFullShardKeyChange struct {
	prev uint32
}

func (ch qkcFullShardKeyChange) revert(s *StateDB) {
	s.fullShardKey = ch.prev
}

func (ch qkcFullShardKeyChange) dirtied() (common.Address, bool) {
	return common.Address{}, false
}

func (ch qkcFullShardKeyChange) copy() journalEntry {
	return ch
}
