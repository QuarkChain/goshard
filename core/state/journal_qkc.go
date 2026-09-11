// Copyright 2026-2027, QuarkChain.

package state

import (
	"maps"

	"github.com/ethereum/go-ethereum/common"
)

// qkcFullShardKeyChange undoes SetFullShardKey. full_shard_key is part of
// STATE_DEFAULTS (state.py:45), which State.revert puts back. The keys already
// frozen into qkcShardKeys stay: pyquarkchain keeps those blank accounts cached.
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

// qkcResetStorageChange undoes ResetStorage. pyquarkchain drops the storage by
// pointing the account's trie at the blank root and emptying its cache
// (state.py:631), journalling both, so the undo has to put back the caches as
// well as the root.
type qkcResetStorageChange struct {
	account     common.Address
	root        common.Hash
	dirty       Storage
	pending     Storage
	origin      Storage
	uncommitted Storage
	// hadDestruct records whether the address was already in the destruct set,
	// so that undoing a reset does not undo an earlier one.
	hadDestruct bool
}

func (ch qkcResetStorageChange) revert(s *StateDB) {
	obj := s.getStateObject(ch.account)
	if obj == nil {
		return
	}
	obj.data.Root = ch.root
	obj.dirtyStorage = maps.Clone(ch.dirty)
	obj.pendingStorage = maps.Clone(ch.pending)
	obj.originStorage = maps.Clone(ch.origin)
	obj.uncommittedStorage = maps.Clone(ch.uncommitted)
	obj.trie = nil
	if !ch.hadDestruct {
		delete(s.stateObjectsDestruct, ch.account)
	}
}

// dirtied reports no address: reset_storage is one of the two QuarkChain
// mutations that leave the account unmarked (the other is reset_balances), and
// on its own it must not bring an account into the block's dirty set.
func (ch qkcResetStorageChange) dirtied() (common.Address, bool) {
	return common.Address{}, false
}

func (ch qkcResetStorageChange) copy() journalEntry {
	return qkcResetStorageChange{
		account:     ch.account,
		root:        ch.root,
		dirty:       maps.Clone(ch.dirty),
		pending:     maps.Clone(ch.pending),
		origin:      maps.Clone(ch.origin),
		uncommitted: maps.Clone(ch.uncommitted),
		hadDestruct: ch.hadDestruct,
	}
}
