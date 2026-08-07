// Copyright 2026-2027, QuarkChain.

package state

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/holiman/uint256"
)

// journalEntry undoes one account-level mutation. pyquarkchain's journal is a
// list of closures appended by State.set_and_journal (quarkchain/evm/state.py:422);
// these entries are the same closures, named.
//
// State-level fields (gas used, logs, receipts, suicides, refunds, the xshard
// list, fee tokens) are not journaled here even though pyquarkchain appends
// entries for them: its revert restores every one of them wholesale from the
// snapshot afterwards (state.py:532), so the snapshot copy alone decides the
// outcome.
type journalEntry interface {
	revert(*StateDB)
}

// object returns the cached account an entry was recorded against. Every entry
// is recorded after the account was cached, so it is always present.
func (s *StateDB) journalObject(addr account.Recipient) *stateObject {
	return s.stateObjects[addr]
}

type nonceChange struct {
	addr account.Recipient
	prev uint64
}

func (c nonceChange) revert(s *StateDB) { s.journalObject(c.addr).nonce = c.prev }

type touchChange struct {
	addr account.Recipient
	prev bool
}

func (c touchChange) revert(s *StateDB) { s.journalObject(c.addr).touched = c.prev }

type deletedChange struct {
	addr account.Recipient
	prev bool
}

func (c deletedChange) revert(s *StateDB) { s.journalObject(c.addr).deleted = c.prev }

type codeChange struct {
	addr     account.Recipient
	prevCode []byte
	prevHash common.Hash
}

func (c codeChange) revert(s *StateDB) {
	obj := s.journalObject(c.addr)
	obj.code, obj.codeHash = c.prevCode, c.prevHash
	obj.codeLoaded = true
}

// balanceChange restores one token's balance. The entry is written back even
// when the token had no entry before, matching state.py:163-166: the restore
// stores the previous balance (zero) under the same key rather than removing
// it, and a zero entry is not the same as an absent one — an account that holds
// one serializes to an empty pair list, not to empty bytes.
type balanceChange struct {
	addr    account.Recipient
	tokenID uint64
	prev    *uint256.Int
}

func (c balanceChange) revert(s *StateDB) {
	s.journalObject(c.addr).balances.SetValue(c.prev, c.tokenID)
}

type storageChange struct {
	addr account.Recipient
	key  common.Hash
	prev common.Hash
}

func (c storageChange) revert(s *StateDB) {
	s.journalObject(c.addr).storage[c.key] = c.prev
}

// storageResetChange restores both halves of reset_storage (state.py:613-620):
// the pending writes and the storage trie's root, which that function overwrites
// directly rather than through the trie.
type storageResetChange struct {
	addr     account.Recipient
	prev     map[common.Hash]common.Hash
	prevRoot common.Hash
}

func (c storageResetChange) revert(s *StateDB) {
	obj := s.journalObject(c.addr)
	obj.storage = c.prev
	obj.setStorageRoot(c.prevRoot)
}
