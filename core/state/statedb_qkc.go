// Copyright 2026-2027, QuarkChain.
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"errors"
	"fmt"
	"maps"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/holiman/uint256"
)

// ===== QuarkChain account lifetime =====
//
// # Deciding existence once per block
//
// quarkchain/evm/state.py keeps its account cache for a whole block and decides
// in commit() (state.py:562) which accounts to write and which to drop, over
// every account the block touched. geth decides in Finalise, over the accounts
// the journal dirtied since the last call — so a block driving this StateDB must
// call Finalise (or Commit, which calls it) exactly once, at the end of the
// block, never per transaction. Finalising per transaction empties the dirty set
// each time, and an account emptied by an early transaction and never touched
// again would then never be reconsidered: it would survive in the trie, where
// pyquarkchain drops it.
//
// Left that way, the two halves line up exactly: the journal's dirty set is
// pyquarkchain's `touched` (a revert that unwinds every entry for an address
// un-dirties it, as unwinding the touched flag does upstream), self-destruction
// is its `deleted`, and empty() is is_blank.

// ErrTokenTrieUnsupported marks the one write this fork cannot represent: an
// account holding more non-zero token balances than the list encoding takes.
// pyquarkchain switches such an account to a secure trie, irreversibly
// (state.py:100-135); nothing here implements that, so the write is refused
// rather than silently dropped. It is distinguishable so that the execution
// layer can report it as a profile boundary rather than as a database fault.
var ErrTokenTrieUnsupported = errors.New("qkc: token trie balance format is not implemented")

// noteQKCShardKey records the shard key an address was first looked up with.
//
// pyquarkchain freezes the key at that moment, not at first write:
// get_and_cache_account builds a blank account with the state's current
// full_shard_key and caches it (state.py:387), and the cache outlives the
// transaction. An address read by one transaction and first written by the next
// therefore keeps the first transaction's key. Recording the key here rather
// than caching a blank object keeps getStateObject's nil-for-absent contract,
// which the rest of geth relies on.
func (s *StateDB) noteQKCShardKey(addr common.Address) {
	if _, ok := s.qkcShardKeys[addr]; !ok {
		s.qkcShardKeys[addr] = s.fullShardKey
	}
}

// qkcShardKey is the key a new account at addr must carry: the one noted at its
// first lookup, or the current one if it is being created without ever having
// been read.
func (s *StateDB) qkcShardKey(addr common.Address) uint32 {
	if key, ok := s.qkcShardKeys[addr]; ok {
		return key
	}
	return s.fullShardKey
}

// DelAccount is del_account (state.py:596): the account is stripped down to
// nothing, which is what makes the end-of-block sweep drop its leaf. It is how
// a suicide is applied — pyquarkchain runs it over the collected suicide list
// at the end of the transaction (messages.py:351), not at the opcode.
//
// The upstream order is kept: balances first (unjournalled, see ResetBalances),
// then nonce, code and storage.
//
// Note what this deliberately does not do: mark the object self-destructed.
// pyquarkchain's `deleted` flag only forces the account to be *reconsidered* at
// commit; whether it survives is decided by is_blank at that moment
// (state.py:562). An account that self-destructs and is then paid again later
// in the same block is therefore alive at the end of it. geth's
// selfDestructed flag is unconditional — Finalise drops the account whatever
// happened afterwards — so using it here would delete an account QuarkChain
// keeps. Stripping the account instead leaves exactly one rule deciding its
// fate, the emptiness sweep, which is is_blank.
func (s *StateDB) DelAccount(addr common.Address) {
	if s.getStateObject(addr) == nil {
		return
	}
	s.ResetBalances(addr)
	s.SetNonce(addr, 0, tracing.NonceChangeUnspecified)
	s.SetCode(addr, nil, tracing.CodeChangeUnspecified)
	s.ResetStorage(addr)
}

// ResetBalances drops every token balance the account holds.
//
// It deliberately journals nothing, and deliberately does not mark the account
// dirty. pyquarkchain's undo is written onto a misspelled attribute
// (state.py:195 assigns _balance where the field is _balances), so reverting
// restores only the token trie — which this implementation does not have — and
// the balances stay dropped. Reverting "correctly" here computes a different
// state root than the reference clients. Not dirtying the account matches
// reset_balances not setting touched: on its own it leaves the stored leaf
// alone, and it is del_account's other steps that mark the account.
func (s *StateDB) ResetBalances(addr common.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	obj.data.Balance = new(uint256.Int)
	// The recorded entries have to go too, or the account would still serialize
	// an entry for a token it no longer holds.
	obj.data.MntBalances = nil
	obj.data.ClearBalanceUpdates()
}

// ResetStorage is reset_storage (state.py:631): the account's storage trie is
// pointed back at the blank root and its cache emptied, leaving nonce, code and
// balances alone. Like ResetBalances it does not mark the account — only
// del_account's other steps and create_contract's SetNonce do that.
//
// It is reached from del_account and from create_contract's preparation of the
// address it is about to deploy to. In a state QuarkChain itself produced, both
// callers reach it with the storage already empty: del_account marks the
// account deleted, so its storage never survives, and create_contract has just
// established that the target has no nonce and no code.
func (s *StateDB) ResetStorage(addr common.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_, hadDestruct := s.stateObjectsDestruct[addr]
	s.journal.append(qkcResetStorageChange{
		account:     addr,
		root:        obj.data.Root,
		dirty:       maps.Clone(obj.dirtyStorage),
		pending:     maps.Clone(obj.pendingStorage),
		origin:      maps.Clone(obj.originStorage),
		uncommitted: maps.Clone(obj.uncommittedStorage),
		hadDestruct: hadDestruct,
	})
	obj.data.Root = types.EmptyRootHash
	obj.dirtyStorage = make(Storage)
	obj.pendingStorage = make(Storage)
	obj.originStorage = make(Storage)
	obj.uncommittedStorage = make(Storage)
	obj.trie = nil
	// Reads have to stop reaching the pre-reset trie, and the abandoned slots
	// have to be removed at commit. The destruct set is what geth uses for
	// both; entering it here does not by itself remove the account.
	if !hadDestruct {
		s.stateObjectsDestruct[addr] = obj
	}
}

// ===== StateDB MNT methods =====

func (s *StateDB) SetMntBalance(addr common.Address, amount *uint256.Int, tokenID uint64) {
	if tokenID == qkccommon.DefaultTokenID {
		s.setError(fmt.Errorf("account %s: MNT balance written with the QKC token id; use SetBalance", addr.Hex()))
		return
	}
	obj := s.getOrNewStateObject(addr)
	if obj == nil || !obj.canSetMntBalance(amount, tokenID) {
		return
	}
	s.journal.mntBalanceChange(addr, tokenID, obj.GetMntBalance(tokenID))
	obj.setMntBalance(amount, tokenID)
}

func (s *StateDB) AddMntBalance(addr common.Address, amount *uint256.Int, tokenID uint64) {
	if amount.IsZero() {
		return
	}
	updated, overflow := new(uint256.Int).AddOverflow(s.GetMntBalance(addr, tokenID), amount)
	if overflow {
		s.setError(fmt.Errorf("account %s: token %d balance overflows 256 bits", addr.Hex(), tokenID))
		return
	}
	s.SetMntBalance(addr, updated, tokenID)
}

func (s *StateDB) SubMntBalance(addr common.Address, amount *uint256.Int, tokenID uint64) {
	if amount.IsZero() {
		return
	}
	// pyquarkchain has no check here either, but its callers all go through
	// deduct_value, which tests the balance first. Wrapping around instead of
	// reporting would turn a caller bug into a fabricated balance.
	updated, underflow := new(uint256.Int).SubOverflow(s.GetMntBalance(addr, tokenID), amount)
	if underflow {
		s.setError(fmt.Errorf("account %s: token %d balance underflow", addr.Hex(), tokenID))
		return
	}
	s.SetMntBalance(addr, updated, tokenID)
}

func (s *StateDB) GetMntBalance(addr common.Address, tokenID uint64) *uint256.Int {
	obj := s.getStateObject(addr)
	if obj == nil {
		return new(uint256.Int)
	}
	return obj.GetMntBalance(tokenID)
}

// SetError records a failure raised by a caller working on top of this state,
// so that a mistake it cannot report through its own return value still stops
// Commit from producing a root.
func (s *StateDB) SetError(err error) { s.setError(err) }

// FullShardKey is the key accounts created from now on will carry.
func (s *StateDB) FullShardKey() uint32 { return s.fullShardKey }

// GetFullShardKey is the shard key frozen into the account when it was created.
func (s *StateDB) GetFullShardKey(addr common.Address) uint32 {
	if obj := s.getStateObject(addr); obj != nil {
		return obj.data.FullShardKey
	}
	// An address with no account yet would be created with this key, which is
	// what pyquarkchain's blank account reports too.
	return s.qkcShardKey(addr)
}

// GetMntBalances returns the account's non-QKC balances, zero-valued entries
// included: an entry that exists at zero is not the same as a token the account
// never held, and callers reporting state have to be able to tell them apart.
func (s *StateDB) GetMntBalances(addr common.Address) map[uint64]*uint256.Int {
	obj := s.getStateObject(addr)
	if obj == nil || obj.data.MntBalances == nil {
		return make(map[uint64]*uint256.Int)
	}
	return obj.data.MntBalances.GetBalanceMap()
}

// SetBalanceByTokenID writes the QKC or an MNT balance based on tokenID.
func (s *StateDB) SetBalanceByTokenID(addr common.Address, tokenID uint64, amount *uint256.Int, reason tracing.BalanceChangeReason) {
	if tokenID == qkccommon.DefaultTokenID {
		s.SetBalance(addr, amount, reason)
	} else {
		s.SetMntBalance(addr, amount, tokenID)
	}
}

// AddBalanceByTokenID adds to the QKC or an MNT balance based on tokenID. The
// EVM's transfer path is token-agnostic (quarkchain/evm/vm.py passes
// transfer_token_id down every frame), so both ends of a transfer go through the
// by-token entry points rather than deciding at the call site.
func (s *StateDB) AddBalanceByTokenID(addr common.Address, amount *uint256.Int, tokenID uint64, reason tracing.BalanceChangeReason) {
	if tokenID == qkccommon.DefaultTokenID {
		s.AddBalance(addr, amount, reason)
	} else {
		s.AddMntBalance(addr, amount, tokenID)
	}
}

// SubBalanceByTokenID subtracts QKC or MNT balance based on tokenID.
func (s *StateDB) SubBalanceByTokenID(addr common.Address, amount *uint256.Int, tokenID uint64, reason tracing.BalanceChangeReason) {
	if tokenID == qkccommon.DefaultTokenID {
		s.SubBalance(addr, amount, reason)
	} else {
		s.SubMntBalance(addr, amount, tokenID)
	}
}

// GetBalanceByTokenID returns QKC or MNT balance for the given tokenID.
func (s *StateDB) GetBalanceByTokenID(addr common.Address, tokenID uint64) *uint256.Int {
	if tokenID == qkccommon.DefaultTokenID {
		return s.GetBalance(addr)
	}
	return s.GetMntBalance(addr, tokenID)
}
