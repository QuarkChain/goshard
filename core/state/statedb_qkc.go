// Copyright 2026-2027, QuarkChain.

package state

import (
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
// pyquarkchain's `touched`, and empty() is is_blank.

// DelAccount is del_account (state.py:596): the account is stripped down to
// nothing, which is what makes the end-of-block sweep drop its leaf. It is how
// a suicide is applied — pyquarkchain runs it over the collected suicide list
// at the end of the transaction (messages.py:351), not at the opcode.
//
// Note what this deliberately does not do: mark the object self-destructed.
// pyquarkchain's `deleted` flag only forces the account to be *reconsidered* at
// commit; whether it survives is decided by is_blank at that moment
// (state.py:562). An account that self-destructs and is then paid again later in
// the same block is therefore alive at the end of it. geth's selfDestructed flag
// is unconditional — Finalise drops the account whatever happened afterwards —
// so using it here would delete an account QuarkChain keeps. Stripping the
// account instead leaves exactly one rule deciding its fate, the emptiness
// sweep, which is is_blank.
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
	obj.data.MntBalances = nil
}

// ResetStorage is reset_storage (state.py:631): the account's storage trie is
// pointed back at the blank root and its cache emptied, leaving nonce, code and
// balances alone. Like ResetBalances it does not mark the account — only
// del_account's other steps and create_contract's SetNonce do that.
//
// It is reached from del_account and from create_contract's preparation of the
// address it is about to deploy to. In a state QuarkChain itself produced, both
// callers reach it with the storage already empty: del_account marks the account
// deleted, so its storage never survives, and create_contract has just
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
	// have to be removed at commit. The destruct set is what geth uses for both;
	// entering it here does not by itself remove the account.
	if !hadDestruct {
		s.stateObjectsDestruct[addr] = obj
	}
}

// Balance updates support two equivalent call styles. Callers that already
// distinguish QKC from MNT can use Set/Add/SubBalance for QKC and the matching
// MntBalance method for MNT. Callers holding an arbitrary token ID can instead
// use the matching BalanceByTokenID method, which dispatches between them.

func (s *StateDB) SetMntBalance(addr common.Address, amount *uint256.Int, tokenID uint64) {
	if tokenID == qkccommon.DefaultTokenID {
		s.setError(fmt.Errorf("account %s: MNT balance written with the QKC token ID; use SetBalance", addr.Hex()))
		return
	}
	obj := s.getOrNewStateObject(addr)
	if obj != nil {
		obj.SetMntBalance(amount, tokenID)
	}
}

func (s *StateDB) AddMntBalance(addr common.Address, amount *uint256.Int, tokenID uint64) {
	if tokenID == qkccommon.DefaultTokenID {
		s.setError(fmt.Errorf("account %s: MNT balance written with the QKC token ID; use AddBalance", addr.Hex()))
		return
	}
	if amount.IsZero() {
		obj := s.getOrNewStateObject(addr)
		if obj != nil {
			obj.touch()
		}
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
	if tokenID == qkccommon.DefaultTokenID {
		s.setError(fmt.Errorf("account %s: MNT balance written with the QKC token ID; use SubBalance", addr.Hex()))
		return
	}
	if amount.IsZero() {
		obj := s.getOrNewStateObject(addr)
		if obj != nil {
			obj.touch()
		}
		return
	}
	updated, underflow := new(uint256.Int).SubOverflow(s.GetMntBalance(addr, tokenID), amount)
	if underflow {
		s.setError(fmt.Errorf("account %s: token %d balance underflows zero", addr.Hex(), tokenID))
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

func (s *StateDB) SetBalanceByTokenID(addr common.Address, amount *uint256.Int, tokenID uint64, reason tracing.BalanceChangeReason) {
	if tokenID == qkccommon.DefaultTokenID {
		s.SetBalance(addr, amount, reason)
		return
	}
	s.SetMntBalance(addr, amount, tokenID)
}

func (s *StateDB) AddBalanceByTokenID(addr common.Address, amount *uint256.Int, tokenID uint64, reason tracing.BalanceChangeReason) {
	if tokenID == qkccommon.DefaultTokenID {
		s.AddBalance(addr, amount, reason)
		return
	}
	s.AddMntBalance(addr, amount, tokenID)
}

func (s *StateDB) SubBalanceByTokenID(addr common.Address, amount *uint256.Int, tokenID uint64, reason tracing.BalanceChangeReason) {
	if tokenID == qkccommon.DefaultTokenID {
		s.SubBalance(addr, amount, reason)
		return
	}
	s.SubMntBalance(addr, amount, tokenID)
}

func (s *StateDB) GetBalanceByTokenID(addr common.Address, tokenID uint64) *uint256.Int {
	if tokenID == qkccommon.DefaultTokenID {
		return s.GetBalance(addr)
	}
	return s.GetMntBalance(addr, tokenID)
}

// SetFullShardKey sets the shard key inherited by accounts first observed while
// processing the current QKC message. The QKC message executor must call this
// with Message.ToFullShardKey before any account access. Transaction and
// cross-shard deposit execution use the same boundary in pyquarkchain and
// goquarkchain.
func (s *StateDB) SetFullShardKey(fullShardKey uint32) {
	s.journal.append(qkcFullShardKeyChange{prev: s.fullShardKey})
	s.fullShardKey = fullShardKey
}

// FullShardKey is the key set by the last SetFullShardKey.
func (s *StateDB) FullShardKey() uint32 { return s.fullShardKey }

func (s *StateDB) noteQKCShardKey(addr common.Address) {
	if _, ok := s.qkcShardKeys[addr]; !ok {
		s.qkcShardKeys[addr] = s.fullShardKey
	}
}

func (s *StateDB) qkcShardKey(addr common.Address) uint32 {
	if fullShardKey, ok := s.qkcShardKeys[addr]; ok {
		return fullShardKey
	}
	return s.fullShardKey
}

// GetFullShardKey is the shard key frozen into the account when it was created.
func (s *StateDB) GetFullShardKey(addr common.Address) uint32 {
	if obj := s.getStateObject(addr); obj != nil {
		return obj.data.FullShardKey
	}
	// An address with no account yet would be created with this key, which is
	// what pyquarkchain's blank account reports too.
	return s.qkcShardKey(addr)
}

// GetTokenBalances returns every balance the account holds, zero-valued entries
// included: an entry that exists at zero is not the same as a token the account
// never held, and callers reporting state have to be able to tell them apart.
func (s *StateDB) GetTokenBalances(addr common.Address) map[uint64]*uint256.Int {
	obj := s.getStateObject(addr)
	if obj == nil || obj.data.MntBalances == nil {
		return make(map[uint64]*uint256.Int)
	}
	return obj.data.MntBalances.GetBalanceMap()
}

// SetError records a failure raised by a caller working on top of this state, so
// that a mistake it cannot report through its own return value still stops
// Commit from producing a root.
func (s *StateDB) SetError(err error) { s.setError(err) }
