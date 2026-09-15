// Copyright 2026-2027, QuarkChain.

package state

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/holiman/uint256"
)

// qkcCachedAccount stores the parts of pyquarkchain's cached blank account
// that can affect persisted account encoding. The full shard key is retained
// until block commit, while balances are transferred to a recreated state
// object and then cleared.
//
// Map presence records whether the account has been observed because zero is
// a valid full shard key.
type qkcCachedAccount struct {
	fullShardKey uint32

	// balances contains explicit zero entries left by reverted balance writes.
	// A nil value means that only the first-observed full shard key is cached.
	balances *qkccommon.TokenBalances
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
	s.fullShardKey = fullShardKey
}

func (s *StateDB) noteQKCShardKey(addr common.Address) {
	if _, ok := s.qkcAccountCache[addr]; !ok {
		s.qkcAccountCache[addr] = qkcCachedAccount{fullShardKey: s.fullShardKey}
	}
}

func (s *StateDB) qkcShardKey(addr common.Address) uint32 {
	if cached, ok := s.qkcAccountCache[addr]; ok {
		return cached.fullShardKey
	}
	return s.fullShardKey
}

func (s *StateDB) cacheQKCBlankBalances(obj *stateObject) {
	if balances := obj.data.MntBalances; balances != nil && balances.Len() != 0 && balances.IsBlank() {
		cached, ok := s.qkcAccountCache[obj.address]
		if !ok {
			cached.fullShardKey = obj.data.FullShardKey
		}
		cached.balances = balances
		s.qkcAccountCache[obj.address] = cached
	}
}

// takeQKCBlankBalances transfers reverted zero-balance entries to a recreated
// state object. It keeps the cache entry so the first-observed full shard key
// remains valid until block commit.
func (s *StateDB) takeQKCBlankBalances(addr common.Address) *qkccommon.TokenBalances {
	cached, ok := s.qkcAccountCache[addr]
	if !ok || cached.balances == nil {
		return nil
	}
	balances := cached.balances
	cached.balances = nil
	s.qkcAccountCache[addr] = cached
	return balances
}
