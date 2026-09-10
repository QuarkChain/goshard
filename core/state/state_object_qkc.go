// Copyright 2026-2027, QuarkChain.

package state

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/holiman/uint256"
)

func (s *stateObject) SetMntBalance(amount *uint256.Int, tokenID uint64) {
	if tokenID == qkccommon.DefaultTokenID {
		s.db.setError(fmt.Errorf("account %s: MNT balance written with the QKC token ID; use SetBalance", s.address.Hex()))
		return
	}
	s.setBalance(amount, tokenID)
}

func (s *stateObject) setBalance(amount *uint256.Int, tokenID uint64) uint256.Int {
	if amount == nil {
		amount = new(uint256.Int)
	}
	prev := s.GetMntBalance(tokenID)
	if prev.Eq(amount) {
		s.touch()
		return *prev
	}
	s.db.journal.balanceChange(s.address, tokenID, prev)
	s.setTokenBalance(amount, tokenID)
	return *prev
}

// SetState updates a value in account storage and returns the previous value.
//
// Writing the value a slot already holds still marks the account, as
// set_storage_data does (state.py:483-488): it journals and sets touched
// without comparing against the old value. The mark is what decides whether the
// leaf is rewritten at commit, so dropping it on an equal-value write leaves a
// storage reset — which deliberately marks nothing — invisible to the block,
// and the pre-reset slots survive in the trie.
func (s *stateObject) SetState(key, value common.Hash) common.Hash {
	prev, origin := s.getState(key)
	if prev == value {
		s.touch()
		return prev
	}
	s.db.journal.storageChange(s.address, key, prev, origin)
	s.setState(key, value, origin)
	return prev
}

func (s *stateObject) setTokenBalance(amount *uint256.Int, tokenID uint64) {
	if s.data.MntBalances == nil {
		s.data.MntBalances = qkccommon.NewEmptyTokenBalances()
	}
	s.data.MntBalances.SetValue(amount, tokenID)
}

func (s *stateObject) GetMntBalance(tokenID uint64) *uint256.Int {
	return s.data.GetMntBalance(tokenID)
}

func (s *stateObject) IsBlankMnt() bool {
	return s.data.MntBalances == nil || s.data.MntBalances.IsBlank()
}
