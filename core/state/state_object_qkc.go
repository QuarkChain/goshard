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
	"fmt"

	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/holiman/uint256"
)

// SetMntBalance writes one non-QKC token balance. A rejected write is recorded
// on the StateDB rather than logged and dropped: a balance that silently fails
// to move is a state root that silently differs from the reference clients, so
// the error has to reach Commit and stop it producing a root.
func (s *stateObject) SetMntBalance(amount *uint256.Int, tokenID uint64) {
	if !s.canSetMntBalance(amount, tokenID) {
		return
	}
	s.setMntBalance(amount, tokenID)
}

func (s *stateObject) canSetMntBalance(amount *uint256.Int, tokenID uint64) bool {
	if tokenID == qkccommon.DefaultTokenID {
		s.db.setError(fmt.Errorf("account %s: MNT balance written with the QKC token id; use SetBalance", s.address.Hex()))
		return false
	}
	// The limit is on entries that survive serialization, which drops zeros
	// (qkc/common.TokenBalances.SerializeToBytes), so zero-valued entries left
	// behind by drained tokens must not count towards it. QKC shares the same
	// list and counts when it is non-zero.
	if amount != nil && !amount.IsZero() && s.GetMntBalance(tokenID).IsZero() {
		cnt := s.nonZeroMntBalanceCount()
		if !s.Balance().IsZero() {
			cnt++
		}
		if cnt >= qkccommon.TokenTrieThreshold {
			s.db.setError(fmt.Errorf("%w: account %s: token %d exceeds the %d token limit",
				ErrTokenTrieUnsupported, s.address.Hex(), tokenID, qkccommon.TokenTrieThreshold))
			return false
		}
	}
	return true
}

func (s *stateObject) nonZeroMntBalanceCount() int {
	if s.data.MntBalances == nil {
		return 0
	}
	return s.data.MntBalances.NonZeroLen()
}

// setMntBalance is the unguarded write behind SetMntBalance, shared with the
// journal's undo. SetValue writes an entry even for a zero amount, which is what
// keeps a drained token's blob at 0x00c0 instead of empty bytes.
func (s *stateObject) setMntBalance(amount *uint256.Int, tokenID uint64) {
	if s.data.MntBalances == nil {
		s.data.MntBalances = qkccommon.NewEmptyTokenBalances()
	}
	s.data.MntBalances.SetValue(amount, tokenID)
}

// There is no stateObject-level Add/SubMntBalance: the arithmetic lives on the
// StateDB (statedb_qkc.go), which journals the previous balance before writing.
// A second pair here would be a write that skips the journal.

func (s *stateObject) GetMntBalance(tokenID uint64) *uint256.Int {
	if s.data.MntBalances == nil {
		return new(uint256.Int)
	}
	return s.data.MntBalances.GetTokenBalance(tokenID)
}

// IsBlankMnt reports whether the account holds no non-QKC (MNT) token balances.
// It is consumed by empty() so that the EIP-158 empty-account check spans every
// token, matching pyquarkchain's _Account.is_blank (which evaluates
// token_balances.is_blank() across all tokens). Without this, an account with
// nonce==0 / QKC==0 / MNT!=0 / no code would be pruned here but kept by
// pyquarkchain, producing a divergent state root.
func (s *stateObject) IsBlankMnt() bool {
	return s.data.MntBalances == nil || s.data.MntBalances.IsBlank()
}
