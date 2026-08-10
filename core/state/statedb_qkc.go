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
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/holiman/uint256"
)

// ===== StateDB MNT methods =====

func (s *StateDB) SetMntBalance(addr common.Address, amount *uint256.Int, tokenID uint64) {
	if tokenID == qkccommon.DefaultTokenID {
		return
	}
	obj := s.getOrNewStateObject(addr)
	if obj == nil || !obj.canSetMntBalance(amount, tokenID) {
		return
	}
	s.journal.mntBalanceChange(addr, obj.data.MntBalances)
	obj.setMntBalance(amount, tokenID)
}

func (s *StateDB) AddMntBalance(addr common.Address, amount *uint256.Int, tokenID uint64) {
	if amount.IsZero() {
		return
	}
	updated, overflow := new(uint256.Int).AddOverflow(s.GetMntBalance(addr, tokenID), amount)
	if overflow {
		return
	}
	s.SetMntBalance(addr, updated, tokenID)
}

func (s *StateDB) SubMntBalance(addr common.Address, amount *uint256.Int, tokenID uint64) {
	if amount.IsZero() {
		return
	}
	updated, underflow := new(uint256.Int).SubOverflow(s.GetMntBalance(addr, tokenID), amount)
	if underflow {
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
