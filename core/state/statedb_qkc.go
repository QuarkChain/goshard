// Copyright 2024 The go-ethereum Authors
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
	obj := s.getOrNewStateObject(addr)
	if obj == nil {
		return
	}
	s.journal.mntBalanceChange(addr, obj.data.MntBalances)
	obj.SetMntBalance(amount, tokenID)
}

func (s *StateDB) AddMntBalance(addr common.Address, amount *uint256.Int, tokenID uint64) {
	if amount.IsZero() {
		return
	}
	obj := s.getOrNewStateObject(addr)
	if obj == nil {
		return
	}
	s.journal.mntBalanceChange(addr, obj.data.MntBalances)
	obj.AddMntBalance(amount, tokenID)
}

func (s *StateDB) SubMntBalance(addr common.Address, amount *uint256.Int, tokenID uint64) {
	if amount.IsZero() {
		return
	}
	obj := s.getOrNewStateObject(addr)
	if obj == nil {
		return
	}
	s.journal.mntBalanceChange(addr, obj.data.MntBalances)
	obj.SubMntBalance(amount, tokenID)
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
