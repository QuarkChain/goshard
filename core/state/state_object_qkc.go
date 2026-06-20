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
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/holiman/uint256"
)

const defaultTokenID = uint64(35760) // TokenIDEncode("QKC")

func (s *stateObject) SetMntBalance(amount *uint256.Int, tokenID uint64) {
	if tokenID == defaultTokenID {
		log.Error("SetMntBalance called with QKC tokenID; use SetBalance", "addr", s.address)
		return
	}
	if s.data.MntBalances == nil {
		s.data.MntBalances = types.NewEmptyTokenBalances()
	}
	s.data.MntBalances.SetValue(amount, tokenID)
}

func (s *stateObject) AddMntBalance(amount *uint256.Int, tokenID uint64) {
	if amount.IsZero() {
		return
	}
	if tokenID == defaultTokenID {
		log.Error("AddMntBalance called with QKC tokenID; use AddBalance", "addr", s.address)
		return
	}
	cur := s.GetMntBalance(tokenID)
	s.SetMntBalance(new(uint256.Int).Add(cur, amount), tokenID)
}

func (s *stateObject) SubMntBalance(amount *uint256.Int, tokenID uint64) {
	if amount.IsZero() {
		return
	}
	if tokenID == defaultTokenID {
		log.Error("SubMntBalance called with QKC tokenID; use SubBalance", "addr", s.address)
		return
	}
	cur := s.GetMntBalance(tokenID)
	s.SetMntBalance(new(uint256.Int).Sub(cur, amount), tokenID)
}

func (s *stateObject) GetMntBalance(tokenID uint64) *uint256.Int {
	if s.data.MntBalances == nil {
		return new(uint256.Int)
	}
	return s.data.MntBalances.GetTokenBalance(tokenID)
}
