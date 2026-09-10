// Copyright 2026-2027, QuarkChain.

package state

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/holiman/uint256"
)

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
