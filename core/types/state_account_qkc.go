// Copyright 2026-2027, QuarkChain.

package types

import (
	"errors"
	"io"

	"github.com/ethereum/go-ethereum/common"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

// qkcAccountRLP is the wire struct for QuarkChain's 6-element account format:
// [Nonce, TokenBal(bytes), Root, CodeHash, FullShardKey(4B fixed), Optional].
// TokenBal is stored as raw serialized bytes (matching pyquarkchain's `binary` type),
// so nil encodes as 0x80 (empty string), not 0xC0 (empty list).
type qkcAccountRLP struct {
	Nonce        uint64
	TokenBal     []byte // SerializeToBytes output; nil = no balances
	Root         common.Hash
	CodeHash     []byte
	FullShardKey qkccommon.Uint32
	Optional     []byte
}

// mergeQKCTokenBalances combines the QKC native balance (tokenID=35760) and MNT
// balances into a single TokenBalances for wire encoding. Returns nil if both empty,
// which causes EncodeRLP to write 0x80 (RLP nil/empty) matching goquarkchain behavior.
func mergeQKCTokenBalances(balance *uint256.Int, mnt *qkccommon.TokenBalances) *qkccommon.TokenBalances {
	if (balance == nil || balance.IsZero()) && (mnt == nil || mnt.Len() == 0) {
		return nil
	}
	merged := qkccommon.NewEmptyTokenBalances()
	if mnt != nil {
		for id, bal := range mnt.GetBalanceMap() {
			merged.SetValue(bal, id)
		}
	}
	if balance != nil && !balance.IsZero() {
		merged.SetValue(balance, qkccommon.DefaultTokenID)
	}
	return merged
}

// EncodeRLP implements rlp.Encoder for StateAccount using QuarkChain's
// 6-element format. Root is always written as 32 bytes (no nil optimization).
func (acct *StateAccount) EncodeRLP(w io.Writer) error {
	var tokenBal []byte
	if balances := mergeQKCTokenBalances(acct.Balance, acct.MntBalances); balances != nil {
		var err error
		tokenBal, err = balances.SerializeToBytes()
		if err != nil {
			return err
		}
	}
	qkc := &qkcAccountRLP{
		Nonce:        acct.Nonce,
		Root:         acct.Root,
		CodeHash:     acct.CodeHash,
		TokenBal:     tokenBal,
		FullShardKey: qkccommon.Uint32(acct.FullShardKey),
		Optional:     nil,
	}
	return rlp.Encode(w, qkc)
}

// DecodeRLP implements rlp.Decoder for StateAccount using QuarkChain's
// 6-element format.
func (acct *StateAccount) DecodeRLP(s *rlp.Stream) error {
	raw, err := s.Raw()
	if err != nil {
		return err
	}
	var qkc qkcAccountRLP
	if err := rlp.DecodeBytes(raw, &qkc); err != nil {
		return err
	}
	acct.Nonce = qkc.Nonce
	acct.CodeHash = qkc.CodeHash
	acct.Root = qkc.Root
	acct.FullShardKey = uint32(qkc.FullShardKey)
	if len(qkc.Optional) != 0 {
		return errors.New("unsupported non-empty QuarkChain account optional field")
	}
	acct.Balance = new(uint256.Int)
	if len(qkc.TokenBal) > 0 {
		tb, err := qkccommon.NewTokenBalances(qkc.TokenBal)
		if err != nil {
			return err
		}
		balMap := tb.GetBalanceMap()
		if qkcBal, ok := balMap[qkccommon.DefaultTokenID]; ok {
			acct.Balance.Set(qkcBal)
			delete(balMap, qkccommon.DefaultTokenID)
		}
		// Keep the decoded non-QKC balances. Empty lists are canonicalized to
		// empty bytes when re-encoded, matching pyquarkchain.
		acct.MntBalances = qkccommon.NewTokenBalancesWithMap(balMap)
	}
	return nil
}
