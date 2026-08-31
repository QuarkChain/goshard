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

// qkcAccountRLP matches pyquarkchain's six-field _Account encoding.
type qkcAccountRLP struct {
	Nonce        uint64
	TokenBal     []byte
	Root         common.Hash
	CodeHash     []byte
	FullShardKey qkccommon.Uint32
	Optional     []byte
}

// ToStateAccount expands a slim snapshot account into the consensus account
// representation without dropping token balances or the full shard key.
func (acct *SlimAccount) ToStateAccount() (*StateAccount, error) {
	balances, err := qkccommon.NewTokenBalances(acct.MntBal)
	if err != nil {
		return nil, err
	}
	full := &StateAccount{
		Nonce:        acct.Nonce,
		MntBalances:  balances,
		FullShardKey: uint32(acct.FullShardKey),
	}
	if len(acct.Root) == 0 {
		full.Root = EmptyRootHash
	} else {
		full.Root = common.BytesToHash(acct.Root)
	}
	if len(acct.CodeHash) == 0 {
		full.CodeHash = EmptyCodeHash.Bytes()
	} else {
		full.CodeHash = common.CopyBytes(acct.CodeHash)
	}
	return full, nil
}

// GetMntBalance returns the balance of tokenID in the account's unified balance map.
func (acct *StateAccount) GetMntBalance(tokenID uint64) *uint256.Int {
	if acct.MntBalances == nil {
		return new(uint256.Int)
	}
	return acct.MntBalances.GetTokenBalance(tokenID)
}

// Balance returns the account's QKC balance.
func (acct *StateAccount) Balance() *uint256.Int {
	return acct.GetMntBalance(qkccommon.DefaultTokenID)
}

// SetBalance sets the account's QKC balance.
func (acct *StateAccount) SetBalance(balance *uint256.Int) {
	if acct.MntBalances == nil {
		acct.MntBalances = qkccommon.NewEmptyTokenBalances()
	}
	acct.MntBalances.SetValue(balance, qkccommon.DefaultTokenID)
}

// NewQKCTokenBalances creates an account balance map with a QKC balance.
func NewQKCTokenBalances(balance *uint256.Int) *qkccommon.TokenBalances {
	balances := qkccommon.NewEmptyTokenBalances()
	if balance != nil {
		balances.SetValue(balance, qkccommon.DefaultTokenID)
	}
	return balances
}

// EncodeRLP implements pyquarkchain's account encoding.
func (acct *StateAccount) EncodeRLP(w io.Writer) error {
	balances := acct.MntBalances
	if balances == nil {
		balances = qkccommon.NewEmptyTokenBalances()
	}
	tokenBal, err := balances.SerializeToBytes()
	if err != nil {
		return err
	}
	return rlp.Encode(w, &qkcAccountRLP{
		Nonce:        acct.Nonce,
		TokenBal:     tokenBal,
		Root:         acct.Root,
		CodeHash:     acct.CodeHash,
		FullShardKey: qkccommon.Uint32(acct.FullShardKey),
	})
}

// DecodeRLP implements pyquarkchain's account decoding.
func (acct *StateAccount) DecodeRLP(s *rlp.Stream) error {
	raw, err := s.Raw()
	if err != nil {
		return err
	}
	var wire qkcAccountRLP
	if err := rlp.DecodeBytes(raw, &wire); err != nil {
		return err
	}
	if len(wire.Optional) != 0 {
		return errors.New("unsupported non-empty QuarkChain account optional field")
	}
	acct.Nonce = wire.Nonce
	acct.Root = wire.Root
	acct.CodeHash = wire.CodeHash
	acct.FullShardKey = uint32(wire.FullShardKey)
	balances, err := qkccommon.NewTokenBalances(wire.TokenBal)
	if err != nil {
		return err
	}
	acct.MntBalances = balances
	return nil
}
