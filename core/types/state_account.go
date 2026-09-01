// Copyright 2021 The go-ethereum Authors
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

package types

import (
	"bytes"

	"github.com/ethereum/go-ethereum/common"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

// NOTE: StateAccount uses a hand-written QuarkChain codec (EncodeRLP/DecodeRLP in
// state_account_qkc.go) for the 6-element MNT account format. The rlpgen go:generate
// directive was intentionally removed: regenerating gen_account_rlp.go would create a
// conflicting standard 4-field codec and silently drop MntBalances / FullShardKey.

// StateAccount is the Ethereum consensus representation of accounts.
// These objects are stored in the main account trie.
type StateAccount struct {
	Nonce        uint64
	Balance      *uint256.Int
	Root         common.Hash // merkle root of the storage trie
	CodeHash     []byte
	MntBalances  *qkccommon.TokenBalances // Non-QKC balances.
	FullShardKey uint32                   // QuarkChain shard key; set on first tx, preserved thereafter
	// balanceUpdated keeps a changed zero QKC balance encoded as 00c0. It remains
	// set after a revert because pyquarkchain restores the previous value by
	// writing it back, preserving the zero-valued token entry.
	balanceUpdated bool
}

// NewEmptyStateAccount constructs an empty state account.
func NewEmptyStateAccount() *StateAccount {
	return &StateAccount{
		Balance:  new(uint256.Int),
		Root:     EmptyRootHash,
		CodeHash: EmptyCodeHash.Bytes(),
	}
}

// Copy returns a deep-copied state account object.
func (acct *StateAccount) Copy() *StateAccount {
	var balance *uint256.Int
	if acct.Balance != nil {
		balance = new(uint256.Int).Set(acct.Balance)
	}
	var mnt *qkccommon.TokenBalances
	if acct.MntBalances != nil {
		mnt = acct.MntBalances.Copy()
	}
	return &StateAccount{
		Nonce:          acct.Nonce,
		Balance:        balance,
		Root:           acct.Root,
		CodeHash:       common.CopyBytes(acct.CodeHash),
		MntBalances:    mnt,
		FullShardKey:   acct.FullShardKey,
		balanceUpdated: acct.balanceUpdated,
	}
}

// IsBalanceUpdated reports whether the QKC balance has been explicitly updated.
func (acct *StateAccount) IsBalanceUpdated() bool {
	return acct.balanceUpdated
}

// MarkBalanceUpdated records that the QKC balance entry has been written.
func (acct *StateAccount) MarkBalanceUpdated() {
	acct.balanceUpdated = true
}

// SlimAccount is the compact RLP account format used by state snapshots,
// pathdb readers, and account iterators. To support snapshots in goshard, the
// standard format must be extended with FullShardKey and MntBal so snapshots
// preserve QuarkChain-specific account state. The added fields are optional
// trailing RLP fields, keeping old snapshots readable and leaving room for
// future extensions without changing the existing account format.
type SlimAccount struct {
	Nonce    uint64
	Balance  *uint256.Int
	Root     []byte // Nil if root equals to types.EmptyRootHash
	CodeHash []byte // Nil if hash equals to types.EmptyCodeHash
	// QKC-specific fields; both optional so old snapshots remain readable.
	FullShardKey uint32 `rlp:"optional"` // QuarkChain shard key
	MntBal       []byte `rlp:"optional"` // Non-QKC TokenBalances.SerializeToBytes output
}

// SlimAccountRLP encodes the state account in 'slim RLP' format.
func SlimAccountRLP(account StateAccount) []byte {
	slim := SlimAccount{
		Nonce:        account.Nonce,
		Balance:      account.Balance,
		FullShardKey: account.FullShardKey,
	}
	if account.Root != EmptyRootHash {
		slim.Root = account.Root[:]
	}
	if !bytes.Equal(account.CodeHash, EmptyCodeHash[:]) {
		slim.CodeHash = account.CodeHash
	}
	if account.MntBalances != nil {
		mntBal, err := account.MntBalances.SerializeToBytes()
		if err != nil {
			panic(err)
		}
		slim.MntBal = mntBal
	}
	if len(slim.MntBal) == 0 && account.IsBalanceUpdated() {
		slim.MntBal = []byte{0x00, 0xc0}
	}
	data, err := rlp.EncodeToBytes(slim)
	if err != nil {
		panic(err)
	}
	return data
}

// FullAccount decodes snapshot data from slim RLP into a StateAccount.
//
// This conversion intentionally follows the semantics of StateAccount's
// EncodeRLP and DecodeRLP methods instead of preserving the original account
// bytes. An explicitly updated zero balance can initially encode as 00c0, but
// decoding loses the zero entry because the serialized token list is empty.
// Re-encoding the decoded account therefore produces an empty TokenBal. Doing
// the same normalization here ensures that snapshot and trie reads return the
// same StateAccount.
//
// Callers reconstructing trie leaves must use FullAccountRLP, which preserves
// the explicit zero-balance update marker. This distinction does not imply general
// byte-preserving snap sync support.
func FullAccount(data []byte) (*StateAccount, error) {
	return fullAccount(data, false)
}

func fullAccount(data []byte, restoreBalanceUpdated bool) (*StateAccount, error) {
	var slim SlimAccount
	if err := rlp.DecodeBytes(data, &slim); err != nil {
		return nil, err
	}
	var account StateAccount
	account.Nonce, account.Balance, account.FullShardKey = slim.Nonce, slim.Balance, slim.FullShardKey
	if len(slim.MntBal) > 0 {
		tb, err := qkccommon.NewTokenBalances(slim.MntBal)
		if err != nil {
			return nil, err
		}
		if tb.Len() != 0 {
			account.MntBalances = tb
		} else if restoreBalanceUpdated {
			account.MarkBalanceUpdated()
		}
	}
	if len(slim.Root) == 0 {
		account.Root = EmptyRootHash
	} else {
		account.Root = common.BytesToHash(slim.Root)
	}
	if len(slim.CodeHash) == 0 {
		account.CodeHash = EmptyCodeHash[:]
	} else {
		account.CodeHash = slim.CodeHash
	}
	return &account, nil
}

// FullAccountRLP converts slim RLP into full RLP while preserving an explicit
// zero-balance update marker for snapshot proof verification and trie regeneration.
func FullAccountRLP(data []byte) ([]byte, error) {
	account, err := fullAccount(data, true)
	if err != nil {
		return nil, err
	}
	return rlp.EncodeToBytes(account)
}
