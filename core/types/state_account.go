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
	// balanceUpdateCount keeps a changed zero QKC balance encoded as 00c0. Using
	// a counter ensures that reverting one update does not clear earlier updates.
	balanceUpdateCount uint64
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
		Nonce:              acct.Nonce,
		Balance:            balance,
		Root:               acct.Root,
		CodeHash:           common.CopyBytes(acct.CodeHash),
		MntBalances:        mnt,
		FullShardKey:       acct.FullShardKey,
		balanceUpdateCount: acct.balanceUpdateCount,
	}
}

// IsBalanceUpdated reports whether the QKC balance has been explicitly updated.
//
// The recorded update outlives a revert on purpose. quarkchain/evm/state.py
// undoes a balance write with `_balances[token_id] = preval` (state.py:166),
// which writes the key back rather than deleting it, so an account whose only
// credit is reverted holds zero rather than holding nothing. There is
// deliberately no counterpart that takes an update back.
func (acct *StateAccount) IsBalanceUpdated() bool {
	return acct.balanceUpdateCount > 0
}

// AddBalanceUpdate records a QKC balance update.
func (acct *StateAccount) AddBalanceUpdate() {
	acct.balanceUpdateCount++
}

// FinaliseBalanceUpdates keeps the recorded presence of an entry without
// retaining a count that only grows over the life of a block.
func (acct *StateAccount) FinaliseBalanceUpdates() {
	if acct.balanceUpdateCount > 0 {
		acct.balanceUpdateCount = 1
	}
}

// SlimAccount is a modified version of an Account, where the root is replaced
// with a byte slice. This format can be used to represent full-consensus format
// or slim format which replaces the empty root and code hash as nil byte slice.
// FullShardKey and MntBal are rlp:"optional" so accounts without QKC-specific
// fields still decode cleanly from older snapshots.
//
// MntBal holds TokenBalances.SerializeToBytes() output rather than the
// *TokenBalances value directly: TokenBalances stores its balances in an
// unexported map, so it is not RLP-struct-encodable and must go through the
// same []byte serialization the trie account uses, and carries exactly what a
// trie leaf would.
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
	// Only what a trie leaf would carry. A recorded update over a zero balance
	// is deliberately dropped: it is a fact about the block in progress, and the
	// trie cannot express it either (see StateAccount.DecodeRLP). Keeping it on
	// this path alone would let a snapshot read and a trie read of the same
	// account hand back different balances.
	if account.MntBalances != nil {
		mntBal, err := account.MntBalances.SerializeToBytes()
		if err != nil {
			panic(err)
		}
		slim.MntBal = mntBal
	}
	data, err := rlp.EncodeToBytes(slim)
	if err != nil {
		panic(err)
	}
	return data
}

// FullAccount decodes the data on the 'slim RLP' format and returns
// the consensus format account.
func FullAccount(data []byte) (*StateAccount, error) {
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
		// An all-zero pair list decodes to an empty map, which is the same
		// nothing DecodeRLP reads out of a 00c0 leaf.
		if tb.Len() != 0 {
			account.MntBalances = tb
		}
	}

	// Interpret the storage root and code hash in slim format.
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

// FullAccountRLP converts data on the 'slim RLP' format into the full RLP-format.
func FullAccountRLP(data []byte) ([]byte, error) {
	account, err := FullAccount(data)
	if err != nil {
		return nil, err
	}
	return rlp.EncodeToBytes(account)
}
