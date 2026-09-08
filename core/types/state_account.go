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

// StateAccount is the QuarkChain consensus representation of accounts.
// It uses the codec in state_account_qkc.go because the standard four-field
// Ethereum codec cannot encode its token and shard fields.
type StateAccount struct {
	Nonce        uint64
	MntBalances  *qkccommon.TokenBalances // QKC and MNT balances.
	Root         common.Hash              // merkle root of the storage trie
	CodeHash     []byte
	FullShardKey uint32
}

// NewEmptyStateAccount constructs an empty state account.
func NewEmptyStateAccount() *StateAccount {
	return &StateAccount{
		MntBalances: qkccommon.NewEmptyTokenBalances(),
		Root:        EmptyRootHash,
		CodeHash:    EmptyCodeHash.Bytes(),
	}
}

// Copy returns a deep-copied state account object.
func (acct *StateAccount) Copy() *StateAccount {
	var mntBalances *qkccommon.TokenBalances
	if acct.MntBalances != nil {
		mntBalances = acct.MntBalances.Copy()
	}
	return &StateAccount{
		Nonce:        acct.Nonce,
		MntBalances:  mntBalances,
		Root:         acct.Root,
		CodeHash:     common.CopyBytes(acct.CodeHash),
		FullShardKey: acct.FullShardKey,
	}
}

// SlimAccount is retained unchanged for the inherited snapshot and pathdb code.
// Goshard currently supports neither snap sync nor the snapshot database flat
// reader, so this representation intentionally carries only the QKC balance.
// Converting between SlimAccount and StateAccount therefore loses MNT balances
// and FullShardKey. This is a known issue which is ignored until those modes are
// supported; they must not be enabled before the conversion becomes lossless.
type SlimAccount struct {
	Nonce    uint64
	Balance  *uint256.Int
	Root     []byte // Nil if root equals to types.EmptyRootHash
	CodeHash []byte // Nil if hash equals to types.EmptyCodeHash
}

// SlimAccountRLP encodes the state account in 'slim RLP' format.
func SlimAccountRLP(account StateAccount) []byte {
	slim := SlimAccount{
		Nonce:   account.Nonce,
		Balance: account.GetBalance(),
	}
	if account.Root != EmptyRootHash {
		slim.Root = account.Root[:]
	}
	if !bytes.Equal(account.CodeHash, EmptyCodeHash[:]) {
		slim.CodeHash = account.CodeHash
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
	account := StateAccount{
		Nonce:       slim.Nonce,
		MntBalances: NewQKCTokenBalances(slim.Balance),
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
