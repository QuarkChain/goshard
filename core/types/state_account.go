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

// SlimAccount is retained for the inherited snapshot and pathdb code. QuarkChain
// nodes support only hashdb-backed state and do not enable those modes. The QKC
// fields remain lossless here so shared conversion code does not discard them.
// Empty roots and code hashes are represented by nil slices.
type SlimAccount struct {
	Nonce        uint64
	MntBal       []byte
	FullShardKey qkccommon.Uint32
	Root         []byte // Nil if root equals to types.EmptyRootHash
	CodeHash     []byte // Nil if hash equals to types.EmptyCodeHash
}

// SlimAccountRLP encodes the state account in 'slim RLP' format.
func SlimAccountRLP(account StateAccount) []byte {
	balances := account.MntBalances
	if balances == nil {
		balances = qkccommon.NewEmptyTokenBalances()
	}
	mntBal, err := balances.SerializeToBytes()
	if err != nil {
		panic(err)
	}
	slim := SlimAccount{
		Nonce:        account.Nonce,
		MntBal:       mntBal,
		FullShardKey: qkccommon.Uint32(account.FullShardKey),
	}
	if account.Root != EmptyRootHash {
		slim.Root = account.Root[:]
	}
	if !bytes.Equal(account.CodeHash, EmptyCodeHash[:]) {
		slim.CodeHash = account.CodeHash
	}
	data, err := rlp.EncodeToBytes(&slim)
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
	return slim.ToStateAccount()
}

// FullAccountRLP converts data on the 'slim RLP' format into the full RLP-format.
func FullAccountRLP(data []byte) ([]byte, error) {
	var slim SlimAccount
	if err := rlp.DecodeBytes(data, &slim); err != nil {
		return nil, err
	}
	if _, err := qkccommon.NewTokenBalances(slim.MntBal); err != nil {
		return nil, err
	}
	root := EmptyRootHash
	if len(slim.Root) != 0 {
		root = common.BytesToHash(slim.Root)
	}
	codeHash := EmptyCodeHash.Bytes()
	if len(slim.CodeHash) != 0 {
		codeHash = slim.CodeHash
	}
	return rlp.EncodeToBytes(&qkcAccountRLP{
		Nonce:        slim.Nonce,
		TokenBal:     slim.MntBal,
		Root:         root,
		CodeHash:     codeHash,
		FullShardKey: slim.FullShardKey,
	})
}
