// Copyright 2026-2027, QuarkChain.

// Transaction signing tests exercise pyquarkchain-compatible QKC signatures.
//
// Copyright 2016 The go-ethereum Authors
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
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/account"
)

func TestQKCSigning(t *testing.T) {
	key, _ := crypto.GenerateKey()
	recipient := publicKey2Recipient(&key.PublicKey)

	signer := NewQKCSigner(1, 1)
	tx, err := SignTx(NewEvmTransaction(0, recipient, new(big.Int), 0, new(big.Int), 0, 0, 1, 0, nil, 0, 0), signer, key)
	if err != nil {
		t.Fatal(err)
	}

	from, err := Sender(signer, tx)
	if err != nil {
		t.Fatal(err)
	}
	if from != recipient {
		t.Errorf("exected from and address to be equal. Got %x want %x", from, recipient)
	}
}

func TestTypedTransactionSigning(t *testing.T) {
	key, _ := crypto.GenerateKey()
	recipient := publicKey2Recipient(&key.PublicKey)
	signer := NewQKCSigner(1, 1)
	tx, err := SignTx(NewEvmTransaction(0, recipient, new(big.Int), 0, new(big.Int), 0, 0, 1, 1, nil, 0, 0), signer, key)
	if err != nil {
		t.Fatal(err)
	}

	from, err := Sender(signer, tx)
	if err != nil {
		t.Fatal(err)
	}
	if from != recipient {
		t.Errorf("expected sender %x, got %x", recipient, from)
	}
}

func TestQKCSignerHashForVersion2(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	tx := NewEvmTransaction(0, publicKey2Recipient(&key.PublicKey), new(big.Int), 0, new(big.Int), 0, 0, 1, 2, nil, 0, 0)
	signer := NewQKCSigner(1, tx.NetworkId())

	if got, want := signer.Hash(tx), tx.getUnsignedHashForEip155(tx.NetworkId()); got != want {
		t.Errorf("EIP-155 hash mismatch, got %x want %x", got, want)
	}
}

func TestQKCSignerRejectsWrongNetworkID(t *testing.T) {
	tests := []struct {
		name    string
		version uint32
		network uint32
		signer  QKCSigner
	}{
		{"qkc", 0, 2, NewQKCSigner(1, 3)},
		{"eip-155", 2, 4, NewQKCSigner(1, 3)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx := NewEvmTransaction(0, account.Recipient{}, new(big.Int), 0, new(big.Int), 0, 0, test.network, test.version, nil, 0, 0)
			_, err := Sender(test.signer, tx)
			if !errors.Is(err, ErrInvalidNetworkID) {
				t.Fatalf("expected ErrInvalidNetworkID, got %v", err)
			}
		})
	}
}
