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
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
)

func TestEIP155Signing(t *testing.T) {
	key, _ := crypto.GenerateKey()
	recipient := publicKey2Recipient(&key.PublicKey)

	signer := NewEIP155Signer(1)
	tx, err := SignTx(NewQkcTransaction(0, recipient, new(big.Int), 0, new(big.Int), 0, 0, 1, 0, nil, 0, 0), signer, key)
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
	signer := NewEIP155Signer(1)
	tx, err := SignTx(NewQkcTransaction(0, recipient, new(big.Int), 0, new(big.Int), 0, 0, 1, 1, nil, 0, 0), signer, key)
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

func TestEIP155SignerHashForVersion2(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	tx := NewQkcTransaction(0, publicKey2Recipient(&key.PublicKey), new(big.Int), 0, new(big.Int), 0, 0, 1, 2, nil, 0, 0)
	signer := NewEIP155Signer(tx.NetworkId())

	if got, want := signer.Hash(tx), qkcTxData(tx).getUnsignedHashForEip155(tx.NetworkId()); got != want {
		t.Errorf("EIP-155 hash mismatch, got %x want %x", got, want)
	}
}

func TestSignTxVersionsRecoverSender(t *testing.T) {
	key, err := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	require.NoError(t, err)
	want := publicKey2Recipient(&key.PublicKey)
	const networkID = uint32(3)
	signer := NewEIP155Signer(networkID)

	for _, version := range []uint32{0, 1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			tx := NewQkcTransaction(
				13,
				common.HexToAddress("0x314b2cd22c6d26618ce051a58c65af1253aecbb8"),
				big.NewInt(1000),
				30000,
				big.NewInt(10_000_000_000),
				0xc47decfd,
				0xc49c1950,
				networkID,
				version,
				[]byte{1, 2, 3},
				0x111,
				0x222,
			)
			signed, err := SignTx(tx, signer, key)
			require.NoError(t, err)
			from, err := Sender(signer, signed)
			require.NoError(t, err)
			require.Equal(t, want, from)

			v, _, _ := signed.RawSignatureValues()
			if version == 2 {
				require.Contains(t, []uint64{41, 42}, v.Uint64())
			} else {
				require.Contains(t, []uint64{27, 28}, v.Uint64())
			}
		})
	}
}
