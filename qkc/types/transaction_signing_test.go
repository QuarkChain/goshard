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
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/stretchr/testify/require"
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

	if got, want := signer.Hash(tx), evmTxData(tx).getUnsignedHashForEip155(tx.NetworkId()); got != want {
		t.Errorf("EIP-155 hash mismatch, got %x want %x", got, want)
	}
}

func TestSignTxVersionsRecoverSender(t *testing.T) {
	key, err := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	require.NoError(t, err)
	want := publicKey2Recipient(&key.PublicKey)
	const networkID = uint32(3)
	signer := NewQKCSigner(networkID, networkID)

	for _, version := range []uint32{0, 1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			fromFullShardKey, toFullShardKey := uint32(0xc47decfd), uint32(0xc49c1950)
			gasTokenID, transferTokenID := uint64(0x111), uint64(0x222)
			if version == 2 {
				fromFullShardKey, toFullShardKey = 0xc47d0000, 0xc47d0000
				gasTokenID = qkccommon.TokenIDEncode("QKC")
				transferTokenID = gasTokenID
			}
			tx := NewEvmTransaction(
				13,
				common.HexToAddress("0x314b2cd22c6d26618ce051a58c65af1253aecbb8"),
				big.NewInt(1000),
				30000,
				big.NewInt(10_000_000_000),
				fromFullShardKey,
				toFullShardKey,
				networkID,
				version,
				[]byte{1, 2, 3},
				gasTokenID,
				transferTokenID,
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

func TestQKCSignerRejectsInvalidFields(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	defaultTokenID := qkccommon.TokenIDEncode("QKC")
	signer := NewQKCSigner(1, 3)
	tests := []struct {
		name             string
		fromFullShardKey uint32
		toFullShardKey   uint32
		gasTokenID       uint64
		transferTokenID  uint64
		wantErr          error
	}{
		{"cross-shard", 0xc47d0000, 0xc49c0000, defaultTokenID, defaultTokenID, ErrV2CrossShard},
		{"non-zero-from-shard-key", 0xc47d0001, 0xc47d0000, defaultTokenID, defaultTokenID, ErrV2CrossShard},
		{"non-zero-to-shard-key", 0xc47d0000, 0xc47d0001, defaultTokenID, defaultTokenID, ErrV2CrossShard},
		{"non-default-gas-token", 0xc47d0000, 0xc47d0000, 1, defaultTokenID, ErrV2NonDefaultToken},
		{"non-default-transfer-token", 0xc47d0000, 0xc47d0000, defaultTokenID, 1, ErrV2NonDefaultToken},
		{"oversized-gas-token", 0xc47d0000, 0xc47d0000, qkccommon.TOKENIDMAX + 1, defaultTokenID, nil},
		{"oversized-transfer-token", 0xc47d0000, 0xc47d0000, defaultTokenID, qkccommon.TOKENIDMAX + 1, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx := NewEvmTransaction(0, account.Recipient{}, nil, 0, nil, test.fromFullShardKey, test.toFullShardKey, 3, 2, nil, test.gasTokenID, test.transferTokenID)
			_, err := SignTx(tx, signer, key)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			} else {
				require.Error(t, err)
			}
		})
	}

	for _, test := range []struct {
		name    string
		version uint32
		network uint32
		signer  QKCSigner
	}{
		{"version-0-network", 0, 2, NewQKCSigner(1, 3)},
		{"version-1-network", 1, 2, NewQKCSigner(1, 3)},
		{"version-2-chain", 2, 4, NewQKCSigner(1, 3)},
	} {
		t.Run(test.name, func(t *testing.T) {
			gasTokenID := uint64(0)
			if test.version == 2 {
				gasTokenID = defaultTokenID
			}
			tx := NewEvmTransaction(0, account.Recipient{}, nil, 0, nil, 0, 0, test.network, test.version, nil, gasTokenID, gasTokenID)
			_, err := SignTx(tx, test.signer, key)
			require.ErrorIs(t, err, ErrInvalidNetworkID)
		})
	}
}

func TestSetSenderUsesMatchingSigner(t *testing.T) {
	tx := NewEvmTransaction(0, account.Recipient{}, nil, 0, nil, 0, 0, 1, 0, nil, 0, 0)
	want := account.BytesToIdentityRecipient([]byte{1})
	signer := NewQKCSigner(1, 1)
	tx.SetSender(signer, want)

	got, err := Sender(signer, tx)
	require.NoError(t, err)
	require.Equal(t, want, got)
	_, err = Sender(NewQKCSigner(2, 1), tx)
	require.Error(t, err)
}
