// Copyright 2026-2027, QuarkChain.

package types

import (
	"bytes"
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/rlp"
)

func TestQkcTransactionCompatibility(t *testing.T) {
	recipient := common.HexToAddress("b94f5374fce5edbc8e2a8697c15331677e6ebf0b")
	tx := NewQkcTransaction(3, recipient, big.NewInt(10), 2000, big.NewInt(1), 0, 0, 1, 0, nil, 0, 0)
	signed, err := tx.WithSignature(NewQkcSigner(1, 1), common.FromHex("98ff921201554726367d2be8c804a7ff89ccf285ebc57dff8ae4c44b9c19ac4a8887321be575c8095f789dd4c743dfe42c1820f9231f98a962b210e3ac2452a301"))
	if err != nil {
		t.Fatal(err)
	}
	wantBytes := common.FromHex("000000006ff86d03018207d094b94f5374fce5edbc8e2a8697c15331677e6ebf0b0a8001840000000084000000008080801ca098ff921201554726367d2be8c804a7ff89ccf285ebc57dff8ae4c44b9c19ac4aa08887321be575c8095f789dd4c743dfe42c1820f9231f98a962b210e3ac2452a3")
	encoded, err := signed.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, wantBytes) {
		t.Fatalf("wire bytes mismatch\ngot  %x\nwant %x", encoded, wantBytes)
	}
	serialized, err := serialize.SerializeToBytes(signed)
	if err != nil || !bytes.Equal(serialized, wantBytes) {
		t.Fatalf("Serialize mismatch: got %x, err %v", serialized, err)
	}
	if got, want := signed.Hash(), common.HexToHash("9ebde4a9b28917420c60fcf6decb98ca61bde4fc026c410f87ea5d58456d7c15"); got != want {
		t.Fatalf("hash mismatch: got %x want %x", got, want)
	}
	var decoded Transaction
	if err := serialize.DeserializeFromBytes(wantBytes, &decoded); err != nil {
		t.Fatal(err)
	}
	roundTrip, err := decoded.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(roundTrip, wantBytes) {
		t.Fatalf("round trip mismatch: got %x", roundTrip)
	}
}

func TestQkcTransactionSigning(t *testing.T) {
	key, err := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	if err != nil {
		t.Fatal(err)
	}
	want := crypto.PubkeyToAddress(key.PublicKey)
	to := common.HexToAddress("314b2cd22c6d26618ce051a58c65af1253aecbb8")
	for _, version := range []uint32{0, 1, 2} {
		// Version 2 uses standard EIP-155 signing, so it must carry the default
		// QKC token and stay within a single shard (see QkcSigner.validate).
		fromShardKey, toShardKey := uint32(0xc47decfd), uint32(0xc49c1950)
		gasToken, transferToken := uint64(0x111), uint64(0x222)
		if version == 2 {
			fromShardKey, toShardKey = 0xc47d0000, 0xc47d0000
			gasToken, transferToken = qkccommon.DefaultTokenID, qkccommon.DefaultTokenID
		}
		tx := NewQkcTransaction(13, to, big.NewInt(1000), 30000, big.NewInt(10_000_000_000), fromShardKey, toShardKey, 3, version, []byte{1, 2, 3}, gasToken, transferToken)
		signer := MakeQkcSigner(3, 3)
		signed, err := SignTx(tx, signer, key)
		if err != nil {
			t.Fatalf("version %d: %v", version, err)
		}
		from, err := Sender(signer, signed)
		if err != nil || from != want {
			t.Fatalf("version %d: sender %x, want %x, err %v", version, from, want, err)
		}
	}
}

func TestQkcV2Validation(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer := NewQkcSigner(3, 3)
	def := qkccommon.DefaultTokenID
	tests := []struct {
		name                    string
		fromShardKey            uint32
		toShardKey              uint32
		gasToken, transferToken uint64
		wantErr                 error
	}{
		{"same-shard-default-token", 0xc47d0000, 0xc47d0000, def, def, nil},
		{"cross-shard", 0xc47d0000, 0xc49c0000, def, def, ErrQkcV2CrossShard},
		{"non-default-gas-token", 0xc47d0000, 0xc47d0000, 0x111, def, ErrQkcV2NonDefaultToken},
		{"non-default-transfer-token", 0xc47d0000, 0xc47d0000, def, 0x222, ErrQkcV2NonDefaultToken},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx := NewQkcTransaction(0, common.Address{}, nil, 0, nil, test.fromShardKey, test.toShardKey, 3, 2, nil, test.gasToken, test.transferToken)
			_, err := SignTx(tx, signer, key)
			if err != test.wantErr {
				t.Fatalf("got %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestQkcTransactionGetters(t *testing.T) {
	legacy := NewTx(&LegacyTx{})
	if _, ok := legacy.inner.(QkcTxData); ok {
		t.Fatal("LegacyTx unexpectedly implements QkcTxData")
	}
	if legacy.NetworkID() != 0 || legacy.FromFullShardKey() != 0 || legacy.GasTokenID() != 0 {
		t.Fatal("Ethereum transaction returned non-default QKC fields")
	}

	qkc := NewQkcTransaction(0, common.Address{}, nil, 0, nil, 11, 12, 13, 2, nil, 14, 15)
	if qkc.FromFullShardKey() != 11 || qkc.ToFullShardKey() != 12 || qkc.NetworkID() != 13 || qkc.Version() != 2 || qkc.GasTokenID() != 14 || qkc.TransferTokenID() != 15 {
		t.Fatal("QKC transaction getters did not return QkcTxData fields")
	}
	if qkc.ChainId().Uint64() != 13 {
		t.Fatalf("version 2 chain ID mismatch: got %d want 13", qkc.ChainId())
	}
	for _, version := range []uint32{0, 1} {
		tx := NewQkcTransaction(0, common.Address{}, nil, 0, nil, 0, 0, 13, version, nil, 0, 0)
		if tx.ChainId().Sign() != 0 {
			t.Fatalf("version %d chain ID is non-zero: %d", version, tx.ChainId())
		}
	}
}

func TestQkcTransactionShardAccessors(t *testing.T) {
	// fromFullShardKey 0xc47decfd -> chainID 0xc47d, shardKey 0xdecfd&0xffff.
	tx := NewQkcTransaction(0, common.Address{}, nil, 0, nil, 0xc47decfd, 0xc49c1950, 3, 0, nil, 0, 0)
	if tx.FromChainID() != 0xc47d || tx.ToChainID() != 0xc49c {
		t.Fatalf("chain ID mismatch: from %x to %x", tx.FromChainID(), tx.ToChainID())
	}
	if tx.FromShardKey() != 0xdecfd&0xffff || tx.ToShardKey() != 0x1950 {
		t.Fatalf("shard key mismatch: from %x to %x", tx.FromShardKey(), tx.ToShardKey())
	}
	// qkcShardSize == 1: shard ID always 0, full shard ID = chainID<<16 | 1.
	if tx.FromShardID() != 0 || tx.ToShardID() != 0 {
		t.Fatalf("shard ID mismatch: from %d to %d", tx.FromShardID(), tx.ToShardID())
	}
	if tx.FromFullShardID() != 0xc47d0001 || tx.ToFullShardID() != 0xc49c0001 {
		t.Fatalf("full shard ID mismatch: from %x to %x", tx.FromFullShardID(), tx.ToFullShardID())
	}
	if !tx.IsCrossShard() {
		t.Fatal("expected cross-shard for differing chain IDs")
	}
	same := NewQkcTransaction(0, common.Address{}, nil, 0, nil, 0xc47d0000, 0xc47d0000, 3, 0, nil, 0, 0)
	if same.IsCrossShard() {
		t.Fatal("expected same-shard for equal full shard keys")
	}

	// Non-QKC transactions return defaults.
	legacy := NewTx(&LegacyTx{})
	if legacy.FromChainID() != 0 || legacy.FromFullShardID() != 0 || legacy.IsCrossShard() {
		t.Fatal("Ethereum transaction returned non-default shard fields")
	}
}

func TestQkcTransactionSetters(t *testing.T) {
	tx := NewQkcTransaction(1, common.Address{}, nil, 100, nil, 0xc47d0000, 0xc47d0000, 3, 0, nil, 0, 0)
	before := tx.Hash()

	tx.SetGas(200)
	tx.SetNonce(2)
	tx.SetFromFullShardKey(0xabcd0000)
	if tx.Gas() != 200 || tx.Nonce() != 2 || tx.FromFullShardKey() != 0xabcd0000 {
		t.Fatalf("setters did not mutate: gas %d nonce %d fromKey %x", tx.Gas(), tx.Nonce(), tx.FromFullShardKey())
	}
	if tx.FromChainID() != 0xabcd {
		t.Fatalf("derived chain ID stale after SetFromFullShardKey: %x", tx.FromChainID())
	}
	if tx.Hash() == before {
		t.Fatal("hash cache not invalidated after mutation")
	}

	// SetVRS updates signature values and invalidates the cache.
	afterMutate := tx.Hash()
	tx.SetVRS(big.NewInt(1), big.NewInt(2), big.NewInt(3))
	v, r, s := tx.RawSignatureValues()
	if v.Sign() == 0 || r.Cmp(big.NewInt(2)) != 0 || s.Cmp(big.NewInt(3)) != 0 {
		t.Fatalf("SetVRS mismatch: v %v r %v s %v", v, r, s)
	}
	if tx.Hash() == afterMutate {
		t.Fatal("hash cache not invalidated after SetVRS")
	}

	// Setters are no-ops on non-QKC transactions.
	legacy := NewTx(&LegacyTx{Nonce: 5})
	legacy.SetNonce(9)
	if legacy.Nonce() != 5 {
		t.Fatalf("SetNonce mutated a non-QKC transaction: %d", legacy.Nonce())
	}
}

func TestQkcSigningValidation(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer := NewQkcSigner(1, 3)
	tests := []struct {
		name      string
		version   uint32
		networkID uint32
		wantErr   bool
	}{
		{"version-0-qkc-network", 0, 1, false},
		{"version-1-qkc-network", 1, 1, false},
		{"version-2-eth-chain", 2, 3, false},
		{"version-0-wrong-qkc-network", 0, 3, true},
		{"version-1-wrong-qkc-network", 1, 3, true},
		{"version-2-wrong-eth-chain", 2, 1, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Use the default token so version-2 clears the token check and only
			// the network-id validation under test decides the outcome.
			gasToken, transferToken := uint64(0), uint64(0)
			if test.version == 2 {
				gasToken, transferToken = qkccommon.DefaultTokenID, qkccommon.DefaultTokenID
			}
			tx := NewQkcTransaction(0, common.Address{}, nil, 0, nil, 0, 0, test.networkID, test.version, nil, gasToken, transferToken)
			_, err := SignTx(tx, signer, key)
			if test.wantErr && err != ErrInvalidQkcNetworkID {
				t.Fatalf("network validation error: got %v", err)
			}
			if !test.wantErr && err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQkcTransactionRejectsMissingShardKeys(t *testing.T) {
	inner := newQkcTx(0, nil, nil, 0, nil, 0, 0, 1, 0, nil, 0, 0)
	inner.FromFullShardKey = nil
	payload, err := rlp.EncodeToBytes(inner)
	if err != nil {
		t.Fatal(err)
	}
	encoded := make([]byte, 5, 5+len(payload))
	binary.BigEndian.PutUint32(encoded[1:], uint32(len(payload)))
	encoded = append(encoded, payload...)
	var decoded Transaction
	if err := decoded.UnmarshalBinary(encoded); err == nil {
		t.Fatal("decoded QKC transaction with missing from full shard key")
	}
}

func TestQkcTransactionDeserializePreservesFollowingFields(t *testing.T) {
	type envelope struct {
		Tx   *Transaction
		Tail uint32
	}
	want := envelope{
		Tx:   NewQkcTransaction(1, common.Address{}, big.NewInt(2), 3, big.NewInt(4), 5, 6, 7, 0, []byte{8}, 9, 10),
		Tail: 11,
	}
	encoded, err := serialize.SerializeToBytes(want)
	if err != nil {
		t.Fatal(err)
	}
	var got envelope
	if err := serialize.DeserializeFromBytes(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got.Tx.Hash() != want.Tx.Hash() || got.Tail != want.Tail {
		t.Fatalf("round trip mismatch: tx %x, tail %d", got.Tx.Hash(), got.Tail)
	}
}

func TestQkcTransactionDeserializeList(t *testing.T) {
	want := []*Transaction{
		NewQkcTransaction(1, common.Address{}, nil, 2, nil, 3, 4, 5, 0, nil, 6, 7),
		NewQkcTransaction(8, common.Address{}, nil, 9, nil, 10, 11, 12, 1, nil, 13, 14),
	}
	encoded, err := serialize.SerializeToBytes(want)
	if err != nil {
		t.Fatal(err)
	}
	var got []*Transaction
	if err := serialize.DeserializeFromBytes(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("transaction count mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Hash() != want[i].Hash() {
			t.Fatalf("transaction %d mismatch: got %x want %x", i, got[i].Hash(), want[i].Hash())
		}
	}
}
