// Copyright 2026-2027, QuarkChain.

// Transaction tests exercise pyquarkchain-compatible QKC wire bytes.

package types

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/hex"
	"math/big"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/rlp"
)

// The values in those tests are from the EvmTransaction Tests
var (
	reciept    = account.BytesToIdentityRecipient(common.Hex2Bytes("b94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
	emptyEvmTx = NewEvmTransaction(
		0,
		reciept,
		big.NewInt(0), 0, big.NewInt(0),
		0, 0, 1, 0, nil, 0, 0,
	)
	emptyTx = Transaction{TxType: 0, EvmTx: emptyEvmTx}
	//nonce , to , amount , gasLimit , gasPrice, fromFullShardKey , toFullShardKey , networkId , version , data
	rightvrsTx = NewEvmTransaction(
		3,
		reciept,
		big.NewInt(10),
		2000,
		big.NewInt(1),
		0,
		0,
		1,
		0,
		nil, 0, 0,
	)
	signTx, _ = rightvrsTx.WithSignature(
		NewQKCSigner(1, 1),
		common.Hex2Bytes("98ff921201554726367d2be8c804a7ff89ccf285ebc57dff8ae4c44b9c19ac4a8887321be575c8095f789dd4c743dfe42c1820f9231f98a962b210e3ac2452a301"),
	)
)

func TestTransactionSigHash(t *testing.T) {
	var signer = NewQKCSigner(1, 1)
	//hash unsigned
	if signer.Hash(emptyEvmTx) != common.HexToHash("15e523e4a18884f01753358af140664007e19b2c67cfa6618cadb85de14f3bd0") {
		t.Errorf("empty transaction unsigned hash mismatch, got %x, expect %x", signer.Hash(emptyEvmTx), common.HexToHash("297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba495"))
	}
	if emptyEvmTx.Hash() != common.HexToHash("a04873d41928c8acc76d4d6495fec31fb58afc7d5a5782d9ba4bb30fdbf1b147") {
		t.Errorf("empty transaction hash mismatch, got %x, expect %x", emptyTx.Hash(), common.HexToHash("a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97"))
	}

	//hash unsigned
	if signer.Hash(rightvrsTx) != common.HexToHash("a8915d9a38bacbdc640ab287d4beb9b06ea1af52da8568c298739c9d7514e87b") {
		t.Errorf("RightVRS transaction unsigned hash mismatch, got %x, expect %x", signer.Hash(rightvrsTx), common.HexToHash("e4f3c1dd000045bf26006df7eb7cb0a882f70a6ab81723d93638151f6418f78a"))
	}
	if rightvrsTx.Hash() != common.HexToHash("4bf87b2a5b39b7894b4b4b197ffe1ef7e67085bbc60d599ed3d4d587aa72af76") {
		t.Errorf("RightVRS transaction hash mismatch, got %x, expect %x", rightvrsTx.Hash(), common.HexToHash("df227f34313c2bc4a4a986817ea46437f049873f2fca8e2b89b1ecd0f9e67a28"))
	}
}

func TestEvmTransactionHashInvalidatedBySetters(t *testing.T) {
	newTx := func() *EvmTransaction {
		return NewEvmTransaction(0, reciept, big.NewInt(0), 0, big.NewInt(0), 0, 0, 1, 0, nil, 0, 0)
	}
	tests := []struct {
		name string
		set  func(*EvmTransaction)
	}{
		{"gas", func(tx *EvmTransaction) { tx.SetGas(1) }},
		{"from full shard key", func(tx *EvmTransaction) { tx.SetFromFullShardKey(1) }},
		{"nonce", func(tx *EvmTransaction) { tx.SetNonce(1) }},
		{"signature", func(tx *EvmTransaction) { tx.SetVRS(big.NewInt(27), big.NewInt(1), big.NewInt(1)) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx := newTx()
			before := tx.Hash()
			test.set(tx)
			if got := tx.Hash(); got == before {
				t.Fatal("hash was not invalidated")
			}
		})
	}
}

func TestSetSenderUsesProvidedSigner(t *testing.T) {
	tx := NewEvmTransaction(0, reciept, big.NewInt(0), 0, big.NewInt(0), 0, 0, 3, 2, nil, 0, 0)
	signer := NewQKCSigner(1, 3)
	want := account.BytesToIdentityRecipient([]byte{1})
	tx.SetSender(signer, want)

	got, err := Sender(signer, tx)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("cached sender mismatch: got %x, want %x", got, want)
	}
}

func TestTransactionEncode(t *testing.T) {
	txb, err := rlp.EncodeToBytes(rightvrsTx)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	should := common.FromHex("ed03018207d094b94f5374fce5edbc8e2a8697c15331677e6ebf0b0a800184000000008400000000808080808080")
	if !bytes.Equal(txb, should) {
		t.Errorf("encoded RLP mismatch, got %x", txb)
	}
}

func decodeTx(data []byte) (*EvmTransaction, error) {
	var tx EvmTransaction
	t, err := &tx, rlp.Decode(bytes.NewReader(data), &tx)

	return t, err
}

func publicKey2Recipient(pk *ecdsa.PublicKey) account.Recipient {
	pubBytes := crypto.FromECDSAPub(pk)
	recipient := account.BytesToIdentityRecipient(crypto.Keccak256(pubBytes[1:])[12:])
	return recipient
}

func defaultTestKey() (*ecdsa.PrivateKey, account.Recipient) {
	key, _ := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	recipient := publicKey2Recipient(&key.PublicKey)
	return key, recipient
}

func TestRecipientEmpty(t *testing.T) {
	_, addr := defaultTestKey()
	tx, err := decodeTx(common.Hex2Bytes("f86b80808094b94f5374fce5edbc8e2a8697c15331677e6ebf0b808001840000000084000000008080801ba0d7265f92d763da5e2ea5016b837bf56f5bf42d22aead9ad5e7be2ddf01efcc68a07159634972d77349a76108c6db0634ea7b65768881b152c656deca190df6e427"))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	from, err := Sender(NewQKCSigner(tx.NetworkId(), tx.NetworkId()), tx)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if addr != from {
		t.Errorf("derived address doesn't match addr %x, from %x", addr, from)
	}
}

func TestRecipientNormal(t *testing.T) {
	_, addr := defaultTestKey()

	tx, err := decodeTx(common.Hex2Bytes("f86b80808094b94f5374fce5edbc8e2a8697c15331677e6ebf0b808001840000000084000000008080801ba0d7265f92d763da5e2ea5016b837bf56f5bf42d22aead9ad5e7be2ddf01efcc68a07159634972d77349a76108c6db0634ea7b65768881b152c656deca190df6e427"))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	from, err := Sender(NewQKCSigner(1, 1), tx)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if addr != from {
		t.Error("derived address doesn't match")
	}
}

func TestTxSize(t *testing.T) {

	id1, err := account.CreatRandomIdentity()
	if err != nil {
		t.Fatal("CreatIdentityFromKey error: ", err)
	}
	defaultFullShardKey, err := id1.GetDefaultFullShardKey()
	if err != nil {
		t.Fatal("GetDefaultFullShardKey error: ", err)
	}
	acc1 := account.CreatAddressFromIdentity(id1, defaultFullShardKey)
	check := func(f string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}
	evmTx := NewEvmTransaction(
		0,
		acc1.Recipient,
		big.NewInt(0),
		30000,
		big.NewInt(0),
		0xFFFF,
		0xFFFF,
		1,
		0,
		nil,
		12345,
		1234,
	)
	signer := NewQKCSigner(1, 1)
	prvKey, err := crypto.HexToECDSA(hex.EncodeToString(id1.GetKey().Bytes()))
	if err != nil {
		t.Fatal("prvKey error: ", err)
	}
	evmTx, err = SignTx(evmTx, signer, prvKey)
	if err != nil {
		t.Fatal("SignTx error: ", err)
	}
	tx := &Transaction{
		EvmTx:  evmTx,
		TxType: EvmTx,
	}
	txBytes, err := serialize.SerializeToBytes(&tx)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	TT256 := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(2), big.NewInt(256), big.NewInt(0)), big.NewInt(1))
	SHARD_KEY_MAX := new(big.Int).Exp(big.NewInt(256), big.NewInt(4), big.NewInt(0))
	TOKEN_ID_MAX, _ := new(big.Int).SetString("4873763662273663091", 10)
	evmTx2 := NewEvmTransaction(
		TT256.Uint64(),
		acc1.Recipient,
		TT256,
		TT256.Uint64(),
		TT256,
		uint32(SHARD_KEY_MAX.Uint64()),
		uint32(SHARD_KEY_MAX.Uint64()),
		1,
		0,
		[]byte{0},
		TOKEN_ID_MAX.Uint64(),
		TOKEN_ID_MAX.Uint64(),
	)

	evmTx2, err = SignTx(evmTx2, signer, prvKey)
	if err != nil {
		t.Fatal("SignTx error: ", err)
	}
	tx2 := &Transaction{
		EvmTx:  evmTx2,
		TxType: EvmTx,
	}
	txBytes2, err := serialize.SerializeToBytes(&tx2)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	check("EvmTransaction min len", len(txBytes), 120)
	check("EvmTransaction max len", len(txBytes2), 210)
}
