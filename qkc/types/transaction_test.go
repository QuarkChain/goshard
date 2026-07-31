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

// The values in those tests are from the QkcTx tests.
var (
	reciept    = account.BytesToIdentityRecipient(common.Hex2Bytes("b94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
	emptyQkcTx = NewQkcTransaction(
		0,
		reciept,
		big.NewInt(0), 0, big.NewInt(0),
		0, 0, 1, 0, nil, 0, 0,
	)
	//nonce , to , amount , gasLimit , gasPrice, fromFullShardKey , toFullShardKey , networkId , version , data
	rightvrsTx = NewQkcTransaction(
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

func qkcTxData(tx *Transaction) *QkcTx {
	return tx.inner.(*QkcTx)
}

func TestTransactionSigHash(t *testing.T) {
	var signer = NewQKCSigner(1, 1)
	//hash unsigned
	if signer.Hash(emptyQkcTx) != common.HexToHash("15e523e4a18884f01753358af140664007e19b2c67cfa6618cadb85de14f3bd0") {
		t.Errorf("empty transaction unsigned hash mismatch, got %x, expect %x", signer.Hash(emptyQkcTx), common.HexToHash("297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba495"))
	}
	if rlpHash(qkcTxData(emptyQkcTx)) != common.HexToHash("a04873d41928c8acc76d4d6495fec31fb58afc7d5a5782d9ba4bb30fdbf1b147") {
		t.Errorf("empty transaction hash mismatch, got %x, expect %x", emptyQkcTx.Hash(), common.HexToHash("a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97"))
	}

	//hash unsigned
	if signer.Hash(rightvrsTx) != common.HexToHash("a8915d9a38bacbdc640ab287d4beb9b06ea1af52da8568c298739c9d7514e87b") {
		t.Errorf("RightVRS transaction unsigned hash mismatch, got %x, expect %x", signer.Hash(rightvrsTx), common.HexToHash("e4f3c1dd000045bf26006df7eb7cb0a882f70a6ab81723d93638151f6418f78a"))
	}
	if rlpHash(qkcTxData(rightvrsTx)) != common.HexToHash("4bf87b2a5b39b7894b4b4b197ffe1ef7e67085bbc60d599ed3d4d587aa72af76") {
		t.Errorf("RightVRS transaction hash mismatch, got %x, expect %x", rlpHash(qkcTxData(rightvrsTx)), common.HexToHash("df227f34313c2bc4a4a986817ea46437f049873f2fca8e2b89b1ecd0f9e67a28"))
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
	var decoded Transaction
	if err := rlp.DecodeBytes(should, &decoded); err != nil {
		t.Fatal(err)
	}
	encoded, err := rlp.EncodeToBytes(&decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, should) {
		t.Fatalf("RLP round trip mismatch, got %x want %x", encoded, should)
	}
}

func TestTransactionCanonicalBytesAndHash(t *testing.T) {
	tx := signTx
	wantBytes := common.FromHex("000000006ff86d03018207d094b94f5374fce5edbc8e2a8697c15331677e6ebf0b0a8001840000000084000000008080801ca098ff921201554726367d2be8c804a7ff89ccf285ebc57dff8ae4c44b9c19ac4aa08887321be575c8095f789dd4c743dfe42c1820f9231f98a962b210e3ac2452a3")
	wantHash := common.HexToHash("9ebde4a9b28917420c60fcf6decb98ca61bde4fc026c410f87ea5d58456d7c15")
	canonical, err := tx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	serialized, err := serialize.SerializeToBytes(tx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonical, wantBytes) || !bytes.Equal(serialized, wantBytes) {
		t.Fatalf("serialized bytes mismatch: marshal %x serialize %x want %x", canonical, serialized, wantBytes)
	}
	if got := tx.Hash(); got != wantHash {
		t.Fatalf("hash mismatch: got %x want %x", got, wantHash)
	}
	if got := (Transactions{tx}).Bytes(0); !bytes.Equal(got, canonical) {
		t.Fatalf("minor-block leaf differs: got %x want %x", got, canonical)
	}
	var decoded Transaction
	if err := serialize.DeserializeFromBytes(wantBytes, &decoded); err != nil {
		t.Fatal(err)
	}
	roundTrip, err := serialize.SerializeToBytes(&decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(roundTrip, wantBytes) || decoded.Hash() != wantHash {
		t.Fatalf("serialize round trip mismatch: bytes %x hash %x", roundTrip, decoded.Hash())
	}
}

func TestTransactionMinorBlockMerkleRootGolden(t *testing.T) {
	txs := Transactions{
		emptyQkcTx,
		signTx,
	}
	root := CalculateMerkleRoot(txs)
	want := common.HexToHash("0x13dc746cc9deaa7427a935ce1643ed70a087ff38c8f46cc4d20367c717a623ac")
	if root != want {
		t.Fatalf("minor block transaction root mismatch: got %x want %x", root, want)
	}
}

func TestWithSignatureDeepCopiesTransaction(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	tx := NewQkcTransaction(1, reciept, big.NewInt(2), 3, big.NewInt(4), 5, 6, 1, 0, []byte{7}, 8, 9)
	signed, err := SignTx(tx, NewQKCSigner(1, 1), key)
	if err != nil {
		t.Fatal(err)
	}
	qkcTxData(signed).Price.SetInt64(99)
	qkcTxData(signed).Amount.SetInt64(98)
	qkcTxData(signed).Payload[0] = 97
	if qkcTxData(tx).Price.Int64() != 4 || qkcTxData(tx).Amount.Int64() != 2 || qkcTxData(tx).Payload[0] != 7 {
		t.Fatal("WithSignature shares mutable transaction data")
	}
}

func TestTransactionSettersClearCaches(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := SignTx(
		NewQkcTransaction(1, reciept, big.NewInt(2), 30_000, big.NewInt(4), 5, 6, 1, 0, []byte{7}, 8, 9),
		NewQKCSigner(1, 1),
		key,
	)
	if err != nil {
		t.Fatal(err)
	}
	oldHash, oldSize := tx.Hash(), tx.Size()
	oldSender, err := Sender(NewQKCSigner(1, 1), tx)
	if err != nil {
		t.Fatal(err)
	}

	tx.SetGas(300_000)
	if tx.Hash() == oldHash || tx.Size() == oldSize {
		t.Fatal("SetGas left a derived cache unchanged")
	}
	newSender, err := Sender(NewQKCSigner(1, 1), tx)
	if err != nil {
		t.Fatal(err)
	}
	if newSender == oldSender {
		t.Fatal("SetGas retained the sender derived from the old signature hash")
	}

}

func decodeTx(data []byte) (*Transaction, error) {
	var inner QkcTx
	if err := rlp.Decode(bytes.NewReader(data), &inner); err != nil {
		return nil, err
	}
	return NewTransaction(&inner), nil
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
	qkcTx := NewQkcTransaction(
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
	qkcTx, err = SignTx(qkcTx, signer, prvKey)
	if err != nil {
		t.Fatal("SignTx error: ", err)
	}
	tx := qkcTx
	txBytes, err := serialize.SerializeToBytes(&tx)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	TT256 := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(2), big.NewInt(256), big.NewInt(0)), big.NewInt(1))
	SHARD_KEY_MAX := new(big.Int).Exp(big.NewInt(256), big.NewInt(4), big.NewInt(0))
	TOKEN_ID_MAX, _ := new(big.Int).SetString("4873763662273663091", 10)
	qkcTx2 := NewQkcTransaction(
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

	qkcTx2, err = SignTx(qkcTx2, signer, prvKey)
	if err != nil {
		t.Fatal("SignTx error: ", err)
	}
	tx2 := qkcTx2
	txBytes2, err := serialize.SerializeToBytes(&tx2)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	check("QkcTx min len", len(txBytes), 120)
	check("QkcTx max len", len(txBytes2), 210)
}
