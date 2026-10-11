// Copyright 2026-2027, QuarkChain.

package types

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/rlp"
)

type qkcTestShardSizes map[uint32]uint32

func (sizes qkcTestShardSizes) GetShardSizeByChainId(chainID uint32) (uint32, error) {
	return sizes[chainID], nil
}

func TestQKCBlockPythonGolden(t *testing.T) {
	headerBytes := common.FromHex("00000001000000010000000000000002d3f86deb4a2bbf85048b3e790460c40dbab1f621000003ff00000002010101010102010200000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000030000000000000005010600000000000000070000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100030102030000000000000000000000000000000000000000000000000000000000000004")
	metaBytes := common.FromHex(
		"a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97" +
			"297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba495" +
			"df227f34313c2bc4a4a986817ea46437f049873f2fca8e2b89b1ecd0f9e67a28" +
			"0000000000000000000000000000000000000000000000000000000000000064" +
			"000000000000000000000000000000000000000000000000000000000000012c" +
			"000000000000000100000000000000020000000000000003" +
			"0000000000000000000000000000000000000000000000000000000000000190")

	var header MinorBlockHeader
	if err := serialize.DeserializeFromBytes(headerBytes, &header); err != nil {
		t.Fatal(err)
	}
	if got, want := header.Hash(), common.HexToHash("0xb0b7dfab9a8f485ea97a4642cdd380182ede101a64ecb3e73eb211496153d869"); got != want {
		t.Fatalf("minor header hash = %s, want %s", got, want)
	}
	headerEncoded, err := serialize.SerializeToBytes(&header)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(headerEncoded, headerBytes) {
		t.Fatalf("minor header encoding mismatch:\n have %x\n want %x", headerEncoded, headerBytes)
	}

	var meta MinorBlockMeta
	if err := serialize.DeserializeFromBytes(metaBytes, &meta); err != nil {
		t.Fatal(err)
	}
	if got, want := meta.Hash(), common.HexToHash("0x9a13cf251684832e712c2125676f93ee7b5080dd4b0b382e3074ae16cc895c5b"); got != want {
		t.Fatalf("minor meta hash = %s, want %s", got, want)
	}
	metaEncoded, err := serialize.SerializeToBytes(&meta)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(metaEncoded, metaBytes) {
		t.Fatalf("minor meta encoding mismatch:\n have %x\n want %x", metaEncoded, metaBytes)
	}

	blockBytes := append(append(append([]byte{}, headerBytes...), metaBytes...), common.FromHex("0000000000020102")...)
	var block Block
	if err := serialize.DeserializeFromBytes(blockBytes, &block); err != nil {
		t.Fatal(err)
	}
	blockEncoded, err := serialize.SerializeToBytes(&block)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(blockEncoded, blockBytes) || block.Hash() != header.Hash() || !bytes.Equal(block.TrackingData(), []byte{1, 2}) {
		t.Fatal("minor block did not preserve the pyquarkchain encoding")
	}
	// The header and meta vectors above are independent pyquarkchain serialization
	// vectors, so their transaction commitment is intentionally not derived from
	// this empty block body. Build a structurally consistent copy for validation.
	valid := block.WithSeal(block.Header())
	valid.header.TxHash = CalculateMerkleRoot(valid.transactions)
	validMeta, err := valid.header.ToMinorBlockMeta()
	if err != nil {
		t.Fatal(err)
	}
	valid.header.MetaHash = validMeta.Hash()
	if err := valid.Validate(); err != nil {
		t.Fatalf("consistent minor block rejected: %v", err)
	}
	invalid := valid.WithSeal(valid.Header())
	invalid.header.TxHash = common.HexToHash("0x01")
	if err := invalid.Validate(); err == nil {
		t.Fatal("minor block with mismatched transaction root was accepted")
	}
	invalid.transactions = Transactions{nil}
	if err := invalid.Validate(); err == nil {
		t.Fatal("minor block with a nil transaction was accepted")
	}
	badSignature := NewQKCContractCreation(0, big.NewInt(0), 21_000, big.NewInt(1), 0, 0, 1, 0, nil, 0, 0)
	badSignature.SetVRS(big.NewInt(-1), big.NewInt(1), big.NewInt(1))
	invalid.transactions = Transactions{badSignature}
	if err := invalid.Validate(); err == nil {
		t.Fatal("minor block with a negative transaction signature value was accepted")
	}
}

func TestQKCTransactionPythonGolden(t *testing.T) {
	to := account.BytesToIdentityRecipient(common.Hex2Bytes("b94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
	tx := NewQKCTransaction(7, to, big.NewInt(1000), 30000, big.NewInt(2), 0xffff, 0x10000, 3, 0,
		[]byte{1, 2, 3}, 12345, 1234)
	tx.SetVRS(big.NewInt(28), big.NewInt(0x11223344), big.NewInt(0x55667788))

	wantEncoded := common.FromHex("0000000040f83e070282753094b94f5374fce5edbc8e2a8697c15331677e6ebf0b8203e88301020303840000ffff84000100008230398204d2801c84112233448455667788")
	gotEncoded, err := serialize.SerializeToBytes(tx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotEncoded, wantEncoded) {
		t.Fatalf("QKC encoding mismatch:\n have %x\n want %x", gotEncoded, wantEncoded)
	}
	if got, want := tx.Hash(), common.HexToHash("0xc1ae9851cb45420191f9ddca329041770808df0b715c7ebf4ad744806858dc61"); got != want {
		t.Fatalf("QKC transaction hash = %s, want %s", got, want)
	}
	var decoded Transaction
	if err := serialize.DeserializeFromBytes(gotEncoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Hash() != tx.Hash() {
		t.Fatalf("decoded transaction hash = %s, want %s", decoded.Hash(), tx.Hash())
	}
}

func TestQKCTransactionInternalRLPSizeAndShardResolution(t *testing.T) {
	to := account.BytesToIdentityRecipient(common.Hex2Bytes("1111111111111111111111111111111111111111"))
	tx := NewQKCTransaction(1, to, big.NewInt(2), 21_000, big.NewInt(3), 0, 1, 1, 0, nil, 0, 0)
	payload, err := rlp.EncodeToBytes(tx)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Transaction
	if err := rlp.DecodeBytes(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	encoded, err := decoded.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := decoded.Size(), uint64(len(encoded)); got != want {
		t.Fatalf("decoded QKC transaction size = %d, want %d", got, want)
	}

	resolver := qkcTestShardSizes{0: 2}
	crossShard, err := decoded.IsCrossShard(resolver)
	if err != nil {
		t.Fatal(err)
	}
	from, err := decoded.FromFullShardID(resolver)
	if err != nil {
		t.Fatal(err)
	}
	destination, err := decoded.ToFullShardID(resolver)
	if err != nil {
		t.Fatal(err)
	}
	if !crossShard || from != 2 || destination != 3 {
		t.Fatalf("unexpected shard resolution: cross=%t from=%d to=%d", crossShard, from, destination)
	}
}

func TestQKCSignerAcceptsPyquarkchainHighS(t *testing.T) {
	key, err := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	if err != nil {
		t.Fatal(err)
	}
	to := account.BytesToIdentityRecipient(common.Hex2Bytes("2222222222222222222222222222222222222222"))
	tx := NewQKCTransaction(1, to, big.NewInt(2), 21_000, big.NewInt(3), 0, 0, 3, 0, nil, 0, 0)
	signer := NewQKCSigner(3, 1)
	signed, err := SignTx(tx, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	want, err := Sender(signer, signed)
	if err != nil {
		t.Fatal(err)
	}
	v, r, s := signed.RawSignatureValues()
	highS := new(big.Int).Sub(crypto.S256().Params().N, s)
	highV := big.NewInt(27)
	if v.Uint64() == 27 {
		highV.SetUint64(28)
	}
	signed.SetVRS(highV, r, highS)
	got, err := Sender(signer, signed)
	if err != nil {
		t.Fatalf("high-S QKC signature was rejected: %v", err)
	}
	if got != want {
		t.Fatalf("high-S sender = %s, want %s", got, want)
	}
}
