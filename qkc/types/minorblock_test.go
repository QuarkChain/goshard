// Copyright 2026-2027, QuarkChain.

// Minor block tests exercise pyquarkchain-compatible QKC wire bytes.

package types

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/holiman/uint256"
)

// testU256 mirrors the helper in qkc/common's tests: TokenBalances balances are
// *uint256.Int, so comparing a decoded balance needs one built the same way.
func testU256(v uint64) *uint256.Int {
	return uint256.NewInt(v)
}

var (
	//	reciept, _ = account.BytesToIdentityRecipient(common.Hex2Bytes("b94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
	tx1 = NewEvmTransaction(
		0,
		reciept,
		big.NewInt(0), 0, big.NewInt(0),
		0, 0, 1, 0, nil, 0, 0,
	)
	//nonce , to , amount , gasLimit , gasPrice, fromFullShardKey , toFullShardKey , networkId , version , data
	tx2 = NewEvmTransaction(
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
)

// from bcValidBlockTest.json, "SimpleTx"
func TestMinorBlockHeaderSerializing(t *testing.T) {
	blocHeaderEnc := common.FromHex("00000001000000010000000000000002d3f86deb4a2bbf85048b3e790460c40dbab1f621000003ff00000002010101010102010200000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000030000000000000005010600000000000000070000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100030102030000000000000000000000000000000000000000000000000000000000000004")
	var blockHeader MinorBlockHeader
	bb := serialize.NewByteBuffer(blocHeaderEnc)
	if err := serialize.Deserialize(bb, &blockHeader); err != nil {
		t.Fatal("Deserialize error: ", err)
	}

	bytes, err := serialize.SerializeToBytes(&blockHeader)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	check := func(f string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}

	check("Version", blockHeader.Version, uint32(1))
	check("Height", blockHeader.Number, uint64(2))
	check("Branch", blockHeader.Branch.Value, uint32(1))
	check("coinbase_Recipient", blockHeader.Coinbase.Recipient[:], common.FromHex("d3f86deb4a2bbf85048b3e790460c40dbab1f621"))
	check("coinbase_FullShardKey", uint32(blockHeader.Coinbase.FullShardKey), uint32(0x000003ff))
	check("CoinbaseAmount", blockHeader.CoinbaseAmount.GetBalanceMap()[1], testU256(1))
	check("CoinbaseAmount", blockHeader.CoinbaseAmount.GetBalanceMap()[2], testU256(2))
	check("CoinbaseAmount", len(blockHeader.CoinbaseAmount.GetBalanceMap()), 2)
	check("ParentHash", blockHeader.ParentHash, common.HexToHash("0000000000000000000000000000000000000000000000000000000000000001"))
	check("PrevRootBlockHash", blockHeader.PrevRootBlockHash, common.HexToHash("0000000000000000000000000000000000000000000000000000000000000002"))
	check("GasLimit", blockHeader.GasLimit.Value.Uint64(), uint64(4))
	check("MetaHash", blockHeader.MetaHash, common.HexToHash("0000000000000000000000000000000000000000000000000000000000000003"))
	check("Time", blockHeader.Time, uint64(5))
	check("Difficulty", blockHeader.Difficulty, big.NewInt(6))
	check("Nonce", blockHeader.Nonce, uint64(7))
	check("Bloom", common.Bytes2Hex(blockHeader.Bloom[:]), "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001")
	check("Extra", common.Bytes2Hex(blockHeader.Extra), "010203")
	check("MixDigest", common.Bytes2Hex(blockHeader.MixDigest.Bytes()), "0000000000000000000000000000000000000000000000000000000000000004")
	check("Hash", common.Bytes2Hex(blockHeader.Hash().Bytes()), "b0b7dfab9a8f485ea97a4642cdd380182ede101a64ecb3e73eb211496153d869")
	check("serialize", hex.EncodeToString(bytes), hex.EncodeToString(blocHeaderEnc))

	blocMetaEnc := common.FromHex("a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba495df227f34313c2bc4a4a986817ea46437f049873f2fca8e2b89b1ecd0f9e67a280000000000000000000000000000000000000000000000000000000000000064000000000000000000000000000000000000000000000000000000000000012c0000000000000001000000000000000200000000000000030000000000000000000000000000000000000000000000000000000000000190")
	var blockMeta MinorBlockMeta
	bb = serialize.NewByteBuffer(blocMetaEnc)
	if err := serialize.Deserialize(bb, &blockMeta); err != nil {
		t.Fatal("Deserialize error: ", err)
	}

	bytes, err = serialize.SerializeToBytes(blockMeta)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	check("TxHash", blockMeta.TxHash[:], common.FromHex("a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97"))
	check("Root", blockMeta.Root[:], common.FromHex("297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba495"))
	check("ReceiptHash", blockMeta.ReceiptHash[:], common.FromHex("df227f34313c2bc4a4a986817ea46437f049873f2fca8e2b89b1ecd0f9e67a28"))
	check("GasUsed", *blockMeta.GasUsed, serialize.Uint256{Value: big.NewInt(100)})
	check("CrossShardGasUsed", *blockMeta.CrossShardGasUsed, serialize.Uint256{Value: big.NewInt(300)})
	check("xshard_tx_cursor_info", blockMeta.XShardTxCursorInfo.RootBlockHeight, uint64(1))
	check("xshard_tx_cursor_info", blockMeta.XShardTxCursorInfo.MinorBlockIndex, uint64(2))
	check("xshard_tx_cursor_info", blockMeta.XShardTxCursorInfo.XShardDepositIndex, uint64(3))
	check("evm_xshard_gas_limit", blockMeta.XShardGasLimit.Value.Uint64(), uint64(400))
	check("bmserialize", bytes, blocMetaEnc)

	signer := NewQKCSigner(1, 1)
	key, _ := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	transactionsEnc := common.FromHex("00000002000000006df86b80808094b94f5374fce5edbc8e2a8697c15331677e6ebf0b808001840000000084000000008080801ba0d7265f92d763da5e2ea5016b837bf56f5bf42d22aead9ad5e7be2ddf01efcc68a07159634972d77349a76108c6db0634ea7b65768881b152c656deca190df6e427000000006ff86d03018207d094b94f5374fce5edbc8e2a8697c15331677e6ebf0b0a8001840000000084000000008080801ba01e681d99a80f28640faa7e224823dd133ffbd59731e3c7009f4375134a4bd58ea0089addb6d4ca918d12471682a9e5f9d03f0738358a72e493a075519cb07cf34f")
	var trans Transactions
	bb = serialize.NewByteBuffer(transactionsEnc)
	if err := serialize.DeserializeWithTags(bb, &trans, serialize.Tags{ByteSizeOfSliceLen: 4}); err != nil {
		t.Fatal("Deserialize error: ", err)
	}

	bytes = nil
	err = serialize.SerializeWithTags(&bytes, trans, serialize.Tags{ByteSizeOfSliceLen: 4})
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	tx1, _ = SignTx(tx1, signer, key)
	tx2, _ = SignTx(tx2, signer, key)
	check("len(Transactions)", len(trans), 2)
	check("Transactions[0].Hash", common.Bytes2Hex(trans[0].Hash().Bytes()), common.Bytes2Hex(tx1.Hash().Bytes()))
	check("Transactions[1]", common.Bytes2Hex(trans[1].Hash().Bytes()), common.Bytes2Hex(tx2.Hash().Bytes()))
	check("txserialize", common.Bytes2Hex(bytes), common.Bytes2Hex(transactionsEnc))

	blockEnc := append(blocHeaderEnc, append(blocMetaEnc, append(transactionsEnc, common.Hex2Bytes("00020102")...)...)...)
	var block MinorBlock
	bb = serialize.NewByteBuffer(blockEnc)
	if err := serialize.Deserialize(bb, &block); err != nil {
		t.Fatal("Deserialize error: ", err)
	}

	bytes, err = serialize.SerializeToBytes(&block)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	check("header", block.header, &blockHeader)
	check("meta", block.meta, &blockMeta)
	check("transactions", block.transactions.Len(), trans.Len())
	check("transactions[0]", block.transactions[0].Hash(), trans[0].Hash())
	check("transactions[1]", block.transactions[1].Hash(), trans[1].Hash())
	check("trackingdata", common.Bytes2Hex(block.trackingdata), "0102")
	check("blockhash", common.Bytes2Hex(block.Hash().Bytes()), "b0b7dfab9a8f485ea97a4642cdd380182ede101a64ecb3e73eb211496153d869")
	check("serialize", common.Bytes2Hex(bytes), common.Bytes2Hex(blockEnc))

}

func TestMinorBlockMetaCopyDoesNotAliasCursor(t *testing.T) {
	cursor := &XShardTxCursorInfo{RootBlockHeight: 1, MinorBlockIndex: 2, XShardDepositIndex: 3}
	meta := getDefaultMinorBlockMeta()
	meta.XShardTxCursorInfo = cursor
	copy := CopyMinorBlockMeta(meta)
	copy.XShardTxCursorInfo.RootBlockHeight = 9
	if cursor.RootBlockHeight != 1 {
		t.Fatalf("copy mutated source cursor: got %d, want 1", cursor.RootBlockHeight)
	}
}

func TestCalculateMerkleRoot(t *testing.T) {
	encList := [][]byte{
		common.FromHex("00000001000000010000000000000002d3f86deb4a2bbf85048b3e790460c40dbab1f621000003ff00000002010101010102010200000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000030000000000000005010600000000000000070000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100030102030000000000000000000000000000000000000000000000000000000000000004"),
		common.FromHex("0000000100000001000000000000006fd3f86deb4a2bbf85048b3e790460c40dbab1f621000003ff00000002010101010102010200000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000030000000000000005010600000000000000070000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100030102030000000000000000000000000000000000000000000000000000000000000004"),
	}
	list := make([]*MinorBlockHeader, 0)
	for _, bytes := range encList {
		var blockHeader MinorBlockHeader
		bb := serialize.NewByteBuffer(bytes)
		if err := serialize.Deserialize(bb, &blockHeader); err != nil {
			t.Fatal("Deserialize error: ", err)
		}
		list = append(list, &blockHeader)
	}
	check := func(f string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}

	check("header", list[0].Hash().Hex(), "0xb0b7dfab9a8f485ea97a4642cdd380182ede101a64ecb3e73eb211496153d869")
	check("header", list[1].Hash().Hex(), "0xc1eaf394ed0b62b881e163c5399ad6342e753e72a6f585cc75a18b06dd45a59c")
	check("merkleRootHash", CalculateMerkleRoot(list).Hex(), "0xf175a1f35419972b352b2e2a7bbba6a6ade1c5a59da57114b23438bd3dbf82f2")
}

func TestNewMinorBlockEmptyDerivedFields(t *testing.T) {
	header, meta := testMinorBlockHeader()
	header.Bloom[0] = 1
	block := NewMinorBlock(header, meta, nil, nil, nil)
	wantTxRoot := common.HexToHash("0xdaa77426c30c02a43d9fba4e841a6556c524d47030762eb14dc4af897e605d9b")
	if got := block.TxHash(); got != wantTxRoot {
		t.Fatalf("empty transaction root mismatch: got %s, want %s", got, wantTxRoot)
	}
	wantReceiptRoot := common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	if got := block.ReceiptHash(); got != wantReceiptRoot {
		t.Fatalf("empty receipt root mismatch: got %s, want %s", got, wantReceiptRoot)
	}
	if got := block.Bloom(); got != (Bloom{}) {
		t.Fatalf("empty receipt bloom mismatch: got %x", got)
	}
	if got, want := block.MetaHash(), block.Meta().Hash(); got != want {
		t.Fatalf("meta hash mismatch: got %s, want %s", got, want)
	}
}

// Golden bytes and hashes below come from pyquarkchain, over the two
// transactions goldenTxs builds:
//
//	PrependedSizeListSerializer(4, TypedTransaction).serialize(tx_list, bytearray())
//	TypedTransaction(SerializedEvmTransaction.from_evm_tx(tx)).get_hash()
//
// Regenerate with (from a pyquarkchain checkout, PYTHONPATH=.):
//
//	from quarkchain.core import TypedTransaction, SerializedEvmTransaction, PrependedSizeListSerializer
//	from quarkchain.evm.transactions import Transaction as EvmTransaction
const (
	// Each envelope is the type tag, the 4-byte payload length, then the RLP.
	goldenTxEnvelope1 = "00" + "0000002e" +
		"ed03018207d094b94f5374fce5edbc8e2a8697c15331677e6ebf0b0a800184000000008400000000808080808080"
	goldenTxEnvelope2 = "00" + "00000040" +
		"f83e070282753094b94f5374fce5edbc8e2a8697c15331677e6ebf0b8203e88301020303840000ffff8400010000" +
		"8230398204d2801c84112233448455667788"

	// The list is the 4-byte element count, then the envelopes.
	goldenTxList0 = "00000000"
	goldenTxList1 = "00000001" + goldenTxEnvelope1
	goldenTxList2 = "00000002" + goldenTxEnvelope1 + goldenTxEnvelope2

	goldenTxHash1 = "0xaf954ea79670ca209a526ca8b4fd276f3454983c930cb1bec25768da566d175c"
	goldenTxHash2 = "0xc1ae9851cb45420191f9ddca329041770808df0b715c7ebf4ad744806858dc61"
)

// goldenTxs builds the two envelopes the goldens above were generated from: an
// unsigned single-shard transfer, and a signed cross-shard one carrying data and
// non-default token ids.
func goldenTxs() []*Transaction {
	to := account.BytesToIdentityRecipient(common.Hex2Bytes("b94f5374fce5edbc8e2a8697c15331677e6ebf0b"))

	tx1 := NewEvmTransaction(3, to, big.NewInt(10), 2000, big.NewInt(1), 0, 0, 1, 0, nil, 0, 0)
	tx2 := NewEvmTransaction(7, to, big.NewInt(1000), 30000, big.NewInt(2), 0xFFFF, 0x10000, 3, 0,
		[]byte{1, 2, 3}, 12345, 1234)
	tx2.SetVRS(big.NewInt(28), big.NewInt(0x11223344), big.NewInt(0x55667788))

	return []*Transaction{tx1, tx2}
}

// testMinorBlockHeader returns a header and meta with every pointer field
// populated, so the block below serializes on its real field layout rather than
// on zero-value substitutes.
func testMinorBlockHeader() (*MinorBlockHeader, *MinorBlockMeta) {
	meta := &MinorBlockMeta{
		TxHash:             common.HexToHash("0x01"),
		Root:               common.HexToHash("0x02"),
		ReceiptHash:        common.HexToHash("0x03"),
		GasUsed:            &serialize.Uint256{Value: big.NewInt(21000)},
		CrossShardGasUsed:  &serialize.Uint256{Value: new(big.Int)},
		XShardTxCursorInfo: &XShardTxCursorInfo{RootBlockHeight: 1},
		XShardGasLimit:     &serialize.Uint256{Value: big.NewInt(6000000)},
	}
	header := &MinorBlockHeader{
		Version:           0,
		Branch:            account.NewBranch(0x00000001),
		Number:            5,
		Coinbase:          account.CreatEmptyAddress(0x00000001),
		CoinbaseAmount:    qkcCommon.NewTokenBalancesWithMap(map[uint64]*uint256.Int{35760: uint256.NewInt(100)}),
		ParentHash:        common.HexToHash("0x04"),
		PrevRootBlockHash: common.HexToHash("0x05"),
		GasLimit:          &serialize.Uint256{Value: big.NewInt(12000000)},
		MetaHash:          meta.Hash(),
		Time:              1519147489,
		Difficulty:        big.NewInt(1000000),
		Nonce:             0,
		Extra:             []byte("qkc"),
	}
	return header, meta
}

// TestMinorBlockTxListEncoding pins the tx_list segment of a serialized minor
// block to pyquarkchain's, for 0, 1 and 2 transactions. This is the contract the
// envelope exists for: the 4-byte element count, then per element the type tag
// and the 4-byte-prefixed RLP payload.
func TestMinorBlockTxListEncoding(t *testing.T) {
	txs := goldenTxs()
	header, meta := testMinorBlockHeader()

	// The block serializes header, meta, tx_list, tracking_data in that order, so
	// the head is fixed across the cases and only the tx_list segment varies.
	var head []byte
	if err := serialize.Serialize(&head, header); err != nil {
		t.Fatalf("serialize header: %v", err)
	}
	if err := serialize.Serialize(&head, meta); err != nil {
		t.Fatalf("serialize meta: %v", err)
	}

	tests := []struct {
		name string
		txs  []*Transaction
		want string
	}{
		{"empty", nil, goldenTxList0},
		{"one transaction", txs[:1], goldenTxList1},
		{"two transactions", txs, goldenTxList2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			block := NewMinorBlockWithHeader(header, meta).WithBody(test.txs, nil)
			got, err := serialize.SerializeToBytes(block)
			if err != nil {
				t.Fatalf("serialize block: %v", err)
			}
			// Trailing 0x0000 is the empty tracking_data (PrependedSizeBytes(2)).
			want := append(append(bytes.Clone(head), common.FromHex(test.want)...), 0x00, 0x00)
			if !bytes.Equal(got, want) {
				t.Errorf("serialized block\n got %x\nwant %x", got, want)
			}
		})
	}
}

// TestMinorBlockTxListRoundTrip checks the decode half: a block's transactions
// survive serialize/deserialize with their EVM payloads intact.
func TestMinorBlockTxListRoundTrip(t *testing.T) {
	header, meta := testMinorBlockHeader()
	want := goldenTxs()

	enc, err := serialize.SerializeToBytes(NewMinorBlockWithHeader(header, meta).WithBody(want, []byte("tracking")))
	if err != nil {
		t.Fatalf("serialize block: %v", err)
	}
	var got MinorBlock
	if err := serialize.DeserializeFromBytes(enc, &got); err != nil {
		t.Fatalf("deserialize block: %v", err)
	}

	if got.Hash() != header.Hash() {
		t.Errorf("block hash = %s, want %s", got.Hash(), header.Hash())
	}
	if string(got.TrackingData()) != "tracking" {
		t.Errorf("TrackingData = %q, want %q", got.TrackingData(), "tracking")
	}
	if len(got.Transactions()) != len(want) {
		t.Fatalf("decoded %d transactions, want %d", len(got.Transactions()), len(want))
	}
	for i, tx := range got.Transactions() {
		if tx.Type() != EvmTxType {
			t.Errorf("tx %d: type = %d, want %d", i, tx.Type(), EvmTxType)
		}
		if tx.Hash() != want[i].Hash() {
			t.Errorf("tx %d hash = %s, want %s", i, tx.Hash(), want[i].Hash())
		}
		if got, want := tx.Nonce(), want[i].Nonce(); got != want {
			t.Errorf("tx %d nonce = %d, want %d", i, got, want)
		}
		if got, want := tx.Data(), want[i].Data(); !bytes.Equal(got, want) {
			t.Errorf("tx %d data = %x, want %x", i, got, want)
		}
	}
}

// TestTypedTransactionHash pins the envelope hash — sha3_256 over the serialized
// envelope, not over the inner RLP — against pyquarkchain's
// TypedTransaction.get_hash().
func TestTypedTransactionHash(t *testing.T) {
	txs := goldenTxs()
	for i, want := range []string{goldenTxHash1, goldenTxHash2} {
		if got := txs[i].Hash(); got != common.HexToHash(want) {
			t.Errorf("tx %d envelope hash\n got %s\nwant %s", i, got.Hex(), want)
		}
	}
}

func TestMinorBlockMutationInvalidatesCaches(t *testing.T) {
	header, meta := testMinorBlockHeader()
	block := NewMinorBlockWithHeader(header, meta)
	originalHash := block.Hash()
	block.Size()

	block.Header().SetNonce(1)
	if block.Hash() != originalHash {
		t.Fatal("Header exposed the block's internal header")
	}
	sealedHeader := block.Header()
	sealed := block.WithSeal(sealedHeader)
	sealedHeader.Difficulty.SetInt64(2)
	if sealed.Difficulty().Cmp(big.NewInt(1_000_000)) != 0 {
		t.Fatal("WithSeal retained mutable header fields")
	}

	block.AddTx(goldenTxs()[0])
	if block.hash.Load() != nil {
		t.Fatal("AddTx did not clear the hash cache")
	}
	if block.size.Load() != nil {
		t.Fatal("AddTx did not clear the size cache")
	}
	block.Size()
	block.Finalize(nil, common.Hash{}, nil, nil, nil, nil)
	if block.size.Load() != nil {
		t.Fatal("Finalize did not clear the size cache")
	}

	cursor := &XShardTxCursorInfo{RootBlockHeight: 1, MinorBlockIndex: 2, XShardDepositIndex: 3}
	block.Finalize(nil, common.Hash{}, nil, nil, nil, cursor)
	wantMetaHash := block.MetaHash()
	wantHash := block.Hash()
	cursor.RootBlockHeight = 9
	if got := block.Meta().XShardTxCursorInfo.RootBlockHeight; got != 1 {
		t.Fatalf("Finalize retained caller's cursor: got height %d, want 1", got)
	}
	if block.MetaHash() != wantMetaHash || block.Hash() != wantHash {
		t.Fatal("caller cursor mutation changed finalized block hashes")
	}

	gasUsed := big.NewInt(11)
	xShardGasUsed := big.NewInt(12)
	coinbaseAmount := qkcCommon.NewEmptyTokenBalances()
	coinbaseAmount.SetValue(uint256.NewInt(13), 1)
	block.Finalize(nil, common.Hash{}, gasUsed, xShardGasUsed, coinbaseAmount, nil)
	wantMetaHash = block.MetaHash()
	wantHash = block.Hash()
	gasUsed.SetInt64(21)
	xShardGasUsed.SetInt64(22)
	coinbaseAmount.SetValue(uint256.NewInt(23), 1)
	if block.GasUsed().Cmp(big.NewInt(11)) != 0 || block.CrossShardGasUsed().Cmp(big.NewInt(12)) != 0 {
		t.Fatal("Finalize retained caller-owned gas values")
	}
	if got := block.CoinbaseAmount().GetBalanceMap()[1]; got.Cmp(uint256.NewInt(13)) != 0 {
		t.Fatalf("Finalize retained caller-owned coinbase amount: got %v, want 13", got)
	}
	if block.MetaHash() != wantMetaHash || block.Hash() != wantHash {
		t.Fatal("caller finalization input mutation changed finalized block hashes")
	}
}

func TestMinorBlockDeserializeClearsHash(t *testing.T) {
	header, meta := testMinorBlockHeader()
	block := NewMinorBlockWithHeader(header, meta)
	oldHash := block.Hash()

	header.Nonce++
	encoded, err := serialize.SerializeToBytes(NewMinorBlockWithHeader(header, meta))
	if err != nil {
		t.Fatal(err)
	}
	if err := serialize.DeserializeFromBytes(encoded, block); err != nil {
		t.Fatal(err)
	}
	if block.Hash() == oldHash || block.Hash() != header.Hash() {
		t.Fatal("Deserialize retained the previous hash cache")
	}
}
