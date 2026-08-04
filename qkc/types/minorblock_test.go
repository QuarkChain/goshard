// Copyright 2026-2027, QuarkChain.

// Minor block body tests pin the tx_list wire segment against pyquarkchain.

package types

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/holiman/uint256"
)

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

	return []*Transaction{
		{TxType: EvmTx, EvmTx: tx1},
		{TxType: EvmTx, EvmTx: tx2},
	}
}

// testMinorBlockHeader returns a header and meta with every pointer field
// populated, so the block below serializes on its real field layout rather than
// on zero-value substitutes.
func testMinorBlockHeader() (*MinorBlockHeader, *MinorBlockMeta) {
	meta := &MinorBlockMeta{
		TxHash:            common.HexToHash("0x01"),
		Root:              common.HexToHash("0x02"),
		ReceiptHash:       common.HexToHash("0x03"),
		GasUsed:           &serialize.Uint256{Value: big.NewInt(21000)},
		CrossShardGasUsed: &serialize.Uint256{Value: new(big.Int)},
		XShardTxCursor:    XShardTxCursorInfo{RootBlockHeight: 1},
		XShardGasLimit:    &serialize.Uint256{Value: big.NewInt(6000000)},
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
			block := NewMinorBlock(header, meta, test.txs, nil)
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

	enc, err := serialize.SerializeToBytes(NewMinorBlock(header, meta, want, []byte("tracking")))
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
	if string(got.TrackingData) != "tracking" {
		t.Errorf("TrackingData = %q, want %q", got.TrackingData, "tracking")
	}
	if len(got.Transactions) != len(want) {
		t.Fatalf("decoded %d transactions, want %d", len(got.Transactions), len(want))
	}
	for i, tx := range got.Transactions {
		if tx.TxType != EvmTx {
			t.Errorf("tx %d: TxType = %d, want %d", i, tx.TxType, EvmTx)
		}
		if tx.Hash() != want[i].Hash() {
			t.Errorf("tx %d hash = %s, want %s", i, tx.Hash(), want[i].Hash())
		}
		if got, want := tx.EvmTx.Nonce(), want[i].EvmTx.Nonce(); got != want {
			t.Errorf("tx %d nonce = %d, want %d", i, got, want)
		}
		if got, want := tx.EvmTx.Data(), want[i].EvmTx.Data(); !bytes.Equal(got, want) {
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
