// Copyright 2026-2027, QuarkChain.

// Receipt tests exercise pyquarkchain-compatible QKC wire bytes.

package types

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	receiptPostStateHex = "df227f34313c2bc4a4a986817ea46437f049873f2fca8e2b89b1ecd0f9e67a28"
	receiptRecipientHex = "d3f86deb4a2bbf85048b3e790460c40dbab1f621"
	receiptTopic0Hex    = "a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97"
	receiptTopic1Hex    = "297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba495"
	receiptBlockHashHex = "1111111111111111111111111111111111111111111111111111111111111111"
	receiptTxHashHex    = "2222222222222222222222222222222222222222222222222222222222222222"
)

var (
	receiptBloomHex = strings.Repeat("00", 254) + "03ff"

	receiptClusterLogHex = receiptRecipientHex +
		"02" + receiptTopic0Hex + receiptTopic1Hex +
		"00000003" + "010203" +
		"000000000000000a" +
		receiptBlockHashHex +
		"00000064" +
		receiptTxHashHex +
		"000000c8"

	receiptClusterReceiptHex = "20" + receiptPostStateHex +
		"00000000000003e8" +
		"0000000000000384" +
		receiptBloomHex +
		receiptRecipientHex + "0000000a" +
		"00000001" + receiptClusterLogHex

	receiptRlpHex = "f901a2" +
		"a0" + receiptPostStateHex +
		"8203e8" +
		"b90100" + receiptBloomHex +
		"f85f" +
		"f85d94" + receiptRecipientHex +
		"f842a0" + receiptTopic0Hex +
		"a0" + receiptTopic1Hex +
		"83010203" +
		"94" + receiptRecipientHex +
		"840000000a"
)

func TestReceiptSerializing(t *testing.T) {
	// Generated from pyquarkchain's quarkchain.core.TransactionReceipt field order:
	// success, gas_used, prev_gas_used, bloom, contract_address, logs. There is no
	// leading tx_hash field. The nested log uses different block_hash and tx_hash.
	receiptEnc := common.FromHex(receiptClusterReceiptHex)
	var receipt Receipt
	bb := serialize.NewByteBuffer(receiptEnc)
	if err := serialize.Deserialize(bb, &receipt); err != nil {
		t.Fatal("Deserialize error: ", err)
	}

	bytes, err := serialize.SerializeToBytes(&receipt)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	check := func(f string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}
	check("PostState", common.Bytes2Hex(receipt.PostState), receiptPostStateHex)
	check("Status", receipt.Status, uint64(0))
	check("CumulativeGasUsed", receipt.CumulativeGasUsed, uint64(1000))
	check("Bloom", common.Bytes2Hex(receipt.Bloom.Bytes()), receiptBloomHex)
	check("Len(Logs)", len(receipt.Logs), 1)
	check("Logs[0]Address", common.Bytes2Hex(receipt.Logs[0].Address.Bytes()), receiptRecipientHex)
	check("Logs[0]Topics", len(receipt.Logs[0].Topics), 2)
	check("Logs[0]Topics[0]", common.Bytes2Hex(receipt.Logs[0].Topics[0].Bytes()), receiptTopic0Hex)
	check("Logs[0]Topics[1]", common.Bytes2Hex(receipt.Logs[0].Topics[1].Bytes()), receiptTopic1Hex)
	check("Logs[0]data", common.Bytes2Hex(receipt.Logs[0].Data), "010203")
	check("Logs[0]BlockNumber", receipt.Logs[0].BlockNumber, uint64(10))
	check("Logs[0]BlockHash", common.Bytes2Hex(receipt.Logs[0].BlockHash.Bytes()), receiptBlockHashHex)
	check("Logs[0]TxIndex", receipt.Logs[0].TxIndex, uint(100))
	check("Logs[0]TxHash", common.Bytes2Hex(receipt.Logs[0].TxHash.Bytes()), receiptTxHashHex)
	check("Logs[0]Index", receipt.Logs[0].Index, uint(200))
	check("ContractAddress", common.Bytes2Hex(receipt.ContractAddress.Bytes()), receiptRecipientHex)
	check("ContractFullShardKey", receipt.ContractFullShardKey, uint32(10))
	check("GasUsed", receipt.GasUsed, uint64(100))
	check("serialize", common.Bytes2Hex(bytes), common.Bytes2Hex(receiptEnc))

	receiptRlpEnc := common.FromHex(receiptRlpHex)
	var receiptRlp Receipt
	if err := rlp.DecodeBytes(receiptRlpEnc, &receiptRlp); err != nil {
		t.Fatal("DecodeBytes error: ", err)
	}

	bytes, err = rlp.EncodeToBytes(&receipt)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	check("PostState", common.Bytes2Hex(receiptRlp.PostState), receiptPostStateHex)
	check("Status", receiptRlp.Status, uint64(0))
	check("CumulativeGasUsed", receiptRlp.CumulativeGasUsed, uint64(1000))
	check("Bloom", common.Bytes2Hex(receiptRlp.Bloom.Bytes()), receiptBloomHex)
	check("Len(Logs)", len(receiptRlp.Logs), 1)
	check("Logs[0]Address", common.Bytes2Hex(receiptRlp.Logs[0].Address.Bytes()), receiptRecipientHex)
	check("Logs[0]Topics", len(receiptRlp.Logs[0].Topics), 2)
	check("Logs[0]Topics[0]", common.Bytes2Hex(receiptRlp.Logs[0].Topics[0].Bytes()), receiptTopic0Hex)
	check("Logs[0]Topics[1]", common.Bytes2Hex(receiptRlp.Logs[0].Topics[1].Bytes()), receiptTopic1Hex)
	check("Logs[0]data", common.Bytes2Hex(receiptRlp.Logs[0].Data), "010203")
	check("rlpContractAddress", common.Bytes2Hex(receiptRlp.ContractAddress.Bytes()), receiptRecipientHex)
	check("ContractFullShardKey", receiptRlp.ContractFullShardKey, uint32(10))
	check("rlpserialize", common.Bytes2Hex(bytes), common.Bytes2Hex(receiptRlpEnc))
}
