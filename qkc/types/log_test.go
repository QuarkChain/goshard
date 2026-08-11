// Copyright 2026-2027, QuarkChain.

// Log tests exercise pyquarkchain-compatible QKC wire bytes.

package types

import (
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/rlp"
)

func TestClusterLogSerializing(t *testing.T) {
	// Generated from pyquarkchain's quarkchain.core.Log field order:
	// recipient, topics, data, block_number, block_hash, tx_idx, tx_hash, log_idx.
	// block_hash and tx_hash intentionally differ to catch swapped field order.
	logEnc := common.FromHex("d3f86deb4a2bbf85048b3e790460c40dbab1f62102a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba49500000003010203000000000000000a1111111111111111111111111111111111111111111111111111111111111111000000642222222222222222222222222222222222222222222222222222222222222222000000c8")
	var log ClusterLog
	bb := serialize.NewByteBuffer(logEnc)
	if err := serialize.Deserialize(bb, &log); err != nil {
		t.Fatal("Deserialize error: ", err)
	}

	bytes, err := serialize.SerializeToBytes(&log)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	check := func(f string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}

	check("Recipient", common.Bytes2Hex(log.Recipient.Bytes()), "d3f86deb4a2bbf85048b3e790460c40dbab1f621")
	check("Topics", len(log.Topics), 2)
	check("Topics[0]", common.Bytes2Hex(log.Topics[0].Bytes()), "a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97")
	check("Topics[1]", common.Bytes2Hex(log.Topics[1].Bytes()), "297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba495")
	check("data", common.Bytes2Hex(log.Data), "010203")
	check("BlockNumber", log.BlockNumber, uint64(10))
	check("BlockHash", common.Bytes2Hex(log.BlockHash.Bytes()), "1111111111111111111111111111111111111111111111111111111111111111")
	check("TxIndex", log.TxIndex, uint32(100))
	check("TxHash", common.Bytes2Hex(log.TxHash.Bytes()), "2222222222222222222222222222222222222222222222222222222222222222")
	check("Index", log.Index, uint32(200))
	check("serialize", common.Bytes2Hex(bytes), common.Bytes2Hex(logEnc))

	coreLog := log.toCoreLog()
	logRlpEnc, err := rlp.EncodeToBytes(coreLog)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	var logRlp coretypes.Log
	if err := rlp.DecodeBytes(logRlpEnc, &logRlp); err != nil {
		t.Fatal("DecodeBytes error: ", err)
	}

	check("Encode", common.Bytes2Hex(logRlpEnc), "f85d94d3f86deb4a2bbf85048b3e790460c40dbab1f621f842a0a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97a0297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba49583010203")
	check("rlp.Address", common.Bytes2Hex(logRlp.Address.Bytes()), "d3f86deb4a2bbf85048b3e790460c40dbab1f621")
	check("rlp.Topics", len(logRlp.Topics), 2)
	check("rlp.Topics[0]", common.Bytes2Hex(logRlp.Topics[0].Bytes()), "a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97")
	check("rlp.Topics[1]", common.Bytes2Hex(logRlp.Topics[1].Bytes()), "297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba495")
	check("rlp.Data", common.Bytes2Hex(logRlp.Data), "010203")
}
