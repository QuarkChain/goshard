// Copyright 2026-2027, QuarkChain.

package types_test

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/rlp"
)

func TestQkcTransactionGoldenVectors(t *testing.T) {
	key, err := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	if err != nil {
		t.Fatal(err)
	}
	to := common.HexToAddress("314b2cd22c6d26618ce051a58c65af1253aecbb8")
	wantSender := common.HexToAddress("a94f5374fce5edbc8e2a8697c15331677e6ebf0b")
	tests := []struct {
		version uint32
		sigHash string
		encoded string
		txHash  string
		v       string
		r       string
		s       string
	}{
		{
			version: 0,
			sigHash: "ef858163a8d230b79ee3c88ecacf1aeee9e986c775f5580a9f87a7eca7d962b6",
			encoded: "000000007df87b0d8502540be40082753094314b2cd22c6d26618ce051a58c65af1253aecbb88203e8830102030384c47decfd84c49c1950820111820222801ca07411f92018d5a6a896a1c6dc44f6b94befac6842a70ab21b7d6020b9671d960da0083582a237ef83cdb10a38aad159dec9ce1a89e8f94b5e7f06477c00956d01da",
			txHash:  "468c2100edf184d0552b015c2acd6f5b18b7ed6e47a9d5067a4f5e72b7656b38",
			v:       "1c",
			r:       "7411f92018d5a6a896a1c6dc44f6b94befac6842a70ab21b7d6020b9671d960d",
			s:       "083582a237ef83cdb10a38aad159dec9ce1a89e8f94b5e7f06477c00956d01da",
		},
		{
			version: 1,
			sigHash: "78174832bba84588049bad3a146aefbaf5b5a6562269b1deea511e3af11b0071",
			encoded: "000000007df87b0d8502540be40082753094314b2cd22c6d26618ce051a58c65af1253aecbb88203e8830102030384c47decfd84c49c1950820111820222011ba094549817504fd4b20f4dfcd8d421a8ebe6066d7e0e4ca8ea3b57a1b819a7bba0a036e9f2344c3d61c59cc12ea883f48d948d13b3cf180d63664535b7b7bb30daea",
			txHash:  "d00136749f4f0258918b8a0d1b00ba3c103c19c8920c69a33603fba8c775e05e",
			v:       "1b",
			r:       "94549817504fd4b20f4dfcd8d421a8ebe6066d7e0e4ca8ea3b57a1b819a7bba0",
			s:       "36e9f2344c3d61c59cc12ea883f48d948d13b3cf180d63664535b7b7bb30daea",
		},
		{
			version: 2,
			sigHash: "925b972ae71821a285738c77330396e575463a75284bb2488f4c3953d5eab8ca",
			encoded: "000000007df87b0d8502540be40082753094314b2cd22c6d26618ce051a58c65af1253aecbb88203e8830102030384c47decfd84c47decfd828bb0828bb0022aa042abe33581e037399fd729e7aa65ef1db220eb35b0dd38b336f2582e9ef17b0aa06546420253313663f83a503adfd4b1e4a84194875e4f56abf143e27a725e7c34",
			txHash:  "f2c60c16855ec40d63e4a4f757f902d90fcb1ad180b7d7a7b461d289f0dd6306",
			v:       "2a",
			r:       "42abe33581e037399fd729e7aa65ef1db220eb35b0dd38b336f2582e9ef17b0a",
			s:       "6546420253313663f83a503adfd4b1e4a84194875e4f56abf143e27a725e7c34",
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("version-%d", test.version), func(t *testing.T) {
			toFullShardKey, gasTokenID, transferTokenID := uint32(0xc49c1950), uint64(0x111), uint64(0x222)
			if test.version == 2 {
				toFullShardKey = 0xc47decfd
				gasTokenID, transferTokenID = qkccommon.DefaultTokenID, qkccommon.DefaultTokenID
			}
			tx := coretypes.NewQkcTransaction(13, to, big.NewInt(1000), 30000, big.NewInt(10_000_000_000), 0xc47decfd, toFullShardKey, 3, test.version, []byte{1, 2, 3}, gasTokenID, transferTokenID)
			signer := coretypes.NewQkcSigner(3, 3)
			if got, want := signer.Hash(tx), common.HexToHash(test.sigHash); got != want {
				t.Fatalf("signature hash mismatch: got %x want %x", got, want)
			}
			signed, err := coretypes.SignTx(tx, signer, key)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := serialize.SerializeToBytes(signed)
			if err != nil {
				t.Fatal(err)
			}
			wantEncoded := common.FromHex(test.encoded)
			if !bytes.Equal(encoded, wantEncoded) {
				t.Fatalf("serialized bytes mismatch\ngot  %x\nwant %x", encoded, wantEncoded)
			}
			if got, want := signed.Hash(), common.HexToHash(test.txHash); got != want {
				t.Fatalf("transaction hash mismatch: got %x want %x", got, want)
			}
			v, r, s := signed.RawSignatureValues()
			if v.Cmp(new(big.Int).SetBytes(common.FromHex(test.v))) != 0 || r.Cmp(new(big.Int).SetBytes(common.FromHex(test.r))) != 0 || s.Cmp(new(big.Int).SetBytes(common.FromHex(test.s))) != 0 {
				t.Fatalf("signature values mismatch: V=%x R=%x S=%x", v, r, s)
			}
			from, err := coretypes.Sender(signer, signed)
			if err != nil || from != wantSender {
				t.Fatalf("sender mismatch: got %x want %x (%v)", from, wantSender, err)
			}

			typedRLP, err := rlp.EncodeToBytes(signed)
			if err != nil {
				t.Fatal(err)
			}
			var decoded coretypes.Transaction
			if err := rlp.DecodeBytes(typedRLP, &decoded); err != nil {
				t.Fatal(err)
			}
			roundTrip, err := rlp.EncodeToBytes(&decoded)
			if err != nil || !bytes.Equal(roundTrip, typedRLP) {
				t.Fatalf("typed RLP round trip: %x (%v)", roundTrip, err)
			}
		})
	}
}
