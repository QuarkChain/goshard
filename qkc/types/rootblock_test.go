// Copyright 2026-2027, QuarkChain.

// Root block tests exercise pyquarkchain-compatible QKC wire bytes.

package types

import (
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

func TestRootBlockEncoding(t *testing.T) {
	rootBlockHeaderEnc := common.FromHex("0000000100000002a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba4950000000000000000000000000000000000000000000000000000000000000000d3f86deb4a2bbf85048b3e790460c40dbab1f621000003ff00000002010101010102010200000000009896800227100227100000000000000064000401020304df227f34313c2bc4a4a986817ea46437f049873f2fca8e2b89b1ecd0f9e67a28c758a15769202219b1fce50049eeac1af1dddb28bc282c1fb79a2208fa24f763308b1b191d656a5123ac979067a6c941867f3000d978a5d34810fe6c194dc38101")
	var blockHeader RootBlockHeader
	bb := serialize.NewByteBuffer(rootBlockHeaderEnc)
	if err := serialize.Deserialize(bb, &blockHeader); err != nil {
		t.Fatal("Deserialize error: ", err)
	}

	bytes, err := serialize.SerializeToBytes(&blockHeader)

	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	key, _ := crypto.HexToECDSA("c987d4506fb6824639f9a9e3b8834584f5165e94680501d1b0044071cd36c3b3")

	check := func(f string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}
	check("Version", blockHeader.Version, uint32(1))
	check("Number", blockHeader.Number, uint32(2))
	check("ParentHash", common.Bytes2Hex(blockHeader.ParentHash.Bytes()), "a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97")
	check("MinorHeaderHash", common.Bytes2Hex(blockHeader.MinorHeaderHash.Bytes()), "297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba495")
	check("coinbase_Recipient", common.Bytes2Hex(blockHeader.Coinbase.Recipient[:]), "d3f86deb4a2bbf85048b3e790460c40dbab1f621")
	check("coinbase_FullShardKey", uint32(blockHeader.Coinbase.FullShardKey), uint32(1023))
	check("CoinbaseAmount", blockHeader.CoinbaseAmount.GetBalanceMap()[1], testU256(1))
	check("CoinbaseAmount", blockHeader.CoinbaseAmount.GetBalanceMap()[2], testU256(2))
	check("Time", blockHeader.Time, uint64(10000000))
	check("Difficulty", blockHeader.Difficulty, big.NewInt(10000))
	check("TotalDifficulty", blockHeader.TotalDifficulty, big.NewInt(10000))
	check("Nonce", blockHeader.Nonce, uint64(100))
	check("Extra", common.Bytes2Hex(blockHeader.Extra), "01020304")
	check("MixDigest", common.Bytes2Hex(blockHeader.MixDigest.Bytes()), "df227f34313c2bc4a4a986817ea46437f049873f2fca8e2b89b1ecd0f9e67a28")
	check("Hash", common.Bytes2Hex(blockHeader.Hash().Bytes()), "725576c58f70f22166767d41d50fd1e22d2913524f967bf1a7fc020cb0e19b10")
	check("Hash", common.Bytes2Hex(blockHeader.Hash().Bytes()), "725576c58f70f22166767d41d50fd1e22d2913524f967bf1a7fc020cb0e19b10")
	check("serialize", common.Bytes2Hex(bytes), common.Bytes2Hex(rootBlockHeaderEnc))

	minorBlockHeadersEnc := common.FromHex("0000000200000457000000010000000000002b67d3f86deb4a2bbf85048b3e790460c40dbab1f621000003ff0000000201010101010201020000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000003000000000000000501060000000000000007000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010003010203000000000000000000000000000000000000000000000000000000000000000400000457000000010000000000a98ac7d3f86deb4a2bbf85048b3e790460c40dbab1f621000003ff00000002010101010102010200000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000030000000000000005010600000000000000070000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100030102030000000000000000000000000000000000000000000000000000000000000004")
	var headers MinorBlockHeaders
	bb = serialize.NewByteBuffer(minorBlockHeadersEnc)
	if err := serialize.DeserializeWithTags(bb, &headers, serialize.Tags{ByteSizeOfSliceLen: 4}); err != nil {
		t.Fatal("Deserialize error: ", err)
	}

	bytes = nil
	err = serialize.SerializeWithTags(&bytes, headers, serialize.Tags{ByteSizeOfSliceLen: 4})
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	check("len(headers)", len(headers), 2)
	check("headers[0].Hash", common.Bytes2Hex(headers[0].Hash().Bytes()), "cfe6b217b566f12e7568d46c47de85d13193902eafb8f39d9d56ae725cf11f7f")
	check("headers[1].Hash", common.Bytes2Hex(headers[1].Hash().Bytes()), "1245f631e4ce43188fd9412d1fcab34db8c62f5728d0d54550d1a0dc67617f01")
	check("serialize", common.Bytes2Hex(bytes), common.Bytes2Hex(minorBlockHeadersEnc))

	blockEnc := append(rootBlockHeaderEnc, append(minorBlockHeadersEnc, common.Hex2Bytes("00020102")...)...)
	var block RootBlock
	bb = serialize.NewByteBuffer(blockEnc)
	if err := serialize.Deserialize(bb, &block); err != nil {
		t.Fatal("Deserialize error: ", err)
	}

	bytes, err = serialize.SerializeToBytes(&block)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	block.SignWithPrivateKey(key)
	check("header", block.header, &blockHeader)
	check("headers", block.minorBlockHeaders.Len(), headers.Len())
	check("headers[0]", block.minorBlockHeaders[0].Hash(), headers[0].Hash())
	check("headers[1]", block.minorBlockHeaders[1].Hash(), headers[1].Hash())
	check("trackingdata", common.Bytes2Hex(block.trackingdata), "0102")
	check("Signature", common.Bytes2Hex(blockHeader.Signature[:]), "c758a15769202219b1fce50049eeac1af1dddb28bc282c1fb79a2208fa24f763308b1b191d656a5123ac979067a6c941867f3000d978a5d34810fe6c194dc38101")
	check("blockhash", common.Bytes2Hex(block.Hash().Bytes()), "725576c58f70f22166767d41d50fd1e22d2913524f967bf1a7fc020cb0e19b10")
	check("serialize", common.Bytes2Hex(bytes), common.Bytes2Hex(blockEnc))

}

func TestNewRootBlockEmptyMinorHeaderRoot(t *testing.T) {
	block := NewRootBlock(&RootBlockHeader{}, nil, nil)
	want := common.HexToHash("0xdaa77426c30c02a43d9fba4e841a6556c524d47030762eb14dc4af897e605d9b")
	if got := block.MinorHeaderHash(); got != want {
		t.Fatalf("empty minor header root mismatch: got %s, want %s", got, want)
	}
}

func TestRootBlockSignRefreshesHash(t *testing.T) {
	block := NewRootBlockWithHeader(&RootBlockHeader{
		CoinbaseAmount:  qkcCommon.NewEmptyTokenBalances(),
		Difficulty:      big.NewInt(1),
		TotalDifficulty: big.NewInt(1),
	})
	unsignedHash := block.Hash()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := block.SignWithPrivateKey(key); err != nil {
		t.Fatal(err)
	}
	if got, want := block.Hash(), block.Header().Hash(); got != want {
		t.Fatalf("cached hash mismatch: got %x, want %x", got, want)
	}
	if block.Hash() == unsignedHash {
		t.Fatal("signing did not refresh the cached hash")
	}
	if !block.header.VerifySignature(key.PublicKey) {
		t.Fatal("generated signature did not verify")
	}
}

func TestRootBlockWithBodyTrackingData(t *testing.T) {
	block := NewRootBlockWithHeader(&RootBlockHeader{}).WithBody(nil, []byte{1, 2, 3})
	if got, want := block.TrackingData(), []byte{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tracking data mismatch: got %x, want %x", got, want)
	}
}

func TestRootBlockMutationInvalidatesCaches(t *testing.T) {
	header := &RootBlockHeader{
		CoinbaseAmount:  qkcCommon.NewEmptyTokenBalances(),
		Difficulty:      big.NewInt(1),
		TotalDifficulty: big.NewInt(1),
	}
	block := NewRootBlockWithHeader(header)
	originalHash := block.Hash()
	block.Size()

	block.IHeader().SetNonce(1)
	if block.Hash() != originalHash {
		t.Fatal("IHeader exposed the block's internal header")
	}
	sealedHeader := block.Header()
	sealed := block.WithSeal(sealedHeader)
	sealedHeader.Difficulty.SetInt64(2)
	if sealed.Difficulty().Cmp(big.NewInt(1)) != 0 {
		t.Fatal("WithSeal retained mutable header fields")
	}

	minorHeader, _ := testMinorBlockHeader()
	block.AddMinorBlockHeader(minorHeader)
	if block.size.Load() != nil {
		t.Fatal("AddMinorBlockHeader did not clear the size cache")
	}
	block.Size()
	block.Finalize(nil, nil, common.Hash{})
	if block.size.Load() != nil {
		t.Fatal("Finalize did not clear the size cache")
	}
}

func TestRootBlockDeserializeClearsHash(t *testing.T) {
	header := &RootBlockHeader{
		CoinbaseAmount:  qkcCommon.NewEmptyTokenBalances(),
		Difficulty:      big.NewInt(1),
		TotalDifficulty: big.NewInt(1),
	}
	block := NewRootBlockWithHeader(header)
	oldHash := block.Hash()

	header.Nonce++
	encoded, err := serialize.SerializeToBytes(NewRootBlockWithHeader(header))
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

func TestDataSize(t *testing.T) {
	check := func(f string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}
	var rootBlockHeader RootBlockHeader
	rootBlockHeaderBytes, err := serialize.SerializeToBytes(&rootBlockHeader)

	if err != nil {
		t.Fatal("Serialize error: ", err)
	}
	var minorBlockHeader MinorBlockHeader
	minorBlockHeaderBytes, err := serialize.SerializeToBytes(&minorBlockHeader)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}
	var minorBlockMeta MinorBlockMeta
	minorBlockMetaBytes, err := serialize.SerializeToBytes(&minorBlockMeta)
	if err != nil {
		t.Fatal("Serialize error: ", err)
	}

	check("RootBlockHeader", len(rootBlockHeaderBytes), 249)
	check("MinorBlockHeader", len(minorBlockHeaderBytes), 479)
	check("MinorBlockMeta", len(minorBlockMetaBytes), 216)
}

func TestRootBlockHeaderSignature(t *testing.T) {
	check := func(f string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}
	checkErr := func(f string, got, want interface{}) {
		if reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		t.Errorf("GenerateKey err:%v", err)
	}

	var rootBlockHeader RootBlockHeader
	rootBlock := NewRootBlockWithHeader(&rootBlockHeader)
	check("rootBlockHeader Signature ", rootBlockHeader.Signature, [65]byte{})
	checkErr("", rootBlockHeader.VerifySignature(privateKey.PublicKey), true)
	rootBlock.SignWithPrivateKey(privateKey)
	checkErr("rootBlockHeader Signature ", rootBlock.header.Signature, [65]byte{})
	check("", rootBlock.header.VerifySignature(privateKey.PublicKey), true)

}

/*
Py code to generate data:

 header=RootBlockHeader()
        header.version=1
        header.height=2
        header.hash_prev_block=bytes.fromhex("a40920ae6f758f88c61b405f9fc39fdd6274666462b14e3887522166e6537a97")
        header.hash_merkle_root=bytes.fromhex("297d6ae9803346cdb059a671dea7e37b684dcabfa767f2d872026ad0a3aba495")
        header.coinbase_address=Address.create_from(bytes.fromhex("d3f86deb4a2bbf85048b3e790460c40dbab1f621000003ff"))
        header.coinbase_amount=1000
        header.create_time=10000000
        header.difficulty=10000
        header.total_difficulty=10000
        header.nonce=100
        header.extra_data=bytes.fromhex("01020304")
        header.mixhash=bytes.fromhex("df227f34313c2bc4a4a986817ea46437f049873f2fca8e2b89b1ecd0f9e67a28")
        privkey = KeyAPI.PrivateKey(
            private_key_bytes=bytes.fromhex("c987d4506fb6824639f9a9e3b8834584f5165e94680501d1b0044071cd36c3b3")
        )

        header.sign_with_private_key(privkey)
        data=header.serialize()
        print("data",len(data),data.hex())
        print("hash",header.get_hash().hex())
        print("sigb",header.signature.hex())
*/

// Golden values produced by pyquarkchain (the compatibility source of truth). See
// qkc/config/singularity/README.md for the regeneration one-liner; the synthetic
// case is built directly in quarkchain.core.RootBlockHeader with every field
// non-zero. These pin both the serialized bytes (so a layout regression is caught
// precisely) and the resulting hash/seal-hash.
func rep(b byte, n int) []byte {
	s := make([]byte, n)
	for i := range s {
		s[i] = b
	}
	return s
}

func sig65(b byte) (s [65]byte) {
	copy(s[:], rep(b, 65))
	return s
}

func TestRootBlockHeaderSerializeAndHash(t *testing.T) {
	cases := []struct {
		name     string
		header   *RootBlockHeader
		ser      string
		hash     string
		sealHash string
	}{
		{
			// The real QuarkChain mainnet (singularity) root genesis, built exactly
			// as qkc.CreateRootBlock builds it from ROOT.GENESIS.
			name: "mainnet_root_genesis",
			header: &RootBlockHeader{
				Time:            1556639999,
				Coinbase:        account.CreatEmptyAddress(0),
				CoinbaseAmount:  qkcCommon.NewEmptyTokenBalances(),
				Difficulty:      big.NewInt(10000000000000),
				TotalDifficulty: big.NewInt(10000000000000),
			},
			ser:      "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005cc870ff0609184e72a0000609184e72a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			hash:     "4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51",
			sealHash: "e7dcdecc09e724ad81e493d70dedcd6d9ea0ee830d7ab2528a5648f2a0cf8178",
		},
		{
			// The QuarkChain devnet (singularity) root genesis.
			name: "devnet_root_genesis",
			header: &RootBlockHeader{
				Time:            1556639999,
				Coinbase:        account.CreatEmptyAddress(0),
				CoinbaseAmount:  qkcCommon.NewEmptyTokenBalances(),
				Difficulty:      big.NewInt(100000),
				TotalDifficulty: big.NewInt(100000),
			},
			ser:      "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005cc870ff030186a0030186a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			hash:     "5ad443efb7cf5246a3d1bbc1734bd02bf3a5d83bedeccfcfe707d0ebee03780d",
			sealHash: "055a7b410a50c098c52c123983e3596c5914ba227bf9e2c0c93309ff8f650d41",
		},
		{
			// Every field non-zero, including a multi-entry coinbase amount map
			// with a zero balance (token id 2) that must be skipped.
			name: "synthetic_all_fields",
			header: &RootBlockHeader{
				Version:         1,
				Number:          2,
				ParentHash:      common.BytesToHash(rep(0x01, 32)),
				MinorHeaderHash: common.BytesToHash(rep(0x02, 32)),
				Root:            common.BytesToHash(rep(0x03, 32)),
				Coinbase:        account.NewAddress(common.BytesToAddress(rep(0xaa, 20)), 0x00010001),
				CoinbaseAmount: qkcCommon.NewTokenBalancesWithMap(map[uint64]*uint256.Int{
					1:       uint256.NewInt(100),
					2:       uint256.NewInt(0),
					1000000: uint256.NewInt(999),
				}),
				Time:            1600000000,
				Difficulty:      big.NewInt(1000000),
				TotalDifficulty: big.NewInt(2000000),
				Nonce:           42,
				Extra:           []byte("hello"),
				MixDigest:       common.BytesToHash(rep(0x04, 32)),
				Signature:       sig65(0x05),
			},
			ser:      "0000000100000002010101010101010101010101010101010101010101010101010101010101010102020202020202020202020202020202020202020202020202020202020202020303030303030303030303030303030303030303030303030303030303030303aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa000100010000000201010164030f42400203e7000000005f5e1000030f4240031e8480000000000000002a000568656c6c6f04040404040404040404040404040404040404040404040404040404040404040505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505",
			hash:     "316595066fd9795df3ba42b3f0e6b3da03bf0f75e14a9f425270095a8768ed4c",
			sealHash: "32e26c6a06cbb68460f4b2c79379e157ac62373cf5f0a7175079ac19d80a9c93",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := serialize.SerializeToBytes(tc.header)
			if err != nil {
				t.Fatalf("serialize: %v", err)
			}
			if h := hex.EncodeToString(got); h != tc.ser {
				t.Errorf("serialized bytes mismatch\n got %s\nwant %s", h, tc.ser)
			}
			if h := tc.header.Hash(); h != common.HexToHash(tc.hash) {
				t.Errorf("hash mismatch\n got %s\nwant 0x%s", h.Hex(), tc.hash)
			}
			if h := tc.header.SealHash(); h != common.HexToHash(tc.sealHash) {
				t.Errorf("seal hash mismatch\n got %s\nwant 0x%s", h.Hex(), tc.sealHash)
			}
		})
	}
}
