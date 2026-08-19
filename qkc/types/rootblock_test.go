// Copyright 2026-2027, QuarkChain.

package types

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/holiman/uint256"
)

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

// A root block carrying the synthetic header and one confirmed minor header,
// serialized by pyquarkchain's RootBlock.
const goldenRootBlockOneHeader = "0000000100000002010101010101010101010101010101010101010101010101010101010101010102020202020202020202020202020202020202020202020202020202020202020303030303030303030303030303030303030303030303030303030303030303aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa000100010000000201010164030f42400203e7000000005f5e1000030f4240031e8480000000000000002a000568656c6c6f04040404040404040404040404040404040404040404040404040404040404040505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505000000010000000000000001000000000000000500000000000000000000000000000000000000000000000100000001028bb00164000000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000050000000000000000000000000000000000000000000000000000000000b71b0027df63791519bb71d974f55cd25a4f9c42a44f12829c48654ea7b8676343164c000000005a8c59e1030f42400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003716b6300000000000000000000000000000000000000000000000000000000000000000000"

// testRootBlockHeader is the synthetic_all_fields header above, reused as the
// head of the body cases so those only vary in the minor header list.
func testRootBlockHeader() *RootBlockHeader {
	return &RootBlockHeader{
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
	}
}

// testConfirmedHeaders returns n minor block headers differing only in height,
// built on the same header/meta pair the minor block goldens use.
func testConfirmedHeaders(n int) []*MinorBlockHeader {
	headers := make([]*MinorBlockHeader, n)
	for i := range headers {
		header, _ := testMinorBlockHeader()
		header.Number = uint64(5 + i)
		headers[i] = header
	}
	return headers
}

// TestRootBlockSerializeLayout pins how the body is laid out around the header
// list: a 4-byte element count, the headers in order, then the 2-byte-prefixed
// tracking data (quarkchain/core.py:989). Element order is consensus data, since
// the cross-shard cursor indexes into this list.
func TestRootBlockSerializeLayout(t *testing.T) {
	header := testRootBlockHeader()
	headHex, err := serialize.SerializeToBytes(header)
	if err != nil {
		t.Fatalf("serialize header: %v", err)
	}

	tests := []struct {
		name         string
		headers      []*MinorBlockHeader
		trackingData []byte
		// Absolute bytes, from pyquarkchain, where given. The other cases are
		// checked against the composition rule alone, which is what varies.
		golden string
	}{
		{name: "empty"},
		{name: "one header", headers: testConfirmedHeaders(1), golden: goldenRootBlockOneHeader},
		{name: "two headers", headers: testConfirmedHeaders(2)},
		{name: "tracking data", headers: testConfirmedHeaders(1), trackingData: []byte("qkc")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := serialize.SerializeToBytes(NewRootBlock(header, test.headers, test.trackingData))
			if err != nil {
				t.Fatalf("serialize block: %v", err)
			}

			want := bytes.Clone(headHex)
			want = binary.BigEndian.AppendUint32(want, uint32(len(test.headers)))
			for _, minorHeader := range test.headers {
				encoded, err := serialize.SerializeToBytes(minorHeader)
				if err != nil {
					t.Fatalf("serialize minor header: %v", err)
				}
				want = append(want, encoded...)
			}
			want = binary.BigEndian.AppendUint16(want, uint16(len(test.trackingData)))
			want = append(want, test.trackingData...)

			if !bytes.Equal(got, want) {
				t.Errorf("serialized block\n got %x\nwant %x", got, want)
			}
			if test.golden != "" && hex.EncodeToString(got) != test.golden {
				t.Errorf("serialized block against pyquarkchain\n got %x\nwant %s", got, test.golden)
			}
		})
	}
}

// TestRootBlockMinorHeaderMerkleRoot pins the root the header commits to over
// the confirmed minor headers against pyquarkchain's calculate_merkle_root.
func TestRootBlockMinorHeaderMerkleRoot(t *testing.T) {
	tests := []struct {
		count int
		want  string
	}{
		{0, "daa77426c30c02a43d9fba4e841a6556c524d47030762eb14dc4af897e605d9b"},
		{1, "f79cc5f96b978a601534959f72f91b236707b4f9231f72cb282a75ddbdf66211"},
		{2, "49a400646be171f2aae4353e3ea56f1a4d287d5fc69d6d73fe0b858d7c928a36"},
	}
	for _, test := range tests {
		block := NewRootBlock(testRootBlockHeader(), testConfirmedHeaders(test.count), nil)
		if got := block.MinorHeaderMerkleRoot(); got != common.HexToHash(test.want) {
			t.Errorf("merkle root over %d headers\n got %s\nwant 0x%s", test.count, got.Hex(), test.want)
		}
	}
}

func TestEmptyTokenBalancesSerialize(t *testing.T) {
	var w []byte
	if err := qkcCommon.NewEmptyTokenBalances().Serialize(&w); err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if h := hex.EncodeToString(w); h != "00000000" {
		t.Fatalf("empty map serialize = %s, want 00000000", h)
	}
}
