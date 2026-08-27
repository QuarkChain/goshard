// Copyright 2026-2027, QuarkChain.

// Root blocks follow pyquarkchain-compatible QKC wire encoding.
// Modified from go-ethereum under GNU Lesser General Public License
//
// NOTE: ported as a passive dependency of the shardchain code only — the root
// chain itself (RootBlockChain, root consensus/mining) is NOT implemented here.

package types

import (
	"crypto/ecdsa"
	"math/big"
	"sync/atomic"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// RootBlockHeader represents a root block header in the QuarkChain.
type RootBlockHeader struct {
	Version         uint32                   `json:"version"          gencodec:"required"`
	Number          uint32                   `json:"number"           gencodec:"required"`
	ParentHash      common.Hash              `json:"parentHash"       gencodec:"required"`
	MinorHeaderHash common.Hash              `json:"transactionsRoot" gencodec:"required"`
	Root            common.Hash              `json:"root"             gencodec:"required"`
	Coinbase        account.Address          `json:"miner"            gencodec:"required"`
	CoinbaseAmount  *qkcCommon.TokenBalances `json:"coinbaseAmount"   gencodec:"required"`
	Time            uint64                   `json:"timestamp"        gencodec:"required"`
	Difficulty      *big.Int                 `json:"difficulty"       gencodec:"required"`
	TotalDifficulty *big.Int                 `json:"total_difficulty" gencodec:"required"`
	Nonce           uint64                   `json:"nonce"`
	Extra           []byte                   `json:"extraData"        gencodec:"required"   bytesizeofslicelen:"2"`
	MixDigest       common.Hash              `json:"mixHash"`
	Signature       [65]byte                 `json:"signature"        gencodec:"required"`
}

// Hash returns the block hash of the header, which is simply the keccak256 hash of its
// Serialize encoding.
func (h *RootBlockHeader) Hash() common.Hash {
	return serHash(*h, nil)
}

// SealHash returns the block hash of the header, which is keccak256 hash of its
// Serialize encoding for Seal.
func (h *RootBlockHeader) SealHash() common.Hash {
	return serHash(*h, map[string]bool{"Signature": true, "MixDigest": true, "Nonce": true})
}

// Size returns the approximate memory used by all internal contents. It is used
// to approximate and limit the memory consumption of various caches.
func (h *RootBlockHeader) Size() common.StorageSize {
	return common.StorageSize(unsafe.Sizeof(*h)) + common.StorageSize(len(h.Signature)) +
		common.StorageSize(len(h.Extra)+(h.Difficulty.BitLen())/8)
}

func (h *RootBlockHeader) GetParentHash() common.Hash   { return h.ParentHash }
func (h *RootBlockHeader) GetCoinbase() account.Address { return h.Coinbase }

func (h *RootBlockHeader) GetTime() uint64              { return h.Time }
func (h *RootBlockHeader) GetDifficulty() *big.Int      { return new(big.Int).Set(h.Difficulty) }
func (h *RootBlockHeader) GetTotalDifficulty() *big.Int { return new(big.Int).Set(h.TotalDifficulty) }
func (h *RootBlockHeader) GetNonce() uint64             { return h.Nonce }
func (h *RootBlockHeader) GetExtra() []byte {
	if h.Extra != nil {
		return common.CopyBytes(h.Extra)
	}
	return nil
}

func (b *RootBlockHeader) GetCoinbaseAmount() *qkcCommon.TokenBalances {
	if b.CoinbaseAmount != nil && b.CoinbaseAmount.GetBalanceMap() != nil {
		return qkcCommon.NewTokenBalancesWithMap(b.CoinbaseAmount.GetBalanceMap())
	}
	return qkcCommon.NewEmptyTokenBalances()
}

func (b *RootBlockHeader) VerifySignature(key ecdsa.PublicKey) bool {

	isSigned := crypto.VerifySignature(crypto.CompressPubkey(&key), b.SealHash().Bytes(), b.Signature[:64])
	if isSigned {
		return true
	} else {
		return false
	}

}

func (h *RootBlockHeader) GetMixDigest() common.Hash { return h.MixDigest }

func (h *RootBlockHeader) NumberU64() uint64 { return uint64(h.Number) }

func (h *RootBlockHeader) GetVersion() uint32 { return h.Version }

// Block represents an entire block in the QuarkChain.
type RootBlock struct {
	header            *RootBlockHeader
	minorBlockHeaders MinorBlockHeaders
	trackingdata      []byte

	// caches
	hash atomic.Pointer[common.Hash]
	size atomic.Pointer[common.StorageSize]
}

func (b *RootBlock) IHeader() IHeader {
	return CopyRootBlockHeader(b.header)
}

// "external" block encoding. used for eth protocol, etc.
type extrootblock struct {
	Header            *RootBlockHeader
	MinorBlockHeaders MinorBlockHeaders `bytesizeofslicelen:"4"`
	Trackingdata      []byte            `bytesizeofslicelen:"2"`
}

// NewRootBlock creates a root block and copies all inputs. MinorHeaderHash in
// the copied header is replaced with the commitment derived from mbHeaders.
func NewRootBlock(header *RootBlockHeader, mbHeaders MinorBlockHeaders, trackingdata []byte) *RootBlock {
	b := &RootBlock{header: CopyRootBlockHeader(header)}

	b.header.MinorHeaderHash = CalculateMerkleRoot(MinorBlockHeaders(mbHeaders))
	if len(mbHeaders) > 0 {
		b.minorBlockHeaders = make(MinorBlockHeaders, len(mbHeaders))
		for i, header := range mbHeaders {
			b.minorBlockHeaders[i] = CopyMinorBlockHeader(header)
		}
	}
	if trackingdata != nil && len(trackingdata) > 0 {
		b.trackingdata = make([]byte, len(trackingdata))
		copy(b.trackingdata, trackingdata)
	}

	return b
}

// NewBlockWithHeader creates a block with the given header data. The
// header data is copied, changes to header and to the field values
// will not affect the block.
func NewRootBlockWithHeader(header *RootBlockHeader) *RootBlock {
	return &RootBlock{header: CopyRootBlockHeader(header)}
}

// CopyRootHeader creates a deep copy of a block header to prevent side effects from
// modifying a header variable.
func CopyRootBlockHeader(h *RootBlockHeader) *RootBlockHeader {
	cpy := *h
	if h.CoinbaseAmount != nil && h.CoinbaseAmount.GetBalanceMap() != nil {
		cpy.CoinbaseAmount = h.CoinbaseAmount.Copy()
	}
	if cpy.Difficulty = new(big.Int); h.Difficulty != nil {
		cpy.Difficulty.Set(h.Difficulty)
	}
	if cpy.TotalDifficulty = new(big.Int); h.TotalDifficulty != nil {
		cpy.TotalDifficulty.Set(h.TotalDifficulty)
	}
	if len(h.Extra) > 0 {
		cpy.Extra = make([]byte, len(h.Extra))
		copy(cpy.Extra, h.Extra)
	}
	cpy.Signature = [65]byte{}
	copy(cpy.Signature[:], h.Signature[:])

	return &cpy
}

// Deserialize deserialize the QKC root block
func (b *RootBlock) Deserialize(bb *serialize.ByteBuffer) error {
	var eb extrootblock
	startIndex := bb.GetOffset()
	if err := serialize.Deserialize(bb, &eb); err != nil {
		return err
	}
	b.header, b.minorBlockHeaders, b.trackingdata = eb.Header, eb.MinorBlockHeaders, eb.Trackingdata
	b.hash.Store(nil)
	size := common.StorageSize(bb.GetOffset() - startIndex)
	b.size.Store(&size)
	return nil
}

// Serialize serialize the QKC root block.
func (b *RootBlock) Serialize(w *[]byte) error {
	offset := len(*w)
	err := serialize.Serialize(w, extrootblock{
		Header:            b.header,
		MinorBlockHeaders: b.minorBlockHeaders,
		Trackingdata:      b.trackingdata,
	})

	size := common.StorageSize(len(*w) - offset)
	b.size.Store(&size)
	return err
}

func (b *RootBlock) MinorBlockHeaders() MinorBlockHeaders {
	headers := make(MinorBlockHeaders, len(b.minorBlockHeaders))
	for i, header := range b.minorBlockHeaders {
		headers[i] = CopyMinorBlockHeader(header)
	}
	return headers
}

func (b *RootBlock) MinorBlockHeader(hash common.Hash) *MinorBlockHeader {
	for _, minorBlockHeader := range b.minorBlockHeaders {
		if minorBlockHeader.Hash() == hash {
			return CopyMinorBlockHeader(minorBlockHeader)
		}
	}

	return nil
}

func (b *RootBlock) TrackingData() []byte { return common.CopyBytes(b.trackingdata) }

func (b *RootBlock) Version() uint32                          { return b.header.Version }
func (b *RootBlock) Number() uint32                           { return b.header.Number }
func (b *RootBlock) NumberU64() uint64                        { return uint64(b.header.Number) }
func (b *RootBlock) ParentHash() common.Hash                  { return b.header.ParentHash }
func (b *RootBlock) MinorHeaderHash() common.Hash             { return b.header.MinorHeaderHash }
func (b *RootBlock) Coinbase() account.Address                { return b.header.Coinbase }
func (b *RootBlock) CoinbaseAmount() *qkcCommon.TokenBalances { return b.header.GetCoinbaseAmount() }
func (b *RootBlock) Time() uint64                             { return b.header.Time }
func (b *RootBlock) Difficulty() *big.Int                     { return new(big.Int).Set(b.header.Difficulty) }
func (b *RootBlock) TotalDifficulty() *big.Int                { return new(big.Int).Set(b.header.TotalDifficulty) }
func (b *RootBlock) Nonce() uint64                            { return b.header.Nonce }
func (b *RootBlock) Extra() []byte                            { return common.CopyBytes(b.header.Extra) }
func (b *RootBlock) MixDigest() common.Hash                   { return b.header.MixDigest }
func (b *RootBlock) Signature() [65]byte                      { return b.header.Signature }

func (b *RootBlock) Header() *RootBlockHeader { return CopyRootBlockHeader(b.header) }
func (b *RootBlock) Content() []IHashable {
	items := make([]IHashable, len(b.minorBlockHeaders), len(b.minorBlockHeaders))
	for i, item := range b.minorBlockHeaders {
		items[i] = CopyMinorBlockHeader(item)
	}
	return items
}

func (b *RootBlock) Size() common.StorageSize {
	if size := b.size.Load(); size != nil {
		return *size
	}

	bytes, err := serialize.SerializeToBytes(b)
	if err != nil {
		panic(err)
	}
	size := common.StorageSize(len(bytes))
	b.size.Store(&size)
	return size
}

// Hash returns the keccak256 hash of b's header.
// The hash is computed on the first call and cached thereafter.
func (b *RootBlock) Hash() common.Hash {
	if hash := b.hash.Load(); hash != nil {
		return *hash
	}
	v := b.header.Hash()
	b.hash.Store(&v)
	return v
}

func (b *RootBlock) GetTrackingData() []byte {
	return common.CopyBytes(b.trackingdata)
}

func (b *RootBlock) GetSize() common.StorageSize {
	return b.Size()
}
