// Copyright 2026-2027, QuarkChain.

package types

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// MinorBlockHeader is the exact QKC protocol projection of Header. It is used
// for peer/master serialization and consensus hashing, not as a second
// executable block-header model.
type MinorBlockHeader struct {
	Version           uint32                   `json:"version"                    gencodec:"required"`
	Branch            account.Branch           `json:"branch"                     gencodec:"required"`
	Number            uint64                   `json:"number"                     gencodec:"required"`
	Coinbase          account.Address          `json:"miner"                      gencodec:"required"`
	CoinbaseAmount    *qkccommon.TokenBalances `json:"coinbaseAmount"             gencodec:"required"`
	ParentHash        common.Hash              `json:"parentHash"                 gencodec:"required"`
	PrevRootBlockHash common.Hash              `json:"prevRootBlockHash"          gencodec:"required"`
	GasLimit          *serialize.Uint256       `json:"gasLimit"                   gencodec:"required"`
	MetaHash          common.Hash              `json:"metaHash"                   gencodec:"required"`
	Time              uint64                   `json:"timestamp"                  gencodec:"required"`
	Difficulty        *big.Int                 `json:"difficulty"                 gencodec:"required"`
	Nonce             uint64                   `json:"nonce"`
	Bloom             Bloom                    `json:"logsBloom"                  gencodec:"required"`
	Extra             []byte                   `json:"extraData"                  gencodec:"required" bytesizeofslicelen:"2"`
	MixDigest         common.Hash              `json:"mixHash"`
}

// MinorBlockMeta is the exact QKC protocol projection of the execution
// commitments flattened into Header.
type MinorBlockMeta struct {
	TxHash             common.Hash         `json:"transactionsRoot"   gencodec:"required"`
	Root               common.Hash         `json:"stateRoot"          gencodec:"required"`
	ReceiptHash        common.Hash         `json:"receiptsRoot"       gencodec:"required"`
	GasUsed            *serialize.Uint256  `json:"gasUsed"            gencodec:"required"`
	CrossShardGasUsed  *serialize.Uint256  `json:"crossShardGasUsed"  gencodec:"required"`
	XShardTxCursorInfo *XShardTxCursorInfo `json:"xShardTxCursorInfo" gencodec:"required"`
	XShardGasLimit     *serialize.Uint256  `json:"xShardGasLimit"     gencodec:"required"`
}

// XShardTxCursorInfo is serialized in the same three-uint64 order as
// pyquarkchain's XshardTxCursorInfo.
type XShardTxCursorInfo struct {
	RootBlockHeight    uint64
	MinorBlockIndex    uint64
	XShardDepositIndex uint64
}

// IsQKC reports whether a geth header carries the QKC minor-block extension.
// Valid QKC genesis and non-genesis headers always carry at least one of these
// markers, while ordinary Ethereum headers carry none of them.
func (h *Header) IsQKC() bool {
	return h != nil && (h.Branch.Value != 0 || h.CoinbaseAmount != nil || h.XShardTxCursorInfo != nil)
}

type MinorBlockHeaders []*MinorBlockHeader

func (headers MinorBlockHeaders) Len() int           { return len(headers) }
func (headers MinorBlockHeaders) Less(i, j int) bool { return headers[i].Number < headers[j].Number }
func (headers MinorBlockHeaders) Swap(i, j int)      { headers[i], headers[j] = headers[j], headers[i] }
func (headers MinorBlockHeaders) Bytes(i int) []byte {
	encoded, err := serialize.SerializeToBytes(headers[i])
	if err != nil {
		panic(err)
	}
	return encoded
}

func MinorHeaderDifference(a, b MinorBlockHeaders) MinorBlockHeaders {
	remove := make(map[common.Hash]struct{}, len(b))
	for _, header := range b {
		remove[header.Hash()] = struct{}{}
	}
	keep := make(MinorBlockHeaders, 0, len(a))
	for _, header := range a {
		if _, exists := remove[header.Hash()]; !exists {
			keep = append(keep, header)
		}
	}
	return keep
}

// CalculateMerkleRoot computes QuarkChain's binary Merkle commitment. Leaves
// are Keccak hashes of QKC external encodings and the final node commits to the
// number of leaves, matching pyquarkchain's calculate_merkle_root.
func CalculateMerkleRoot(list any) common.Hash {
	value := reflect.ValueOf(list)
	if value.Kind() != reflect.Slice {
		panic("expect slice input for CalculateMerkleRoot")
	}
	hashes := make([]common.Hash, value.Len())
	if value.Len() == 0 {
		hashes = append(hashes, common.Hash{})
	} else {
		for i := 0; i < value.Len(); i++ {
			encoded, err := serialize.SerializeToBytes(value.Index(i).Interface())
			if err != nil {
				panic(fmt.Errorf("serialize QKC merkle leaf %d: %w", i, err))
			}
			hashes[i] = crypto.Keccak256Hash(encoded)
		}
	}
	zero := common.Hash{}
	for len(hashes) != 1 {
		if len(hashes)%2 != 0 {
			hashes = append(hashes, zero)
		}
		next := make([]common.Hash, 0, len(hashes)/2)
		for i := 0; i < len(hashes); i += 2 {
			next = append(next, crypto.Keccak256Hash(hashes[i].Bytes(), hashes[i+1].Bytes()))
		}
		hashes = next
		zero = crypto.Keccak256Hash(zero.Bytes(), zero.Bytes())
	}
	return crypto.Keccak256Hash(hashes[0].Bytes(), qkccommon.Uint64ToBytes(uint64(value.Len())))
}

func qkcSerializedHash(value any, exclude map[string]bool) common.Hash {
	var encoded []byte
	if err := serialize.SerializeStructWithout(reflect.ValueOf(value), &encoded, exclude); err != nil {
		panic(err)
	}
	return crypto.Keccak256Hash(encoded)
}

func (h *MinorBlockHeader) Hash() common.Hash {
	return qkcSerializedHash(*h, nil)
}

func (h *MinorBlockHeader) SealHash() common.Hash {
	return qkcSerializedHash(*h, map[string]bool{"MixDigest": true, "Nonce": true})
}

func (m *MinorBlockMeta) Hash() common.Hash {
	return qkcSerializedHash(*m, nil)
}

func (h *MinorBlockHeader) GetParentHash() common.Hash        { return h.ParentHash }
func (h *MinorBlockHeader) GetPrevRootBlockHash() common.Hash { return h.PrevRootBlockHash }
func (h *MinorBlockHeader) GetCoinbase() account.Address      { return h.Coinbase }
func (h *MinorBlockHeader) GetTime() uint64                   { return h.Time }
func (h *MinorBlockHeader) GetNonce() uint64                  { return h.Nonce }
func (h *MinorBlockHeader) GetBranch() account.Branch         { return h.Branch }
func (h *MinorBlockHeader) GetMetaHash() common.Hash          { return h.MetaHash }
func (h *MinorBlockHeader) GetBloom() Bloom                   { return h.Bloom }
func (h *MinorBlockHeader) GetMixDigest() common.Hash         { return h.MixDigest }
func (h *MinorBlockHeader) GetVersion() uint32                { return h.Version }
func (h *MinorBlockHeader) NumberU64() uint64                 { return h.Number }

func (h *MinorBlockHeader) GetDifficulty() *big.Int {
	if h.Difficulty == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(h.Difficulty)
}

func (h *MinorBlockHeader) GetGasLimit() *big.Int {
	if h.GasLimit == nil || h.GasLimit.Value == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(h.GasLimit.Value)
}

func (h *MinorBlockHeader) GetExtra() []byte { return common.CopyBytes(h.Extra) }

func (h *MinorBlockHeader) GetCoinbaseAmount() *qkccommon.TokenBalances {
	if h.CoinbaseAmount == nil {
		return qkccommon.NewEmptyTokenBalances()
	}
	return h.CoinbaseAmount.Copy()
}

func (h *MinorBlockHeader) SetExtra(data []byte)             { h.Extra = common.CopyBytes(data) }
func (h *MinorBlockHeader) SetNonce(nonce uint64)            { h.Nonce = nonce }
func (h *MinorBlockHeader) SetCoinbase(addr account.Address) { h.Coinbase = addr }

func CopyMinorBlockHeader(header *MinorBlockHeader) *MinorBlockHeader {
	if header == nil {
		return nil
	}
	copyHeader := *header
	copyHeader.Difficulty = copyBigInt(header.Difficulty)
	copyHeader.Extra = common.CopyBytes(header.Extra)
	if header.CoinbaseAmount != nil {
		copyHeader.CoinbaseAmount = header.CoinbaseAmount.Copy()
	}
	if header.GasLimit != nil {
		copyHeader.GasLimit = &serialize.Uint256{Value: copyBigInt(header.GasLimit.Value)}
	}
	return &copyHeader
}

func CopyMinorBlockMeta(meta *MinorBlockMeta) *MinorBlockMeta {
	if meta == nil {
		return nil
	}
	copyMeta := *meta
	if meta.GasUsed != nil {
		copyMeta.GasUsed = &serialize.Uint256{Value: copyBigInt(meta.GasUsed.Value)}
	}
	if meta.CrossShardGasUsed != nil {
		copyMeta.CrossShardGasUsed = &serialize.Uint256{Value: copyBigInt(meta.CrossShardGasUsed.Value)}
	}
	if meta.XShardGasLimit != nil {
		copyMeta.XShardGasLimit = &serialize.Uint256{Value: copyBigInt(meta.XShardGasLimit.Value)}
	}
	if meta.XShardTxCursorInfo != nil {
		cursor := *meta.XShardTxCursorInfo
		copyMeta.XShardTxCursorInfo = &cursor
	}
	return &copyMeta
}

func copyBigInt(value *big.Int) *big.Int {
	if value == nil {
		return nil
	}
	return new(big.Int).Set(value)
}

func uint256ToUint64(name string, value *serialize.Uint256) (uint64, error) {
	if value == nil || value.Value == nil {
		return 0, fmt.Errorf("%s is nil", name)
	}
	if value.Value.Sign() < 0 || !value.Value.IsUint64() {
		return 0, fmt.Errorf("%s does not fit uint64: %s", name, value.Value)
	}
	return value.Value.Uint64(), nil
}

func (h *Header) ToMinorBlockHeader() (*MinorBlockHeader, error) {
	if h == nil {
		return nil, errors.New("nil QKC header")
	}
	if h.Number == nil || !h.Number.IsUint64() {
		return nil, fmt.Errorf("minor block number does not fit uint64: %v", h.Number)
	}
	if h.Difficulty == nil || h.Difficulty.Sign() < 0 {
		return nil, fmt.Errorf("invalid minor block difficulty: %v", h.Difficulty)
	}
	coinbaseAmount := qkccommon.NewEmptyTokenBalances()
	if h.CoinbaseAmount != nil {
		coinbaseAmount = h.CoinbaseAmount.Copy()
	}
	return &MinorBlockHeader{
		Version:           h.Version,
		Branch:            h.Branch,
		Number:            h.Number.Uint64(),
		Coinbase:          account.Address{Recipient: h.Coinbase, FullShardKey: h.CoinbaseFullShardKey},
		CoinbaseAmount:    coinbaseAmount,
		ParentHash:        h.ParentHash,
		PrevRootBlockHash: h.PrevRootBlockHash,
		GasLimit:          &serialize.Uint256{Value: new(big.Int).SetUint64(h.GasLimit)},
		MetaHash:          h.MetaHash,
		Time:              h.Time,
		Difficulty:        new(big.Int).Set(h.Difficulty),
		Nonce:             h.Nonce.Uint64(),
		Bloom:             h.Bloom,
		Extra:             common.CopyBytes(h.Extra),
		MixDigest:         h.MixDigest,
	}, nil
}

func (h *Header) ToMinorBlockMeta() (*MinorBlockMeta, error) {
	if h == nil {
		return nil, errors.New("nil QKC header")
	}
	var cursor *XShardTxCursorInfo
	if h.XShardTxCursorInfo != nil {
		copyCursor := *h.XShardTxCursorInfo
		cursor = &copyCursor
	}
	return &MinorBlockMeta{
		TxHash:             h.TxHash,
		Root:               h.Root,
		ReceiptHash:        h.ReceiptHash,
		GasUsed:            &serialize.Uint256{Value: new(big.Int).SetUint64(h.GasUsed)},
		CrossShardGasUsed:  &serialize.Uint256{Value: new(big.Int).SetUint64(h.CrossShardGasUsed)},
		XShardTxCursorInfo: cursor,
		XShardGasLimit:     &serialize.Uint256{Value: new(big.Int).SetUint64(h.XShardGasLimit)},
	}, nil
}

func NewHeaderFromMinorBlock(header *MinorBlockHeader, meta *MinorBlockMeta) (*Header, error) {
	if header == nil || meta == nil {
		return nil, errors.New("nil QKC minor header or meta")
	}
	gasLimit, err := uint256ToUint64("gas limit", header.GasLimit)
	if err != nil {
		return nil, err
	}
	gasUsed, err := uint256ToUint64("gas used", meta.GasUsed)
	if err != nil {
		return nil, err
	}
	crossShardGasUsed, err := uint256ToUint64("cross-shard gas used", meta.CrossShardGasUsed)
	if err != nil {
		return nil, err
	}
	xShardGasLimit, err := uint256ToUint64("cross-shard gas limit", meta.XShardGasLimit)
	if err != nil {
		return nil, err
	}
	if header.Difficulty == nil || header.Difficulty.Sign() < 0 {
		return nil, fmt.Errorf("invalid minor block difficulty: %v", header.Difficulty)
	}
	result := &Header{
		ParentHash:           header.ParentHash,
		UncleHash:            EmptyUncleHash,
		Coinbase:             header.Coinbase.Recipient,
		Root:                 meta.Root,
		TxHash:               meta.TxHash,
		ReceiptHash:          meta.ReceiptHash,
		Bloom:                header.Bloom,
		Difficulty:           new(big.Int).Set(header.Difficulty),
		Number:               new(big.Int).SetUint64(header.Number),
		GasLimit:             gasLimit,
		GasUsed:              gasUsed,
		Time:                 header.Time,
		Extra:                common.CopyBytes(header.Extra),
		MixDigest:            header.MixDigest,
		Nonce:                EncodeNonce(header.Nonce),
		Version:              header.Version,
		Branch:               header.Branch,
		CoinbaseFullShardKey: header.Coinbase.FullShardKey,
		PrevRootBlockHash:    header.PrevRootBlockHash,
		MetaHash:             header.MetaHash,
		CrossShardGasUsed:    crossShardGasUsed,
		XShardGasLimit:       xShardGasLimit,
	}
	if header.CoinbaseAmount != nil {
		result.CoinbaseAmount = header.CoinbaseAmount.Copy()
	} else {
		result.CoinbaseAmount = qkccommon.NewEmptyTokenBalances()
	}
	if meta.XShardTxCursorInfo != nil {
		cursor := *meta.XShardTxCursorInfo
		result.XShardTxCursorInfo = &cursor
	}
	return result, nil
}

func (h *Header) SealHash() common.Hash {
	header, err := h.ToMinorBlockHeader()
	if err != nil {
		panic(err)
	}
	return header.SealHash()
}

func (b *Block) ToMinorBlockHeader() (*MinorBlockHeader, error) {
	if b == nil {
		return nil, errors.New("nil QKC block")
	}
	return b.header.ToMinorBlockHeader()
}

func (b *Block) Meta() *MinorBlockMeta {
	meta, err := b.header.ToMinorBlockMeta()
	if err != nil {
		panic(err)
	}
	return meta
}

func (b *Block) GetMetaData() *MinorBlockMeta { return b.Meta() }

// minorBlockEncoding defines the pyquarkchain-compatible minor block encoding.
type minorBlockEncoding struct {
	Header       *MinorBlockHeader
	Meta         *MinorBlockMeta
	Transactions Transactions `bytesizeofslicelen:"4"`
	TrackingData []byte       `bytesizeofslicelen:"2"`
}

// Serialize writes the external pyquarkchain-compatible minor-block encoding.
func (b *Block) Serialize(out *[]byte) error {
	// QKC RPC responses use a non-optional MinorBlock field even when their
	// error code is nonzero. The QKC serializer represents a nil pointer to such
	// a field as the type's zero value, so preserve pyquarkchain's serializable
	// empty-block sentinel instead of dereferencing an absent internal header.
	if b == nil || b.header == nil {
		return serialize.Serialize(out, minorBlockEncoding{
			Header: &MinorBlockHeader{
				CoinbaseAmount: qkccommon.NewEmptyTokenBalances(),
				GasLimit:       &serialize.Uint256{Value: new(big.Int)},
				Difficulty:     new(big.Int),
			},
			Meta: &MinorBlockMeta{
				GasUsed:            &serialize.Uint256{Value: new(big.Int)},
				CrossShardGasUsed:  &serialize.Uint256{Value: new(big.Int)},
				XShardTxCursorInfo: &XShardTxCursorInfo{},
				XShardGasLimit:     &serialize.Uint256{Value: new(big.Int)},
			},
		})
	}
	header, err := b.ToMinorBlockHeader()
	if err != nil {
		return err
	}
	return serialize.Serialize(out, minorBlockEncoding{
		Header:       header,
		Meta:         b.Meta(),
		Transactions: b.transactions,
		TrackingData: b.trackingData,
	})
}

// Deserialize reads an external QKC minor block and converts the protocol
// header/meta pair into the internal flattened Header.
func (b *Block) Deserialize(buffer *serialize.ByteBuffer) error {
	var decoded minorBlockEncoding
	if err := serialize.Deserialize(buffer, &decoded); err != nil {
		return err
	}
	header, err := NewHeaderFromMinorBlock(decoded.Header, decoded.Meta)
	if err != nil {
		return err
	}
	b.header = header
	b.transactions = append(Transactions(nil), decoded.Transactions...)
	b.uncles = nil
	b.withdrawals = nil
	b.accessList = nil
	b.trackingData = common.CopyBytes(decoded.TrackingData)
	b.hash.Store(nil)
	b.size.Store(0)
	return nil
}

// Validate checks the QKC block's self-contained fields and commitments.
func (b *Block) Validate() error {
	if b == nil || b.header == nil {
		return errors.New("nil QKC block")
	}
	h := b.header
	if len(b.uncles) != 0 || h.UncleHash != EmptyUncleHash {
		return errors.New("QKC minor block must not contain uncles")
	}
	if len(b.withdrawals) != 0 || h.WithdrawalsHash != nil {
		return errors.New("QKC minor block must not contain withdrawals")
	}
	if h.BaseFee != nil || h.BlobGasUsed != nil || h.ExcessBlobGas != nil ||
		h.ParentBeaconRoot != nil || h.RequestsHash != nil ||
		h.BlockAccessListHash != nil || h.SlotNumber != nil || b.accessList != nil {
		return errors.New("QKC minor block contains Ethereum-only fields")
	}
	for index, tx := range b.transactions {
		if tx == nil {
			return fmt.Errorf("nil QKC transaction at index %d", index)
		}
		if err := tx.Validate(); err != nil {
			return fmt.Errorf("invalid QKC transaction %d: %w", index, err)
		}
	}
	txHash := CalculateMerkleRoot(b.transactions)
	if txHash != h.TxHash {
		return fmt.Errorf("QKC transaction root mismatch: have %s want %s", h.TxHash, txHash)
	}
	meta, err := h.ToMinorBlockMeta()
	if err != nil {
		return err
	}
	if meta.Hash() != h.MetaHash {
		return fmt.Errorf("QKC meta hash mismatch: have %s want %s", h.MetaHash, meta.Hash())
	}
	return nil
}
