// Copyright 2026-2027, QuarkChain.

// Minor blocks follow pyquarkchain-compatible QKC wire encoding.
// Modified from go-ethereum under GNU Lesser General Public License
package types

import (
	"math/big"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/params"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// MinorBlockHeaderList represents a minor block header in the QuarkChain.
type MinorBlockHeader struct {
	Version           uint32                   `json:"version"                    gencodec:"required"`
	Branch            account.Branch           `json:"branch"                     gencodec:"required"`
	Number            uint64                   `json:"number"                     gencodec:"required"`
	Coinbase          account.Address          `json:"miner"                      gencodec:"required"`
	CoinbaseAmount    *qkcCommon.TokenBalances `json:"coinbaseAmount"             gencodec:"required"`
	ParentHash        common.Hash              `json:"parentHash"                 gencodec:"required"`
	PrevRootBlockHash common.Hash              `json:"prevRootBlockHash"          gencodec:"required"`
	GasLimit          *serialize.Uint256       `json:"gasLimit"                   gencodec:"required"`
	MetaHash          common.Hash              `json:"metaHash"                   gencodec:"required"`
	Time              uint64                   `json:"timestamp"                  gencodec:"required"`
	Difficulty        *big.Int                 `json:"difficulty"                 gencodec:"required"`
	Nonce             uint64                   `json:"nonce"`
	Bloom             Bloom                    `json:"logsBloom"                  gencodec:"required"`
	Extra             []byte                   `json:"extraData"                  gencodec:"required"   bytesizeofslicelen:"2"`
	MixDigest         common.Hash              `json:"mixHash"`
}

type MinorBlockMeta struct {
	TxHash             common.Hash         `json:"transactionsRoot"           gencodec:"required"`
	Root               common.Hash         `json:"stateRoot"                  gencodec:"required"`
	ReceiptHash        common.Hash         `json:"receiptsRoot"               gencodec:"required"`
	GasUsed            *serialize.Uint256  `json:"gasUsed"                    gencodec:"required"`
	CrossShardGasUsed  *serialize.Uint256  `json:"crossShardGasUsed"          gencodec:"required"`
	XShardTxCursorInfo *XShardTxCursorInfo `json:"xShardTxCursorInfo"         gencodec:"required"`
	XShardGasLimit     *serialize.Uint256  `json:"xShardGasLimit"             gencodec:"required"`
}

func (m *MinorBlockMeta) Hash() common.Hash {
	return serHash(*m, nil)
}

// Hash returns the block hash of the header, which is simply the keccak256 hash of its
// Serialize encoding.
func (h *MinorBlockHeader) Hash() common.Hash {
	return serHash(*h, nil)
}

// SealHash returns the block hash of the header, which is keccak256 hash of its
// Serialize encoding for Seal.
func (h *MinorBlockHeader) SealHash() common.Hash {
	excludeList := map[string]bool{"MixDigest": true, "Nonce": true}
	return serHash(*h, excludeList)
}

func (h *MinorBlockHeader) GetParentHash() common.Hash        { return h.ParentHash }
func (h *MinorBlockHeader) GetPrevRootBlockHash() common.Hash { return h.PrevRootBlockHash }
func (h *MinorBlockHeader) GetCoinbase() account.Address      { return h.Coinbase }
func (h *MinorBlockHeader) GetTime() uint64                   { return h.Time }
func (h *MinorBlockHeader) GetDifficulty() *big.Int           { return new(big.Int).Set(h.Difficulty) }
func (h *MinorBlockHeader) GetNonce() uint64                  { return h.Nonce }
func (h *MinorBlockHeader) GetGasLimit() *big.Int             { return new(big.Int).Set(h.GasLimit.Value) }
func (h *MinorBlockHeader) GetBranch() account.Branch         { return h.Branch }
func (h *MinorBlockHeader) GetMetaHash() common.Hash          { return h.MetaHash }
func (h *MinorBlockHeader) GetBloom() Bloom                   { return h.Bloom }
func (h *MinorBlockHeader) GetMixDigest() common.Hash         { return h.MixDigest }
func (h *MinorBlockHeader) NumberU64() uint64                 { return h.Number }
func (h *MinorBlockHeader) GetVersion() uint32                { return h.Version }

func (h *MinorBlockHeader) GetExtra() []byte {
	if h.Extra != nil {
		return common.CopyBytes(h.Extra)
	}
	return nil
}

func (h *MinorBlockHeader) GetCoinbaseAmount() *qkcCommon.TokenBalances {
	if h.CoinbaseAmount != nil {
		return h.CoinbaseAmount.Copy()
	}
	return qkcCommon.NewEmptyTokenBalances()
}

func (h *MinorBlockHeader) SetExtra(data []byte)             { h.Extra = common.CopyBytes(data) }
func (h *MinorBlockHeader) SetNonce(nonce uint64)            { h.Nonce = nonce }
func (h *MinorBlockHeader) SetCoinbase(addr account.Address) { h.Coinbase = addr }

// MinorBlockHeaders is a MinorBlockHeaderList slice type for basic sorting.
type MinorBlockHeaders []*MinorBlockHeader

// Len returns the length of s.
func (s MinorBlockHeaders) Len() int { return len(s) }

// Swap swaps the i'th and the j'th element in s.
func (s MinorBlockHeaders) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Bytes implements DerivableList and returns the i'th element of s in serialize.
func (s MinorBlockHeaders) Bytes(i int) []byte {
	enc, err := serialize.SerializeToBytes(s[i])
	if err != nil {
		panic(err)
	}
	return enc
}

// MinorHeaderDifference returns a new set which is the difference between a and b.
func MinorHeaderDifference(a, b MinorBlockHeaders) MinorBlockHeaders {
	keep := make(MinorBlockHeaders, 0, len(a))

	remove := make(map[common.Hash]struct{})
	for _, header := range b {
		remove[header.Hash()] = struct{}{}
	}

	for _, header := range a {
		if _, ok := remove[header.Hash()]; !ok {
			keep = append(keep, header)
		}
	}

	return keep
}

// MinorBlock represents an entire block in the Ethereum blockchain.
type MinorBlock struct {
	header       *MinorBlockHeader
	meta         *MinorBlockMeta
	transactions Transactions
	trackingdata []byte

	// caches
	hash atomic.Pointer[common.Hash]
	size atomic.Pointer[common.StorageSize]
}

// "external" block encoding. used for qkc protocol, etc.
type extminorblock struct {
	Header       *MinorBlockHeader
	Meta         *MinorBlockMeta
	Txs          Transactions `bytesizeofslicelen:"4"`
	Trackingdata []byte       `bytesizeofslicelen:"2"`
}

func copyTransaction(tx *Transaction) *Transaction {
	if tx == nil {
		return nil
	}
	return NewTransaction(tx.inner)
}

func copyTransactions(txs []*Transaction) Transactions {
	if txs == nil {
		return nil
	}
	cpy := make(Transactions, len(txs))
	for i, tx := range txs {
		cpy[i] = copyTransaction(tx)
	}
	return cpy
}

// NewBlock creates a new block. The input data is copied,
// changes to header and to the field values will not affect the
// block.
//
// TxHash and ReceiptHash in meta, and Bloom and MetaHash in header,
// are replaced with values derived from the transactions and receipts.
func NewMinorBlock(header *MinorBlockHeader, meta *MinorBlockMeta, txs []*Transaction, receipts []*Receipt, trackingdata []byte) *MinorBlock {
	// Every local transaction produces a receipt, while incoming cross-shard
	// deposits may add receipts that have no corresponding local transaction.
	if len(receipts) < len(txs) {
		panic("receipts count is less than txs count")
	}
	b := &MinorBlock{header: CopyMinorBlockHeader(header), meta: CopyMinorBlockMeta(meta)}
	if len(txs) > 0 {
		b.transactions = copyTransactions(txs)
	}
	b.meta.TxHash = CalculateMerkleRoot(b.transactions)

	b.meta.ReceiptHash = DeriveSha(Receipts(receipts))
	b.header.Bloom = CreateBloom(receipts)
	b.header.MetaHash = b.meta.Hash()

	if len(trackingdata) > 0 {
		b.trackingdata = make([]byte, len(trackingdata))
		copy(b.trackingdata, trackingdata)
	}

	return b
}

// NewBlockWithHeader creates a block with the given header data. The
// header data is copied, changes to header and to the field values
// will not affect the block.
func NewMinorBlockWithHeader(header *MinorBlockHeader, meta *MinorBlockMeta) *MinorBlock {
	return &MinorBlock{header: CopyMinorBlockHeader(header), meta: CopyMinorBlockMeta(meta)}
}

// CopyHeader creates a deep copy of a block header to prevent side effects from
// modifying a header variable.
func CopyMinorBlockHeader(h *MinorBlockHeader) *MinorBlockHeader {
	cpy := *h
	if cpy.Difficulty = new(big.Int); h.Difficulty != nil {
		cpy.Difficulty.Set(h.Difficulty)
	}
	if h.CoinbaseAmount != nil {
		cpy.CoinbaseAmount = h.CoinbaseAmount.Copy()
	}
	if cpy.GasLimit = new(serialize.Uint256); h.GasLimit != nil && h.GasLimit.Value != nil {
		cpy.GasLimit.Value = new(big.Int).Set(h.GasLimit.Value)
	}
	if len(h.Extra) > 0 {
		cpy.Extra = make([]byte, len(h.Extra))
		copy(cpy.Extra, h.Extra)
	}

	return &cpy //todo verify the copy for struct
}

func CopyMinorBlockMeta(m *MinorBlockMeta) *MinorBlockMeta {
	cpy := *m
	if cpy.GasUsed = new(serialize.Uint256); m.GasUsed != nil && m.GasUsed.Value != nil {
		cpy.GasUsed.Value = new(big.Int).Set(m.GasUsed.Value)
	}
	if cpy.CrossShardGasUsed = new(serialize.Uint256); m.CrossShardGasUsed != nil && m.CrossShardGasUsed.Value != nil {
		cpy.CrossShardGasUsed.Value = new(big.Int).Set(m.CrossShardGasUsed.Value)
	}
	if cpy.XShardGasLimit = new(serialize.Uint256); m.XShardGasLimit != nil && m.XShardGasLimit.Value != nil {
		cpy.XShardGasLimit.Value = new(big.Int).Set(m.XShardGasLimit.Value)
	}
	if m.XShardTxCursorInfo != nil {
		cursor := *m.XShardTxCursorInfo
		cpy.XShardTxCursorInfo = &cursor
	}
	return &cpy
}

// Deserialize deserialize the QKC minor block
func (b *MinorBlock) Deserialize(bb *serialize.ByteBuffer) error {
	var eb extminorblock
	startIndex := bb.GetOffset()
	if err := serialize.Deserialize(bb, &eb); err != nil {
		return err
	}
	b.header, b.meta, b.transactions, b.trackingdata = eb.Header, eb.Meta, eb.Txs, eb.Trackingdata
	b.hash.Store(nil)
	size := common.StorageSize(bb.GetOffset() - startIndex)
	b.size.Store(&size)
	return nil
}

// Serialize serialize the QKC minor block.
func (b *MinorBlock) Serialize(w *[]byte) error {
	offset := len(*w)
	if err := serialize.Serialize(w, extminorblock{
		Header:       b.header,
		Meta:         b.meta,
		Txs:          b.transactions,
		Trackingdata: b.trackingdata,
	}); err != nil {
		return err
	}

	size := common.StorageSize(len(*w) - offset)
	b.size.Store(&size)
	return nil
}

func (b *MinorBlock) Transactions() Transactions { return copyTransactions(b.transactions) }

func (b *MinorBlock) Transaction(hash common.Hash) *Transaction {
	for _, transaction := range b.transactions {
		if transaction.Hash() == hash {
			return copyTransaction(transaction)
		}
	}
	return nil
}

func (b *MinorBlock) TrackingData() []byte { return common.CopyBytes(b.trackingdata) }

// header properties
func (b *MinorBlock) GetXShardGasLimit() *big.Int {
	return new(big.Int).Set(b.meta.XShardGasLimit.Value)
}
func (b *MinorBlock) Version() uint32                          { return b.header.Version }
func (b *MinorBlock) Branch() account.Branch                   { return b.header.Branch }
func (b *MinorBlock) Number() uint64                           { return b.header.Number }
func (b *MinorBlock) Coinbase() account.Address                { return b.header.Coinbase }
func (b *MinorBlock) ParentHash() common.Hash                  { return b.header.ParentHash }
func (b *MinorBlock) PrevRootBlockHash() common.Hash           { return b.header.PrevRootBlockHash }
func (b *MinorBlock) GasLimit() *big.Int                       { return new(big.Int).Set(b.header.GasLimit.Value) }
func (b *MinorBlock) MetaHash() common.Hash                    { return b.header.MetaHash }
func (b *MinorBlock) Time() uint64                             { return b.header.Time }
func (b *MinorBlock) Difficulty() *big.Int                     { return new(big.Int).Set(b.header.Difficulty) }
func (b *MinorBlock) Nonce() uint64                            { return b.header.Nonce }
func (b *MinorBlock) Extra() []byte                            { return common.CopyBytes(b.header.Extra) }
func (b *MinorBlock) Bloom() Bloom                             { return b.header.Bloom }
func (b *MinorBlock) MixDigest() common.Hash                   { return b.header.MixDigest }
func (b *MinorBlock) CoinbaseAmount() *qkcCommon.TokenBalances { return b.header.GetCoinbaseAmount() }

// meta properties
func (b *MinorBlock) Root() common.Hash        { return b.meta.Root }
func (b *MinorBlock) TxHash() common.Hash      { return b.meta.TxHash }
func (b *MinorBlock) ReceiptHash() common.Hash { return b.meta.ReceiptHash }
func (b *MinorBlock) GasUsed() *big.Int        { return new(big.Int).Set(b.meta.GasUsed.Value) }
func (b *MinorBlock) CrossShardGasUsed() *big.Int {
	return new(big.Int).Set(b.meta.CrossShardGasUsed.Value)
}

func (b *MinorBlock) Header() *MinorBlockHeader { return CopyMinorBlockHeader(b.header) }
func (b *MinorBlock) Meta() *MinorBlockMeta     { return CopyMinorBlockMeta(b.meta) }

// Size returns the true RLP encoded storage size of the block, either by encoding
// and returning it, or returning a previsouly cached value.
func (b *MinorBlock) Size() common.StorageSize {
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

// WithSeal returns a new block with the data from b but the header replaced with
// the sealed one.
func (b *MinorBlock) WithSeal(header *MinorBlockHeader) *MinorBlock {
	return &MinorBlock{
		header:       CopyMinorBlockHeader(header),
		meta:         CopyMinorBlockMeta(b.meta),
		transactions: copyTransactions(b.transactions),
		trackingdata: common.CopyBytes(b.trackingdata),
	}
}

// WithBody returns a new block with the given transaction and uncle contents.
func (b *MinorBlock) WithBody(transactions []*Transaction, trackingData []byte) *MinorBlock {
	block := &MinorBlock{
		header:       CopyMinorBlockHeader(b.header),
		meta:         CopyMinorBlockMeta(b.meta),
		transactions: make(Transactions, len(transactions)),
		trackingdata: make([]byte, len(trackingData)),
	}
	for i, transaction := range transactions {
		block.transactions[i] = copyTransaction(transaction)
	}
	copy(block.trackingdata, trackingData)
	return block
}

// Hash returns the keccak256 hash of b's header.
// The hash is computed on the first call and cached thereafter.
func (b *MinorBlock) Hash() common.Hash {
	if hash := b.hash.Load(); hash != nil {
		return *hash
	}
	v := b.header.Hash()
	b.hash.Store(&v)
	return v
}

func (b *MinorBlock) NumberU64() uint64 {
	return b.header.Number
}

func (b *MinorBlock) IHeader() IHeader {
	return CopyMinorBlockHeader(b.header)
}

// WithMiningResult returns a new block with the data from b and update nonce and mixDigest.
//
// signature is ignored because minor block headers carry no signature field.
func (b *MinorBlock) WithMiningResult(nonce uint64, mixDigest common.Hash, signature *[65]byte) *MinorBlock {
	cpy := CopyMinorBlockHeader(b.header)
	cpy.Nonce = nonce
	cpy.MixDigest = mixDigest

	return b.WithSeal(cpy)
}

func (b *MinorBlock) Content() []IHashable {
	items := make([]IHashable, len(b.transactions))
	for i, item := range b.transactions {
		if item != nil {
			items[i] = NewTransaction(item.inner)
		}
	}
	return items
}

func (b *MinorBlock) GetMetaData() *MinorBlockMeta {
	return b.Meta()
}

func (b *MinorBlock) GetTrackingData() []byte {
	return b.TrackingData()
}

func (b *MinorBlock) GetTransactions() Transactions {
	return b.Transactions()
}

func (b *MinorBlock) GetSize() common.StorageSize {
	return b.Size()
}

func (m *MinorBlock) Finalize(receipts Receipts, rootHash common.Hash, gasUsed *big.Int, xShardReceiveGasUsed *big.Int, coinbaseAmount *qkcCommon.TokenBalances, xShardTxCursorInfo *XShardTxCursorInfo) {
	if len(receipts) < len(m.transactions) {
		panic("receipts count is less than txs count")
	}
	if gasUsed == nil {
		gasUsed = new(big.Int)
	}
	if xShardReceiveGasUsed == nil {
		xShardReceiveGasUsed = new(big.Int)
	}

	if xShardTxCursorInfo == nil {
		m.meta.XShardTxCursorInfo = &XShardTxCursorInfo{}
	} else {
		cursor := *xShardTxCursorInfo
		m.meta.XShardTxCursorInfo = &cursor
	}
	m.meta.Root = rootHash
	m.meta.GasUsed = &serialize.Uint256{Value: new(big.Int).Set(gasUsed)}
	m.meta.CrossShardGasUsed = &serialize.Uint256{Value: new(big.Int).Set(xShardReceiveGasUsed)}
	if coinbaseAmount == nil {
		m.header.CoinbaseAmount = nil
	} else {
		m.header.CoinbaseAmount = coinbaseAmount.Copy()
	}
	m.meta.TxHash = CalculateMerkleRoot(m.transactions)
	m.meta.ReceiptHash = DeriveSha(receipts)
	m.header.MetaHash = m.meta.Hash()
	m.header.Bloom = CreateBloom(receipts)
	hash := m.header.Hash()
	m.hash.Store(&hash)
	m.size.Store(nil)
}

func (h *MinorBlock) CreateBlockToAppend(createTime *uint64, difficulty *big.Int, address *account.Address, nonce *uint64, gasLimit *big.Int, xShardGasLimit *big.Int, extraData []byte, coinbaseAmount *qkcCommon.TokenBalances, prevRootHash *common.Hash) *MinorBlock {
	if createTime == nil {
		preTime := h.Time() + 1
		createTime = &preTime
	}

	if difficulty == nil {
		difficulty = h.Difficulty()
	}

	if address == nil {
		emptyAddress := account.CreatEmptyAddress(h.header.Coinbase.FullShardKey)
		address = &emptyAddress
	}

	if nonce == nil {
		zeroNonce := uint64(0)
		nonce = &zeroNonce
	}

	if gasLimit == nil {
		gasLimit = h.GasLimit()
	}

	if xShardGasLimit == nil {
		xShardGasLimit = new(big.Int).Div(gasLimit, new(big.Int).SetUint64(2))
	}

	if extraData == nil {
		extraData = make([]byte, 0)
	}

	if coinbaseAmount == nil {
		coinbaseAmount = qkcCommon.NewEmptyTokenBalances()
	}

	if prevRootHash == nil {
		preHash := h.PrevRootBlockHash()
		prevRootHash = &preHash
	}
	header := &MinorBlockHeader{
		Version:           h.Version(),
		Number:            h.Number() + 1,
		Branch:            h.Branch(),
		Coinbase:          *address,
		CoinbaseAmount:    coinbaseAmount.Copy(),
		ParentHash:        h.Hash(),
		PrevRootBlockHash: *prevRootHash,
		GasLimit:          &serialize.Uint256{Value: new(big.Int).Set(gasLimit)},
		Time:              *createTime,
		Difficulty:        new(big.Int).Set(difficulty),
		Nonce:             *nonce,
		Extra:             common.CopyBytes(extraData),
	}
	var cursor *XShardTxCursorInfo
	if h.meta.XShardTxCursorInfo != nil {
		cpy := *h.meta.XShardTxCursorInfo
		cursor = &cpy
	}
	meta := MinorBlockMeta{
		GasUsed:            &serialize.Uint256{Value: new(big.Int)},
		CrossShardGasUsed:  &serialize.Uint256{Value: new(big.Int)},
		XShardTxCursorInfo: cursor,
		XShardGasLimit:     &serialize.Uint256{Value: new(big.Int).Set(xShardGasLimit)},
	}
	return &MinorBlock{
		header:       header,
		meta:         &meta,
		trackingdata: []byte{},
	}
}

// AddTx appends to the body without touching meta or header, so it leaves
// meta.TxHash, header.MetaHash, and any Hash() already cached stale. Callers
// must Finalize before relying on Hash(); Finalize recomputes both hashes and
// refreshes the cache.
func (h *MinorBlock) AddTx(tx *Transaction) {
	h.transactions = append(h.transactions, copyTransaction(tx))
	h.hash.Store(nil)
	h.size.Store(nil)
}

func GetEmptyMinorBlock() *MinorBlock {
	return NewMinorBlockWithHeader(getDefaultMinorBlockHeader(), getDefaultMinorBlockMeta())
}

func getDefaultMinorBlockHeader() *MinorBlockHeader {
	return &MinorBlockHeader{
		CoinbaseAmount: qkcCommon.NewEmptyTokenBalances(),
		Branch:         account.Branch{Value: 1},
		GasLimit:       &serialize.Uint256{Value: params.DefaultBlockGasLimit},
		Difficulty:     new(big.Int).SetUint64(0),
	}
}

func getDefaultMinorBlockMeta() *MinorBlockMeta {
	return &MinorBlockMeta{
		GasUsed:            &serialize.Uint256{Value: new(big.Int)},
		CrossShardGasUsed:  &serialize.Uint256{Value: new(big.Int)},
		XShardTxCursorInfo: &XShardTxCursorInfo{},
		XShardGasLimit: &serialize.Uint256{
			Value: new(big.Int).Div(params.DefaultBlockGasLimit, big.NewInt(2)),
		},
	}
}
