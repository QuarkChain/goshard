// Copyright 2026-2027, QuarkChain.

// Transactions follow pyquarkchain-compatible QKC wire encoding.
// Modified from go-ethereum under GNU Lesser General Public License
// Adaptation: sha3.NewKeccak256() -> crypto.NewKeccakState() (identical Keccak-256 digest).

package types

import (
	"container/heap"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/rlp"
)

// Transaction types.
const (
	QkcTxType = 0
)

// QKC transactions support one shard per chain. This value is only used
// to derive shard IDs and full shard IDs and is not part of transaction encoding.
const qkcShardSize = 1

//go:generate gencodec -type QkcTx -field-override txdataMarshaling -out gen_tx_json.go

var (
	ErrInvalidSig = errors.New("invalid transaction v, r, s values")
)

type TxData interface {
	copy() TxData
	txType() uint8
	encode(io.Writer) error
	decode(*rlp.Stream) error
	nonce() uint64
	gas() uint64
	gasPrice() *big.Int
	value() *big.Int
	data() []byte
	to() *account.Recipient
	networkID() uint32
	gasTokenID() uint64
	transferTokenID() uint64
	version() uint32

	fromFullShardKey() uint32
	toFullShardKey() uint32
	fromFullShardID() uint32
	toFullShardID() uint32
	fromChainID() uint32
	toChainID() uint32
	fromShardKey() uint32
	toShardKey() uint32
	fromShardID() uint32
	toShardID() uint32

	isCrossShard() bool
	rawSignatureValues() (*big.Int, *big.Int, *big.Int)
	setSignatureValues(v, r, s *big.Int)

	setGas(uint64)
	setFromFullShardKey(uint32)
	setNonce(uint64)

	sigHash() (common.Hash, error)
	cost() *big.Int
}

type QkcTx struct {
	AccountNonce     uint64             `json:"nonce"              gencodec:"required"`
	Price            *big.Int           `json:"gasPrice"           gencodec:"required"`
	GasLimit         uint64             `json:"gas"                gencodec:"required"`
	Recipient        *account.Recipient `json:"to"                 rlp:"nil"`
	Amount           *big.Int           `json:"value"              gencodec:"required"`
	Payload          []byte             `json:"input"              gencodec:"required"`
	NetworkID        uint32             `json:"networkId"          gencodec:"required"`
	FromFullShardKey *qkcCommon.Uint32  `json:"fromfullshardkey"   gencodec:"required"`
	ToFullShardKey   *qkcCommon.Uint32  `json:"tofullshardkey"     gencodec:"required"`
	GasTokenID       uint64             `json:"gas_token_id"       gencodec:"required"`
	TransferTokenID  uint64             `json:"transfer_token_id"  gencodec:"required"`
	Version          uint32             `json:"version"            gencodec:"required"`
	V                *big.Int           `json:"v"                  gencodec:"required"`
	R                *big.Int           `json:"r"                  gencodec:"required"`
	S                *big.Int           `json:"s"                  gencodec:"required"`
}

func NewQkcTransaction(nonce uint64, to account.Recipient, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32,
	toFullShardKey uint32, networkId uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *Transaction {
	return NewTransaction(newQkcTransaction(nonce, &to, amount, gasLimit, gasPrice, fromFullShardKey, toFullShardKey, networkId, version, data, gasTokenID, transferTokenID))
}

func (tx *QkcTx) copy() TxData {
	return tx.copyData()
}

func (*QkcTx) txType() uint8 { return QkcTxType }

func NewQkcContractCreation(nonce uint64, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32, toFullShardKey uint32, networkId uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *Transaction {
	return NewTransaction(newQkcTransaction(nonce, nil, amount, gasLimit, gasPrice, fromFullShardKey, toFullShardKey, networkId, version, data, gasTokenID, transferTokenID))
}

func newQkcTransaction(nonce uint64, to *account.Recipient, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32, toFullShardKey uint32, networkId uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *QkcTx {
	newFromFullShardKey := qkcCommon.Uint32(fromFullShardKey)
	newToFullShardKey := qkcCommon.Uint32(toFullShardKey)
	if len(data) > 0 {
		data = common.CopyBytes(data)
	}
	d := &QkcTx{
		AccountNonce:     nonce,
		Recipient:        to,
		Payload:          data,
		Amount:           new(big.Int),
		GasLimit:         gasLimit,
		Price:            new(big.Int),
		FromFullShardKey: &newFromFullShardKey,
		ToFullShardKey:   &newToFullShardKey,
		GasTokenID:       gasTokenID,
		TransferTokenID:  transferTokenID,
		NetworkID:        networkId,
		Version:          version,
		V:                new(big.Int),
		R:                new(big.Int),
		S:                new(big.Int),
	}
	if amount != nil {
		d.Amount.Set(amount)
	}
	if gasPrice != nil {
		d.Price.Set(gasPrice)
	}

	return d
}

func (tx *QkcTx) encode(w io.Writer) error   { return rlp.Encode(w, tx) }
func (tx *QkcTx) decode(s *rlp.Stream) error { return s.Decode(tx) }

type txdataUnsigned struct {
	AccountNonce     uint64             `json:"nonce"              gencodec:"required"`
	Price            *big.Int           `json:"gasPrice"           gencodec:"required"`
	GasLimit         uint64             `json:"gas"                gencodec:"required"`
	Recipient        *account.Recipient `json:"to"                 rlp:"nil"` // nil means contract creation
	Amount           *big.Int           `json:"value"              gencodec:"required"`
	Payload          []byte             `json:"input"              gencodec:"required"`
	NetworkId        uint32             `json:"networkid"          gencodec:"required"`
	FromFullShardKey *qkcCommon.Uint32  `json:"fromfullshardid"    gencodec:"required"`
	ToFullShardKey   *qkcCommon.Uint32  `json:"tofullshardid"      gencodec:"required"`
	GasTokenID       uint64             `json:"gasTokenID"      gencodec:"required"`
	TransferTokenID  uint64             `json:"transferTokenID"      gencodec:"required"`
}

func (tx *QkcTx) getUnsignedHash() common.Hash {
	unsigntx := txdataUnsigned{
		AccountNonce:     tx.AccountNonce,
		Price:            tx.Price,
		GasLimit:         tx.GasLimit,
		Recipient:        tx.Recipient,
		Amount:           tx.Amount,
		Payload:          tx.Payload,
		FromFullShardKey: tx.FromFullShardKey,
		ToFullShardKey:   tx.ToFullShardKey,
		GasTokenID:       tx.GasTokenID,
		TransferTokenID:  tx.TransferTokenID,
		NetworkId:        tx.NetworkID,
	}
	return rlpHash(unsigntx)
}

func (tx *QkcTx) getUnsignedHashForEip155(chainId uint32) common.Hash {
	return rlpHash([]interface{}{
		tx.AccountNonce,
		tx.Price,
		tx.GasLimit,
		tx.Recipient,
		tx.Amount,
		tx.Payload,
		chainId, uint(0), uint(0),
	})
}

func (tx *QkcTx) typedHash() (common.Hash, error) {
	sigHash, err := typedSignatureHash(qkcTxToTypedData(tx))
	if err != nil {
		return common.Hash{}, err
	}
	bytes := common.FromHex(sigHash)
	return common.BytesToHash(bytes), nil
}

func (tx *QkcTx) data() []byte       { return common.CopyBytes(tx.Payload) }
func (tx *QkcTx) gas() uint64        { return tx.GasLimit }
func (tx *QkcTx) gasPrice() *big.Int { return new(big.Int).Set(tx.Price) }
func (tx *QkcTx) value() *big.Int    { return new(big.Int).Set(tx.Amount) }
func (tx *QkcTx) nonce() uint64      { return tx.AccountNonce }
func (tx *QkcTx) fromFullShardID() uint32 {
	return tx.fromChainID()<<16 | qkcShardSize | tx.fromShardID()
}
func (tx *QkcTx) toFullShardID() uint32 {
	return tx.toChainID()<<16 | qkcShardSize | tx.toShardID()
}
func (tx *QkcTx) networkID() uint32 { return tx.NetworkID }
func (tx *QkcTx) version() uint32   { return tx.Version }
func (tx *QkcTx) isCrossShard() bool {
	return tx.fromChainID() != tx.toChainID() || tx.fromShardID() != tx.toShardID()
}
func (tx *QkcTx) gasTokenID() uint64       { return tx.GasTokenID }
func (tx *QkcTx) transferTokenID() uint64  { return tx.TransferTokenID }
func (tx *QkcTx) fromFullShardKey() uint32 { return tx.FromFullShardKey.GetValue() }
func (tx *QkcTx) toFullShardKey() uint32   { return tx.ToFullShardKey.GetValue() }
func (tx *QkcTx) fromChainID() uint32      { return tx.fromFullShardKey() >> 16 }
func (tx *QkcTx) toChainID() uint32        { return tx.toFullShardKey() >> 16 }
func (tx *QkcTx) fromShardKey() uint32 {
	shardMask := uint32(65535)
	return tx.fromFullShardKey() & shardMask
}

func (tx *QkcTx) toShardKey() uint32 {
	shardMask := uint32(65535)
	return tx.toFullShardKey() & shardMask
}

func (tx *QkcTx) fromShardID() uint32 {
	shardMask := uint32(qkcShardSize - 1)
	return tx.fromFullShardKey() & shardMask
}
func (tx *QkcTx) toShardID() uint32 {
	shardMask := uint32(qkcShardSize - 1)
	return tx.toFullShardKey() & shardMask
}

// to returns the recipient address of the transaction.
// It returns nil if the transaction is a contract creation.
func (tx *QkcTx) to() *account.Recipient {
	if tx.Recipient == nil {
		return nil
	}
	recipient := *tx.Recipient
	return &recipient
}

func (tx *QkcTx) setGas(gas uint64) { tx.GasLimit = gas }

func (tx *QkcTx) setFromFullShardKey(fullShardKey uint32) {
	key := Uint32(fullShardKey)
	tx.FromFullShardKey = &key
}

func (tx *QkcTx) setNonce(nonce uint64) { tx.AccountNonce = nonce }

func (tx *QkcTx) setSignatureValues(v, r, s *big.Int) {
	tx.V = copyBigInt(v)
	tx.R = copyBigInt(r)
	tx.S = copyBigInt(s)
}

func (tx *QkcTx) sigHash() (common.Hash, error) {
	switch tx.Version {
	case 0:
		return tx.getUnsignedHash(), nil
	case 1:
		return tx.typedHash()
	case 2:
		return tx.getUnsignedHashForEip155(tx.NetworkID), nil
	default:
		return common.Hash{}, fmt.Errorf("unsupported transaction version %d", tx.Version)
	}
}

func (tx *QkcTx) copyData() *QkcTx {
	cpy := *tx
	cpy.Price = copyBigInt(tx.Price)
	cpy.Amount = copyBigInt(tx.Amount)
	cpy.Payload = common.CopyBytes(tx.Payload)
	cpy.V = copyBigInt(tx.V)
	cpy.R = copyBigInt(tx.R)
	cpy.S = copyBigInt(tx.S)
	if tx.Recipient != nil {
		recipient := *tx.Recipient
		cpy.Recipient = &recipient
	}
	if tx.FromFullShardKey != nil {
		key := *tx.FromFullShardKey
		cpy.FromFullShardKey = &key
	}
	if tx.ToFullShardKey != nil {
		key := *tx.ToFullShardKey
		cpy.ToFullShardKey = &key
	}
	return &cpy
}

func copyBigInt(value *big.Int) *big.Int {
	if value == nil {
		return nil
	}
	return new(big.Int).Set(value)
}

// Cost returns amount + gasprice * gaslimit.
func (tx *QkcTx) cost() *big.Int {
	total := new(big.Int).Mul(tx.Price, new(big.Int).SetUint64(tx.GasLimit))
	total.Add(total, tx.Amount)
	return total
}

func (tx *QkcTx) rawSignatureValues() (*big.Int, *big.Int, *big.Int) {
	return tx.V, tx.R, tx.S
}

func rlpHash(x interface{}) (h common.Hash) {
	hw := crypto.NewKeccakState()
	rlp.Encode(hw, x)
	hw.Sum(h[:0])
	return h
}

type Transaction struct {
	inner TxData

	hash atomic.Value
	size atomic.Value
	from atomic.Value
}

func NewTransaction(inner TxData) *Transaction {
	return &Transaction{inner: inner.copy()}
}

func (tx *Transaction) WithSignature(signer Signer, sig []byte) (*Transaction, error) {
	r, s, v, err := signer.SignatureValues(tx, sig)
	if err != nil {
		return nil, err
	}
	cpy := NewTransaction(tx.inner)
	cpy.inner.setSignatureValues(v, r, s)
	return cpy, nil
}

func (tx *Transaction) Data() []byte             { return tx.inner.data() }
func (tx *Transaction) Gas() uint64              { return tx.inner.gas() }
func (tx *Transaction) GasPrice() *big.Int       { return tx.inner.gasPrice() }
func (tx *Transaction) Value() *big.Int          { return tx.inner.value() }
func (tx *Transaction) Nonce() uint64            { return tx.inner.nonce() }
func (tx *Transaction) NetworkId() uint32        { return tx.inner.networkID() }
func (tx *Transaction) Version() uint32          { return tx.inner.version() }
func (tx *Transaction) To() *account.Recipient   { return tx.inner.to() }
func (tx *Transaction) GasTokenID() uint64       { return tx.inner.gasTokenID() }
func (tx *Transaction) TransferTokenID() uint64  { return tx.inner.transferTokenID() }
func (tx *Transaction) FromFullShardKey() uint32 { return tx.inner.fromFullShardKey() }
func (tx *Transaction) ToFullShardKey() uint32   { return tx.inner.toFullShardKey() }
func (tx *Transaction) FromFullShardId() uint32  { return tx.inner.fromFullShardID() }
func (tx *Transaction) ToFullShardId() uint32    { return tx.inner.toFullShardID() }
func (tx *Transaction) FromChainID() uint32      { return tx.inner.fromChainID() }
func (tx *Transaction) ToChainID() uint32        { return tx.inner.toChainID() }
func (tx *Transaction) FromShardKey() uint32     { return tx.inner.fromShardKey() }
func (tx *Transaction) ToShardKey() uint32       { return tx.inner.toShardKey() }
func (tx *Transaction) FromShardID() uint32      { return tx.inner.fromShardID() }
func (tx *Transaction) ToShardID() uint32        { return tx.inner.toShardID() }
func (tx *Transaction) IsCrossShard() bool       { return tx.inner.isCrossShard() }
func (tx *Transaction) RawSignatureValues() (*big.Int, *big.Int, *big.Int) {
	return tx.inner.rawSignatureValues()
}
func (tx *Transaction) Size() common.StorageSize {
	if size := tx.size.Load(); size != nil {
		return size.(common.StorageSize)
	}
	c := writeCounter(0)
	tx.inner.encode(&c)
	size := common.StorageSize(c)
	tx.size.Store(size)
	return size
}
func (tx *Transaction) Cost() *big.Int { return tx.inner.cost() }

func (tx *Transaction) Type() uint8 { return tx.inner.txType() }

func newTxData(txType uint8) (TxData, error) {
	switch txType {
	case QkcTxType:
		return new(QkcTx), nil
	default:
		return nil, fmt.Errorf("transaction type %d is not supported", txType)
	}
}

func (tx *Transaction) EncodeRLP(w io.Writer) error {
	return tx.inner.encode(w)
}

func (tx *Transaction) DecodeRLP(s *rlp.Stream) error {
	// The inner RLP does not contain a transaction type byte. If another
	// transaction type is added, this encoding needs a type-aware envelope.
	inner, err := newTxData(QkcTxType)
	if err != nil {
		return err
	}
	if err := inner.decode(s); err != nil {
		return err
	}
	tx.inner = inner
	tx.clearCaches()
	return nil
}

func (tx *Transaction) clearCaches() {
	tx.hash = atomic.Value{}
	tx.size = atomic.Value{}
	tx.from = atomic.Value{}
}

func (tx *Transaction) SetGas(gas uint64) {
	tx.inner.setGas(gas)
	tx.clearCaches()
}

func (tx *Transaction) SetFromFullShardKey(fullShardKey uint32) {
	tx.inner.setFromFullShardKey(fullShardKey)
	tx.clearCaches()
}

func (tx *Transaction) SetNonce(nonce uint64) {
	tx.inner.setNonce(nonce)
	tx.clearCaches()
}

func (tx *Transaction) SetVRS(v, r, s *big.Int) {
	tx.inner.setSignatureValues(v, r, s)
	tx.clearCaches()
}

func (tx *Transaction) CopyQkcTx() (*Transaction, error) {
	data, err := serialize.SerializeToBytes(tx)
	if err != nil {
		return nil, err
	}
	var qkcTx Transaction
	err = serialize.DeserializeFromBytes(data, &qkcTx)
	if err != nil {
		return nil, err
	}
	return &qkcTx, nil
}

func (tx *Transaction) Serialize(w *[]byte) error {
	*w = append(*w, tx.Type())

	switch tx.Type() {
	case QkcTxType:
		bytes, err := rlp.EncodeToBytes(tx.inner)
		if err != nil {
			return err
		}
		if err := serialize.Serialize(w, uint32(len(bytes))); err != nil {
			return err
		}
		*w = append(*w, bytes...)
		return nil
	default:
		return fmt.Errorf("ser: Transacton type %d is not supported", tx.Type())
	}
}

// MarshalBinary returns the canonical QKC transaction wire encoding.
func (tx *Transaction) MarshalBinary() ([]byte, error) {
	return serialize.SerializeToBytes(tx)
}

func (tx *Transaction) Deserialize(bb *serialize.ByteBuffer) error {
	txType, err := bb.GetUInt8()
	if err != nil {
		return err
	}

	switch txType {
	case QkcTxType:
		bytes, err := bb.GetVarBytes(4)
		if err != nil {
			return err
		}

		inner, err := newTxData(txType)
		if err != nil {
			return err
		}
		if err := rlp.DecodeBytes(bytes, inner); err != nil {
			return err
		}
		tx.inner = inner
		tx.clearCaches()
		return nil
	default:
		return fmt.Errorf("deser: Transacton type %d is not supported", txType)
	}
}

// Hash return the hash of the transaction it contained
func (tx *Transaction) Hash() (h common.Hash) {
	if tx.Type() == QkcTxType {
		if hash := tx.hash.Load(); hash != nil {
			return hash.(common.Hash)
		}
		serialTxBytes, err := serialize.SerializeToBytes(tx)
		if err != nil {
			panic(err)
		}
		hw := crypto.NewKeccakState()
		hw.Write(serialTxBytes)
		hw.Sum(h[:0])
		tx.hash.Store(h)
		return h
	}

	log.Error(fmt.Sprintf("do not support tx type %d", tx.Type()))
	return *new(common.Hash)
}

func (tx *Transaction) getNonce() uint64 {
	if tx.Type() == QkcTxType {
		return tx.Nonce()
	}

	//todo verify the default value when have more type of tx
	return 0
}

func (tx *Transaction) getPrice() *big.Int {
	if tx.Type() == QkcTxType {
		return tx.inner.gasPrice()
	}

	//todo verify the default value when have more type of tx
	return big.NewInt(0)
}

func (tx *Transaction) Sender(signer Signer) (account.Recipient, error) {
	if tx.Type() == QkcTxType {
		addr, err := Sender(signer, tx)
		if err != nil {
			log.Error(err.Error(), "tx", tx)
			return account.Recipient{}, err
		}

		return addr, nil
	} else {
		err := errors.New(fmt.Sprintf("do not support tx type %d", tx.Type()))
		log.Error(err.Error())
		return account.Recipient{}, err
	}
}

// Transactions is a Transaction slice type for basic sorting.
type Transactions []*Transaction

// Len returns the length of s.
func (s Transactions) Len() int { return len(s) }

func (s Transactions) Bytes(i int) []byte {
	enc, err := serialize.SerializeToBytes(s[i])
	if err != nil {
		panic(err)
	}
	return enc
}

// Swap swaps the i'th and the j'th element in s.
func (s Transactions) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// TxDifference returns a new set which is the difference between a and b.
func TxDifference(a, b Transactions) Transactions {
	keep := make(Transactions, 0, len(a))

	remove := make(map[common.Hash]struct{})
	for _, tx := range b {
		remove[tx.Hash()] = struct{}{}
	}

	for _, tx := range a {
		if _, ok := remove[tx.Hash()]; !ok {
			keep = append(keep, tx)
		}
	}

	return keep
}

// TxByNonce implements the sort interface to allow sorting a list of transactions
// by their nonces. This is usually only useful for sorting transactions from a
// single account, otherwise a nonce comparison doesn't make much sense.
type TxByNonce Transactions

func (s TxByNonce) Len() int           { return len(s) }
func (s TxByNonce) Less(i, j int) bool { return s[i].getNonce() < s[j].getNonce() }
func (s TxByNonce) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// TxByPrice implements both the sort and the heap interface, making it useful
// for all at once sorting as well as individually adding and removing elements.
type TxByPrice Transactions

func (s TxByPrice) Len() int           { return len(s) }
func (s TxByPrice) Less(i, j int) bool { return s[i].getPrice().Cmp(s[j].getPrice()) > 0 }
func (s TxByPrice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s *TxByPrice) Push(x interface{}) {
	*s = append(*s, x.(*Transaction))
}

func (s *TxByPrice) Pop() interface{} {
	old := *s
	n := len(old)
	x := old[n-1]
	*s = old[0 : n-1]
	return x
}

// TransactionsByPriceAndNonce represents a set of transactions that can return
// transactions in a profit-maximizing sorted order, while supporting removing
// entire batches of transactions for non-executable accounts.
type TransactionsByPriceAndNonce struct {
	txs    map[account.Recipient]Transactions // Per account nonce-sorted list of transactions
	heads  TxByPrice                          // Next transaction for each unique account (price heap)
	signer Signer                             // Signer for the set of transactions
}

// NewTransactionsByPriceAndNonce creates a transaction set that can retrieve
// price sorted transactions in a nonce-honouring way.
//
// Note, the input map is reowned so the caller should not interact any more with
// if after providing it to the constructor.
func NewTransactionsByPriceAndNonce(signer Signer, txs map[account.Recipient]Transactions) (*TransactionsByPriceAndNonce, error) {
	// Initialize a price based heap with the head transactions
	heads := make(TxByPrice, 0, len(txs))
	for from, accTxs := range txs {
		heads = append(heads, accTxs[0])
		// Ensure the sender address is from the signer
		acc, err := accTxs[0].Sender(signer)
		if err != nil {
			return nil, err
		}
		txs[acc] = accTxs[1:]
		if from != acc {
			delete(txs, from)
		}
	}
	heap.Init(&heads)

	// Assemble and return the transaction set
	return &TransactionsByPriceAndNonce{
		txs:    txs,
		heads:  heads,
		signer: signer,
	}, nil
}

// Peek returns the next transaction by price.
func (t *TransactionsByPriceAndNonce) Peek() *Transaction {
	if len(t.heads) == 0 {
		return nil
	}
	return t.heads[0]
}

// Shift replaces the current best head with the next one from the same account.
func (t *TransactionsByPriceAndNonce) Shift() error {
	acc, err := t.heads[0].Sender(t.signer)
	if err != nil {
		return err
	}
	if txs, ok := t.txs[acc]; ok && len(txs) > 0 {
		t.heads[0], t.txs[acc] = txs[0], txs[1:]
		heap.Fix(&t.heads, 0)
	} else {
		heap.Pop(&t.heads)
	}
	return nil
}

// Pop removes the best transaction, *not* replacing it with the next one from
// the same account. This should be used when a transaction cannot be executed
// and hence all subsequent ones should be discarded from the same account.
func (t *TransactionsByPriceAndNonce) Pop() {
	heap.Pop(&t.heads)
}
