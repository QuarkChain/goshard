// Copyright 2026-2027, QuarkChain.

// Transactions follow pyquarkchain-compatible QKC wire encoding.
// Modified from go-ethereum under GNU Lesser General Public License
// Adaptation: sha3.NewKeccak256() -> crypto.NewKeccakState() (identical Keccak-256 digest).

package types

import (
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

const (
	EvmTx = 0
)

//go:generate gencodec -type txdata -field-override txdataMarshaling -out gen_tx_json.go

var (
	ErrInvalidSig = errors.New("invalid transaction v, r, s values")
)

type EvmTransaction struct {
	data txdata
	// caches
	hashDirty     bool         // true after a setter changes the inner RLP payload.
	hash          atomic.Value // RLP hash of the inner EVM transaction.
	size          atomic.Value
	from          atomic.Value
	FromShardsize uint32
	ToShardsize   uint32
}

type txdata struct {
	AccountNonce     uint64             `json:"nonce"              gencodec:"required"`
	Price            *big.Int           `json:"gasPrice"           gencodec:"required"`
	GasLimit         uint64             `json:"gas"                gencodec:"required"`
	Recipient        *account.Recipient `json:"to"                 rlp:"nil"` // nil means contract creation
	Amount           *big.Int           `json:"value"              gencodec:"required"`
	Payload          []byte             `json:"input"              gencodec:"required"`
	NetworkId        uint32             `json:"networkId"          gencodec:"required"`
	FromFullShardKey *Uint32            `json:"fromfullshardkey"   gencodec:"required"`
	ToFullShardKey   *Uint32            `json:"tofullshardkey"     gencodec:"required"`
	GasTokenID       uint64             `json:"gas_token_id"       gencodec:"required"`
	TransferTokenID  uint64             `json:"transfer_token_id"  gencodec:"required"`
	Version          uint32             `json:"version"            gencodec:"required"`
	// Signature values
	V *big.Int `json:"v"             gencodec:"required"`
	R *big.Int `json:"r"             gencodec:"required"`
	S *big.Int `json:"s"             gencodec:"required"`
}

func NewEvmTransaction(nonce uint64, to account.Recipient, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32,
	toFullShardKey uint32, networkId uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *EvmTransaction {
	return newEvmTransaction(nonce, &to, amount, gasLimit, gasPrice, fromFullShardKey, toFullShardKey, networkId, version, data, gasTokenID, transferTokenID)
}

func (e *EvmTransaction) SetGas(data uint64) {
	e.data.GasLimit = data
	e.hashDirty = true
}

func (e *EvmTransaction) SetFromFullShardKey(data uint32) {
	t := Uint32(data)
	e.data.FromFullShardKey = &t
	e.hashDirty = true
}

func (e *EvmTransaction) SetNonce(data uint64) {
	e.data.AccountNonce = data
	e.hashDirty = true
}

func (e *EvmTransaction) SetVRS(v, r, s *big.Int) {
	e.data.V = v
	e.data.R = r
	e.data.S = s
	e.hashDirty = true
}

func (e *EvmTransaction) SetSender(signer Signer, addr account.Recipient) {
	e.from.Store(sigCache{signer: signer, from: addr})
}

func NewEvmContractCreation(nonce uint64, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32, toFullShardKey uint32, networkId uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *EvmTransaction {
	return newEvmTransaction(nonce, nil, amount, gasLimit, gasPrice, fromFullShardKey, toFullShardKey, networkId, version, data, gasTokenID, transferTokenID)
}

func newEvmTransaction(nonce uint64, to *account.Recipient, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32, toFullShardKey uint32, networkId uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *EvmTransaction {
	newFromFullShardKey := Uint32(fromFullShardKey)
	newToFullShardKey := Uint32(toFullShardKey)
	if len(data) > 0 {
		data = common.CopyBytes(data)
	}
	d := txdata{
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
		NetworkId:        networkId,
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

	return &EvmTransaction{data: d}
}

// EncodeRLP implements rlp.Encoder
func (tx *EvmTransaction) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &tx.data)
}

// DecodeRLP implements rlp.Decoder
func (tx *EvmTransaction) DecodeRLP(s *rlp.Stream) error {
	_, size, _ := s.Kind()
	err := s.Decode(&tx.data)
	if err == nil {
		tx.size.Store(common.StorageSize(rlp.ListSize(size)))
	}

	return err
}

type txdataUnsigned struct {
	AccountNonce     uint64             `json:"nonce"              gencodec:"required"`
	Price            *big.Int           `json:"gasPrice"           gencodec:"required"`
	GasLimit         uint64             `json:"gas"                gencodec:"required"`
	Recipient        *account.Recipient `json:"to"                 rlp:"nil"` // nil means contract creation
	Amount           *big.Int           `json:"value"              gencodec:"required"`
	Payload          []byte             `json:"input"              gencodec:"required"`
	NetworkId        uint32             `json:"networkid"          gencodec:"required"`
	FromFullShardKey *Uint32            `json:"fromfullshardid"    gencodec:"required"`
	ToFullShardKey   *Uint32            `json:"tofullshardid"      gencodec:"required"`
	GasTokenID       uint64             `json:"gasTokenID"      gencodec:"required"`
	TransferTokenID  uint64             `json:"transferTokenID"      gencodec:"required"`
}

func (tx *EvmTransaction) getUnsignedHash() common.Hash {
	unsigntx := txdataUnsigned{
		AccountNonce:     tx.data.AccountNonce,
		Price:            tx.data.Price,
		GasLimit:         tx.data.GasLimit,
		Recipient:        tx.data.Recipient,
		Amount:           tx.data.Amount,
		Payload:          tx.data.Payload,
		FromFullShardKey: tx.data.FromFullShardKey,
		ToFullShardKey:   tx.data.ToFullShardKey,
		GasTokenID:       tx.data.GasTokenID,
		TransferTokenID:  tx.data.TransferTokenID,
		NetworkId:        tx.data.NetworkId,
	}
	return rlpHash(unsigntx)
}

func (tx *EvmTransaction) getUnsignedHashForEip155(chainId uint32) common.Hash {
	return rlpHash([]interface{}{
		tx.data.AccountNonce,
		tx.data.Price,
		tx.data.GasLimit,
		tx.data.Recipient,
		tx.data.Amount,
		tx.data.Payload,
		chainId, uint(0), uint(0),
	})
}

func (tx *EvmTransaction) typedHash() (common.Hash, error) {
	sigHash, err := typedSignatureHash(evmTxToTypedData(tx))
	if err != nil {
		return common.Hash{}, err
	}
	bytes := common.FromHex(sigHash)
	return common.BytesToHash(bytes), nil
}

func (tx *EvmTransaction) Data() []byte       { return common.CopyBytes(tx.data.Payload) }
func (tx *EvmTransaction) Gas() uint64        { return tx.data.GasLimit }
func (tx *EvmTransaction) GasPrice() *big.Int { return new(big.Int).Set(tx.data.Price) }
func (tx *EvmTransaction) Value() *big.Int    { return new(big.Int).Set(tx.data.Amount) }
func (tx *EvmTransaction) Nonce() uint64      { return tx.data.AccountNonce }
func (tx *EvmTransaction) FromFullShardId() uint32 {
	return tx.FromChainID()<<16 | tx.FromShardSize() | tx.FromShardID()
}
func (tx *EvmTransaction) ToFullShardId() uint32 {
	return tx.ToChainID()<<16 | tx.ToShardSize() | tx.ToShardID()
}
func (tx *EvmTransaction) NetworkId() uint32 { return tx.data.NetworkId }
func (tx *EvmTransaction) Version() uint32   { return tx.data.Version }
func (tx *EvmTransaction) IsCrossShard() bool {
	return !(tx.FromChainID() == tx.ToChainID() && tx.FromShardID() == tx.ToShardID())
}
func (tx *EvmTransaction) GasTokenID() uint64 {
	return tx.data.GasTokenID
}
func (tx *EvmTransaction) TransferTokenID() uint64 {
	return tx.data.TransferTokenID
}
func (tx *EvmTransaction) FromFullShardKey() uint32 { return tx.data.FromFullShardKey.GetValue() }
func (tx *EvmTransaction) ToFullShardKey() uint32   { return tx.data.ToFullShardKey.GetValue() }
func (tx *EvmTransaction) FromChainID() uint32      { return tx.data.FromFullShardKey.GetValue() >> 16 }
func (tx *EvmTransaction) ToChainID() uint32        { return tx.data.ToFullShardKey.GetValue() >> 16 }
func (tx *EvmTransaction) FromShardSize() uint32 {
	return tx.FromShardsize
}
func (tx *EvmTransaction) ToShardSize() uint32 {
	return tx.ToShardsize
}
func (tx *EvmTransaction) SetFromShardSize(shardSize uint32) error {
	if !qkcCommon.IsP2(shardSize) || shardSize == 0 {
		return errors.New("shardSize is not Usable")
	}
	tx.FromShardsize = shardSize
	return nil
}
func (tx *EvmTransaction) SetToShardSize(shardSize uint32) error {
	if !qkcCommon.IsP2(shardSize) || shardSize == 0 {
		return errors.New("shardSize is not Usable")
	}
	tx.ToShardsize = shardSize
	return nil
}

func (tx *EvmTransaction) FromShardKey() uint32 {
	shardMask := uint32(65535)
	return tx.data.FromFullShardKey.GetValue() & shardMask
}

func (tx *EvmTransaction) ToShardKey() uint32 {
	shardMask := uint32(65535)
	return tx.data.ToFullShardKey.GetValue() & shardMask
}

func (tx *EvmTransaction) FromShardID() uint32 {
	shardMask := tx.FromShardSize() - 1
	return tx.data.FromFullShardKey.GetValue() & shardMask
}
func (tx *EvmTransaction) ToShardID() uint32 {
	shardMask := tx.ToShardSize() - 1
	return tx.data.ToFullShardKey.GetValue() & shardMask
}

// To returns the recipient address of the transaction.
// It returns nil if the transaction is a contract creation.
func (tx *EvmTransaction) To() *account.Recipient {
	if tx.data.Recipient == nil {
		return nil
	}

	to := *tx.data.Recipient
	return &to
}

// Hash hashes the RLP encoding of tx.
// It uniquely identifies the transaction.
func (tx *EvmTransaction) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil && !tx.hashDirty {
		return hash.(common.Hash)
	}
	v := rlpHash(tx)
	tx.hash.Store(v)
	tx.hashDirty = false
	return v
}

// Size returns the true RLP encoded storage size of the transaction, either by
// encoding and returning it, or returning a previsouly cached value.
func (tx *EvmTransaction) Size() common.StorageSize {
	if size := tx.size.Load(); size != nil {
		return size.(common.StorageSize)
	}
	c := writeCounter(0)
	rlp.Encode(&c, &tx.data)
	tx.size.Store(common.StorageSize(c))
	return common.StorageSize(c)
}

// WithSignature returns a new transaction with the given signature.
// This signature needs to be formatted as described in the yellow paper (v+27).
func (tx *EvmTransaction) WithSignature(signer Signer, sig []byte) (*EvmTransaction, error) {
	r, s, v, err := signer.SignatureValues(tx, sig)
	if err != nil {
		return nil, err
	}
	cpy := &EvmTransaction{data: tx.data}
	cpy.data.R, cpy.data.S, cpy.data.V = r, s, v
	return cpy, nil
}

// Cost returns amount + gasprice * gaslimit.
func (tx *EvmTransaction) Cost() *big.Int {
	total := new(big.Int).Mul(tx.data.Price, new(big.Int).SetUint64(tx.data.GasLimit))
	total.Add(total, tx.data.Amount)
	return total
}

func (tx *EvmTransaction) RawSignatureValues() (*big.Int, *big.Int, *big.Int) {
	return tx.data.V, tx.data.R, tx.data.S
}

func rlpHash(x interface{}) (h common.Hash) {
	hw := crypto.NewKeccakState()
	rlp.Encode(hw, x)
	hw.Sum(h[:0])
	return h
}

type Transaction struct {
	TxType uint8
	EvmTx  *EvmTransaction

	hash atomic.Value // Hash of the typed QKC transaction envelope.
}

func (tx *Transaction) CopyEvmTx() (*Transaction, error) {
	data, err := serialize.SerializeToBytes(tx)
	if err != nil {
		return nil, err
	}
	var evmTx Transaction
	err = serialize.DeserializeFromBytes(data, &evmTx)
	if err != nil {
		return nil, err
	}
	return &evmTx, nil
}

func (tx *Transaction) Serialize(w *[]byte) error {
	*w = append(*w, tx.TxType)

	switch tx.TxType {
	case EvmTx:
		bytes, err := rlp.EncodeToBytes(tx.EvmTx)
		if err != nil {
			return err
		}
		serialize.Serialize(w, uint32(len(bytes)))
		*w = append(*w, bytes...)
		return nil
	default:
		return fmt.Errorf("ser: Transacton type %d is not supported", tx.TxType)
	}
}

func (tx *Transaction) Deserialize(bb *serialize.ByteBuffer) error {
	txType, err := bb.GetUInt8()
	if err != nil {
		return err
	}

	switch txType {
	case EvmTx:
		tx.TxType = txType
		bytes, err := bb.GetVarBytes(4)
		if err != nil {
			return err
		}

		if tx.EvmTx == nil {
			tx.EvmTx = new(EvmTransaction)
		}
		return rlp.DecodeBytes(bytes, tx.EvmTx)
	default:
		return fmt.Errorf("deser: Transacton type %d is not supported", txType)
	}
}

// Hash return the hash of the transaction it contained
func (tx *Transaction) Hash() (h common.Hash) {
	if tx.TxType == EvmTx {
		if hash := tx.hash.Load(); hash != nil {
			return hash.(common.Hash)
		}
		hw := crypto.NewKeccakState()
		serialTxBytes, err := serialize.SerializeToBytes(tx)
		if err != nil {
			//TODO  panic ?
			//TODO  not cache?
			panic(err)
		}
		hw.Write(serialTxBytes)
		hw.Sum(h[:0])
		tx.hash.Store(h)
		return h
	}

	log.Error(fmt.Sprintf("do not support tx type %d", tx.TxType))
	return *new(common.Hash)
}

func (tx *Transaction) getNonce() uint64 {
	if tx.TxType == EvmTx {
		return tx.EvmTx.data.AccountNonce
	}

	//todo verify the default value when have more type of tx
	return 0
}

func (tx *Transaction) getPrice() *big.Int {
	if tx.TxType == EvmTx {
		return tx.EvmTx.data.Price
	}

	//todo verify the default value when have more type of tx
	return big.NewInt(0)
}

func (tx *Transaction) Sender(signer Signer) (account.Recipient, error) {
	if tx.TxType == EvmTx {
		addr, err := Sender(signer, tx.EvmTx)
		if err != nil {
			log.Error(err.Error(), "tx", tx)
			return account.Recipient{}, err
		}

		return addr, nil
	} else {
		err := errors.New(fmt.Sprintf("do not support tx type %d", tx.TxType))
		log.Error(err.Error())
		return account.Recipient{}, err
	}
}

// Transactions is a EvmTransaction slice type for basic sorting.
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
