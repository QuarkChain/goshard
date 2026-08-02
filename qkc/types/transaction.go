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

// Transaction types.
const (
	EvmTxType = 0
)

// QKC transactions support one shard per chain. This value is only used
// to derive shard IDs and full shard IDs and is not part of transaction encoding.
const qkcShardSize = 1

//go:generate gencodec -type EvmTx -field-override txdataMarshaling -out gen_tx_json.go

var (
	ErrInvalidSig = errors.New("invalid transaction v, r, s values")
)

type TxData interface {
	copy() TxData
	txType() uint8
	encode(io.Writer) error
	decode(*rlp.Stream) error
	validate() error
	nonce() uint64
	gas() uint64
	gasPrice() *big.Int
	value() *big.Int
	data() []byte
	to() *account.Recipient
	chainID() *big.Int
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

type EvmTx struct {
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

func NewEvmTransaction(nonce uint64, to account.Recipient, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32,
	toFullShardKey uint32, networkId uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *Transaction {
	return NewTransaction(newEvmTransaction(nonce, &to, amount, gasLimit, gasPrice, fromFullShardKey, toFullShardKey, networkId, version, data, gasTokenID, transferTokenID))
}

func (tx *EvmTx) copy() TxData {
	return tx.copyData()
}

func (*EvmTx) txType() uint8 { return EvmTxType }

func NewEvmContractCreation(nonce uint64, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32, toFullShardKey uint32, networkId uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *Transaction {
	return NewTransaction(newEvmTransaction(nonce, nil, amount, gasLimit, gasPrice, fromFullShardKey, toFullShardKey, networkId, version, data, gasTokenID, transferTokenID))
}

func newEvmTransaction(nonce uint64, to *account.Recipient, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32, toFullShardKey uint32, networkId uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *EvmTx {
	newFromFullShardKey := qkcCommon.Uint32(fromFullShardKey)
	newToFullShardKey := qkcCommon.Uint32(toFullShardKey)
	if len(data) > 0 {
		data = common.CopyBytes(data)
	}
	d := &EvmTx{
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

func (tx *EvmTx) encode(w io.Writer) error { return rlp.Encode(w, tx) }
func (tx *EvmTx) decode(s *rlp.Stream) error {
	if err := s.Decode(tx); err != nil {
		return err
	}
	return tx.validate()
}

// validate enforces intrinsic transaction-type rules on every decode path.
// Inputs that break them are rejected at decode time, so an invalid version 2
// transaction (cross-shard or non-default token) cannot be deserialized at all.
func (tx *EvmTx) validate() error {
	if tx.FromFullShardKey == nil {
		return errors.New("missing from full shard key")
	}
	if tx.ToFullShardKey == nil {
		return errors.New("missing to full shard key")
	}
	if tx.Version > 2 {
		return fmt.Errorf("unsupported transaction version %d", tx.Version)
	}
	if tx.Amount != nil && len(tx.Amount.Bytes()) > 32 {
		return errors.New("amount exceeds 32 bytes")
	}
	if tx.Amount != nil && tx.Amount.Sign() < 0 {
		return errors.New("amount must not be negative")
	}
	if tx.Price != nil && len(tx.Price.Bytes()) > 32 {
		return errors.New("gas price exceeds 32 bytes")
	}
	if tx.Price != nil && tx.Price.Sign() < 0 {
		return errors.New("gas price must not be negative")
	}
	if tx.Version == 2 {
		defaultTokenID := qkcCommon.TokenIDEncode("QKC")
		if tx.GasTokenID != defaultTokenID || tx.TransferTokenID != defaultTokenID {
			return ErrV2NonDefaultToken
		}
		if tx.isCrossShard() {
			return ErrV2CrossShard
		}
	}
	return nil
}

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

func (tx *EvmTx) getUnsignedHash() common.Hash {
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

func (tx *EvmTx) getUnsignedHashForEip155(chainId uint32) common.Hash {
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

func (tx *EvmTx) typedHash() (common.Hash, error) {
	sigHash, err := typedSignatureHash(evmTxToTypedData(tx))
	if err != nil {
		return common.Hash{}, err
	}
	bytes := common.FromHex(sigHash)
	return common.BytesToHash(bytes), nil
}

func (tx *EvmTx) data() []byte       { return common.CopyBytes(tx.Payload) }
func (tx *EvmTx) gas() uint64        { return tx.GasLimit }
func (tx *EvmTx) gasPrice() *big.Int { return new(big.Int).Set(tx.Price) }
func (tx *EvmTx) value() *big.Int    { return new(big.Int).Set(tx.Amount) }
func (tx *EvmTx) nonce() uint64      { return tx.AccountNonce }
func (tx *EvmTx) fromFullShardID() uint32 {
	return tx.fromChainID()<<16 | qkcShardSize | tx.fromShardID()
}
func (tx *EvmTx) toFullShardID() uint32 {
	return tx.toChainID()<<16 | qkcShardSize | tx.toShardID()
}
func (tx *EvmTx) networkID() uint32 { return tx.NetworkID }
func (tx *EvmTx) version() uint32   { return tx.Version }
func (tx *EvmTx) chainID() *big.Int {
	if tx.Version == 2 {
		return new(big.Int).SetUint64(uint64(tx.NetworkID))
	}
	return new(big.Int)
}
func (tx *EvmTx) isCrossShard() bool {
	return tx.fromChainID() != tx.toChainID() || tx.fromShardID() != tx.toShardID()
}
func (tx *EvmTx) gasTokenID() uint64       { return tx.GasTokenID }
func (tx *EvmTx) transferTokenID() uint64  { return tx.TransferTokenID }
func (tx *EvmTx) fromFullShardKey() uint32 { return tx.FromFullShardKey.GetValue() }
func (tx *EvmTx) toFullShardKey() uint32   { return tx.ToFullShardKey.GetValue() }
func (tx *EvmTx) fromChainID() uint32      { return tx.fromFullShardKey() >> 16 }
func (tx *EvmTx) toChainID() uint32        { return tx.toFullShardKey() >> 16 }
func (tx *EvmTx) fromShardKey() uint32 {
	shardMask := uint32(65535)
	return tx.fromFullShardKey() & shardMask
}

func (tx *EvmTx) toShardKey() uint32 {
	shardMask := uint32(65535)
	return tx.toFullShardKey() & shardMask
}

func (tx *EvmTx) fromShardID() uint32 {
	shardMask := uint32(qkcShardSize - 1)
	return tx.fromFullShardKey() & shardMask
}
func (tx *EvmTx) toShardID() uint32 {
	shardMask := uint32(qkcShardSize - 1)
	return tx.toFullShardKey() & shardMask
}

// to returns the recipient address of the transaction.
// It returns nil if the transaction is a contract creation.
func (tx *EvmTx) to() *account.Recipient {
	if tx.Recipient == nil {
		return nil
	}
	recipient := *tx.Recipient
	return &recipient
}

func (tx *EvmTx) setGas(gas uint64) { tx.GasLimit = gas }

func (tx *EvmTx) setFromFullShardKey(fullShardKey uint32) {
	key := qkcCommon.Uint32(fullShardKey)
	tx.FromFullShardKey = &key
}

func (tx *EvmTx) setNonce(nonce uint64) { tx.AccountNonce = nonce }

func (tx *EvmTx) setSignatureValues(v, r, s *big.Int) {
	tx.V = copyBigInt(v)
	tx.R = copyBigInt(r)
	tx.S = copyBigInt(s)
}

func (tx *EvmTx) sigHash() (common.Hash, error) {
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

func (tx *EvmTx) copyData() *EvmTx {
	cpy := *tx
	cpy.Price = new(big.Int)
	cpy.Amount = new(big.Int)
	cpy.Payload = common.CopyBytes(tx.Payload)
	cpy.V = new(big.Int)
	cpy.R = new(big.Int)
	cpy.S = new(big.Int)
	if tx.Price != nil {
		cpy.Price.Set(tx.Price)
	}
	if tx.Amount != nil {
		cpy.Amount.Set(tx.Amount)
	}
	if tx.V != nil {
		cpy.V.Set(tx.V)
	}
	if tx.R != nil {
		cpy.R.Set(tx.R)
	}
	if tx.S != nil {
		cpy.S.Set(tx.S)
	}
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
func (tx *EvmTx) cost() *big.Int {
	total := new(big.Int).Mul(tx.Price, new(big.Int).SetUint64(tx.GasLimit))
	total.Add(total, tx.Amount)
	return total
}

func (tx *EvmTx) rawSignatureValues() (*big.Int, *big.Int, *big.Int) {
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
func (tx *Transaction) ChainId() *big.Int        { return tx.inner.chainID() }
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

func (tx *Transaction) Validate() error {
	if tx == nil || tx.inner == nil {
		return errors.New("invalid nil transaction")
	}
	return tx.inner.validate()
}

func newTxData(txType uint8) (TxData, error) {
	switch txType {
	case EvmTxType:
		return new(EvmTx), nil
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
	inner, err := newTxData(EvmTxType)
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

// SetGas mutates tx in place and clears its derived caches.
// It is not safe for concurrent use and must only be called before tx is shared.
func (tx *Transaction) SetGas(gas uint64) {
	tx.inner.setGas(gas)
	tx.clearCaches()
}

// SetFromFullShardKey mutates tx in place and clears its derived caches.
// It is not safe for concurrent use and must only be called before tx is shared.
func (tx *Transaction) SetFromFullShardKey(fullShardKey uint32) {
	tx.inner.setFromFullShardKey(fullShardKey)
	tx.clearCaches()
}

// SetNonce mutates tx in place and clears its derived caches.
// It is not safe for concurrent use and must only be called before tx is shared.
func (tx *Transaction) SetNonce(nonce uint64) {
	tx.inner.setNonce(nonce)
	tx.clearCaches()
}

// SetVRS mutates tx in place and clears its derived caches.
// It is not safe for concurrent use and must only be called before tx is shared.
func (tx *Transaction) SetVRS(v, r, s *big.Int) {
	tx.inner.setSignatureValues(v, r, s)
	tx.clearCaches()
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
	*w = append(*w, tx.Type())

	switch tx.Type() {
	case EvmTxType:
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
		return fmt.Errorf("ser: transaction type %d is not supported", tx.Type())
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
	case EvmTxType:
		payload, err := bb.GetVarBytes(4)
		if err != nil {
			return err
		}

		inner, err := newTxData(txType)
		if err != nil {
			return err
		}
		if err := rlp.DecodeBytes(payload, inner); err != nil {
			return err
		}
		if err := inner.validate(); err != nil {
			return err
		}
		tx.inner = inner
		tx.clearCaches()
		return nil
	default:
		return fmt.Errorf("deser: transaction type %d is not supported", txType)
	}
}

// Hash return the hash of the transaction it contained
func (tx *Transaction) Hash() (h common.Hash) {
	if tx.Type() == EvmTxType {
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

func (tx *Transaction) Sender(signer Signer) (account.Recipient, error) {
	if tx.Type() == EvmTxType {
		addr, err := Sender(signer, tx)
		if err != nil {
			log.Error(err.Error(), "tx", tx)
			return account.Recipient{}, err
		}

		return addr, nil
	} else {
		err := fmt.Errorf("do not support tx type %d", tx.Type())
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
