// Copyright 2026-2027, QuarkChain.

package types

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// QkcTxData extends TxData with fields used by QKC transactions.
// Ethereum transaction types don't implement this interface and return the
// default values exposed by Transaction's QKC getters.
type QkcTxData interface {
	TxData

	networkID() uint32
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
	gasTokenID() uint64
	transferTokenID() uint64
	isCrossShard() bool

	setGas(uint64)
	setNonce(uint64)
	setFromFullShardKey(uint32)
}

// Serialize implements QKC protocol serialization for QKC transactions.
func (tx *Transaction) Serialize(output *[]byte) error {
	if _, ok := tx.inner.(QkcTxData); !ok {
		return ErrTxTypeNotSupported
	}
	encoded, err := tx.MarshalBinary()
	if err != nil {
		return err
	}
	*output = append(*output, encoded...)
	return nil
}

// Deserialize implements QKC protocol deserialization.
func (tx *Transaction) Deserialize(input *serialize.ByteBuffer) error {
	txType, err := input.GetUInt8()
	if err != nil {
		return err
	}
	if txType != qkcWireTxType {
		return ErrTxTypeNotSupported
	}
	length, err := input.GetUInt32()
	if err != nil {
		return err
	}
	if uint64(length) > uint64(input.Remaining()) {
		return fmt.Errorf("transaction length %d exceeds remaining buffer %d", length, input.Remaining())
	}
	payload, err := input.ReadBytes(int(length))
	if err != nil {
		return err
	}
	inner := new(QkcTx)
	if err := inner.decode(payload); err != nil {
		return err
	}
	tx.setDecoded(inner, uint64(length)+5)
	return nil
}

func (tx *Transaction) NetworkID() uint32 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.networkID()
	}
	return 0
}

func (tx *Transaction) Version() uint32 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.version()
	}
	return 0
}

func (tx *Transaction) FromFullShardKey() uint32 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.fromFullShardKey()
	}
	return 0
}

func (tx *Transaction) ToFullShardKey() uint32 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.toFullShardKey()
	}
	return 0
}

func (tx *Transaction) GasTokenID() uint64 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.gasTokenID()
	}
	return 0
}

func (tx *Transaction) TransferTokenID() uint64 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.transferTokenID()
	}
	return 0
}

func (tx *Transaction) FromFullShardID() uint32 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.fromFullShardID()
	}
	return 0
}

func (tx *Transaction) ToFullShardID() uint32 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.toFullShardID()
	}
	return 0
}

func (tx *Transaction) FromChainID() uint32 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.fromChainID()
	}
	return 0
}

func (tx *Transaction) ToChainID() uint32 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.toChainID()
	}
	return 0
}

func (tx *Transaction) FromShardKey() uint32 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.fromShardKey()
	}
	return 0
}

func (tx *Transaction) ToShardKey() uint32 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.toShardKey()
	}
	return 0
}

func (tx *Transaction) FromShardID() uint32 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.fromShardID()
	}
	return 0
}

func (tx *Transaction) ToShardID() uint32 {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.toShardID()
	}
	return 0
}

func (tx *Transaction) IsCrossShard() bool {
	if inner, ok := tx.inner.(QkcTxData); ok {
		return inner.isCrossShard()
	}
	return false
}

// clearCaches invalidates the cached hash, size, and sender after a mutation.
func (tx *Transaction) clearCaches() {
	tx.hash.Store(nil)
	tx.size.Store(0)
	tx.from.Store(nil)
}

// SetGas, SetNonce, SetFromFullShardKey, and SetVRS mutate the transaction in
// place and are only valid for QKC transactions; they are no-ops on other types.
// They are not safe for concurrent use and must only be called before the
// transaction is shared with other goroutines.
func (tx *Transaction) SetGas(gas uint64) {
	inner, ok := tx.inner.(QkcTxData)
	if !ok {
		return
	}
	inner.setGas(gas)
	tx.clearCaches()
}

func (tx *Transaction) SetNonce(nonce uint64) {
	inner, ok := tx.inner.(QkcTxData)
	if !ok {
		return
	}
	inner.setNonce(nonce)
	tx.clearCaches()
}

func (tx *Transaction) SetFromFullShardKey(fullShardKey uint32) {
	inner, ok := tx.inner.(QkcTxData)
	if !ok {
		return
	}
	inner.setFromFullShardKey(fullShardKey)
	tx.clearCaches()
}

func (tx *Transaction) SetVRS(v, r, s *big.Int) {
	if _, ok := tx.inner.(QkcTxData); !ok {
		return
	}
	tx.inner.setSignatureValues(tx.inner.chainID(), v, r, s)
	tx.clearCaches()
}
