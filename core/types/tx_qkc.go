// Copyright 2026-2027, QuarkChain.

package types

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

const qkcWireTxType = 0x00

// qkcShardSize matches the fixed shard size assumed by the pyquarkchain-compatible
// qkc/types reference. See fromShardID/toShardID for the dynamic-shard-size caveat.
const qkcShardSize = 1

type qkcUint32 uint32

func (value *qkcUint32) EncodeRLP(w io.Writer) error {
	var encoded [5]byte
	encoded[0] = 0x84
	binary.BigEndian.PutUint32(encoded[1:], uint32(*value))
	_, err := w.Write(encoded[:])
	return err
}

func (value *qkcUint32) DecodeRLP(stream *rlp.Stream) error {
	encoded, err := stream.Raw()
	if err != nil {
		return err
	}
	if len(encoded) != 5 || encoded[0] != 0x84 {
		return fmt.Errorf("invalid fixed-width uint32 RLP %x", encoded)
	}
	*value = qkcUint32(binary.BigEndian.Uint32(encoded[1:]))
	return nil
}

// QkcTx is the QKC implementation of TxData.
type QkcTx struct {
	AccountNonce     uint64
	Price            *big.Int
	GasLimit         uint64
	Recipient        *common.Address `rlp:"nil"`
	Amount           *big.Int
	Payload          []byte
	NetworkID        uint32
	FromFullShardKey *qkcUint32
	ToFullShardKey   *qkcUint32
	GasTokenID       uint64
	TransferTokenID  uint64
	Version          uint32
	V                *big.Int
	R                *big.Int
	S                *big.Int
}

func NewQkcTransaction(nonce uint64, to common.Address, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey, toFullShardKey, networkID, version uint32, data []byte, gasTokenID, transferTokenID uint64) *Transaction {
	return NewTx(newQkcTx(nonce, &to, amount, gasLimit, gasPrice, fromFullShardKey, toFullShardKey, networkID, version, data, gasTokenID, transferTokenID))
}

func NewQkcContractCreation(nonce uint64, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey, toFullShardKey, networkID, version uint32, data []byte, gasTokenID, transferTokenID uint64) *Transaction {
	return NewTx(newQkcTx(nonce, nil, amount, gasLimit, gasPrice, fromFullShardKey, toFullShardKey, networkID, version, data, gasTokenID, transferTokenID))
}

func newQkcTx(nonce uint64, to *common.Address, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey, toFullShardKey, networkID, version uint32, data []byte, gasTokenID, transferTokenID uint64) *QkcTx {
	newFromFullShardKey := qkcUint32(fromFullShardKey)
	newToFullShardKey := qkcUint32(toFullShardKey)
	if len(data) > 0 {
		data = common.CopyBytes(data)
	}
	d := &QkcTx{
		AccountNonce:     nonce,
		Recipient:        copyAddressPtr(to),
		Payload:          data,
		Amount:           new(big.Int),
		GasLimit:         gasLimit,
		Price:            new(big.Int),
		FromFullShardKey: &newFromFullShardKey,
		ToFullShardKey:   &newToFullShardKey,
		GasTokenID:       gasTokenID,
		TransferTokenID:  transferTokenID,
		NetworkID:        networkID,
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

func (tx *QkcTx) copy() TxData {
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

type qkcTxUnsigned struct {
	AccountNonce     uint64
	Price            *big.Int
	GasLimit         uint64
	Recipient        *common.Address `rlp:"nil"`
	Amount           *big.Int
	Payload          []byte
	NetworkID        uint32
	FromFullShardKey *qkcUint32
	ToFullShardKey   *qkcUint32
	GasTokenID       uint64
	TransferTokenID  uint64
}

func (tx *QkcTx) getUnsignedHash() common.Hash {
	unsignedTx := qkcTxUnsigned{
		AccountNonce:     tx.AccountNonce,
		Price:            tx.Price,
		GasLimit:         tx.GasLimit,
		Recipient:        tx.Recipient,
		Amount:           tx.Amount,
		Payload:          tx.Payload,
		NetworkID:        tx.NetworkID,
		FromFullShardKey: tx.FromFullShardKey,
		ToFullShardKey:   tx.ToFullShardKey,
		GasTokenID:       tx.GasTokenID,
		TransferTokenID:  tx.TransferTokenID,
	}
	return rlpHash(unsignedTx)
}

func (tx *QkcTx) getUnsignedHashForEip155(chainID uint32) common.Hash {
	return rlpHash([]interface{}{
		tx.AccountNonce,
		tx.Price,
		tx.GasLimit,
		tx.Recipient,
		tx.Amount,
		tx.Payload,
		chainID, uint(0), uint(0),
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

func (*QkcTx) txType() byte { return QkcTxType }
func (tx *QkcTx) chainID() *big.Int {
	if tx.Version == 2 {
		return new(big.Int).SetUint64(uint64(tx.NetworkID))
	}
	return new(big.Int)
}
func (*QkcTx) accessList() AccessList { return nil }
func (tx *QkcTx) data() []byte        { return tx.Payload }
func (tx *QkcTx) gas() uint64         { return tx.GasLimit }
func (tx *QkcTx) gasPrice() *big.Int  { return tx.Price }
func (tx *QkcTx) gasTipCap() *big.Int { return tx.Price }
func (tx *QkcTx) gasFeeCap() *big.Int { return tx.Price }
func (tx *QkcTx) value() *big.Int     { return tx.Amount }
func (tx *QkcTx) nonce() uint64       { return tx.AccountNonce }
func (tx *QkcTx) networkID() uint32   { return tx.NetworkID }
func (tx *QkcTx) version() uint32     { return tx.Version }
func (tx *QkcTx) gasTokenID() uint64  { return tx.GasTokenID }
func (tx *QkcTx) transferTokenID() uint64 {
	return tx.TransferTokenID
}
func (tx *QkcTx) fromFullShardKey() uint32 {
	if tx.FromFullShardKey == nil {
		return 0
	}
	return uint32(*tx.FromFullShardKey)
}
func (tx *QkcTx) toFullShardKey() uint32 {
	if tx.ToFullShardKey == nil {
		return 0
	}
	return uint32(*tx.ToFullShardKey)
}

func (tx *QkcTx) fromChainID() uint32 { return tx.fromFullShardKey() >> 16 }
func (tx *QkcTx) toChainID() uint32   { return tx.toFullShardKey() >> 16 }

// fromShardID/toShardID mirror the pyquarkchain-compatible qkc/types reference,
// which fixes the shard size at qkcShardSize (currently 1). With a shard size of
// 1 the mask is 0, so both are always 0 and cross-shard is decided by chainID
// alone. If a dynamic shard size is ever introduced, QkcTx must carry it and
// these helpers (and the v2 same-shard check in validate) must use it.
func (tx *QkcTx) fromShardID() uint32 { return tx.fromFullShardKey() & (qkcShardSize - 1) }
func (tx *QkcTx) toShardID() uint32   { return tx.toFullShardKey() & (qkcShardSize - 1) }

func (tx *QkcTx) isCrossShard() bool {
	return tx.fromChainID() != tx.toChainID() || tx.fromShardID() != tx.toShardID()
}

// fromShardKey/toShardKey return the low 16 bits of the full shard key, matching
// the qkc/types reference (mask 0xffff, independent of shard size).
func (tx *QkcTx) fromShardKey() uint32 { return tx.fromFullShardKey() & 0xffff }
func (tx *QkcTx) toShardKey() uint32   { return tx.toFullShardKey() & 0xffff }

func (tx *QkcTx) fromFullShardID() uint32 {
	return tx.fromChainID()<<16 | qkcShardSize | tx.fromShardID()
}
func (tx *QkcTx) toFullShardID() uint32 {
	return tx.toChainID()<<16 | qkcShardSize | tx.toShardID()
}

func (tx *QkcTx) setGas(gas uint64)     { tx.GasLimit = gas }
func (tx *QkcTx) setNonce(nonce uint64) { tx.AccountNonce = nonce }
func (tx *QkcTx) setFromFullShardKey(fullShardKey uint32) {
	key := qkcUint32(fullShardKey)
	tx.FromFullShardKey = &key
}

// to returns the recipient address of the transaction.
// It returns nil if the transaction is a contract creation.
func (tx *QkcTx) to() *common.Address {
	return copyAddressPtr(tx.Recipient)
}

func (tx *QkcTx) rawSignatureValues() (v, r, s *big.Int) { return tx.V, tx.R, tx.S }
func (tx *QkcTx) setSignatureValues(_ *big.Int, v, r, s *big.Int) {
	tx.V = copyBigInt(v)
	tx.R = copyBigInt(r)
	tx.S = copyBigInt(s)
}
func (tx *QkcTx) effectiveGasPrice(dst *big.Int, _ *big.Int) *big.Int { return dst.Set(tx.Price) }

func (tx *QkcTx) encode(buffer *bytes.Buffer) error { return rlp.Encode(buffer, tx) }
func (tx *QkcTx) decode(input []byte) error {
	if err := rlp.DecodeBytes(input, tx); err != nil {
		return err
	}
	if tx.FromFullShardKey == nil {
		return errors.New("missing from full shard key")
	}
	if tx.ToFullShardKey == nil {
		return errors.New("missing to full shard key")
	}
	if tx.Version > 2 {
		return fmt.Errorf("unsupported QKC transaction version %d", tx.Version)
	}
	// RLP imposes no upper bound on big.Int size; reject values that cannot fit
	// in the uint256 fields QKC signing assumes, so the signer never panics on
	// an over-long integer coming off the wire.
	if tx.Amount != nil && len(tx.Amount.Bytes()) > 32 {
		return errors.New("amount exceeds 32 bytes")
	}
	if tx.Price != nil && len(tx.Price.Bytes()) > 32 {
		return errors.New("gas price exceeds 32 bytes")
	}
	return nil
}

func (tx *QkcTx) sigHash(chainID *big.Int) common.Hash {
	switch tx.Version {
	case 0:
		return tx.getUnsignedHash()
	case 1:
		hashTyped, err := tx.typedHash()
		if err != nil {
			panic(err)
		}
		return hashTyped
	case 2:
		return tx.getUnsignedHashForEip155(uint32(chainID.Uint64()))
	default:
		panic(fmt.Sprintf("unsupported QKC transaction version %d", tx.Version))
	}
}

func copyBigInt(value *big.Int) *big.Int {
	if value == nil {
		return nil
	}
	return new(big.Int).Set(value)
}
