// Copyright 2026-2027, QuarkChain.

package types

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/rlp"
)

var (
	ErrInvalidNetworkID  = errors.New("invalid network ID for signer")
	ErrV2NonDefaultToken = errors.New("version 2 transaction must use the default QKC token")
	ErrV2CrossShard      = errors.New("version 2 transaction must not be cross-shard")
)

const qkcTransactionEnvelopeSize = uint64(5)

// QKCShardSizeResolver supplies the chain-specific shard size needed to map a
// full-shard key to a shard ID. The serialized transaction does not carry this value.
type QKCShardSizeResolver interface {
	GetShardSizeByChainId(chainID uint32) (uint32, error)
}

// NewQKCTransaction creates the only signed QKC transaction type. A transfer
// is cross-shard when its from/to full-shard keys resolve to different shards;
// it is not represented by a second transaction envelope type.
func NewQKCTransaction(nonce uint64, to account.Recipient, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32,
	toFullShardKey uint32, networkID uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *Transaction {
	return newQKCTransaction(nonce, &to, amount, gasLimit, gasPrice, fromFullShardKey, toFullShardKey, networkID, version, data, gasTokenID, transferTokenID)
}

func NewQKCContractCreation(nonce uint64, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32,
	toFullShardKey uint32, networkID uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *Transaction {
	return newQKCTransaction(nonce, nil, amount, gasLimit, gasPrice, fromFullShardKey, toFullShardKey, networkID, version, data, gasTokenID, transferTokenID)
}

// These names keep QKC callers source-compatible while the executable type
// itself lives in core/types.
func NewEvmTransaction(nonce uint64, to account.Recipient, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32,
	toFullShardKey uint32, networkID uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *Transaction {
	return NewQKCTransaction(nonce, to, amount, gasLimit, gasPrice, fromFullShardKey, toFullShardKey, networkID, version, data, gasTokenID, transferTokenID)
}

func NewEvmContractCreation(nonce uint64, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32,
	toFullShardKey uint32, networkID uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *Transaction {
	return NewQKCContractCreation(nonce, amount, gasLimit, gasPrice, fromFullShardKey, toFullShardKey, networkID, version, data, gasTokenID, transferTokenID)
}

func newQKCTransaction(nonce uint64, to *common.Address, amount *big.Int, gasLimit uint64, gasPrice *big.Int, fromFullShardKey uint32,
	toFullShardKey uint32, networkID uint32, version uint32, data []byte, gasTokenID, transferTokenID uint64) *Transaction {
	fromKey, toKey := qkccommon.Uint32(fromFullShardKey), qkccommon.Uint32(toFullShardKey)
	inner := &LegacyTx{
		Nonce:            nonce,
		GasPrice:         new(big.Int),
		Gas:              gasLimit,
		To:               copyAddressPtr(to),
		Value:            new(big.Int),
		Data:             common.CopyBytes(data),
		NetworkID:        networkID,
		FromFullShardKey: &fromKey,
		ToFullShardKey:   &toKey,
		GasTokenID:       gasTokenID,
		TransferTokenID:  transferTokenID,
		Version:          version,
		V:                new(big.Int),
		R:                new(big.Int),
		S:                new(big.Int),
	}
	if amount != nil {
		inner.Value.Set(amount)
	}
	if gasPrice != nil {
		inner.GasPrice.Set(gasPrice)
	}
	return NewTx(inner)
}

func (tx *Transaction) qkcData() (*LegacyTx, error) {
	if tx == nil || tx.inner == nil {
		return nil, errors.New("invalid nil transaction")
	}
	inner, ok := tx.inner.(*LegacyTx)
	if !ok || tx.Type() != LegacyTxType {
		return nil, ErrTxTypeNotSupported
	}
	return inner, nil
}

// EncodeQKCPayload returns the internal payload RLP without the external QKC
// transaction encoding. It is useful for storage and golden-vector tests.
func (tx *Transaction) EncodeQKCPayload() ([]byte, error) {
	inner, err := tx.qkcData()
	if err != nil {
		return nil, err
	}
	return rlp.EncodeToBytes(inner)
}

func (tx *Transaction) NetworkId() uint32 {
	inner, err := tx.qkcData()
	if err != nil {
		return 0
	}
	return inner.NetworkID
}

func (tx *Transaction) NetworkID() uint32 { return tx.NetworkId() }

func (tx *Transaction) Version() uint32 {
	inner, err := tx.qkcData()
	if err != nil {
		return 0
	}
	return inner.Version
}

func (tx *Transaction) FromFullShardKey() uint32 {
	inner, err := tx.qkcData()
	if err != nil || inner.FromFullShardKey == nil {
		return 0
	}
	return inner.FromFullShardKey.GetValue()
}

func (tx *Transaction) ToFullShardKey() uint32 {
	inner, err := tx.qkcData()
	if err != nil || inner.ToFullShardKey == nil {
		return 0
	}
	return inner.ToFullShardKey.GetValue()
}

func (tx *Transaction) FromChainID() uint32 { return tx.FromFullShardKey() >> 16 }
func (tx *Transaction) ToChainID() uint32   { return tx.ToFullShardKey() >> 16 }
func (tx *Transaction) FromShardKey() uint32 {
	return tx.FromFullShardKey() & 0xffff
}
func (tx *Transaction) ToShardKey() uint32 { return tx.ToFullShardKey() & 0xffff }

func qkcShardSize(resolver QKCShardSizeResolver, chainID uint32) (uint32, error) {
	if resolver == nil {
		return 0, errors.New("QKC shard-size resolver is unavailable")
	}
	shardSize, err := resolver.GetShardSizeByChainId(chainID)
	if err != nil {
		return 0, err
	}
	if shardSize == 0 || shardSize&(shardSize-1) != 0 {
		return 0, fmt.Errorf("invalid QKC shard size %d for chain %d", shardSize, chainID)
	}
	return shardSize, nil
}

func (tx *Transaction) FromShardID(resolver QKCShardSizeResolver) (uint32, error) {
	shardSize, err := qkcShardSize(resolver, tx.FromChainID())
	if err != nil {
		return 0, err
	}
	return tx.FromShardKey() & (shardSize - 1), nil
}

func (tx *Transaction) ToShardID(resolver QKCShardSizeResolver) (uint32, error) {
	shardSize, err := qkcShardSize(resolver, tx.ToChainID())
	if err != nil {
		return 0, err
	}
	return tx.ToShardKey() & (shardSize - 1), nil
}

func (tx *Transaction) FromFullShardID(resolver QKCShardSizeResolver) (uint32, error) {
	shardSize, err := qkcShardSize(resolver, tx.FromChainID())
	if err != nil {
		return 0, err
	}
	shardID := tx.FromShardKey() & (shardSize - 1)
	return tx.FromChainID()<<16 | shardSize | shardID, nil
}

func (tx *Transaction) ToFullShardID(resolver QKCShardSizeResolver) (uint32, error) {
	shardSize, err := qkcShardSize(resolver, tx.ToChainID())
	if err != nil {
		return 0, err
	}
	shardID := tx.ToShardKey() & (shardSize - 1)
	return tx.ToChainID()<<16 | shardSize | shardID, nil
}

func (tx *Transaction) IsCrossShard(resolver QKCShardSizeResolver) (bool, error) {
	from, err := tx.FromFullShardID(resolver)
	if err != nil {
		return false, err
	}
	to, err := tx.ToFullShardID(resolver)
	if err != nil {
		return false, err
	}
	return from != to, nil
}

func (tx *Transaction) GasTokenID() uint64 {
	inner, err := tx.qkcData()
	if err != nil {
		return 0
	}
	return inner.GasTokenID
}

func (tx *Transaction) TransferTokenID() uint64 {
	inner, err := tx.qkcData()
	if err != nil {
		return 0
	}
	return inner.TransferTokenID
}

func (tx *Transaction) Validate() error {
	inner, err := tx.qkcData()
	if err != nil {
		return err
	}
	if inner.FromFullShardKey == nil {
		return errors.New("missing from full shard key")
	}
	if inner.ToFullShardKey == nil {
		return errors.New("missing to full shard key")
	}
	if inner.Version > 2 {
		return fmt.Errorf("unsupported transaction version %d", inner.Version)
	}
	if inner.Value == nil || inner.Value.Sign() < 0 || len(inner.Value.Bytes()) > 32 {
		return errors.New("invalid transaction value")
	}
	if inner.GasPrice == nil || inner.GasPrice.Sign() < 0 || len(inner.GasPrice.Bytes()) > 32 {
		return errors.New("invalid transaction gas price")
	}
	if inner.V == nil || inner.R == nil || inner.S == nil ||
		inner.V.Sign() < 0 || inner.R.Sign() < 0 || inner.S.Sign() < 0 {
		return errors.New("invalid transaction signature values")
	}
	if inner.GasTokenID > qkccommon.TOKENIDMAX || inner.TransferTokenID > qkccommon.TOKENIDMAX {
		return errors.New("transaction token ID exceeds maximum")
	}
	if inner.Version == 2 {
		// Version 2 predates configurable shard routing and is valid only on
		// shard key zero of one chain. The default token is chain-configured
		// and is therefore validated at the execution boundary.
		if tx.FromChainID() != tx.ToChainID() || tx.FromShardKey() != 0 || tx.ToShardKey() != 0 {
			return ErrV2CrossShard
		}
	}
	return nil
}

func (tx *Transaction) Serialize(out *[]byte) error {
	inner, err := tx.qkcData()
	if err != nil {
		return err
	}
	payload, err := rlp.EncodeToBytes(inner)
	if err != nil {
		return err
	}
	*out = append(*out, LegacyTxType)
	if err := serialize.Serialize(out, uint32(len(payload))); err != nil {
		return err
	}
	*out = append(*out, payload...)
	return nil
}

func (tx *Transaction) Deserialize(buffer *serialize.ByteBuffer) error {
	txType, err := buffer.GetUInt8()
	if err != nil {
		return err
	}
	if txType != LegacyTxType {
		return fmt.Errorf("deser: transaction type %d is not supported", txType)
	}
	payload, err := buffer.GetVarBytes(4)
	if err != nil {
		return err
	}
	var inner LegacyTx
	if err := rlp.DecodeBytes(payload, &inner); err != nil {
		return err
	}
	tx.hash.Store(nil)
	tx.size.Store(0)
	tx.from.Store(nil)
	tx.setDecoded(&inner, uint64(len(payload)+5))
	return tx.Validate()
}

func (tx *Transaction) qkcEncode() ([]byte, error) {
	return serialize.SerializeToBytes(tx)
}

func (tx *Transaction) qkcDecode(encoded []byte) error {
	return serialize.DeserializeFromBytes(encoded, tx)
}

// Bytes implements the QKC derivable-list contract used by the binary Merkle
// root and returns the complete external transaction encoding.
func (s Transactions) Bytes(i int) []byte {
	encoded, err := s[i].qkcEncode()
	if err != nil {
		panic(err)
	}
	return encoded
}

func (tx *Transaction) clearQKCCaches() {
	tx.hash.Store(nil)
	tx.size.Store(0)
	tx.from.Store(nil)
}

func (tx *Transaction) SetGas(gas uint64) {
	inner, err := tx.qkcData()
	if err != nil {
		return
	}
	inner.Gas = gas
	tx.clearQKCCaches()
}

func (tx *Transaction) SetFromFullShardKey(fullShardKey uint32) {
	inner, err := tx.qkcData()
	if err != nil {
		return
	}
	key := qkccommon.Uint32(fullShardKey)
	inner.FromFullShardKey = &key
	tx.clearQKCCaches()
}

func (tx *Transaction) SetNonce(nonce uint64) {
	inner, err := tx.qkcData()
	if err != nil {
		return
	}
	inner.Nonce = nonce
	tx.clearQKCCaches()
}

func (tx *Transaction) SetVRS(v, r, s *big.Int) {
	inner, err := tx.qkcData()
	if err != nil {
		return
	}
	inner.V, inner.R, inner.S = copyBigInt(v), copyBigInt(r), copyBigInt(s)
	tx.clearQKCCaches()
}
