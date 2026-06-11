// Copyright 2026-2027, QuarkChain.

package replay

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	TypedTransactionEVM uint8 = 0

	typedTransactionHeaderSize = 5
)

type TypedTransaction struct {
	TxType           uint8
	Raw              []byte
	SerializedEVMRLP []byte
	EVM              *EVMTransaction
	Hash             common.Hash
}

type EVMTransaction struct {
	Nonce            uint64
	GasPrice         *big.Int
	Gas              uint64
	To               *common.Address
	Value            *big.Int
	Data             []byte
	NetworkID        uint32
	FromFullShardKey uint32
	ToFullShardKey   uint32
	GasTokenID       uint64
	TransferTokenID  uint64
	Version          uint32
	V                *big.Int
	R                *big.Int
	S                *big.Int
}

type evmTransactionRLP struct {
	Nonce            uint64
	GasPrice         *big.Int
	Gas              uint64
	To               *common.Address `rlp:"nil"`
	Value            *big.Int
	Data             []byte
	NetworkID        uint32
	FromFullShardKey *rlpUint32
	ToFullShardKey   *rlpUint32
	GasTokenID       uint64
	TransferTokenID  uint64
	Version          uint32
	V                *big.Int
	R                *big.Int
	S                *big.Int
}

type rlpUint32 uint32

func (u *rlpUint32) EncodeRLP(w io.Writer) error {
	var out [5]byte
	out[0] = 0x84
	binary.BigEndian.PutUint32(out[1:], uint32(*u))
	_, err := w.Write(out[:])
	return err
}

func (u *rlpUint32) DecodeRLP(s *rlp.Stream) error {
	raw, err := s.Raw()
	if err != nil {
		return err
	}
	if len(raw) != 5 {
		return fmt.Errorf("invalid fixed uint32 rlp length %d", len(raw))
	}
	if raw[0] != 0x84 {
		return fmt.Errorf("invalid fixed uint32 rlp prefix %#x", raw[0])
	}
	*u = rlpUint32(binary.BigEndian.Uint32(raw[1:]))
	return nil
}

func ParseTypedTransaction(raw []byte) (*TypedTransaction, error) {
	if len(raw) < typedTransactionHeaderSize {
		return nil, fmt.Errorf("typed transaction too short: %d", len(raw))
	}
	txType := raw[0]
	if txType != TypedTransactionEVM {
		return nil, fmt.Errorf("unsupported typed transaction type %d", txType)
	}
	payloadSize := binary.BigEndian.Uint32(raw[1:typedTransactionHeaderSize])
	if uint64(payloadSize) != uint64(len(raw)-typedTransactionHeaderSize) {
		return nil, fmt.Errorf("typed transaction payload length mismatch: header %d bytes, actual %d bytes", payloadSize, len(raw)-typedTransactionHeaderSize)
	}
	serializedEVMRLP := common.CopyBytes(raw[typedTransactionHeaderSize:])
	evmTx, err := ParseEVMTransactionRLP(serializedEVMRLP)
	if err != nil {
		return nil, fmt.Errorf("parse serialized evm transaction rlp: %w", err)
	}
	return &TypedTransaction{
		TxType:           txType,
		Raw:              common.CopyBytes(raw),
		SerializedEVMRLP: serializedEVMRLP,
		EVM:              evmTx,
		Hash:             crypto.Keccak256Hash(raw),
	}, nil
}

func ParseEVMTransactionRLP(raw []byte) (*EVMTransaction, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty evm transaction rlp")
	}
	var tx evmTransactionRLP
	if err := rlp.DecodeBytes(raw, &tx); err != nil {
		return nil, err
	}
	if tx.FromFullShardKey == nil {
		return nil, fmt.Errorf("missing fromFullShardKey")
	}
	if tx.ToFullShardKey == nil {
		return nil, fmt.Errorf("missing toFullShardKey")
	}
	return &EVMTransaction{
		Nonce:            tx.Nonce,
		GasPrice:         copyBig(tx.GasPrice),
		Gas:              tx.Gas,
		To:               copyAddressPtr(tx.To),
		Value:            copyBig(tx.Value),
		Data:             common.CopyBytes(tx.Data),
		NetworkID:        tx.NetworkID,
		FromFullShardKey: uint32(*tx.FromFullShardKey),
		ToFullShardKey:   uint32(*tx.ToFullShardKey),
		GasTokenID:       tx.GasTokenID,
		TransferTokenID:  tx.TransferTokenID,
		Version:          tx.Version,
		V:                copyBig(tx.V),
		R:                copyBig(tx.R),
		S:                copyBig(tx.S),
	}, nil
}

func (tx *EVMTransaction) MatchesRLP(raw []byte) bool {
	fromFullShardKey := rlpUint32(tx.FromFullShardKey)
	toFullShardKey := rlpUint32(tx.ToFullShardKey)
	encoded, err := rlp.EncodeToBytes(evmTransactionRLP{
		Nonce:            tx.Nonce,
		GasPrice:         copyBig(tx.GasPrice),
		Gas:              tx.Gas,
		To:               copyAddressPtr(tx.To),
		Value:            copyBig(tx.Value),
		Data:             common.CopyBytes(tx.Data),
		NetworkID:        tx.NetworkID,
		FromFullShardKey: &fromFullShardKey,
		ToFullShardKey:   &toFullShardKey,
		GasTokenID:       tx.GasTokenID,
		TransferTokenID:  tx.TransferTokenID,
		Version:          tx.Version,
		V:                copyBig(tx.V),
		R:                copyBig(tx.R),
		S:                copyBig(tx.S),
	})
	return err == nil && bytes.Equal(encoded, raw)
}

func copyAddressPtr(addr *common.Address) *common.Address {
	if addr == nil {
		return nil
	}
	cpy := *addr
	return &cpy
}

func copyBig(value *big.Int) *big.Int {
	if value == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(value)
}
