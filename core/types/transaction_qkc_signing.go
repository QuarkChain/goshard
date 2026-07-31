// Copyright 2026-2027, QuarkChain.

package types

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
)

var (
	ErrInvalidQkcNetworkID  = errors.New("invalid QKC network id for signer")
	ErrQkcV2NonDefaultToken = errors.New("version 2 transaction must use the default QKC token")
	ErrQkcV2CrossShard      = errors.New("version 2 transaction must not be cross-shard")
)

type QkcSigner struct {
	networkID  uint32
	ethChainID uint32
}

func MakeQkcSigner(networkID, ethChainID uint32) Signer {
	return NewQkcSigner(networkID, ethChainID)
}

func NewQkcSigner(networkID, ethChainID uint32) QkcSigner {
	return QkcSigner{networkID: networkID, ethChainID: ethChainID}
}

func (signer QkcSigner) validate(tx *Transaction) error {
	inner, ok := tx.inner.(QkcTxData)
	if !ok {
		return ErrTxTypeNotSupported
	}
	if tx.Version() > 2 {
		return fmt.Errorf("version %d is not supported", tx.Version())
	}
	if tx.Version() < 2 && tx.NetworkID() != signer.networkID {
		return ErrInvalidQkcNetworkID
	}
	if tx.Version() == 2 {
		if tx.NetworkID() != signer.ethChainID {
			return ErrInvalidQkcNetworkID
		}
		// Version 2 uses standard EIP-155 signing, so token IDs and shard keys
		// are not covered by the signature. Constrain them here so they cannot
		// be tampered with: only the default QKC token, and same-shard only.
		if inner.gasTokenID() != qkccommon.DefaultTokenID || inner.transferTokenID() != qkccommon.DefaultTokenID {
			return ErrQkcV2NonDefaultToken
		}
		if inner.isCrossShard() {
			return ErrQkcV2CrossShard
		}
	}
	return nil
}

func (signer QkcSigner) ChainID() *big.Int { return new(big.Int).SetUint64(uint64(signer.ethChainID)) }

func (signer QkcSigner) Equal(other Signer) bool {
	qkcSigner, ok := other.(QkcSigner)
	return ok && qkcSigner.networkID == signer.networkID && qkcSigner.ethChainID == signer.ethChainID
}

func (signer QkcSigner) Hash(tx *Transaction) common.Hash {
	if tx == nil {
		panic("cannot hash nil transaction")
	}
	if _, ok := tx.inner.(QkcTxData); !ok {
		panic(ErrTxTypeNotSupported)
	}
	return tx.inner.sigHash(signer.ChainID())
}

func (signer QkcSigner) SignatureValues(tx *Transaction, signature []byte) (r, s, v *big.Int, err error) {
	if err := signer.validate(tx); err != nil {
		return nil, nil, nil, err
	}
	if len(signature) != 65 {
		return nil, nil, nil, fmt.Errorf("wrong size for signature: got %d, want 65", len(signature))
	}
	r = new(big.Int).SetBytes(signature[:32])
	s = new(big.Int).SetBytes(signature[32:64])
	if tx.Version() == 2 {
		v = new(big.Int).SetUint64(uint64(signature[64]) + 35 + 2*uint64(signer.ethChainID))
	} else {
		v = new(big.Int).SetUint64(uint64(signature[64]) + 27)
	}
	return r, s, v, nil
}

func (signer QkcSigner) Sender(tx *Transaction) (common.Address, error) {
	if err := signer.validate(tx); err != nil {
		return common.Address{}, err
	}
	hash := signer.Hash(tx)
	v, r, sigS := tx.RawSignatureValues()
	if tx.Version() == 2 {
		chainIDMul := new(big.Int).Mul(big.NewInt(int64(signer.ethChainID)), big.NewInt(2))
		v = new(big.Int).Sub(v, chainIDMul)
		v.Sub(v, big.NewInt(8))
	}
	return recoverPlain(hash, r, sigS, v, true)
}
