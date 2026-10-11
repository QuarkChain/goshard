// Copyright 2026-2027, QuarkChain.

package types

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
)

type qkcUnsignedTx struct {
	Nonce            uint64
	GasPrice         *big.Int
	Gas              uint64
	To               *common.Address `rlp:"nil"`
	Value            *big.Int
	Data             []byte
	NetworkID        uint32
	FromFullShardKey *qkccommon.Uint32
	ToFullShardKey   *qkccommon.Uint32
	GasTokenID       uint64
	TransferTokenID  uint64
}

func (tx *LegacyTx) qkcSigHash() (common.Hash, error) {
	switch tx.Version {
	case 0:
		return rlpHash(qkcUnsignedTx{
			Nonce: tx.Nonce, GasPrice: tx.GasPrice, Gas: tx.Gas, To: tx.To,
			Value: tx.Value, Data: tx.Data, NetworkID: tx.NetworkID,
			FromFullShardKey: tx.FromFullShardKey, ToFullShardKey: tx.ToFullShardKey,
			GasTokenID: tx.GasTokenID, TransferTokenID: tx.TransferTokenID,
		}), nil
	case 1:
		return qkcTypedSignatureHash(tx)
	case 2:
		return rlpHash([]any{
			tx.Nonce, tx.GasPrice, tx.Gas, tx.To, tx.Value, tx.Data,
			tx.NetworkID, uint(0), uint(0),
		}), nil
	default:
		return common.Hash{}, fmt.Errorf("unsupported transaction version %d", tx.Version)
	}
}

// QKCSigner implements pyquarkchain's three supported transaction signing
// versions while satisfying the geth Signer interface.
type QKCSigner struct {
	qkcNetworkID uint32
	ethChainID   uint32
}

func NewQKCSigner(qkcNetworkID, ethChainID uint32) QKCSigner {
	return QKCSigner{qkcNetworkID: qkcNetworkID, ethChainID: ethChainID}
}

func (s QKCSigner) ChainID() *big.Int { return new(big.Int).SetUint64(uint64(s.ethChainID)) }

func (s QKCSigner) Equal(other Signer) bool {
	signer, ok := other.(QKCSigner)
	return ok && signer.qkcNetworkID == s.qkcNetworkID && signer.ethChainID == s.ethChainID
}

func (s QKCSigner) Validate(tx *Transaction) error {
	if err := tx.Validate(); err != nil {
		return err
	}
	want := s.qkcNetworkID
	if tx.Version() == 2 {
		want = s.ethChainID
	}
	if tx.NetworkID() != want {
		return fmt.Errorf("%w: have %d want %d", ErrInvalidNetworkID, tx.NetworkID(), want)
	}
	return nil
}

func (s QKCSigner) Hash(tx *Transaction) common.Hash {
	inner, err := tx.qkcData()
	if err != nil {
		panic(err)
	}
	hash, err := inner.qkcSigHash()
	if err != nil {
		panic(err)
	}
	return hash
}

func (s QKCSigner) Sender(tx *Transaction) (common.Address, error) {
	if err := s.Validate(tx); err != nil {
		return common.Address{}, err
	}
	inner, _ := tx.qkcData()
	hash, err := inner.qkcSigHash()
	if err != nil {
		return common.Address{}, err
	}
	v, r, signatureS := tx.RawSignatureValues()
	if tx.Version() == 2 {
		v = new(big.Int).Sub(v, new(big.Int).SetUint64(2*uint64(s.ethChainID)))
		v.Sub(v, big.NewInt(8))
	}
	// pyquarkchain accepts the full 0 < S < secp256k1N range. Ethereum's
	// Homestead low-S restriction is not part of QKC transaction validation.
	return recoverPlain(hash, r, signatureS, v, false)
}

func (s QKCSigner) SignatureValues(tx *Transaction, signature []byte) (r, signatureS, v *big.Int, err error) {
	if err := s.Validate(tx); err != nil {
		return nil, nil, nil, err
	}
	if len(signature) != crypto.SignatureLength {
		return nil, nil, nil, fmt.Errorf("wrong size for signature: got %d, want %d", len(signature), crypto.SignatureLength)
	}
	r = new(big.Int).SetBytes(signature[:32])
	signatureS = new(big.Int).SetBytes(signature[32:64])
	if tx.Version() == 2 {
		v = new(big.Int).SetUint64(uint64(signature[64]) + 35 + 2*uint64(s.ethChainID))
	} else {
		v = new(big.Int).SetUint64(uint64(signature[64]) + 27)
	}
	return r, signatureS, v, nil
}
