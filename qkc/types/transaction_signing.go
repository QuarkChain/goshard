// Copyright 2026-2027, QuarkChain.

// Transaction signing follows pyquarkchain-compatible QKC signing semantics.
// Modified from go-ethereum under GNU Lesser General Public License

package types

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/account"
)

var (
	ErrInvalidNetworkID = errors.New("invalid network ID for signer")
)

type sigCache struct {
	signer Signer
	from   account.Recipient
}

// MakeSigner returns a signer with the expected QKC network and Ethereum chain IDs.
func MakeSigner(qkcNetworkID, ethChainID uint32) Signer {
	return NewQKCSigner(qkcNetworkID, ethChainID)
}

// SignTx signs the transaction using the given signer and private key
func SignTx(tx *Transaction, s Signer, prv *ecdsa.PrivateKey) (*Transaction, error) {
	if tx == nil {
		return nil, errors.New("cannot sign nil transaction")
	}
	if tx.Version() > 2 {
		return nil, fmt.Errorf("version %d is not supported", tx.Version())
	}
	if tx.Version() != 2 && tx.NetworkId() != s.NetworkID() {
		return nil, ErrInvalidNetworkID
	}
	h := s.Hash(tx)
	sig, err := crypto.Sign(h[:], prv)
	if err != nil {
		return nil, err
	}
	return tx.WithSignature(s, sig)
}

// Sender returns the address derived from the signature (V, R, S) using secp256k1
// elliptic curve and an error if it failed deriving or upon an incorrect
// signature.
//
// Sender may cache the address, allowing it to be used regardless of
// signing method. The cache is invalidated if the cached signer does
// not match the signer used in the current call.
func Sender(signer Signer, tx *Transaction) (account.Recipient, error) {
	if tx == nil {
		return account.Recipient{}, errors.New("cannot recover sender from nil transaction")
	}
	if cached := tx.from.Load(); cached != nil {
		cache := cached.(sigCache)
		if cache.signer.Equal(signer) {
			return cache.from, nil
		}
	}
	sender, err := signer.Sender(tx)
	if err != nil {
		return account.Recipient{}, err
	}
	tx.from.Store(sigCache{signer: signer, from: sender})
	return sender, nil
}

// Signer encapsulates transaction signature handling. Note that this interface is not a
// stable API and may change at any time to accommodate new protocol rules.
type Signer interface {
	// Sender returns the sender address of the transaction.
	Sender(tx *Transaction) (account.Recipient, error)
	// SignatureValues returns the raw R, S, V values corresponding to the
	// given signature.
	SignatureValues(tx *Transaction, sig []byte) (r, s, v *big.Int, err error)
	// Hash returns the hash to be signed.
	Hash(tx *Transaction) common.Hash
	// NetworkID returns the network accepted by the signer.
	NetworkID() uint32
	// Equal returns true if the given signer is the same as the receiver.
	Equal(Signer) bool
}

// QKCSigner implements QKC transaction signature rules for all supported versions.
// Version 0 and 1 use qkcNetworkID, while version 2 uses ethChainID.
type QKCSigner struct {
	qkcNetworkID uint32
	ethChainID   uint32
}

func NewQKCSigner(qkcNetworkID, ethChainID uint32) QKCSigner {
	return QKCSigner{
		qkcNetworkID: qkcNetworkID,
		ethChainID:   ethChainID,
	}
}

func (s QKCSigner) Equal(s2 Signer) bool {
	other, ok := s2.(QKCSigner)
	return ok && other.qkcNetworkID == s.qkcNetworkID && other.ethChainID == s.ethChainID
}

func (s QKCSigner) NetworkID() uint32 { return s.qkcNetworkID }

func (s QKCSigner) Sender(tx *Transaction) (account.Recipient, error) {
	if tx.Version() != 2 && tx.NetworkId() != s.qkcNetworkID {
		return account.Recipient{}, ErrInvalidNetworkID
	}
	if tx.Version() == 2 && tx.NetworkId() != s.ethChainID {
		return account.Recipient{}, ErrInvalidNetworkID
	}

	hash, err := tx.inner.sigHash()
	if err != nil {
		return account.Recipient{}, err
	}
	v, r, sigS := tx.RawSignatureValues()
	if tx.Version() == 2 {
		chainIDMul := new(big.Int).Mul(big.NewInt(int64(s.ethChainID)), big.NewInt(2))
		v = new(big.Int).Sub(v, chainIDMul)
		v.Sub(v, big.NewInt(8))
	}
	return recoverPlain(hash, r, sigS, v, true)
}

// SignatureValues returns signature values. This signature
// needs to be in the [R || S || V] format where V is 0 or 1.
func (s QKCSigner) SignatureValues(tx *Transaction, sig []byte) (R, S, V *big.Int, err error) {
	if len(sig) != 65 {
		return nil, nil, nil, fmt.Errorf("wrong size for signature: got %d, want 65", len(sig))
	}
	R = new(big.Int).SetBytes(sig[:32])
	S = new(big.Int).SetBytes(sig[32:64])
	if tx.Version() == 2 {
		V = new(big.Int).SetUint64(uint64(sig[64]) + 35 + 2*uint64(s.ethChainID))
	} else {
		V = new(big.Int).SetUint64(uint64(sig[64]) + 27)
	}

	return R, S, V, nil
}

// Hash returns the hash to be signed by the sender.
// It does not uniquely identify the transaction.
func (s QKCSigner) Hash(tx *Transaction) common.Hash {
	hash, err := tx.inner.sigHash()
	if err != nil {
		panic(err)
	}
	return hash
}

func recoverPlain(sighash common.Hash, R, S, Vb *big.Int, homestead bool) (account.Recipient, error) {
	if Vb.BitLen() > 8 {
		return account.Recipient{}, ErrInvalidSig
	}
	// QuarkChain use NetworkId to store the chain Id instead of added to V,
	// so do not need to remove chain Id from VB
	V := byte(Vb.Uint64() - 27)
	if !crypto.ValidateSignatureValues(V, R, S, homestead) {
		return account.Recipient{}, ErrInvalidSig
	}
	// encode the signature in uncompressed format
	r, s := R.Bytes(), S.Bytes()
	sig := make([]byte, 65)
	copy(sig[32-len(r):32], r)
	copy(sig[64-len(s):64], s)
	sig[64] = V
	// recover the public key from the signature
	pub, err := crypto.Ecrecover(sighash[:], sig)
	if err != nil {
		return account.Recipient{}, err
	}
	if len(pub) == 0 || pub[0] != 4 {
		return account.Recipient{}, errors.New("invalid public key")
	}
	var addr account.Recipient
	copy(addr[:], crypto.Keccak256(pub[1:])[12:])
	return addr, nil
}
