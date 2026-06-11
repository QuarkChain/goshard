// Copyright 2026-2027, QuarkChain.

package replay

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	qkcstate "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	TransactionVersionLegacy uint32 = 0
	TransactionVersionTyped  uint32 = 1
	TransactionVersionEIP155 uint32 = 2

	qkcTypedSignatureDomain = "bottom-quark"
)

var (
	errInvalidSignature = errors.New("invalid transaction signature")
)

type evmUnsignedTransactionRLP struct {
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
}

type evmEIP155UnsignedTransactionRLP struct {
	Nonce    uint64
	GasPrice *big.Int
	Gas      uint64
	To       *common.Address `rlp:"nil"`
	Value    *big.Int
	Data     []byte
	ChainID  uint32
	Param1   uint
	Param2   uint
}

func (tx *EVMTransaction) RecoverSender() (common.Address, error) {
	hash, recoveryV, err := tx.signatureHashAndRecoveryV()
	if err != nil {
		return common.Address{}, err
	}
	return recoverPlain(hash, tx.R, tx.S, recoveryV)
}

func (tx *EVMTransaction) SigningHash() (common.Hash, error) {
	if tx == nil {
		return common.Hash{}, errors.New("nil evm transaction")
	}
	switch tx.Version {
	case TransactionVersionLegacy:
		return tx.LegacySigningHash()
	case TransactionVersionTyped:
		return tx.TypedSigningHash()
	case TransactionVersionEIP155:
		return tx.EIP155SigningHash()
	default:
		return common.Hash{}, fmt.Errorf("unsupported transaction version %d", tx.Version)
	}
}

func (tx *EVMTransaction) LegacySigningHash() (common.Hash, error) {
	if tx == nil {
		return common.Hash{}, errors.New("nil evm transaction")
	}
	fromFullShardKey := rlpUint32(tx.FromFullShardKey)
	toFullShardKey := rlpUint32(tx.ToFullShardKey)
	return encodeRLPHash(evmUnsignedTransactionRLP{
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
	})
}

func (tx *EVMTransaction) TypedSigningHash() (common.Hash, error) {
	if tx == nil {
		return common.Hash{}, errors.New("nil evm transaction")
	}
	typedTypes := []string{
		"uint256",
		"uint256",
		"uint256",
		"uint160",
		"uint256",
		"bytes",
		"uint256",
		"uint32",
		"uint32",
		"uint64",
		"uint64",
		"string",
	}
	names := []string{
		"nonce",
		"gasPrice",
		"gasLimit",
		"to",
		"value",
		"data",
		"networkId",
		"fromFullShardKey",
		"toFullShardKey",
		"gasTokenId",
		"transferTokenId",
		"qkcDomain",
	}
	values := []string{
		uint64ToMinimalHex(tx.Nonce),
		bigToMinimalHex(tx.GasPrice),
		uint64ToMinimalHex(tx.Gas),
		addressToMinimalHex(tx.To),
		bigToMinimalHex(tx.Value),
		"0x" + hex.EncodeToString(tx.Data),
		uint64ToMinimalHex(uint64(tx.NetworkID)),
		uint64ToMinimalHex(uint64(tx.FromFullShardKey)),
		uint64ToMinimalHex(uint64(tx.ToFullShardKey)),
		uint64ToMinimalHex(tx.GasTokenID),
		uint64ToMinimalHex(tx.TransferTokenID),
		qkcTypedSignatureDomain,
	}
	schema := make([]string, len(typedTypes))
	schemaTypes := make([]string, len(typedTypes))
	for i := range typedTypes {
		schema[i] = typedTypes[i] + " " + names[i]
		schemaTypes[i] = "string"
	}
	schemaHash, err := soliditySha3(schemaTypes, schema)
	if err != nil {
		return common.Hash{}, err
	}
	dataHash, err := soliditySha3(typedTypes, values)
	if err != nil {
		return common.Hash{}, err
	}
	return soliditySha3([]string{"bytes32", "bytes32"}, []string{schemaHash.Hex(), dataHash.Hex()})
}

func (tx *EVMTransaction) EIP155SigningHash() (common.Hash, error) {
	if tx == nil {
		return common.Hash{}, errors.New("nil evm transaction")
	}
	return encodeRLPHash(evmEIP155UnsignedTransactionRLP{
		Nonce:    tx.Nonce,
		GasPrice: copyBig(tx.GasPrice),
		Gas:      tx.Gas,
		To:       copyAddressPtr(tx.To),
		Value:    copyBig(tx.Value),
		Data:     common.CopyBytes(tx.Data),
		ChainID:  tx.NetworkID,
	})
}

func (tx *EVMTransaction) signatureHashAndRecoveryV() (common.Hash, *big.Int, error) {
	if tx == nil {
		return common.Hash{}, nil, errors.New("nil evm transaction")
	}
	switch tx.Version {
	case TransactionVersionLegacy:
		hash, err := tx.LegacySigningHash()
		return hash, copyBig(tx.V), err
	case TransactionVersionTyped:
		hash, err := tx.TypedSigningHash()
		return hash, copyBig(tx.V), err
	case TransactionVersionEIP155:
		hash, err := tx.EIP155SigningHash()
		if err != nil {
			return common.Hash{}, nil, err
		}
		recoveryV := copyBig(tx.V)
		recoveryV.Sub(recoveryV, new(big.Int).Mul(new(big.Int).SetUint64(uint64(tx.NetworkID)), big.NewInt(2)))
		recoveryV.Sub(recoveryV, big.NewInt(8))
		return hash, recoveryV, nil
	default:
		return common.Hash{}, nil, fmt.Errorf("unsupported transaction version %d", tx.Version)
	}
}

func recoverPlain(signingHash common.Hash, r, s, v *big.Int) (common.Address, error) {
	if r == nil || s == nil || v == nil {
		return common.Address{}, errInvalidSignature
	}
	if v.BitLen() > 8 || v.Sign() < 0 || v.Uint64() < 27 {
		return common.Address{}, errInvalidSignature
	}
	recoveryID := byte(v.Uint64() - 27)
	if !crypto.ValidateSignatureValues(recoveryID, r, s, true) {
		return common.Address{}, errInvalidSignature
	}
	sig := make([]byte, crypto.SignatureLength)
	rBytes := r.Bytes()
	sBytes := s.Bytes()
	copy(sig[32-len(rBytes):32], rBytes)
	copy(sig[64-len(sBytes):64], sBytes)
	sig[64] = recoveryID
	pub, err := crypto.Ecrecover(signingHash[:], sig)
	if err != nil {
		return common.Address{}, err
	}
	if len(pub) == 0 || pub[0] != 4 {
		return common.Address{}, errInvalidSignature
	}
	return common.BytesToAddress(crypto.Keccak256(pub[1:])[12:]), nil
}

func ValidateHistoricalTransaction(config *qkcstate.QuarkChainClusterGenesisConfig, block *MinorBlockInput, tx *TransactionInput) error {
	if tx == nil {
		return errors.New("nil transaction")
	}
	if tx.EVMTransaction == nil {
		return errors.New("missing parsed evm transaction")
	}
	recovered, err := tx.EVMTransaction.RecoverSender()
	if err != nil {
		return fmt.Errorf("recover sender: %w", err)
	}
	if recovered != tx.From {
		return fmt.Errorf("sender mismatch: json %s signature %s", tx.From, recovered)
	}
	if config != nil {
		if err := validateTransactionNetwork(config, block, tx.EVMTransaction); err != nil {
			return err
		}
	}
	if block != nil && config != nil {
		fullShardID, err := FullShardIDFromKey(config, tx.EVMTransaction.FromFullShardKey)
		if err != nil {
			return fmt.Errorf("fromFullShardKey: %w", err)
		}
		if fullShardID != block.FullShardID {
			return fmt.Errorf("sender shard mismatch: transaction fullShard=%#x block fullShard=%#x", fullShardID, block.FullShardID)
		}
	}
	return nil
}

func validateTransactionNetwork(config *qkcstate.QuarkChainClusterGenesisConfig, block *MinorBlockInput, tx *EVMTransaction) error {
	if tx.Version != TransactionVersionEIP155 {
		if config.QuarkChain.NetworkID != 0 && tx.NetworkID != config.QuarkChain.NetworkID {
			return fmt.Errorf("networkId mismatch: transaction %d config %d", tx.NetworkID, config.QuarkChain.NetworkID)
		}
		return nil
	}
	return validateEIP155Transaction(config, block, tx)
}

func validateEIP155Transaction(config *qkcstate.QuarkChainClusterGenesisConfig, block *MinorBlockInput, tx *EVMTransaction) error {
	if config.QuarkChain.EnableEIP155SignerTimestamp != nil {
		var timestamp uint64
		if block != nil {
			timestamp = block.Timestamp
		}
		if timestamp < *config.QuarkChain.EnableEIP155SignerTimestamp {
			return fmt.Errorf("EIP155 signer is not enabled at timestamp %d, requires %d", timestamp, *config.QuarkChain.EnableEIP155SignerTimestamp)
		}
	}
	expectedV0 := new(big.Int).SetUint64(uint64(tx.NetworkID)*2 + 35)
	expectedV1 := new(big.Int).Add(expectedV0, common.Big1)
	if tx.V == nil || (tx.V.Cmp(expectedV0) != 0 && tx.V.Cmp(expectedV1) != 0) {
		return fmt.Errorf("version 2 signature v mismatch: got %s want %s or %s", tx.V, expectedV0, expectedV1)
	}
	if tx.FromChainID() != tx.ToChainID() {
		return fmt.Errorf("EIP155 signer does not support cross-chain transaction: from chain %d to chain %d", tx.FromChainID(), tx.ToChainID())
	}
	if tx.FromShardKey() != 0 || tx.ToShardKey() != 0 {
		return fmt.Errorf("EIP155 signer does not support nonzero shard keys: from %#x to %#x", tx.FromShardKey(), tx.ToShardKey())
	}
	chain, ok := findChainConfig(config, tx.FromChainID())
	if !ok {
		return fmt.Errorf("chain %d not found in cluster config", tx.FromChainID())
	}
	defaultTokenID, err := defaultChainTokenID(config, chain)
	if err != nil {
		return err
	}
	if tx.GasTokenID != defaultTokenID {
		return fmt.Errorf("version 2 gasTokenId mismatch: got %d want %d", tx.GasTokenID, defaultTokenID)
	}
	if tx.TransferTokenID != defaultTokenID {
		return fmt.Errorf("version 2 transferTokenId mismatch: got %d want %d", tx.TransferTokenID, defaultTokenID)
	}
	ethChainID := chainEthChainID(config, chain)
	if tx.NetworkID != ethChainID {
		return fmt.Errorf("version 2 networkId mismatch: got %d want ethChainId %d", tx.NetworkID, ethChainID)
	}
	if tx.NetworkID <= config.QuarkChain.BaseEthChainID || tx.NetworkID-config.QuarkChain.BaseEthChainID-1 != tx.FromChainID() {
		return fmt.Errorf("version 2 eth chain id mismatch: networkId %d base %d fromChain %d", tx.NetworkID, config.QuarkChain.BaseEthChainID, tx.FromChainID())
	}
	return nil
}

func (tx *EVMTransaction) FromChainID() uint32 {
	return tx.FromFullShardKey >> 16
}

func (tx *EVMTransaction) ToChainID() uint32 {
	return tx.ToFullShardKey >> 16
}

func (tx *EVMTransaction) FromShardKey() uint32 {
	return tx.FromFullShardKey & 0xffff
}

func (tx *EVMTransaction) ToShardKey() uint32 {
	return tx.ToFullShardKey & 0xffff
}

func findChainConfig(config *qkcstate.QuarkChainClusterGenesisConfig, chainID uint32) (qkcstate.QuarkChainGenesisChain, bool) {
	if config == nil {
		return qkcstate.QuarkChainGenesisChain{}, false
	}
	for _, chain := range config.QuarkChain.Chains {
		if chain.ChainID == chainID {
			return chain, true
		}
	}
	return qkcstate.QuarkChainGenesisChain{}, false
}

func chainEthChainID(config *qkcstate.QuarkChainClusterGenesisConfig, chain qkcstate.QuarkChainGenesisChain) uint32 {
	if chain.EthChainID != 0 {
		return chain.EthChainID
	}
	return config.QuarkChain.BaseEthChainID + chain.ChainID + 1
}

func defaultChainTokenID(config *qkcstate.QuarkChainClusterGenesisConfig, chain qkcstate.QuarkChainGenesisChain) (uint64, error) {
	token := chain.DefaultChainToken
	if token == "" {
		token = config.QuarkChain.GenesisToken
	}
	return qkcstate.QuarkChainTokenIDEncode(token)
}

func encodeRLPHash(value interface{}) (common.Hash, error) {
	encoded, err := rlp.EncodeToBytes(value)
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(encoded), nil
}

func soliditySha3(types []string, values []string) (common.Hash, error) {
	packed, err := solidityPack(types, values)
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(packed), nil
}

func solidityPack(types []string, values []string) ([]byte, error) {
	if len(types) != len(values) {
		return nil, fmt.Errorf("solidity pack type/value length mismatch: %d != %d", len(types), len(values))
	}
	var out []byte
	for i, typ := range types {
		value := values[i]
		switch {
		case typ == "bytes":
			blob, err := decodeSolidityHex(value)
			if err != nil {
				return nil, err
			}
			out = append(out, blob...)
		case typ == "string":
			out = append(out, []byte(value)...)
		case strings.HasPrefix(typ, "bytes"):
			size, err := solidityTypeSize(typ)
			if err != nil {
				return nil, err
			}
			if size < 1 || size > 32 {
				return nil, fmt.Errorf("unsupported solidity bytes size %d", size)
			}
			blob, err := decodeSolidityHex(value)
			if err != nil {
				return nil, err
			}
			if len(blob) > size {
				return nil, fmt.Errorf("solidity bytes value is larger than %d bytes", size)
			}
			out = appendLeftPadded(out, blob, size)
		case strings.HasPrefix(typ, "uint") || strings.HasPrefix(typ, "int"):
			size, err := solidityTypeSize(typ)
			if err != nil {
				return nil, err
			}
			if size%8 != 0 || size < 8 || size > 256 {
				return nil, fmt.Errorf("unsupported solidity integer size %d", size)
			}
			blob, err := decodeSolidityHex(value)
			if err != nil {
				return nil, err
			}
			width := size / 8
			if len(blob) > width {
				return nil, fmt.Errorf("solidity integer value is larger than %d bytes", width)
			}
			out = appendLeftPadded(out, blob, width)
		default:
			return nil, fmt.Errorf("unsupported solidity type %q", typ)
		}
	}
	return out, nil
}

func solidityTypeSize(typ string) (int, error) {
	start := -1
	for i, ch := range typ {
		if ch >= '0' && ch <= '9' {
			start = i
			break
		}
	}
	if start == -1 {
		return 0, fmt.Errorf("missing size in solidity type %q", typ)
	}
	end := start
	for end < len(typ) && typ[end] >= '0' && typ[end] <= '9' {
		end++
	}
	return strconv.Atoi(typ[start:end])
}

func appendLeftPadded(out []byte, blob []byte, width int) []byte {
	for i := len(blob); i < width; i++ {
		out = append(out, 0)
	}
	return append(out, blob...)
}

func decodeSolidityHex(input string) ([]byte, error) {
	input = strings.TrimPrefix(input, "0x")
	if len(input)%2 != 0 {
		input = "0" + input
	}
	if input == "" {
		return nil, nil
	}
	return hex.DecodeString(input)
}

func uint64ToMinimalHex(value uint64) string {
	if value == 0 {
		return "0x"
	}
	return "0x" + strings.TrimLeft(fmt.Sprintf("%x", value), "0")
}

func bigToMinimalHex(value *big.Int) string {
	if value == nil || value.Sign() == 0 {
		return "0x"
	}
	return "0x" + hex.EncodeToString(value.Bytes())
}

func addressToMinimalHex(addr *common.Address) string {
	if addr == nil {
		return "0x"
	}
	return addr.Hex()
}
