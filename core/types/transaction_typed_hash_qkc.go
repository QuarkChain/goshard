// Copyright 2026-2027, QuarkChain.

package types

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
)

type qkcTypedField struct {
	typeName string
	name     string
	value    string
}

func qkcTypedSignatureHash(tx *LegacyTx) (common.Hash, error) {
	to := "0x"
	if tx.To != nil {
		to = strings.ToLower(tx.To.String())
	}
	fields := []qkcTypedField{
		{"uint256", "nonce", qkcUint64Hex(tx.Nonce)},
		{"uint256", "gasPrice", qkcBigHex(tx.GasPrice)},
		{"uint256", "gasLimit", qkcUint64Hex(tx.Gas)},
		{"uint160", "to", to},
		{"uint256", "value", qkcBigHex(tx.Value)},
		{"bytes", "data", "0x" + hex.EncodeToString(tx.Data)},
		{"uint256", "networkId", qkcUint32Hex(tx.NetworkID)},
		{"uint32", "fromFullShardKey", qkcUint32Hex(tx.FromFullShardKey.GetValue())},
		{"uint32", "toFullShardKey", qkcUint32Hex(tx.ToFullShardKey.GetValue())},
		{"uint64", "gasTokenId", qkcUint64Hex(tx.GasTokenID)},
		{"uint64", "transferTokenId", qkcUint64Hex(tx.TransferTokenID)},
		{"string", "qkcDomain", "bottom-quark"},
	}
	schemaTypes := make([]string, len(fields))
	schemaValues := make([]string, len(fields))
	dataTypes := make([]string, len(fields))
	dataValues := make([]string, len(fields))
	for i, field := range fields {
		schemaTypes[i] = "string"
		schemaValues[i] = field.typeName + " " + field.name
		dataTypes[i] = field.typeName
		dataValues[i] = field.value
		if field.typeName == "bytes" {
			dataValues[i] = strings.TrimPrefix(field.value, "0x")
		}
	}
	schemaHash, err := qkcSolidityHash(schemaTypes, schemaValues)
	if err != nil {
		return common.Hash{}, err
	}
	dataHash, err := qkcSolidityHash(dataTypes, dataValues)
	if err != nil {
		return common.Hash{}, err
	}
	return qkcSolidityHash([]string{"bytes32", "bytes32"}, []string{schemaHash.String(), dataHash.String()})
}

func qkcBigHex(value *big.Int) string {
	if value == nil {
		return "0x"
	}
	return hexutil.Encode(value.Bytes())
}
func qkcUint64Hex(value uint64) string { return qkcBigHex(new(big.Int).SetUint64(value)) }
func qkcUint32Hex(value uint32) string { return qkcUint64Hex(uint64(value)) }

var qkcTypeSize = regexp.MustCompile(`\d+`)

func qkcSolidityHash(types, values []string) (common.Hash, error) {
	packed, err := qkcSolidityPack(types, values)
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(packed), nil
}

func qkcSolidityPack(types, values []string) ([]byte, error) {
	if len(types) != len(values) {
		return nil, errors.New("solidity types and values length mismatch")
	}
	var result []byte
	for i, typeName := range types {
		value := values[i]
		switch {
		case typeName == "bytes":
			if value == "" {
				continue
			}
			decoded, err := hex.DecodeString(value)
			if err != nil {
				return nil, err
			}
			result = append(result, decoded...)
		case typeName == "string":
			result = append(result, value...)
		case strings.HasPrefix(typeName, "bytes"), strings.HasPrefix(typeName, "uint"), strings.HasPrefix(typeName, "int"):
			sizeText := qkcTypeSize.FindString(typeName)
			size, err := strconv.Atoi(sizeText)
			if err != nil {
				return nil, fmt.Errorf("invalid solidity type %q: %w", typeName, err)
			}
			if strings.HasPrefix(typeName, "bytes") {
				if size < 1 || size > 32 {
					return nil, fmt.Errorf("invalid byte width %d", size)
				}
			} else {
				if size < 8 || size > 256 || size%8 != 0 {
					return nil, fmt.Errorf("invalid integer width %d", size)
				}
				size /= 8
			}
			decoded, err := hex.DecodeString(strings.TrimPrefix(value, "0x"))
			if err != nil {
				return nil, err
			}
			if len(decoded) > size {
				return nil, fmt.Errorf("value for %s exceeds %d bytes", typeName, size)
			}
			result = append(result, make([]byte, size-len(decoded))...)
			result = append(result, decoded...)
		default:
			return nil, fmt.Errorf("unsupported solidity type %q", typeName)
		}
	}
	return result, nil
}
