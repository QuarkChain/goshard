// Copyright 2026-2027, QuarkChain.

package replay

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

type QKCAddress struct {
	Recipient    common.Address
	FullShardKey uint32
}

type TokenBalance struct {
	TokenID uint64
	Balance *big.Int
}

type TransactionInput struct {
	Hash             common.Hash
	From             common.Address
	To               *common.Address
	Nonce            uint64
	Value            *big.Int
	GasPrice         *big.Int
	Gas              uint64
	FromFullShardKey uint32
	ToFullShardKey   uint32
	GasTokenID       uint64
	TransferTokenID  uint64
	RawEVMRLP        []byte
}

type XShardDepositInput struct {
	TxHash          common.Hash
	FromAddress     QKCAddress
	ToAddress       QKCAddress
	Value           *big.Int
	GasPrice        *big.Int
	GasTokenID      uint64
	TransferTokenID uint64
	GasRemained     uint64
	MessageData     []byte
	CreateContract  bool
	IsFromRootChain bool
	RefundRate      uint64
}

type MinorBlockInput struct {
	FullShardID                 uint32
	Height                      uint64
	Hash                        common.Hash
	ExpectedStateRoot           common.Hash
	ExpectedReceiptRoot         common.Hash
	GasUsed                     uint64
	CrossShardReceiveGasUsed    uint64
	Coinbase                    QKCAddress
	CoinbaseAmountMap           []TokenBalance
	Transactions                []TransactionInput
	XShardReceiveDeposits       []XShardDepositInput
	XShardReceiveDepositHashCnt int
}

type rpcBlock struct {
	Hash                     string            `json:"hash"`
	Height                   string            `json:"height"`
	FullShardID              string            `json:"fullShardId"`
	Miner                    string            `json:"miner"`
	Coinbase                 []rpcTokenBalance `json:"coinbase"`
	HashEVMStateRoot         string            `json:"hashEvmStateRoot"`
	HashEVMReceiptRoot       string            `json:"hashEvmReceiptRoot"`
	GasUsed                  string            `json:"gasUsed"`
	CrossShardReceiveGasUsed string            `json:"crossShardReceiveGasUsed"`
	Transactions             []json.RawMessage `json:"transactions"`
	XShardReceiveDeposits    []json.RawMessage `json:"xshardReceiveDeposits"`
	XShardReceiveHashes      []json.RawMessage `json:"xshardReceiveDepositHashes"`
}

type rpcTokenBalance struct {
	TokenID string `json:"tokenId"`
	Balance string `json:"balance"`
}

type rpcTransaction struct {
	Hash             string `json:"hash"`
	From             string `json:"from"`
	To               string `json:"to"`
	Nonce            string `json:"nonce"`
	Value            string `json:"value"`
	GasPrice         string `json:"gasPrice"`
	Gas              string `json:"gas"`
	FromFullShardKey string `json:"fromFullShardKey"`
	ToFullShardKey   string `json:"toFullShardKey"`
	GasTokenID       string `json:"gasTokenId"`
	TransferTokenID  string `json:"transferTokenId"`
	RawEVMRLP        string `json:"rawEvmRlp"`
}

type rpcXShardDeposit struct {
	TxHash          string `json:"txHash"`
	FromAddress     string `json:"fromAddress"`
	ToAddress       string `json:"toAddress"`
	Value           string `json:"value"`
	GasPrice        string `json:"gasPrice"`
	GasTokenID      string `json:"gasTokenId"`
	TransferTokenID string `json:"transferTokenId"`
	GasRemained     string `json:"gasRemained"`
	MessageData     string `json:"messageData"`
	CreateContract  bool   `json:"createContract"`
	IsFromRootChain bool   `json:"isFromRootChain"`
	RefundRate      string `json:"refundRate"`
}

func ParseMinorBlockInputJSON(input []byte) (*MinorBlockInput, error) {
	var raw rpcBlock
	if err := json.Unmarshal(input, &raw); err != nil {
		return nil, err
	}
	return normalizeRPCBlock(raw)
}

func normalizeRPCBlock(raw rpcBlock) (*MinorBlockInput, error) {
	fullShardID, err := parseUint32(raw.FullShardID)
	if err != nil {
		return nil, fmt.Errorf("parse fullShardId: %w", err)
	}
	height, err := parseUint64(raw.Height)
	if err != nil {
		return nil, fmt.Errorf("parse height: %w", err)
	}
	hash, err := parseHash(raw.Hash)
	if err != nil {
		return nil, fmt.Errorf("parse hash: %w", err)
	}
	stateRoot, err := parseHash(raw.HashEVMStateRoot)
	if err != nil {
		return nil, fmt.Errorf("parse hashEvmStateRoot: %w", err)
	}
	receiptRoot := common.Hash{}
	if raw.HashEVMReceiptRoot != "" {
		receiptRoot, err = parseHash(raw.HashEVMReceiptRoot)
		if err != nil {
			return nil, fmt.Errorf("parse hashEvmReceiptRoot: %w", err)
		}
	}
	gasUsed, err := parseOptionalUint64(raw.GasUsed)
	if err != nil {
		return nil, fmt.Errorf("parse gasUsed: %w", err)
	}
	xGasUsed, err := parseOptionalUint64(raw.CrossShardReceiveGasUsed)
	if err != nil {
		return nil, fmt.Errorf("parse crossShardReceiveGasUsed: %w", err)
	}
	coinbase, err := parseQKCAddress(raw.Miner)
	if err != nil {
		return nil, fmt.Errorf("parse miner: %w", err)
	}
	coinbaseAmountMap, err := parseTokenBalances(raw.Coinbase)
	if err != nil {
		return nil, fmt.Errorf("parse coinbase: %w", err)
	}
	txs, err := parseTransactions(raw.Transactions)
	if err != nil {
		return nil, err
	}
	deposits, err := parseXShardDeposits(raw.XShardReceiveDeposits)
	if err != nil {
		return nil, err
	}
	return &MinorBlockInput{
		FullShardID:                 fullShardID,
		Height:                      height,
		Hash:                        hash,
		ExpectedStateRoot:           stateRoot,
		ExpectedReceiptRoot:         receiptRoot,
		GasUsed:                     gasUsed,
		CrossShardReceiveGasUsed:    xGasUsed,
		Coinbase:                    coinbase,
		CoinbaseAmountMap:           coinbaseAmountMap,
		Transactions:                txs,
		XShardReceiveDeposits:       deposits,
		XShardReceiveDepositHashCnt: len(raw.XShardReceiveHashes),
	}, nil
}

func parseXShardDeposits(rawDeposits []json.RawMessage) ([]XShardDepositInput, error) {
	deposits := make([]XShardDepositInput, 0, len(rawDeposits))
	for idx, raw := range rawDeposits {
		var deposit rpcXShardDeposit
		if err := json.Unmarshal(raw, &deposit); err != nil {
			return nil, fmt.Errorf("parse xshard deposit %d: %w", idx, err)
		}
		txHash, err := parseHash(deposit.TxHash)
		if err != nil {
			return nil, fmt.Errorf("parse xshard deposit %d txHash: %w", idx, err)
		}
		fromAddress, err := parseQKCAddress(deposit.FromAddress)
		if err != nil {
			return nil, fmt.Errorf("parse xshard deposit %d fromAddress: %w", idx, err)
		}
		toAddress, err := parseQKCAddress(deposit.ToAddress)
		if err != nil {
			return nil, fmt.Errorf("parse xshard deposit %d toAddress: %w", idx, err)
		}
		value, err := parseBig(deposit.Value)
		if err != nil {
			return nil, fmt.Errorf("parse xshard deposit %d value: %w", idx, err)
		}
		gasPrice, err := parseBig(deposit.GasPrice)
		if err != nil {
			return nil, fmt.Errorf("parse xshard deposit %d gasPrice: %w", idx, err)
		}
		gasTokenID, err := parseUint64(deposit.GasTokenID)
		if err != nil {
			return nil, fmt.Errorf("parse xshard deposit %d gasTokenId: %w", idx, err)
		}
		transferTokenID, err := parseUint64(deposit.TransferTokenID)
		if err != nil {
			return nil, fmt.Errorf("parse xshard deposit %d transferTokenId: %w", idx, err)
		}
		gasRemained, err := parseUint64(deposit.GasRemained)
		if err != nil {
			return nil, fmt.Errorf("parse xshard deposit %d gasRemained: %w", idx, err)
		}
		messageData, err := parseOptionalBytes(deposit.MessageData)
		if err != nil {
			return nil, fmt.Errorf("parse xshard deposit %d messageData: %w", idx, err)
		}
		refundRate, err := parseUint64(deposit.RefundRate)
		if err != nil {
			return nil, fmt.Errorf("parse xshard deposit %d refundRate: %w", idx, err)
		}
		deposits = append(deposits, XShardDepositInput{
			TxHash:          txHash,
			FromAddress:     fromAddress,
			ToAddress:       toAddress,
			Value:           value,
			GasPrice:        gasPrice,
			GasTokenID:      gasTokenID,
			TransferTokenID: transferTokenID,
			GasRemained:     gasRemained,
			MessageData:     messageData,
			CreateContract:  deposit.CreateContract,
			IsFromRootChain: deposit.IsFromRootChain,
			RefundRate:      refundRate,
		})
	}
	return deposits, nil
}

func parseTransactions(rawTxs []json.RawMessage) ([]TransactionInput, error) {
	txs := make([]TransactionInput, 0, len(rawTxs))
	for idx, raw := range rawTxs {
		var tx rpcTransaction
		if err := json.Unmarshal(raw, &tx); err != nil {
			return nil, fmt.Errorf("parse transaction %d: %w", idx, err)
		}
		hash, err := parseHash(tx.Hash)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d hash: %w", idx, err)
		}
		from, err := parseAddress20(tx.From)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d from: %w", idx, err)
		}
		var to *common.Address
		if tx.To != "" && tx.To != "0x" {
			addr, err := parseAddress20(tx.To)
			if err != nil {
				return nil, fmt.Errorf("parse transaction %d to: %w", idx, err)
			}
			to = &addr
		}
		nonce, err := parseUint64(tx.Nonce)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d nonce: %w", idx, err)
		}
		value, err := parseBig(tx.Value)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d value: %w", idx, err)
		}
		gasPrice, err := parseBig(tx.GasPrice)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d gasPrice: %w", idx, err)
		}
		gas, err := parseUint64(tx.Gas)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d gas: %w", idx, err)
		}
		fromFullShardKey, err := parseUint32(tx.FromFullShardKey)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d fromFullShardKey: %w", idx, err)
		}
		toFullShardKey, err := parseUint32(tx.ToFullShardKey)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d toFullShardKey: %w", idx, err)
		}
		gasTokenID, err := parseUint64(tx.GasTokenID)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d gasTokenId: %w", idx, err)
		}
		transferTokenID, err := parseUint64(tx.TransferTokenID)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d transferTokenId: %w", idx, err)
		}
		rawEVMRLP, err := parseOptionalBytes(tx.RawEVMRLP)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d rawEvmRlp: %w", idx, err)
		}
		txs = append(txs, TransactionInput{
			Hash:             hash,
			From:             from,
			To:               to,
			Nonce:            nonce,
			Value:            value,
			GasPrice:         gasPrice,
			Gas:              gas,
			FromFullShardKey: fromFullShardKey,
			ToFullShardKey:   toFullShardKey,
			GasTokenID:       gasTokenID,
			TransferTokenID:  transferTokenID,
			RawEVMRLP:        rawEVMRLP,
		})
	}
	return txs, nil
}

func parseTokenBalances(raw []rpcTokenBalance) ([]TokenBalance, error) {
	balances := make([]TokenBalance, 0, len(raw))
	for _, item := range raw {
		tokenID, err := parseUint64(item.TokenID)
		if err != nil {
			return nil, err
		}
		balance, err := parseBig(item.Balance)
		if err != nil {
			return nil, err
		}
		balances = append(balances, TokenBalance{TokenID: tokenID, Balance: balance})
	}
	return balances, nil
}

func parseQKCAddress(input string) (QKCAddress, error) {
	blob, err := parseBytes(input)
	if err != nil {
		return QKCAddress{}, err
	}
	if len(blob) != common.AddressLength+4 {
		return QKCAddress{}, fmt.Errorf("invalid qkc address length %d", len(blob))
	}
	return QKCAddress{
		Recipient:    common.BytesToAddress(blob[:common.AddressLength]),
		FullShardKey: binary.BigEndian.Uint32(blob[common.AddressLength:]),
	}, nil
}

func parseAddress20(input string) (common.Address, error) {
	blob, err := parseBytes(input)
	if err != nil {
		return common.Address{}, err
	}
	if len(blob) != common.AddressLength {
		return common.Address{}, fmt.Errorf("invalid address length %d", len(blob))
	}
	return common.BytesToAddress(blob), nil
}

func parseHash(input string) (common.Hash, error) {
	blob, err := parseBytes(input)
	if err != nil {
		return common.Hash{}, err
	}
	if len(blob) != common.HashLength {
		return common.Hash{}, fmt.Errorf("invalid hash length %d", len(blob))
	}
	return common.BytesToHash(blob), nil
}

func parseBytes(input string) ([]byte, error) {
	input = strings.TrimPrefix(input, "0x")
	if len(input)%2 != 0 {
		input = "0" + input
	}
	return hex.DecodeString(input)
}

func parseOptionalBytes(input string) ([]byte, error) {
	if input == "" {
		return nil, nil
	}
	return parseBytes(input)
}

func parseUint32(input string) (uint32, error) {
	value, err := parseUint64(input)
	if err != nil {
		return 0, err
	}
	if value > uint64(^uint32(0)) {
		return 0, fmt.Errorf("uint32 overflow: %d", value)
	}
	return uint32(value), nil
}

func parseOptionalUint64(input string) (uint64, error) {
	if input == "" {
		return 0, nil
	}
	return parseUint64(input)
}

func parseUint64(input string) (uint64, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return 0, fmt.Errorf("empty integer")
	}
	if strings.HasPrefix(input, "0x") || strings.HasPrefix(input, "0X") {
		return strconv.ParseUint(input[2:], 16, 64)
	}
	return strconv.ParseUint(input, 10, 64)
}

func parseBig(input string) (*big.Int, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("empty integer")
	}
	value := new(big.Int)
	var ok bool
	if strings.HasPrefix(input, "0x") || strings.HasPrefix(input, "0X") {
		_, ok = value.SetString(input[2:], 16)
	} else {
		_, ok = value.SetString(input, 10)
	}
	if !ok {
		return nil, fmt.Errorf("invalid integer %q", input)
	}
	return value, nil
}
