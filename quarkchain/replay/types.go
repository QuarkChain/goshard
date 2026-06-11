// Copyright 2026-2027, QuarkChain.

package replay

import (
	"bytes"
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
	Hash                common.Hash
	From                common.Address
	RecoveredSender     common.Address
	To                  *common.Address
	Nonce               uint64
	Value               *big.Int
	GasPrice            *big.Int
	Gas                 uint64
	Data                []byte
	NetworkID           uint32
	FromFullShardKey    uint32
	ToFullShardKey      uint32
	GasTokenID          uint64
	TransferTokenID     uint64
	Version             uint32
	V                   *big.Int
	R                   *big.Int
	S                   *big.Int
	RawTypedTransaction []byte
	RawEVMRLP           []byte
	TypedTransaction    *TypedTransaction
	EVMTransaction      *EVMTransaction
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
	Timestamp                   uint64
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
	Timestamp                string            `json:"timestamp"`
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
	Hash                string `json:"hash"`
	From                string `json:"from"`
	To                  string `json:"to"`
	Nonce               string `json:"nonce"`
	Value               string `json:"value"`
	GasPrice            string `json:"gasPrice"`
	Gas                 string `json:"gas"`
	NetworkID           string `json:"networkId"`
	FromFullShardKey    string `json:"fromFullShardKey"`
	ToFullShardKey      string `json:"toFullShardKey"`
	GasTokenID          string `json:"gasTokenId"`
	TransferTokenID     string `json:"transferTokenId"`
	Version             string `json:"version"`
	RawTypedTransaction string `json:"rawTypedTransaction"`
	RawEVMRLP           string `json:"rawEvmRlp"`
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
	timestamp, err := parseOptionalUint64(raw.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("parse timestamp: %w", err)
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
		Timestamp:                   timestamp,
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
		networkID, err := parseOptionalUint32(tx.NetworkID)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d networkId: %w", idx, err)
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
		version, err := parseOptionalUint32(tx.Version)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d version: %w", idx, err)
		}
		rawTypedTransaction, err := parseOptionalBytes(tx.RawTypedTransaction)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d rawTypedTransaction: %w", idx, err)
		}
		rawEVMRLP, err := parseOptionalBytes(tx.RawEVMRLP)
		if err != nil {
			return nil, fmt.Errorf("parse transaction %d rawEvmRlp: %w", idx, err)
		}
		input := TransactionInput{
			Hash:                hash,
			From:                from,
			To:                  to,
			Nonce:               nonce,
			Value:               value,
			GasPrice:            gasPrice,
			Gas:                 gas,
			NetworkID:           networkID,
			FromFullShardKey:    fromFullShardKey,
			ToFullShardKey:      toFullShardKey,
			GasTokenID:          gasTokenID,
			TransferTokenID:     transferTokenID,
			Version:             version,
			RawTypedTransaction: rawTypedTransaction,
			RawEVMRLP:           rawEVMRLP,
		}
		if err := input.applySerializedTransaction(tx, idx); err != nil {
			return nil, err
		}
		txs = append(txs, input)
	}
	return txs, nil
}

func (tx *TransactionInput) applySerializedTransaction(raw rpcTransaction, idx int) error {
	if len(tx.RawTypedTransaction) != 0 {
		typedTx, err := ParseTypedTransaction(tx.RawTypedTransaction)
		if err != nil {
			return fmt.Errorf("parse transaction %d rawTypedTransaction: %w", idx, err)
		}
		if typedTx.Hash != tx.Hash {
			return fmt.Errorf("parse transaction %d rawTypedTransaction hash mismatch: json %s envelope %s", idx, tx.Hash, typedTx.Hash)
		}
		if len(tx.RawEVMRLP) != 0 && !bytes.Equal(tx.RawEVMRLP, typedTx.SerializedEVMRLP) {
			return fmt.Errorf("parse transaction %d rawEvmRlp mismatch with typed transaction payload", idx)
		}
		tx.TypedTransaction = typedTx
		tx.EVMTransaction = typedTx.EVM
		tx.RawEVMRLP = common.CopyBytes(typedTx.SerializedEVMRLP)
	} else if len(tx.RawEVMRLP) != 0 {
		evmTx, err := ParseEVMTransactionRLP(tx.RawEVMRLP)
		if err != nil {
			return fmt.Errorf("parse transaction %d rawEvmRlp: %w", idx, err)
		}
		tx.EVMTransaction = evmTx
	}
	if tx.EVMTransaction == nil {
		return nil
	}
	return tx.applyEVMTransaction(raw, idx)
}

func (tx *TransactionInput) applyEVMTransaction(raw rpcTransaction, idx int) error {
	evmTx := tx.EVMTransaction
	if tx.Nonce != evmTx.Nonce {
		return fmt.Errorf("parse transaction %d nonce mismatch: json %d rlp %d", idx, tx.Nonce, evmTx.Nonce)
	}
	if tx.GasPrice.Cmp(evmTx.GasPrice) != 0 {
		return fmt.Errorf("parse transaction %d gasPrice mismatch: json %s rlp %s", idx, tx.GasPrice, evmTx.GasPrice)
	}
	if tx.Gas != evmTx.Gas {
		return fmt.Errorf("parse transaction %d gas mismatch: json %d rlp %d", idx, tx.Gas, evmTx.Gas)
	}
	if !sameAddressPtr(tx.To, evmTx.To) {
		return fmt.Errorf("parse transaction %d to mismatch: json %v rlp %v", idx, tx.To, evmTx.To)
	}
	if tx.Value.Cmp(evmTx.Value) != 0 {
		return fmt.Errorf("parse transaction %d value mismatch: json %s rlp %s", idx, tx.Value, evmTx.Value)
	}
	if raw.NetworkID != "" && tx.NetworkID != evmTx.NetworkID {
		return fmt.Errorf("parse transaction %d networkId mismatch: json %d rlp %d", idx, tx.NetworkID, evmTx.NetworkID)
	}
	if tx.FromFullShardKey != evmTx.FromFullShardKey {
		return fmt.Errorf("parse transaction %d fromFullShardKey mismatch: json %#x rlp %#x", idx, tx.FromFullShardKey, evmTx.FromFullShardKey)
	}
	if tx.ToFullShardKey != evmTx.ToFullShardKey {
		return fmt.Errorf("parse transaction %d toFullShardKey mismatch: json %#x rlp %#x", idx, tx.ToFullShardKey, evmTx.ToFullShardKey)
	}
	if tx.GasTokenID != evmTx.GasTokenID {
		return fmt.Errorf("parse transaction %d gasTokenId mismatch: json %d rlp %d", idx, tx.GasTokenID, evmTx.GasTokenID)
	}
	if tx.TransferTokenID != evmTx.TransferTokenID {
		return fmt.Errorf("parse transaction %d transferTokenId mismatch: json %d rlp %d", idx, tx.TransferTokenID, evmTx.TransferTokenID)
	}
	if raw.Version != "" && tx.Version != evmTx.Version {
		return fmt.Errorf("parse transaction %d version mismatch: json %d rlp %d", idx, tx.Version, evmTx.Version)
	}
	tx.Data = common.CopyBytes(evmTx.Data)
	tx.NetworkID = evmTx.NetworkID
	tx.Version = evmTx.Version
	tx.V = copyBig(evmTx.V)
	tx.R = copyBig(evmTx.R)
	tx.S = copyBig(evmTx.S)
	recovered, err := evmTx.RecoverSender()
	if err != nil {
		return fmt.Errorf("parse transaction %d sender: %w", idx, err)
	}
	if recovered != tx.From {
		return fmt.Errorf("parse transaction %d sender mismatch: json %s signature %s", idx, tx.From, recovered)
	}
	tx.RecoveredSender = recovered
	return nil
}

func sameAddressPtr(a *common.Address, b *common.Address) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
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

func parseOptionalUint32(input string) (uint32, error) {
	if input == "" {
		return 0, nil
	}
	return parseUint32(input)
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
