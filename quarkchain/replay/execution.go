// Copyright 2026-2027, QuarkChain.

package replay

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	qkcstate "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
)

const qkcTransferIntrinsicGas uint64 = 21000

var maxUint128 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), common.Big1)

func (v *Verifier) applyTransactions(block *MinorBlockInput) (uint64, map[uint64]*big.Int, error) {
	var gasUsed uint64
	fees := make(map[uint64]*big.Int)
	for idx := range block.Transactions {
		txGasUsed, feeTokenID, fee, err := v.applyTransaction(block, &block.Transactions[idx])
		if err != nil {
			return 0, nil, fmt.Errorf("transaction %d: %w", idx, err)
		}
		gasUsed += txGasUsed
		addTokenAmount(fees, feeTokenID, fee)
	}
	return gasUsed, fees, nil
}

func (v *Verifier) applyTransaction(block *MinorBlockInput, tx *TransactionInput) (uint64, uint64, *big.Int, error) {
	if tx == nil {
		return 0, 0, nil, errors.New("nil transaction")
	}
	evmTx := tx.EVMTransaction
	if evmTx == nil {
		return 0, 0, nil, errors.New("missing parsed evm transaction")
	}
	if err := v.validateMinimalTransfer(block, tx); err != nil {
		return 0, 0, nil, err
	}
	intrinsicGas := qkcTransferIntrinsicGas
	if evmTx.Gas < intrinsicGas {
		return 0, 0, nil, fmt.Errorf("insufficient start gas: got %d want at least %d", evmTx.Gas, intrinsicGas)
	}
	if evmTx.Nonce != v.accountNonce(tx.From) {
		return 0, 0, nil, fmt.Errorf("invalid nonce: got %d want %d", evmTx.Nonce, v.accountNonce(tx.From))
	}
	gasCost := new(big.Int).Mul(new(big.Int).SetUint64(evmTx.Gas), evmTx.GasPrice)
	totalCost := new(big.Int).Add(gasCost, evmTx.Value)
	if balance := v.tokenBalance(tx.From, evmTx.GasTokenID); balance.Cmp(totalCost) < 0 {
		return 0, 0, nil, fmt.Errorf("insufficient balance: token %d balance %s cost %s", evmTx.GasTokenID, balance, totalCost)
	}

	v.stateFullShardKey = evmTx.ToFullShardKey
	v.incrementNonce(tx.From)
	if err := v.debitToken(tx.From, evmTx.GasTokenID, gasCost); err != nil {
		return 0, 0, nil, err
	}
	if evmTx.Value.Sign() != 0 {
		if err := v.transferToken(tx.From, *evmTx.To, evmTx.TransferTokenID, evmTx.Value); err != nil {
			return 0, 0, nil, err
		}
	}

	gasUsed := intrinsicGas
	gasRefund := new(big.Int).Mul(new(big.Int).SetUint64(evmTx.Gas-gasUsed), evmTx.GasPrice)
	if err := v.creditToken(QKCAddress{Recipient: tx.From, FullShardKey: v.stateFullShardKey}, evmTx.GasTokenID, gasRefund); err != nil {
		return 0, 0, nil, err
	}
	fee := new(big.Int).Mul(new(big.Int).SetUint64(gasUsed), evmTx.GasPrice)
	localFeeNum, localFeeDen, err := v.localFeeRate()
	if err != nil {
		return 0, 0, nil, err
	}
	fee.Mul(fee, localFeeNum)
	fee.Div(fee, localFeeDen)
	if err := v.creditToken(QKCAddress{Recipient: block.Coinbase.Recipient, FullShardKey: v.stateFullShardKey}, evmTx.GasTokenID, fee); err != nil {
		return 0, 0, nil, err
	}
	return gasUsed, evmTx.GasTokenID, fee, nil
}

func (v *Verifier) validateMinimalTransfer(block *MinorBlockInput, tx *TransactionInput) error {
	evmTx := tx.EVMTransaction
	if evmTx.To == nil {
		return errors.New("contract creation execution is not implemented yet")
	}
	if len(evmTx.Data) != 0 {
		return errors.New("transaction data execution is not implemented yet")
	}
	if evmTx.Value.Sign() < 0 || evmTx.GasPrice.Sign() < 0 {
		return errors.New("negative transaction value or gas price")
	}
	if exceedsUint128(new(big.Int).SetUint64(evmTx.Gas)) || exceedsUint128(evmTx.GasPrice) {
		return errors.New("startgas and gasprice must be <= uint128")
	}
	defaultTokenID, err := v.genesisTokenID()
	if err != nil {
		return err
	}
	if evmTx.GasTokenID != defaultTokenID {
		return fmt.Errorf("non-default gas token execution is not implemented yet: got %d want %d", evmTx.GasTokenID, defaultTokenID)
	}
	if evmTx.TransferTokenID != defaultTokenID {
		return fmt.Errorf("non-default transfer token execution is not implemented yet: got %d want %d", evmTx.TransferTokenID, defaultTokenID)
	}
	toFullShardID, err := FullShardIDFromKey(v.config, evmTx.ToFullShardKey)
	if err != nil {
		return fmt.Errorf("toFullShardKey: %w", err)
	}
	if toFullShardID != block.FullShardID {
		return fmt.Errorf("cross-shard transaction execution is not implemented yet: to fullShard=%#x block fullShard=%#x", toFullShardID, block.FullShardID)
	}
	if v.accountHasCode(*evmTx.To) {
		return errors.New("EVM code execution is not implemented yet")
	}
	return nil
}

func (v *Verifier) transferToken(from common.Address, to common.Address, tokenID uint64, amount *big.Int) error {
	if err := v.debitToken(from, tokenID, amount); err != nil {
		return err
	}
	return v.creditToken(QKCAddress{Recipient: to, FullShardKey: v.stateFullShardKey}, tokenID, amount)
}

func (v *Verifier) debitToken(from common.Address, tokenID uint64, amount *big.Int) error {
	if amount == nil || amount.Sign() == 0 {
		return nil
	}
	account, exists := v.accounts[from]
	if !exists {
		return fmt.Errorf("insufficient balance: account %s is missing", from)
	}
	next := account.QuarkChainTokenBalance(tokenID)
	next.Sub(next, amount)
	if err := account.SetQuarkChainTokenBalance(tokenID, next); err != nil {
		return err
	}
	v.accounts[from] = account
	v.pruneBlankAccount(from)
	return nil
}

func (v *Verifier) incrementNonce(addr common.Address) {
	account, exists := v.accounts[addr]
	if !exists {
		account.FullShardKey = v.stateFullShardKey
	}
	account.Nonce++
	v.accounts[addr] = account
}

func (v *Verifier) accountNonce(addr common.Address) uint64 {
	return v.accounts[addr].Nonce
}

func (v *Verifier) tokenBalance(addr common.Address, tokenID uint64) *big.Int {
	return v.accounts[addr].QuarkChainTokenBalance(tokenID)
}

func (v *Verifier) accountHasCode(addr common.Address) bool {
	account := v.accounts[addr]
	if len(account.Code) != 0 {
		return true
	}
	if len(account.CodeHash) == 0 {
		return false
	}
	return !bytes.Equal(account.CodeHash, types.EmptyCodeHash.Bytes())
}

func (v *Verifier) pruneBlankAccount(addr common.Address) {
	account, exists := v.accounts[addr]
	if !exists || !isBlankQuarkChainAccount(account) {
		return
	}
	delete(v.accounts, addr)
}

func isBlankQuarkChainAccount(account qkcstate.QuarkChainAccount) bool {
	if account.Nonce != 0 || len(account.Code) != 0 || len(account.Storage) != 0 || len(account.Optional) != 0 {
		return false
	}
	if account.StorageRoot != (common.Hash{}) && account.StorageRoot != types.EmptyRootHash {
		return false
	}
	if len(account.CodeHash) != 0 && !bytes.Equal(account.CodeHash, types.EmptyCodeHash.Bytes()) {
		return false
	}
	for _, balance := range account.TokenBalances {
		if balance != nil && balance.Sign() != 0 {
			return false
		}
	}
	return true
}

func (v *Verifier) genesisTokenID() (uint64, error) {
	token := "QKC"
	if v.config != nil && v.config.QuarkChain.GenesisToken != "" {
		token = v.config.QuarkChain.GenesisToken
	}
	return qkcstate.QuarkChainTokenIDEncode(token)
}

func (v *Verifier) localFeeRate() (*big.Int, *big.Int, error) {
	if v.config == nil {
		return common.Big1, common.Big1, nil
	}
	rawTaxRate := strings.TrimSpace(v.config.QuarkChain.RewardTaxRate.String())
	if rawTaxRate == "" {
		return common.Big1, common.Big1, nil
	}
	taxRate, ok := new(big.Rat).SetString(rawTaxRate)
	if !ok {
		return nil, nil, fmt.Errorf("invalid reward tax rate %q", rawTaxRate)
	}
	if taxRate.Sign() < 0 || taxRate.Cmp(new(big.Rat).SetInt(common.Big1)) > 0 {
		return nil, nil, fmt.Errorf("reward tax rate out of range: %s", rawTaxRate)
	}
	localFeeRate := new(big.Rat).Sub(new(big.Rat).SetInt(common.Big1), taxRate)
	return localFeeRate.Num(), localFeeRate.Denom(), nil
}

func exceedsUint128(value *big.Int) bool {
	return value == nil || value.Sign() < 0 || value.Cmp(maxUint128) > 0
}

func addTokenAmount(amounts map[uint64]*big.Int, tokenID uint64, amount *big.Int) {
	if amount == nil || amount.Sign() == 0 {
		return
	}
	if amounts[tokenID] == nil {
		amounts[tokenID] = new(big.Int)
	}
	amounts[tokenID].Add(amounts[tokenID], amount)
}

func subtractTokenAmounts(balances []TokenBalance, subtrahends map[uint64]*big.Int) ([]TokenBalance, error) {
	if len(subtrahends) == 0 {
		return balances, nil
	}
	remaining := make(map[uint64]*big.Int, len(subtrahends))
	for tokenID, amount := range subtrahends {
		if amount != nil && amount.Sign() != 0 {
			remaining[tokenID] = new(big.Int).Set(amount)
		}
	}
	out := make([]TokenBalance, 0, len(balances))
	for _, balance := range balances {
		amount := copyBig(balance.Balance)
		if fee := remaining[balance.TokenID]; fee != nil {
			amount.Sub(amount, fee)
			if amount.Sign() < 0 {
				return nil, fmt.Errorf("coinbase token %d balance %s is smaller than executed tx fees %s", balance.TokenID, balance.Balance, fee)
			}
			delete(remaining, balance.TokenID)
		}
		out = append(out, TokenBalance{TokenID: balance.TokenID, Balance: amount})
	}
	for tokenID, amount := range remaining {
		return nil, fmt.Errorf("coinbase is missing executed tx fee token %d amount %s", tokenID, amount)
	}
	return out, nil
}
