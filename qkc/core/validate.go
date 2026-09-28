// Copyright 2026-2027, QuarkChain.

package core

import (
	"errors"
	"fmt"
	"math"
	"math/big"

	ethcore "github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/types"
)

const (
	txGas                 = uint64(21000)
	txGasContractCreation = uint64(32000)
	txDataZeroGas         = uint64(4)
	txDataNonZeroGas      = uint64(68)
)

var uint128Max = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))

// IntrinsicGas returns pyquarkchain's intrinsic gas for an in-shard
// transaction under its Petersburg-era schedule.
func IntrinsicGas(tx *types.Transaction) uint64 {
	gas := txGas
	if tx.To() == nil {
		gas += txGasContractCreation
	}
	for _, b := range tx.Data() {
		if b == 0 {
			gas += txDataZeroGas
		} else {
			gas += txDataNonZeroGas
		}
	}
	return gas
}

// TxSender recovers the transaction sender using the signing domain selected
// by the transaction version.
func TxSender(ctx *ExecutionContext, tx *types.Transaction) (account.Recipient, error) {
	if ctx == nil || ctx.QKCConfig == nil || ctx.ShardConfig == nil {
		return account.Recipient{}, fmt.Errorf("%w: missing execution config", ErrInvalidTransaction)
	}
	if tx == nil {
		return account.Recipient{}, fmt.Errorf("%w: nil transaction", ErrInvalidTransaction)
	}
	signer := types.MakeSigner(ctx.QKCConfig.NetworkID, ctx.ShardConfig.EthChainID)
	sender, err := types.Sender(signer, tx)
	if err == nil {
		return sender, nil
	}
	if errors.Is(err, types.ErrV2NonDefaultToken) || errors.Is(err, types.ErrV2CrossShard) || errors.Is(err, types.ErrInvalidNetworkID) {
		return account.Recipient{}, fmt.Errorf("%w: %v", ErrInvalidTransaction, err)
	}
	return account.Recipient{}, fmt.Errorf("%w: %v", ErrUnsignedTransaction, err)
}

// ValidateTransaction applies the admission rules needed by intra-shard
// execution. It performs no writes.
func ValidateTransaction(ctx *ExecutionContext, statedb *state.StateDB, gp *ethcore.GasPool, tx *types.Transaction, sender account.Recipient, blockTime uint64) error {
	if ctx == nil || ctx.QKCConfig == nil || ctx.ShardConfig == nil || statedb == nil || gp == nil || tx == nil {
		return fmt.Errorf("%w: incomplete transaction context", ErrInvalidTransaction)
	}
	if err := tx.Validate(); err != nil {
		if errors.Is(err, types.ErrV2NonDefaultToken) || errors.Is(err, types.ErrV2CrossShard) {
			return fmt.Errorf("%w: %v", ErrInvalidTransaction, err)
		}
		return fmt.Errorf("%w: %v", ErrInvalidTransaction, err)
	}
	branch := ctx.Branch()
	if tx.IsCrossShard() || !branch.IsInBranch(tx.ToFullShardKey()) {
		return fmt.Errorf("%w: cross-shard transaction", ErrInvalidTransaction)
	}
	if !branch.IsInBranch(tx.FromFullShardKey()) {
		return fmt.Errorf("%w: source shard key %#x is outside branch %#x", ErrInvalidTransaction, tx.FromFullShardKey(), branch.GetFullShardID())
	}
	if blockTime < ctx.QKCConfig.EnableTxTimeStamp && !ctx.QKCConfig.IsTxSenderWhitelisted(sender) {
		return fmt.Errorf("%w: sender %s is not enabled yet", ErrInvalidTransaction, sender)
	}
	if blockTime < ctx.QKCConfig.EnableEvmTimeStamp && (tx.To() == nil || len(tx.Data()) != 0) {
		return fmt.Errorf("%w: contract transactions are not enabled yet", ErrInvalidTransaction)
	}
	if tx.Version() == 2 {
		if err := validateEIP155(ctx, tx, blockTime); err != nil {
			return err
		}
	}
	if tx.GasPrice().Cmp(uint128Max) > 0 {
		return fmt.Errorf("%w: gas price exceeds uint128", ErrInvalidTransaction)
	}
	if tx.GasTokenID() != qkccommon.DefaultTokenID || tx.TransferTokenID() != qkccommon.DefaultTokenID {
		return vm.ErrQKCUnsupportedMNT
	}
	if nonce := statedb.GetNonce(sender); nonce != tx.Nonce() {
		return fmt.Errorf("%w: nonce %d, want %d", ErrInvalidNonce, tx.Nonce(), nonce)
	}
	if tx.Nonce() == math.MaxUint64 {
		return fmt.Errorf("%w: nonce overflow", ErrInvalidNonce)
	}
	intrinsic := IntrinsicGas(tx)
	if tx.Gas() < intrinsic {
		return fmt.Errorf("%w: gas %d, intrinsic %d", ErrInsufficientStartGas, tx.Gas(), intrinsic)
	}
	gasCost := new(big.Int).Mul(tx.GasPrice(), new(big.Int).SetUint64(tx.Gas()))
	totalCost := new(big.Int).Add(gasCost, tx.Value())
	if statedb.GetBalance(sender).ToBig().Cmp(totalCost) < 0 {
		return fmt.Errorf("%w: have %s, need %s", ErrInsufficientBalance, statedb.GetBalance(sender), totalCost)
	}
	if gp.Gas() < tx.Gas() {
		return fmt.Errorf("%w: have %d gas, need %d", ErrBlockGasLimitReached, gp.Gas(), tx.Gas())
	}
	return nil
}

func validateEIP155(ctx *ExecutionContext, tx *types.Transaction, blockTime uint64) error {
	if blockTime < ctx.QKCConfig.EnableEIP155SignerTimestamp {
		return fmt.Errorf("%w: EIP-155 signer is not enabled yet", ErrInvalidTransaction)
	}
	if tx.FromChainID() != tx.ToChainID() || tx.FromShardKey() != 0 || tx.ToShardKey() != 0 {
		return fmt.Errorf("%w: EIP-155 transaction must use zero shard keys on one chain", ErrInvalidTransaction)
	}
	if tx.NetworkId() != ctx.ShardConfig.EthChainID {
		return fmt.Errorf("%w: network ID %d, want eth chain ID %d", ErrInvalidTransaction, tx.NetworkId(), ctx.ShardConfig.EthChainID)
	}
	chain, ok := ctx.QKCConfig.Chains[tx.FromChainID()]
	if !ok || chain.EthChainID != tx.NetworkId() {
		return fmt.Errorf("%w: transaction does not name a configured chain", ErrInvalidTransaction)
	}
	want := uint64(ctx.QKCConfig.BaseEthChainID) + uint64(tx.FromChainID()) + 1
	if want > math.MaxUint32 || uint64(tx.NetworkId()) != want {
		return fmt.Errorf("%w: eth chain ID %d does not encode chain %d", ErrInvalidTransaction, tx.NetworkId(), tx.FromChainID())
	}
	return nil
}
