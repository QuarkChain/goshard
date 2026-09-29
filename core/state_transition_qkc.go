// Copyright 2026-2027, QuarkChain.

package core

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	qkcparams "github.com/ethereum/go-ethereum/qkc/params"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	qkctypes "github.com/ethereum/go-ethereum/qkc/types"
	"github.com/holiman/uint256"
)

// QKCExecutionContext contains the network and shard rules for transaction admission.
type QKCExecutionContext struct {
	QKCConfig   *config.QuarkChainConfig
	ShardConfig *config.ShardConfig
}

func (ctx *QKCExecutionContext) branch() account.Branch {
	return account.NewBranch(ctx.ShardConfig.GetFullShardId())
}

var (
	ErrQKCInvalidTransaction   = errors.New("qkc: invalid transaction")
	ErrQKCInvalidNonce         = errors.New("qkc: invalid nonce")
	ErrQKCInsufficientStartGas = errors.New("qkc: insufficient start gas")
	ErrQKCInsufficientBalance  = errors.New("qkc: insufficient balance")
	ErrQKCBlockGasLimitReached = errors.New("qkc: block gas limit reached")
)

func txSender(ctx *QKCExecutionContext, tx *qkctypes.Transaction) (account.Recipient, error) {
	if ctx == nil || ctx.QKCConfig == nil || ctx.ShardConfig == nil || tx == nil {
		return account.Recipient{}, fmt.Errorf("%w: incomplete transaction context", ErrQKCInvalidTransaction)
	}
	signer := qkctypes.MakeSigner(ctx.QKCConfig.NetworkID, ctx.ShardConfig.EthChainID)
	sender, err := qkctypes.Sender(signer, tx)
	if err != nil {
		return account.Recipient{}, fmt.Errorf("%w: %v", ErrQKCInvalidTransaction, err)
	}
	return sender, nil
}

// ValidateQKCTransaction checks the QKC admission rules without changing balances,
// nonce or the gas pool. It returns the sender recovered from the signature.
func ValidateQKCTransaction(ctx *QKCExecutionContext, statedb *state.StateDB, gp *GasPool, tx *qkctypes.Transaction, blockTime uint64) (account.Recipient, error) {
	sender, err := txSender(ctx, tx)
	if err != nil {
		return account.Recipient{}, err
	}
	if err := validateTransaction(ctx, statedb, gp, tx, sender, blockTime); err != nil {
		return account.Recipient{}, err
	}
	return sender, nil
}

func validateTransaction(ctx *QKCExecutionContext, statedb *state.StateDB, gp *GasPool, tx *qkctypes.Transaction, sender account.Recipient, blockTime uint64) error {
	if ctx == nil || ctx.QKCConfig == nil || ctx.ShardConfig == nil || statedb == nil || gp == nil || tx == nil {
		return fmt.Errorf("%w: incomplete transaction context", ErrQKCInvalidTransaction)
	}
	if err := tx.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrQKCInvalidTransaction, err)
	}
	branch := ctx.branch()
	if !branch.IsInBranch(tx.FromFullShardKey()) {
		return fmt.Errorf("%w: transaction outside shard %#x", ErrQKCInvalidTransaction, branch.GetFullShardID())
	}
	if _, err := ctx.QKCConfig.GetFullShardIdByFullShardKey(tx.ToFullShardKey()); err != nil {
		return fmt.Errorf("%w: destination shard: %v", ErrQKCInvalidTransaction, err)
	}
	if tx.Version() == 2 {
		if err := validateV2(ctx, tx, blockTime); err != nil {
			return err
		}
	}
	if blockTime < ctx.QKCConfig.EnableTxTimeStamp && !senderWhitelisted(ctx.QKCConfig.TxWhitelistSenders, sender) {
		return fmt.Errorf("%w: sender not yet enabled", ErrQKCInvalidTransaction)
	}
	if blockTime < ctx.QKCConfig.EnableEvmTimeStamp && (tx.To() == nil || len(tx.Data()) != 0) {
		return fmt.Errorf("%w: contract transaction not yet enabled", ErrQKCInvalidTransaction)
	}
	if qkccommon.BiggerThanUint128Max(tx.GasPrice()) {
		return fmt.Errorf("%w: gas price exceeds uint128", ErrQKCInvalidTransaction)
	}
	if tx.GasTokenID() != qkccommon.DefaultTokenID || tx.TransferTokenID() != qkccommon.DefaultTokenID {
		return vm.ErrQKCUnsupportedMNT
	}
	if nonce := statedb.GetNonce(sender); nonce != tx.Nonce() || nonce == math.MaxUint64 {
		return fmt.Errorf("%w: nonce %d, state %d", ErrQKCInvalidNonce, tx.Nonce(), nonce)
	}
	cost, err := IntrinsicGas(tx.Data(), nil, nil, tx.To() == nil, true, false, false, false)
	if err != nil {
		return err
	}
	if tx.IsCrossShard() {
		if cost.RegularGas > math.MaxUint64-qkcparams.GtxxShardCost.Uint64() {
			return ErrGasUintOverflow
		}
		cost.RegularGas += qkcparams.GtxxShardCost.Uint64()
	}
	if tx.Gas() < cost.RegularGas {
		return fmt.Errorf("%w: have %d, need %d", ErrQKCInsufficientStartGas, tx.Gas(), cost.RegularGas)
	}
	gasCost := new(big.Int).Mul(tx.GasPrice(), new(big.Int).SetUint64(tx.Gas()))
	upfront := new(big.Int).Add(gasCost, tx.Value())
	if statedb.GetBalance(sender).ToBig().Cmp(upfront) < 0 {
		return fmt.Errorf("%w: have %s, need %s", ErrQKCInsufficientBalance, statedb.GetBalance(sender), upfront)
	}
	if gp.Gas() < tx.Gas() {
		return fmt.Errorf("%w: have %d, need %d", ErrQKCBlockGasLimitReached, gp.Gas(), tx.Gas())
	}
	return nil
}

func validateV2(ctx *QKCExecutionContext, tx *qkctypes.Transaction, blockTime uint64) error {
	if blockTime < ctx.QKCConfig.EnableEIP155SignerTimestamp {
		return fmt.Errorf("%w: EIP-155 signer not yet enabled", ErrQKCInvalidTransaction)
	}
	if tx.FromChainID() != tx.ToChainID() || tx.FromShardKey() != 0 || tx.ToShardKey() != 0 {
		return fmt.Errorf("%w: EIP-155 requires one chain and zero shard keys", ErrQKCInvalidTransaction)
	}
	chain := ctx.QKCConfig.Chains[tx.FromChainID()]
	if chain == nil || tx.NetworkId() != chain.EthChainID || tx.NetworkId() != ctx.ShardConfig.EthChainID {
		return fmt.Errorf("%w: EIP-155 network ID", ErrQKCInvalidTransaction)
	}
	want := uint64(ctx.QKCConfig.BaseEthChainID) + uint64(tx.FromChainID()) + 1
	if want > math.MaxUint32 || uint64(tx.NetworkId()) != want {
		return fmt.Errorf("%w: EIP-155 chain ID", ErrQKCInvalidTransaction)
	}
	v, _, _ := tx.RawSignatureValues()
	base := new(big.Int).Mul(new(big.Int).SetUint64(uint64(tx.NetworkId())), big.NewInt(2))
	base.Add(base, big.NewInt(35))
	if v.Cmp(base) != 0 && v.Cmp(new(big.Int).Add(base, big.NewInt(1))) != 0 {
		return fmt.Errorf("%w: EIP-155 signature v", ErrQKCInvalidTransaction)
	}
	return nil
}

func senderWhitelisted(entries []string, sender common.Address) bool {
	for _, entry := range entries {
		decoded, err := hex.DecodeString(strings.TrimPrefix(entry, "0x"))
		if err == nil && len(decoded) == common.AddressLength && common.BytesToAddress(decoded) == sender {
			return true
		}
	}
	return false
}

type qkcExecutionResult struct {
	*ExecutionResult
	ContractAddress common.Address
}

// applyQKCMessage shares geth's gas accounting with QKC's nonce, VM entry and
// fee rules. Its caller restores state and gas pool on an error.
func applyQKCMessage(evm *vm.EVM, msg *Message, gp *GasPool, fromFullShardKey, toFullShardKey uint32, transferTokenID uint64, feeRate *big.Rat) (*qkcExecutionResult, error) {
	if evm == nil || msg == nil || gp == nil || feeRate == nil {
		return nil, fmt.Errorf("incomplete QKC message")
	}
	if feeRate.Sign() < 0 || feeRate.Cmp(big.NewRat(1, 1)) > 0 {
		return nil, fmt.Errorf("invalid QKC local fee rate")
	}
	st := newStateTransition(evm, msg, gp)
	evm.SetTxContext(vm.TxContext{
		Origin:           msg.From,
		GasPrice:         msg.GasPrice,
		FromFullShardKey: fromFullShardKey,
		ToFullShardKey:   toFullShardKey,
	})
	st.state.SetNonce(msg.From, msg.Nonce+1, tracing.NonceChangeEoACall)
	if err := st.buyGas(); err != nil {
		return nil, err
	}
	cost, err := IntrinsicGas(msg.Data, nil, nil, msg.To == nil, true, false, false, false)
	if err != nil {
		return nil, err
	}
	if _, ok := st.gasRemaining.Charge(cost); !ok {
		return nil, fmt.Errorf("%w: have %d, want %d", ErrIntrinsicGas, msg.GasLimit, cost.RegularGas)
	}
	rules := evm.ChainConfig().Rules(evm.Context.BlockNumber, false, evm.Context.Time)
	st.state.Prepare(rules, msg.From, evm.Context.Coinbase, msg.To, vm.ActivePrecompiles(rules), nil)

	var output []byte
	var created common.Address
	var vmerr error
	if msg.To == nil {
		output, created, st.gasRemaining, vmerr = evm.QKCCreateContract(msg.From, msg.Data, st.gasRemaining, msg.Value, transferTokenID, toFullShardKey, nil)
	} else {
		output, st.gasRemaining, vmerr = evm.QKCApplyMessage(msg.From, *msg.To, msg.Data, st.gasRemaining, msg.Value, transferTokenID, toFullShardKey)
	}
	if errors.Is(vmerr, vm.ErrQKCUnsupportedMNT) {
		return nil, vm.ErrQKCUnsupportedMNT
	}
	peakGasUsed := st.gasUsed()
	if vmerr == nil {
		st.gasRemaining.Refund(st.calcRefund())
	} else {
		output = nil
		created = common.Address{}
	}
	st.returnGas()
	if err := gp.ReturnGas(st.gasRemaining.RegularGas, st.gasUsed()); err != nil {
		return nil, err
	}
	fee := new(big.Int).Mul(msg.GasPrice.ToBig(), new(big.Int).SetUint64(st.gasUsed()))
	fee.Mul(fee, feeRate.Num())
	fee.Quo(fee, feeRate.Denom())
	localFee, overflow := uint256.FromBig(fee)
	if overflow {
		return nil, fmt.Errorf("QKC local fee exceeds uint256")
	}
	st.state.AddBalance(evm.Context.Coinbase, localFee, tracing.BalanceIncreaseRewardTransactionFee)
	return &qkcExecutionResult{
		ExecutionResult: &ExecutionResult{UsedGas: st.gasUsed(), MaxUsedGas: peakGasUsed, Err: vmerr, ReturnData: output},
		ContractAddress: created,
	}, nil
}

// ApplyQKCTransaction applies an intra-shard transaction. Cross-shard callers
// must use ApplyQKCTransactionWithDeposit so the outgoing deposit is retained.
func ApplyQKCTransaction(ctx *QKCExecutionContext, evm *vm.EVM, gp *GasPool, statedb *state.StateDB, tx *qkctypes.Transaction, txIndex int) (*qkctypes.Receipt, []byte, error) {
	if tx != nil && tx.IsCrossShard() {
		return nil, nil, fmt.Errorf("%w: cross-shard transaction requires deposit output", ErrQKCInvalidTransaction)
	}
	receipt, _, output, err := ApplyQKCTransactionWithDeposit(ctx, evm, gp, statedb, tx, txIndex)
	return receipt, output, err
}

// ApplyQKCTransactionWithDeposit returns a deposit for a successful cross-shard
// source transaction. Admission errors reject the transaction; PoSW failures
// produce a failed receipt. MNT errors require abandoning the whole block.
func ApplyQKCTransactionWithDeposit(ctx *QKCExecutionContext, evm *vm.EVM, gp *GasPool, statedb *state.StateDB, tx *qkctypes.Transaction, txIndex int) (*qkctypes.Receipt, *qkctypes.CrossShardTransactionDeposit, []byte, error) {
	if evm == nil || gp == nil || statedb == nil {
		return nil, nil, nil, fmt.Errorf("%w: missing execution state", ErrQKCInvalidTransaction)
	}
	sender, err := txSender(ctx, tx)
	if err != nil {
		return nil, nil, nil, err
	}
	stateSnapshot := statedb.Snapshot()
	poolSnapshot := gp.Snapshot()
	revert := func(err error) (*qkctypes.Receipt, *qkctypes.CrossShardTransactionDeposit, []byte, error) {
		statedb.RevertToSnapshot(stateSnapshot)
		gp.Set(poolSnapshot)
		return nil, nil, nil, err
	}
	if err := validateTransaction(ctx, statedb, gp, tx, sender, evm.Context.Time); err != nil {
		return revert(err)
	}
	// pyquarkchain validates before setting the destination shard key, so a
	// sender first observed during validation retains the previous key.
	statedb.SetFullShardKey(tx.ToFullShardKey())
	price, overflow := uint256.FromBig(tx.GasPrice())
	if overflow {
		return revert(fmt.Errorf("%w: gas price exceeds uint256", ErrQKCInvalidTransaction))
	}
	value, overflow := uint256.FromBig(tx.Value())
	if overflow {
		return revert(fmt.Errorf("%w: value exceeds uint256", ErrQKCInvalidTransaction))
	}
	var to *common.Address
	if recipient := tx.To(); recipient != nil {
		to = recipient
	}
	txHash := tx.Hash()
	statedb.SetTxContext(txHash, txIndex)
	evm.Context.EVMEnableTimestamp = ctx.QKCConfig.EnableEvmTimeStamp
	evm.Context.MNTEnableTimestamp = ctx.QKCConfig.EnableNonReservedNativeTokenTimestamp
	if tx.IsCrossShard() {
		receipt, deposit, output, err := applyQKCCrossShardSource(ctx, evm, gp, statedb, tx, sender, price, value)
		if err != nil {
			return revert(err)
		}
		return receipt, deposit, output, nil
	}
	result, err := applyQKCMessage(evm, &Message{
		From: sender, To: to, Nonce: tx.Nonce(), Value: value,
		GasLimit: tx.Gas(), GasPrice: price, Data: tx.Data(),
	}, gp, tx.FromFullShardKey(), tx.ToFullShardKey(), tx.TransferTokenID(), ctx.QKCConfig.LocalFeeRate)
	if err != nil {
		return revert(err)
	}
	statedb.Finalise(true)
	var blockNumber uint64
	if evm.Context.BlockNumber != nil {
		blockNumber = evm.Context.BlockNumber.Uint64()
	}
	receipt := qkctypes.NewReceipt(result.Failed(), gp.CumulativeUsed())
	receipt.GasUsed = result.UsedGas
	receipt.TxHash = txHash
	receipt.ContractAddress = result.ContractAddress
	receipt.ContractFullShardKey = tx.ToFullShardKey()
	receipt.Logs = statedb.GetLogs(txHash, blockNumber, common.Hash{}, evm.Context.Time)
	receipt.Bloom = qkctypes.CreateBloom(qkctypes.Receipts{receipt})
	if tx.To() == nil && !result.Failed() {
		return receipt, nil, result.ContractAddress.Bytes(), nil
	}
	return receipt, nil, result.Return(), nil
}

func applyQKCCrossShardSource(ctx *QKCExecutionContext, evm *vm.EVM, gp *GasPool, statedb *state.StateDB, tx *qkctypes.Transaction, sender common.Address, price, value *uint256.Int) (*qkctypes.Receipt, *qkctypes.CrossShardTransactionDeposit, []byte, error) {
	msg := &Message{From: sender, Nonce: tx.Nonce(), Value: value, GasLimit: tx.Gas(), GasPrice: price}
	st := newStateTransition(evm, msg, gp)
	statedb.SetNonce(sender, tx.Nonce()+1, tracing.NonceChangeEoACall)
	if err := st.buyGas(); err != nil {
		return nil, nil, nil, err
	}
	cost, err := IntrinsicGas(tx.Data(), nil, nil, tx.To() == nil, true, false, false, false)
	if err != nil {
		return nil, nil, nil, err
	}
	cost.RegularGas += qkcparams.GtxxShardCost.Uint64() // checked during admission
	localGasUsed := cost.RegularGas
	var deposit *qkctypes.CrossShardTransactionDeposit
	locked := evm.Context.SenderDisallowMap[sender]
	blocked := false
	if locked != nil {
		required, overflow := new(uint256.Int).AddOverflow(value, locked)
		blocked = overflow || required.Gt(statedb.GetBalance(sender))
	}
	if blocked {
		localGasUsed = tx.Gas()
	} else {
		statedb.SubBalance(sender, value, tracing.BalanceChangeTransfer)
		remoteGas := uint64(0)
		if evm.Context.Time >= ctx.QKCConfig.EnableEvmTimeStamp {
			remoteGas = tx.Gas() - cost.RegularGas
		}
		to := tx.To()
		if to == nil {
			created := vm.QKCContractAddress(sender, tx.FromFullShardKey(), tx.Nonce()+1)
			to = &created
		}
		deposit = &qkctypes.CrossShardTransactionDeposit{
			TxHash:          tx.Hash(),
			From:            account.NewAddress(sender, tx.FromFullShardKey()),
			To:              account.NewAddress(*to, tx.ToFullShardKey()),
			Value:           &serialize.Uint256{Value: tx.Value()},
			GasPrice:        &serialize.Uint256{Value: tx.GasPrice()},
			GasTokenID:      qkccommon.DefaultTokenID,
			TransferTokenID: tx.TransferTokenID(),
			GasRemained:     &serialize.Uint256{Value: new(big.Int).SetUint64(remoteGas)},
			MessageData:     tx.Data(),
			CreateContract:  tx.To() == nil,
			RefundRate:      100,
		}
	}
	feeGas := localGasUsed
	if !blocked {
		feeGas -= qkcparams.GtxxShardCost.Uint64()
		if evm.Context.Time >= ctx.QKCConfig.EnableEvmTimeStamp {
			localGasUsed = feeGas
		}
	}
	if err := gp.ReturnGas(tx.Gas()-localGasUsed, localGasUsed); err != nil {
		return nil, nil, nil, err
	}
	if !blocked && evm.Context.Time < ctx.QKCConfig.EnableEvmTimeStamp {
		refund := new(big.Int).Mul(tx.GasPrice(), new(big.Int).SetUint64(tx.Gas()-cost.RegularGas))
		refunded, overflow := uint256.FromBig(refund)
		if overflow {
			return nil, nil, nil, fmt.Errorf("QKC gas refund exceeds uint256")
		}
		statedb.AddBalance(sender, refunded, tracing.BalanceIncreaseGasReturn)
	}
	fee := new(big.Int).Mul(tx.GasPrice(), new(big.Int).SetUint64(feeGas))
	fee.Mul(fee, ctx.QKCConfig.LocalFeeRate.Num())
	fee.Quo(fee, ctx.QKCConfig.LocalFeeRate.Denom())
	localFee, overflow := uint256.FromBig(fee)
	if overflow {
		return nil, nil, nil, fmt.Errorf("QKC local fee exceeds uint256")
	}
	statedb.AddBalance(evm.Context.Coinbase, localFee, tracing.BalanceIncreaseRewardTransactionFee)
	statedb.Finalise(true)
	receipt := qkctypes.NewReceipt(blocked, gp.CumulativeUsed())
	receipt.GasUsed = localGasUsed
	receipt.TxHash = tx.Hash()
	receipt.ContractFullShardKey = tx.ToFullShardKey()
	var blockNumber uint64
	if evm.Context.BlockNumber != nil {
		blockNumber = evm.Context.BlockNumber.Uint64()
	}
	receipt.Logs = statedb.GetLogs(tx.Hash(), blockNumber, common.Hash{}, evm.Context.Time)
	receipt.Bloom = qkctypes.CreateBloom(qkctypes.Receipts{receipt})
	return receipt, deposit, nil, nil
}
