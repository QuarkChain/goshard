// Copyright 2026-2027, QuarkChain.

package core

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	corestate "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	qkcstate "github.com/ethereum/go-ethereum/qkc/state"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/holiman/uint256"
)

// ApplyTransaction is apply_transaction (messages.py:415): one transaction
// against one state, in-shard or cross-shard.
//
// The order at the head of it is not geth's and is load-bearing: the nonce is
// incremented *before* the gas is bought (messages.py:430, 460), so a
// transaction that cannot afford its gas still moves the sender's nonce in
// pyquarkchain's ordering — and the state root records it.
func ApplyTransaction(ctx *ExecutionContext, state *qkcstate.EvmState, tx *types.Transaction, txHash common.Hash, txIndex int) (success bool, output []byte, err error) {
	state.BeginMessage(txHash, txIndex)
	state.ResetRefunds()

	sender, err := TxSender(ctx, tx)
	if err != nil {
		return false, nil, err
	}
	// apply_transaction validates a second time, even when run_block already
	// did (messages.py:422). That is not redundant: __validate_tx skips
	// validate_transaction for a future-nonce transaction, so this is the only
	// check some transactions get.
	if err := ValidateTransaction(ctx, state, tx, sender); err != nil {
		return false, nil, err
	}

	// Everything from here on writes. A profile boundary can only be recognised
	// once it has been hit, which is after the nonce moved and the gas was
	// bought, so unwind to here when one is — the block is being abandoned
	// either way, and a caller that keeps the state should not have to reason
	// about how far a refused transaction got.
	entry := state.Snapshot()
	defer func() {
		if isProfileBoundary(err) {
			state.RevertToSnapshot(entry)
		}
	}()

	state.SetFullShardKey(tx.ToFullShardKey())

	intrinsic := IntrinsicGas(tx)
	state.IncrementNonce(sender)

	// The message is always priced in the genesis token, whatever the sender
	// pays in: a foreign gas token is sold to the general native token manager
	// first, and the manager's own genesis token is what funds the frame.
	gasToken := state.GenesisTokenID()
	gasPrice := tx.GasPrice()
	refundRate := uint8(defaultRefundRate)
	var managerSuicides []common.Address
	if gasPrice.Sign() != 0 && tx.GasTokenID() != gasToken {
		rate, converted, marks, err := payNativeTokenAsGas(ctx, state, tx.GasTokenID(), tx.Gas(), tx.GasPrice())
		if err != nil {
			return false, nil, err
		}
		managerSuicides = marks
		// Validation already took this quote and refused the transaction on a
		// zero price, so a zero here means the two runs disagreed.
		if converted.Sign() == 0 {
			return false, nil, fmt.Errorf("%w: gas token %d priced at zero after validation",
				ErrInvalidNativeToken, tx.GasTokenID())
		}
		if rate > 100 {
			return false, nil, fmt.Errorf("%w: refund rate %d is above 100", ErrInvalidNativeToken, rate)
		}
		refundRate = uint8(rate)
		gasPrice = converted

		// The manager hands over the genesis token the frame will spend and
		// keeps the native token the sender is about to pay.
		reserve, overflow := uint256.FromBig(new(big.Int).Mul(converted, new(big.Int).SetUint64(tx.Gas())))
		if overflow {
			return false, nil, fmt.Errorf("%w: converted gas cost overflows 256 bits", ErrInvalidTransaction)
		}
		if !state.DeductValue(generalNativeTokenContractAddress, gasToken, reserve) {
			return false, nil, fmt.Errorf("%w: gas reserve does not cover %s", ErrInvalidNativeToken, reserve)
		}
		state.DeltaTokenBalance(generalNativeTokenContractAddress, tx.GasTokenID(),
			new(big.Int).Mul(tx.GasPrice(), new(big.Int).SetUint64(tx.Gas())))
	}

	// Buying the gas up front, in whatever token the sender chose. Validation
	// guarantees the balance covers it, so a shortfall here is a bug rather
	// than a rejected transaction.
	cost, overflow := uint256.FromBig(new(big.Int).Mul(tx.GasPrice(), new(big.Int).SetUint64(tx.Gas())))
	if overflow {
		return false, nil, fmt.Errorf("%w: gas cost overflows 256 bits", ErrInvalidTransaction)
	}
	if !state.DeductValue(sender, tx.GasTokenID(), cost) {
		return false, nil, fmt.Errorf("%w: cannot buy %s gas", ErrInsufficientBalance, cost)
	}

	evm, err := newEVM(ctx, state, sender, gasPrice, txHash, tx.FromFullShardKey(), tx.ToFullShardKey())
	if err != nil {
		return false, nil, err
	}
	evm.QKCAdoptSuicides(managerSuicides)

	value, overflow := uint256.FromBig(tx.Value())
	if overflow {
		return false, nil, fmt.Errorf("%w: value overflows 256 bits", ErrInvalidTransaction)
	}
	msg := &messageParams{
		evm:            evm,
		sender:         sender,
		to:             tx.To(),
		value:          value,
		gas:            tx.Gas() - intrinsic,
		data:           tx.Data(),
		gasPrice:       gasPrice,
		gasToken:       gasToken,
		transferToken:  tx.TransferTokenID(),
		toFullShardKey: tx.ToFullShardKey(),
		refundRate:     refundRate,
		gasUsedStart:   intrinsic,
		createContract: tx.To() == nil,
	}

	if !tx.IsCrossShard() {
		return applyTransactionMessage(state, msg)
	}
	return applyCrossShardSource(ctx, state, evm, tx, sender, txHash, intrinsic, value, gasPrice, refundRate)
}

// messageParams is apply_transaction_message's argument list (messages.py:280),
// which both the in-shard path and the cross-shard target path share.
type messageParams struct {
	evm    *vm.EVM
	sender account.Recipient
	to     *account.Recipient
	value  *uint256.Int
	gas    uint64
	data   []byte

	gasPrice       *big.Int
	gasToken       uint64
	transferToken  uint64
	toFullShardKey uint32

	// createContract selects create_contract over apply_msg, and
	// contractAddress pins the address when the source shard already derived
	// it for a cross-shard deployment.
	createContract  bool
	contractAddress *common.Address

	// gasUsedStart is what the transaction has already spent before the EVM
	// starts: the intrinsic gas in-shard, the cross-shard surcharge at a
	// target.
	gasUsedStart uint64
	refundRate   uint8

	// isCrossShard routes the receipt to the deposit list instead of the
	// transaction list, and gates it on the EVM switch.
	isCrossShard bool
}

// applyTransactionMessage is apply_transaction_message (messages.py:280): run
// the message, settle the refund and the fee, apply the suicides, and produce
// the receipt.
func applyTransactionMessage(state *qkcstate.EvmState, p *messageParams) (bool, []byte, error) {
	evmGasStart := p.gas
	var (
		ret     []byte
		left    vm.GasBudget
		vmErr   error
		created common.Address
	)
	if p.createContract {
		ret, created, left, vmErr = p.evm.QKCCreateContract(p.sender, p.data, vm.NewGasBudget(p.gas),
			p.value, p.transferToken, p.toFullShardKey, p.contractAddress)
	} else {
		ret, left, vmErr = p.evm.QKCApplyMessage(p.sender, *p.to, p.data, vm.NewGasBudget(p.gas),
			p.value, p.transferToken, p.toFullShardKey)
	}
	if err := profileError(p.evm, state, vmErr); err != nil {
		return false, nil, err
	}

	gasRemained := left.RegularGas
	gasUsed := evmGasStart - gasRemained + p.gasUsedStart

	var (
		output          []byte
		success         bool
		contractAddress common.Address
	)
	if vmErr != nil {
		// A failed message produces no output and keeps no contract, whatever
		// the frame returned.
		output, success = nil, false
	} else {
		success = true
		// The refund counter already holds the self-destruct refunds: the
		// interpreter adds them at the opcode, deduplicated by address, which
		// is what len(set(state.suicides)) * GSUICIDEREFUND comes to.
		if refunds := state.Refunds(); refunds > 0 {
			capped := min(refunds, gasUsed/2)
			gasRemained += capped
			gasUsed -= capped
			state.ResetRefunds()
		}
		if p.createContract {
			// create_contract answers with the address, and that answer is
			// both the transaction's output and the receipt's contract field.
			contractAddress = created
			output = created.Bytes()
		} else {
			output = ret
		}
	}

	// Whatever is left is bought back at the price it was sold for.
	refundGas(state, p.sender, p.gasToken, new(big.Int).Mul(p.gasPrice, new(big.Int).SetUint64(gasRemained)), p.refundRate)

	// The shard keeps only its share of the fee; the rest is the root chain's
	// tax and is credited to nobody (messages.py:336).
	fee := localFee(state, p.gasPrice, gasUsed)
	creditFee(state, p.gasToken, fee)

	state.AddGasUsed(gasUsed)

	// The suicides collected during the message are applied now, after the
	// refund has been settled (messages.py:347). del_account only strips the
	// account; whether its leaf survives is decided by the end-of-block
	// emptiness sweep, so an account paid again later in the block stays.
	for _, addr := range p.evm.QKCSuicides() {
		state.DelAccount(addr)
	}

	receipt := makeReceipt(state, success, contractAddress)
	if p.isCrossShard {
		// A deposit receipt only exists once the EVM is switched on: before
		// that the target never ran one (messages.py:356).
		if state.Timestamp() >= state.QKCConfig().EnableEvmTimeStamp {
			state.AddXShardDepositReceipt(receipt)
		}
	} else {
		state.AddReceipt(receipt)
	}
	return success, output, nil
}

// applyCrossShardSource is the is_cross_shard branch of apply_transaction
// (messages.py:487). Nothing executes here: the value leaves the shard, a
// deposit is queued for the target, and the receipt records only that.
func applyCrossShardSource(ctx *ExecutionContext, state *qkcstate.EvmState, evm *vm.EVM, tx *types.Transaction, sender account.Recipient, txHash common.Hash, intrinsic uint64, value *uint256.Int, gasPrice *big.Int, refundRate uint8) (bool, []byte, error) {
	gasToken := state.GenesisTokenID()
	localGasUsed := intrinsic
	remoteGasReserved := uint64(0)
	success := false
	evmEnabled := state.Timestamp() >= ctx.QKCConfig.EnableEvmTimeStamp

	switch {
	case evm.QKCPOSWDisallows(sender, value):
		// A barred sender loses the whole allowance and sends nothing.
		localGasUsed = tx.Gas()

	case tx.To() == nil:
		// A cross-shard deployment. The address is derived here, on the source
		// shard, from the sender's nonce as it now stands; the code is
		// deployed on the target. Note the gas reservation is unconditional,
		// unlike the plain transfer below.
		if !state.DeductValue(sender, tx.TransferTokenID(), value) {
			return false, nil, fmt.Errorf("%w: cannot move %s cross-shard", ErrInsufficientBalance, value)
		}
		remoteGasReserved = tx.Gas() - intrinsic
		contract := vm.QKCContractAddress(sender, tx.FromFullShardKey(), state.GetNonce(sender))
		state.AddXShardDeposit(newDeposit(txHash, sender, tx.FromFullShardKey(), contract, tx.ToFullShardKey(),
			value, gasPrice, gasToken, tx.TransferTokenID(), tx.Data(), true, remoteGasReserved, refundRate))
		success = true

	default:
		if !state.DeductValue(sender, tx.TransferTokenID(), value) {
			return false, nil, fmt.Errorf("%w: cannot move %s cross-shard", ErrInsufficientBalance, value)
		}
		// Before the EVM was switched on, the target could not execute
		// anything, so nothing was reserved for it (messages.py:521).
		if evmEnabled {
			remoteGasReserved = tx.Gas() - intrinsic
		}
		to := *tx.To()
		state.AddXShardDeposit(newDeposit(txHash, sender, tx.FromFullShardKey(), to, tx.ToFullShardKey(),
			value, gasPrice, gasToken, tx.TransferTokenID(), tx.Data(), false, remoteGasReserved, refundRate))
		success = true
	}

	gasRemained := tx.Gas() - localGasUsed - remoteGasReserved
	refundGas(state, sender, gasToken, new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gasRemained)), refundRate)

	// The cross-shard surcharge is not the source shard's to keep: it pays the
	// target shard's miner for running the deposit, so it comes out of the fee
	// here and out of gas_used below.
	feeGas := localGasUsed
	if success {
		feeGas -= GTXXShardCost
	}
	creditFee(state, gasToken, localFee(state, gasPrice, feeGas))

	state.AddGasUsed(localGasUsed)
	if evmEnabled && success {
		state.SubGasUsed(GTXXShardCost)
	}

	state.AddReceipt(makeReceipt(state, success, common.Address{}))
	return success, nil, nil
}

// ApplyXShardDeposit is apply_xshard_deposit (messages.py:368): the target
// shard's half of a cross-shard transaction, once the EVM is switched on.
//
// The two-step credit is deliberate and must not be shortened. The value is
// first given to the *sender's* account on this shard and only then moved to
// the recipient by a real message, so that a failed message leaves the funds
// with the sender here rather than returning them to the source shard.
func ApplyXShardDeposit(ctx *ExecutionContext, state *qkcstate.EvmState, deposit *types.CrossShardTransactionDeposit, gasUsedStart uint64, index int) (bool, []byte, error) {
	// The rate rides on the deposit as a byte, so a source shard could hand over
	// one above 100 that the source side would have refused (see
	// ApplyTransaction). Upstream has no check at either end: _refund would give
	// the sender more than it paid and burn a negative amount, which the account
	// layer neither rejects nor represents. Refusing the block is the same
	// boundary the gas underflow gets — the general native token manager cannot
	// produce such a rate, so no deposit carrying one exists to replay.
	if deposit.RefundRate > 100 {
		return false, nil, fmt.Errorf("%w: deposit refund rate %d is above 100",
			ErrInvalidNativeToken, deposit.RefundRate)
	}
	state.BeginMessage(deposit.TxHash, index)
	state.ResetRefunds()
	state.SetFullShardKey(deposit.To.FullShardKey)

	from, to := deposit.From.Recipient, deposit.To.Recipient
	value := depositUint256(deposit.Value)
	state.DeltaTokenBalance(from, deposit.TransferTokenID, value.ToBig())

	gasPrice := depositUint256(deposit.GasPrice).ToBig()
	evm, err := newEVM(ctx, state, from, gasPrice, deposit.TxHash, deposit.From.FullShardKey, deposit.To.FullShardKey)
	if err != nil {
		return false, nil, err
	}

	p := &messageParams{
		evm:            evm,
		sender:         from,
		to:             &to,
		value:          value,
		gas:            depositUint256(deposit.GasRemained).Uint64(),
		data:           deposit.MessageData,
		gasPrice:       gasPrice,
		gasToken:       deposit.GasTokenID,
		transferToken:  deposit.TransferTokenID,
		toFullShardKey: deposit.To.FullShardKey,
		createContract: deposit.CreateContract,
		gasUsedStart:   gasUsedStart,
		refundRate:     deposit.RefundRate,
		isCrossShard:   true,
	}
	if deposit.CreateContract {
		p.contractAddress = &to
	}
	return applyTransactionMessage(state, p)
}

// profileError separates the two kinds of failure a message can report. One
// that merely failed is a consensus outcome and shows up as a zero-status
// receipt; one that left the supported profile has to reach the caller and
// abandon the block.
//
// Neither of the two it looks for arrives as the message's own error, because
// neither is something a frame can report: one is a write the account layer
// could not represent, the other a frame that ran past the point pyquarkchain
// would have stopped. Both are sticky and both are read here, where the block
// can still be abandoned — left alone the first would surface only at Commit,
// as a block that mysteriously produces no root, and the second not at all.
// isProfileBoundary reports whether err is one of the two refusals that mean
// "this fork cannot answer" rather than "the transaction failed".
func isProfileBoundary(err error) bool {
	return errors.Is(err, ErrUnsupportedNativeToken) || errors.Is(err, ErrGasUnderflow)
}

func profileError(evm *vm.EVM, state *qkcstate.EvmState, err error) error {
	if stateErr := state.Error(); errors.Is(stateErr, corestate.ErrTokenTrieUnsupported) {
		return fmt.Errorf("%w: %v", ErrUnsupportedNativeToken, stateErr)
	}
	if evm.QKCGasUnderflow() {
		return ErrGasUnderflow
	}
	return nil
}

// refundGas is _refund (messages.py:268): the sender gets its share back and
// the remainder is burned to the zero address. Inside this profile the rate is
// always 100, so nothing is ever burned — the burn is kept because the rate is
// carried on the deposit and a future profile will use it.
func refundGas(state *qkcstate.EvmState, sender account.Recipient, tokenID uint64, total *big.Int, refundRate uint8) {
	rate := big.NewInt(int64(refundRate))
	toRefund := new(big.Int).Div(new(big.Int).Mul(total, rate), big.NewInt(100))
	toBurn := new(big.Int).Sub(total, toRefund)
	state.DeltaTokenBalance(sender, tokenID, toRefund)
	if toBurn.Sign() != 0 {
		state.DeltaTokenBalance(account.Recipient{}, tokenID, toBurn)
	}
}

// localFee is the shard's share of a fee: gasprice × gas × (1 - reward tax
// rate), truncated (messages.py:336). The truncated remainder is the root
// chain's tax and is credited to no account at all.
func localFee(state *qkcstate.EvmState, gasPrice *big.Int, gas uint64) *big.Int {
	rate := state.LocalFeeRate()
	fee := new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gas))
	fee.Mul(fee, rate.Num())
	return fee.Div(fee, rate.Denom())
}

// creditFee pays the block's coinbase and records the same amount in
// block_fee_tokens, which the block's coinbase map is checked against.
func creditFee(state *qkcstate.EvmState, tokenID uint64, fee *big.Int) {
	state.DeltaTokenBalance(state.BlockCoinbase(), tokenID, fee)
	amount, overflow := uint256.FromBig(fee)
	if overflow {
		state.SetError(fmt.Errorf("block fee %s overflows 256 bits", fee))
		return
	}
	state.AddBlockFee(tokenID, amount)
}

// makeReceipt is mk_receipt (messages.py:107). The cumulative gas is the
// block's running total at this point, and the shard key is the state's — the
// destination key of the transaction just applied.
func makeReceipt(state *qkcstate.EvmState, success bool, contractAddress common.Address) *types.Receipt {
	logs := state.MessageLogs()
	receipt := &types.Receipt{
		CumulativeGasUsed:    state.GasUsed(),
		Logs:                 logs,
		ContractAddress:      contractAddress,
		ContractFullShardKey: state.FullShardKey(),
	}
	if success {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	return receipt
}

// newDeposit assembles the CrossShardTransactionDeposit the source shard hands
// to the target.
func newDeposit(txHash common.Hash, from account.Recipient, fromKey uint32, to account.Recipient, toKey uint32,
	value *uint256.Int, gasPrice *big.Int, gasToken, transferToken uint64, data []byte, createContract bool, gasRemained uint64,
	refundRate uint8,
) *types.CrossShardTransactionDeposit {
	return &types.CrossShardTransactionDeposit{
		TxHash:          txHash,
		From:            account.NewAddress(from, fromKey),
		To:              account.NewAddress(to, toKey),
		Value:           &serialize.Uint256{Value: value.ToBig()},
		GasPrice:        &serialize.Uint256{Value: new(big.Int).Set(gasPrice)},
		GasTokenID:      gasToken,
		TransferTokenID: transferToken,
		GasRemained:     &serialize.Uint256{Value: new(big.Int).SetUint64(gasRemained)},
		MessageData:     data,
		CreateContract:  createContract,
		RefundRate:      refundRate,
	}
}

func depositUint256(v *serialize.Uint256) *uint256.Int {
	if v == nil || v.Value == nil {
		return new(uint256.Int)
	}
	out, _ := uint256.FromBig(v.Value)
	return out
}
