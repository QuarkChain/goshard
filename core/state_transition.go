// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"bytes"
	"fmt"
	"math"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/holiman/uint256"
)

// ExecutionResult includes all output after executing given evm
// message no matter the execution itself is successful or not.
type ExecutionResult struct {
	UsedGas    uint64 // Total used gas, refunded gas is deducted
	MaxUsedGas uint64 // Maximum gas consumed during execution, excluding gas refunds.
	Err        error  // Any error encountered during the execution(listed in core/vm/errors.go)
	ReturnData []byte // Returned data from evm(function result or data supplied with revert opcode)
}

// Unwrap returns the internal evm error which allows us for further
// analysis outside.
func (result *ExecutionResult) Unwrap() error {
	return result.Err
}

// Failed returns the indicator whether the execution is successful or not
func (result *ExecutionResult) Failed() bool { return result.Err != nil }

// Return is a helper function to help caller distinguish between revert reason
// and function return. Return returns the data after execution if no error occurs.
func (result *ExecutionResult) Return() []byte {
	if result.Err != nil {
		return nil
	}
	return common.CopyBytes(result.ReturnData)
}

// Revert returns the concrete revert reason if the execution is aborted by `REVERT`
// opcode. Note the reason can be nil if no data supplied with revert opcode.
func (result *ExecutionResult) Revert() []byte {
	if result.Err != vm.ErrExecutionReverted {
		return nil
	}
	return common.CopyBytes(result.ReturnData)
}

// IntrinsicGas computes the 'intrinsic gas' for a message with the given data.
func IntrinsicGas(data []byte, accessList types.AccessList, authList []types.SetCodeAuthorization, isContractCreation, isHomestead, isEIP2028, isEIP3860, isAmsterdam bool) (vm.GasCosts, error) {
	// Set the starting gas for the raw transaction
	var gas uint64
	if isContractCreation && isHomestead {
		gas = params.TxGasContractCreation
	} else {
		gas = params.TxGas
	}
	dataLen := uint64(len(data))
	// Bump the required gas by the amount of transactional data
	if dataLen > 0 {
		// Zero and non-zero bytes are priced differently
		z := uint64(bytes.Count(data, []byte{0}))
		nz := dataLen - z

		// Make sure we don't exceed uint64 for all data combinations
		nonZeroGas := params.TxDataNonZeroGasFrontier
		if isEIP2028 {
			nonZeroGas = params.TxDataNonZeroGasEIP2028
		}
		if (math.MaxUint64-gas)/nonZeroGas < nz {
			return vm.GasCosts{}, ErrGasUintOverflow
		}
		gas += nz * nonZeroGas

		if (math.MaxUint64-gas)/params.TxDataZeroGas < z {
			return vm.GasCosts{}, ErrGasUintOverflow
		}
		gas += z * params.TxDataZeroGas

		if isContractCreation && isEIP3860 {
			lenWords := toWordSize(dataLen)
			if (math.MaxUint64-gas)/params.InitCodeWordGas < lenWords {
				return vm.GasCosts{}, ErrGasUintOverflow
			}
			gas += lenWords * params.InitCodeWordGas
		}
	}
	if accessList != nil {
		addresses := uint64(len(accessList))
		storageKeys := uint64(accessList.StorageKeys())
		if (math.MaxUint64-gas)/params.TxAccessListAddressGas < addresses {
			return vm.GasCosts{}, ErrGasUintOverflow
		}
		gas += addresses * params.TxAccessListAddressGas
		if (math.MaxUint64-gas)/params.TxAccessListStorageKeyGas < storageKeys {
			return vm.GasCosts{}, ErrGasUintOverflow
		}
		gas += storageKeys * params.TxAccessListStorageKeyGas

		// EIP-7981: access list data is charged in addition to the base charge.
		if isAmsterdam {
			const (
				addressCost    = common.AddressLength * params.TxCostFloorPerToken7976 * params.TxTokenPerNonZeroByte
				storageKeyCost = common.HashLength * params.TxCostFloorPerToken7976 * params.TxTokenPerNonZeroByte
			)
			if (math.MaxUint64-gas)/addressCost < addresses {
				return vm.GasCosts{}, ErrGasUintOverflow
			}
			gas += addresses * addressCost
			if (math.MaxUint64-gas)/storageKeyCost < storageKeys {
				return vm.GasCosts{}, ErrGasUintOverflow
			}
			gas += storageKeys * storageKeyCost
		}
	}
	if authList != nil {
		gas += uint64(len(authList)) * params.CallNewAccountGas
	}
	return vm.GasCosts{RegularGas: gas}, nil
}

// FloorDataGas computes the minimum gas required for a transaction based on its data tokens (EIP-7623).
func FloorDataGas(rules params.Rules, data []byte, accessList types.AccessList) (uint64, error) {
	var (
		tokens    uint64
		tokenCost uint64
	)
	if rules.IsAmsterdam {
		// EIP-7976 changes how calldata is priced.
		// From 10/40 to 64/64 for zero/non-zero bytes.
		tokenCost = params.TxCostFloorPerToken7976
		dataLen := uint64(len(data))
		if math.MaxUint64/params.TxTokenPerNonZeroByte < dataLen {
			return 0, ErrGasUintOverflow
		}
		tokens = dataLen * params.TxTokenPerNonZeroByte

		// EIP-7981 adds additional tokens for every entry in the accesslist
		const addressTokenCost = uint64(common.AddressLength) * params.TxTokenPerNonZeroByte
		addresses := uint64(len(accessList))
		if (math.MaxUint64-tokens)/addressTokenCost < addresses {
			return 0, ErrGasUintOverflow
		}
		tokens += addresses * addressTokenCost

		const storageKeyTokenCost = uint64(common.HashLength) * params.TxTokenPerNonZeroByte
		storageKeys := uint64(accessList.StorageKeys())
		if (math.MaxUint64-tokens)/storageKeyTokenCost < storageKeys {
			return 0, ErrGasUintOverflow
		}
		tokens += storageKeys * storageKeyTokenCost
	} else {
		var (
			z  = uint64(bytes.Count(data, []byte{0}))
			nz = uint64(len(data)) - z
		)
		// Pre-Amsterdam
		if math.MaxUint64/params.TxTokenPerNonZeroByte < nz {
			return 0, ErrGasUintOverflow
		}
		tokens = nz * params.TxTokenPerNonZeroByte
		if math.MaxUint64-tokens < z {
			return 0, ErrGasUintOverflow
		}
		tokens += z
		tokenCost = params.TxCostFloorPerToken
	}

	// Check for overflow
	if (math.MaxUint64-params.TxGas)/tokenCost < tokens {
		return 0, ErrGasUintOverflow
	}
	// Minimum gas required for a transaction based on its data tokens (EIP-7623).
	return params.TxGas + tokens*tokenCost, nil
}

// toWordSize returns the ceiled word size required for init code payment calculation.
func toWordSize(size uint64) uint64 {
	if size > math.MaxUint64-31 {
		return math.MaxUint64/32 + 1
	}

	return (size + 31) / 32
}

// A Message contains the data derived from a single transaction that is relevant to state
// processing.
type Message struct {
	To                    *common.Address
	From                  common.Address
	Nonce                 uint64
	Value                 *uint256.Int
	GasLimit              uint64
	GasPrice              *uint256.Int
	GasFeeCap             *uint256.Int
	GasTipCap             *uint256.Int
	Data                  []byte
	AccessList            types.AccessList
	BlobGasFeeCap         *uint256.Int
	BlobHashes            []common.Hash
	SetCodeAuthorizations []types.SetCodeAuthorization

	// Token IDs are explicit consensus values. Zero identifies token "0"; it
	// does not mean unspecified. QKC-only execution fixes the default token to
	// qkccommon.DefaultTokenID and does not support configured non-QKC defaults.
	GasTokenID      uint64
	TransferTokenID uint64
	ToFullShardKey  *uint32

	// When SkipNonceChecks is true, the message nonce is not checked against the
	// account nonce in state.
	//
	// This field will be set to true for operations like RPC eth_call
	// or the state prefetching.
	SkipNonceChecks bool

	// When set, the message is not treated as a transaction, and certain
	// transaction-specific checks are skipped:
	//
	// - From is not verified to be an EOA
	// - GasLimit is not checked against the protocol defined tx gaslimit
	SkipTransactionChecks bool
}

// TransactionToMessage converts a transaction into a Message.
func TransactionToMessage(tx *types.Transaction, s types.Signer, baseFee *big.Int) (*Message, error) {
	from, err := types.Sender(s, tx)
	if err != nil {
		return nil, err
	}
	gasPrice, overflow := uint256.FromBig(tx.GasPrice())
	if overflow {
		return nil, fmt.Errorf("%w: address %v, maxFeePerGas bit length: %d", ErrFeeCapVeryHigh,
			from.Hex(), tx.GasPrice().BitLen())
	}
	txGasFeeCap := tx.GasFeeCap()
	gasFeeCap, overflow := uint256.FromBig(txGasFeeCap)
	if overflow {
		return nil, fmt.Errorf("%w: address %v, maxFeePerGas bit length: %d", ErrFeeCapVeryHigh,
			from.Hex(), tx.GasFeeCap().BitLen())
	}
	txGasTipCap := tx.GasTipCap()
	gasTipCap, overflow := uint256.FromBig(txGasTipCap)
	if overflow {
		return nil, fmt.Errorf("%w: address %v, maxPriorityFeePerGas bit length: %d", ErrTipVeryHigh,
			from.Hex(), tx.GasTipCap().BitLen())
	}
	value, overflow := uint256.FromBig(tx.Value())
	if overflow {
		return nil, fmt.Errorf("value exceeds 256 bits: address %v", from.Hex())
	}
	blobGasFeeCap, overflow := uint256.FromBig(tx.BlobGasFeeCap())
	if overflow {
		return nil, fmt.Errorf("blobGasFeeCap exceeds 256 bits: address %v", from.Hex())
	}

	msg := &Message{
		From:                  from,
		Nonce:                 tx.Nonce(),
		GasLimit:              tx.Gas(),
		GasPrice:              gasPrice,
		GasFeeCap:             gasFeeCap,
		GasTipCap:             gasTipCap,
		To:                    tx.To(),
		Value:                 value,
		Data:                  tx.Data(),
		AccessList:            tx.AccessList(),
		SetCodeAuthorizations: tx.SetCodeAuthorizations(),
		SkipNonceChecks:       false,
		SkipTransactionChecks: false,
		BlobHashes:            tx.BlobHashes(),
		BlobGasFeeCap:         blobGasFeeCap,
		GasTokenID:            qkccommon.DefaultTokenID,
		TransferTokenID:       qkccommon.DefaultTokenID,
	}
	// If baseFee provided, set gasPrice to effectiveGasPrice.
	if baseFee != nil {
		effectiveGasPrice := new(big.Int).Add(baseFee, txGasTipCap)
		if effectiveGasPrice.Cmp(txGasFeeCap) > 0 {
			effectiveGasPrice = txGasFeeCap
		}
		// EffectiveGasPrice is already capped by txGasFeeCap, therefore
		// the overflow check is not required.
		msg.GasPrice = uint256.MustFromBig(effectiveGasPrice)
	}
	return msg, nil
}

// ApplyMessage computes the new state by applying the given message
// against the old state within the environment.
//
// ApplyMessage returns the bytes returned by any EVM execution (if it took place),
// the gas used (which includes gas refunds) and an error if it failed. An error always
// indicates a core error meaning that the message would always fail for that particular
// state and would never be accepted within a block.
func ApplyMessage(evm *vm.EVM, msg *Message, gp *GasPool) (*ExecutionResult, error) {
	// Do not panic if the gas pool is nil. This is allowed when executing
	// a single message via RPC invocation.
	if gp == nil {
		gp = NewGasPool(msg.GasLimit)
	}
	stateSnapshot := evm.StateDB.Snapshot()
	gasPoolSnapshot := gp.Snapshot()
	previousExecutionFullShardKey := evm.QKCExecutionFullShardKey()
	previousTxContext := evm.TxContext
	evm.SetTxContext(NewEVMTxContext(msg))
	result, err := newStateTransition(evm, msg, gp).execute()
	if err != nil {
		evm.StateDB.RevertToSnapshot(stateSnapshot)
		gp.Set(gasPoolSnapshot)
		evm.SetQKCExecutionFullShardKey(previousExecutionFullShardKey)
		evm.SetTxContext(previousTxContext)
		return nil, err
	}
	return result, nil
}

// stateTransition represents a state transition.
//
// == The State Transitioning Model
//
// A state transition is a change made when a transaction is applied to the current world
// state. The state transitioning model does all the necessary work to work out a valid new
// state root.
//
//  1. Nonce handling
//  2. Pre pay gas
//  3. Create a new state object if the recipient is nil
//  4. Value transfer
//
// == If contract creation ==
//
//	4a. Attempt to run transaction data
//	4b. If valid, use result as code for the new state object
//
// == end ==
//
//  5. Run Script section
//  6. Derive new state root
type stateTransition struct {
	gp                *GasPool
	msg               *Message
	initialBudget     vm.GasBudget
	gasRemaining      vm.GasBudget
	state             vm.StateDB
	evm               *vm.EVM
	intrinsicGas      vm.GasCosts
	convertedGasPrice *uint256.Int
	refundRate        *big.Int
}

// newStateTransition initialises and returns a new state transition object.
func newStateTransition(evm *vm.EVM, msg *Message, gp *GasPool) *stateTransition {
	return &stateTransition{
		gp:                gp,
		evm:               evm,
		msg:               msg,
		state:             evm.StateDB,
		convertedGasPrice: new(uint256.Int).Set(msg.GasPrice),
		refundRate:        big.NewInt(100),
	}
}

// to returns the recipient of the message.
func (st *stateTransition) to() common.Address {
	if st.msg == nil || st.msg.To == nil /* contract creation */ {
		return common.Address{}
	}
	return *st.msg.To
}

func (st *stateTransition) buyGas() error {
	msg := st.msg
	defaultTokenID := qkccommon.DefaultTokenID
	if msg.GasTokenID != defaultTokenID && !msg.GasPrice.IsZero() {
		actualRefundRate, actualPrice, err := st.payAsGas(st.evm)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrQKCGasConversion, err)
		}
		if actualPrice.IsZero() {
			return fmt.Errorf("%w: zero converted gas price", ErrQKCGasConversion)
		}
		manager := vm.GeneralNativeTokenManagerAddress()
		reserveCost, overflow := mulGasPrice(msg.GasLimit, actualPrice)
		if overflow || st.state.GetBalanceByTokenID(manager, defaultTokenID).Cmp(reserveCost) < 0 {
			return fmt.Errorf("%w: insufficient manager reserve", ErrQKCGasConversion)
		}
		originalCost, _ := mulGasPrice(msg.GasLimit, msg.GasPrice)
		if !st.canCredit(manager, originalCost, msg.GasTokenID) {
			return fmt.Errorf("%w: manager token balance overflow", ErrQKCGasConversion)
		}
		st.refundRate = actualRefundRate
		st.convertedGasPrice = actualPrice
		st.state.SubBalanceByTokenID(manager, reserveCost, defaultTokenID, tracing.BalanceChangeUnspecified)
		st.state.AddBalanceByTokenID(manager, originalCost, msg.GasTokenID, tracing.BalanceChangeUnspecified)
	}
	originalCost, overflow := mulGasPrice(msg.GasLimit, msg.GasPrice)
	if overflow {
		return ErrInsufficientFunds
	}
	st.state.SubBalanceByTokenID(msg.From, originalCost, msg.GasTokenID, tracing.BalanceDecreaseGasBuy)
	if err := st.gp.SubGas(msg.GasLimit); err != nil {
		return err
	}
	st.gasRemaining = vm.NewGasBudget(msg.GasLimit)
	st.initialBudget = st.gasRemaining.Copy()
	st.evm.SetTxContext(vm.TxContext{
		Origin:          msg.From,
		GasPrice:        new(uint256.Int).Set(st.convertedGasPrice),
		GasTokenID:      defaultTokenID,
		TransferTokenID: msg.TransferTokenID,
		ToFullShardKey:  cloneFullShardKey(msg.ToFullShardKey),
	})
	return nil
}

func (st *stateTransition) preCheck() error {
	msg := st.msg
	if !msg.SkipNonceChecks {
		stateNonce := st.state.GetNonce(msg.From)
		if stateNonce < msg.Nonce {
			return fmt.Errorf("%w: address %v, tx: %d state: %d", ErrNonceTooHigh, msg.From.Hex(), msg.Nonce, stateNonce)
		}
		if stateNonce > msg.Nonce {
			return fmt.Errorf("%w: address %v, tx: %d state: %d", ErrNonceTooLow, msg.From.Hex(), msg.Nonce, stateNonce)
		}
		if stateNonce+1 < stateNonce {
			return fmt.Errorf("%w: address %v, nonce: %d", ErrNonceMax, msg.From.Hex(), stateNonce)
		}
	}

	var err error
	st.intrinsicGas, err = IntrinsicGas(msg.Data, nil, nil, msg.To == nil, true, false, false, false)
	if err != nil {
		return err
	}
	if msg.GasLimit < st.intrinsicGas.RegularGas {
		return fmt.Errorf("%w: have %d, want %d", ErrIntrinsicGas, msg.GasLimit, st.intrinsicGas.RegularGas)
	}
	if msg.GasPrice.BitLen() > 128 || msg.GasTokenID > qkccommon.TOKENIDMAX || msg.TransferTokenID > qkccommon.TOKENIDMAX {
		return ErrQKCInvalidToken
	}
	defaultTokenID := qkccommon.DefaultTokenID
	if err := st.validateBalances(defaultTokenID); err != nil {
		return err
	}
	if st.gp.Gas() < msg.GasLimit {
		return ErrGasLimitReached
	}
	return st.validateGasConversion(defaultTokenID)
}

// execute will transition the state by applying the current message and
// returning the evm execution result with following fields.
//
//   - used gas: total gas used (including gas being refunded)
//   - returndata: the returned data from evm
//   - concrete execution error: various EVM errors which abort the execution, e.g.
//     ErrOutOfGas, ErrExecutionReverted
//
// However if any consensus issue encountered, return the error directly with
// nil evm execution result.
func (st *stateTransition) execute() (*ExecutionResult, error) {
	// First check this message satisfies all consensus rules before
	// applying the message. The rules include these clauses
	//
	// 1. the nonce of the message caller is correct
	// 2. caller has enough token balance to cover the gas fee
	// 3. the amount of gas required is available in the block
	// 4. the purchased gas is enough to cover intrinsic usage
	// 5. there is no overflow when calculating intrinsic gas
	// 6. caller has enough balance to cover asset transfer for **topmost** call

	// Validate all clauses without mutating state. Gas purchase is committed below.
	if err := st.preCheck(); err != nil {
		return nil, err
	}
	var (
		msg              = st.msg
		rules            = st.evm.ChainConfig().Rules(st.evm.Context.BlockNumber, st.evm.Context.Random != nil, st.evm.Context.Time)
		contractCreation = msg.To == nil
	)
	if msg.ToFullShardKey != nil {
		st.state.SetFullShardKey(*msg.ToFullShardKey)
	}
	// Presence is consensus-significant: a missing shard key must not become
	// chain 0 through StateDB's uint32 zero value or a previous message.
	st.evm.SetQKCExecutionFullShardKey(msg.ToFullShardKey)
	// A normal EVM CREATE increments the creator nonce itself. Calls still need
	// the transaction-level increment performed by pyquarkchain.
	if !contractCreation {
		st.state.SetNonce(msg.From, st.state.GetNonce(msg.From)+1, tracing.NonceChangeEoACall)
	}
	if err := st.buyGas(); err != nil {
		return nil, err
	}

	prior, sufficient := st.gasRemaining.Charge(st.intrinsicGas)
	if !sufficient {
		return nil, fmt.Errorf("%w: have %d, want %d", ErrIntrinsicGas, st.gasRemaining.RegularGas, st.intrinsicGas.RegularGas)
	}
	if tracer := st.evm.Config.Tracer; tracer != nil && tracer.OnGasChange != nil {
		tracer.OnGasChange(prior, st.gasRemaining.RegularGas, tracing.GasChangeTxIntrinsicGas)
	}
	st.state.Prepare(rules, msg.From, st.evm.Context.Coinbase, msg.To, vm.ActivePrecompiles(rules), nil)

	value := msg.Value
	if value == nil {
		value = new(uint256.Int)
	}
	var (
		ret     []byte
		vmerr   error
		created common.Address
	)
	if contractCreation {
		ret, created, st.gasRemaining, vmerr = st.evm.Create(msg.From, msg.Data, st.gasRemaining, value)
		if vmerr == nil {
			ret = created.Bytes()
		}
	} else {
		ret, st.gasRemaining, vmerr = st.evm.Call(msg.From, st.to(), msg.Data, st.gasRemaining, value)
	}

	peakGasUsed := st.gasUsed()
	if vmerr == nil {
		st.gasRemaining.Refund(st.calcRefund())
	}
	if err := st.returnGas(); err != nil {
		return nil, err
	}
	if vmerr != nil {
		ret = nil
	}
	return &ExecutionResult{
		UsedGas:    st.gasUsed(),
		MaxUsedGas: peakGasUsed,
		Err:        vmerr,
		ReturnData: ret,
	}, nil
}

// validateAuthorization validates an EIP-7702 authorization against the state.
func (st *stateTransition) validateAuthorization(auth *types.SetCodeAuthorization) (authority common.Address, err error) {
	// Verify chain ID is null or equal to current chain ID.
	if !auth.ChainID.IsZero() && auth.ChainID.CmpBig(st.evm.ChainConfig().ChainID) != 0 {
		return authority, ErrAuthorizationWrongChainID
	}
	// Limit nonce to 2^64-1 per EIP-2681.
	if auth.Nonce+1 < auth.Nonce {
		return authority, ErrAuthorizationNonceOverflow
	}
	// Validate signature values and recover authority.
	authority, err = auth.Authority()
	if err != nil {
		return authority, fmt.Errorf("%w: %v", ErrAuthorizationInvalidSignature, err)
	}
	// Check the authority account
	//  1) doesn't have code or has exisiting delegation
	//  2) matches the auth's nonce
	//
	// Note it is added to the access list even if the authorization is invalid.
	st.state.AddAddressToAccessList(authority)
	code := st.state.GetCode(authority)
	if _, ok := types.ParseDelegation(code); len(code) != 0 && !ok {
		return authority, ErrAuthorizationDestinationHasCode
	}
	if have := st.state.GetNonce(authority); have != auth.Nonce {
		return authority, ErrAuthorizationNonceMismatch
	}
	return authority, nil
}

// applyAuthorization applies an EIP-7702 code delegation to the state.
func (st *stateTransition) applyAuthorization(auth *types.SetCodeAuthorization) error {
	authority, err := st.validateAuthorization(auth)
	if err != nil {
		return err
	}

	// If the account already exists in state, refund the new account cost
	// charged in the intrinsic calculation.
	if st.state.Exist(authority) {
		st.state.AddRefund(params.CallNewAccountGas - params.TxAuthTupleGas)
	}

	// Update nonce and account code.
	st.state.SetNonce(authority, auth.Nonce+1, tracing.NonceChangeAuthorization)
	if auth.Address == (common.Address{}) {
		// Delegation to zero address means clear.
		st.state.SetCode(authority, nil, tracing.CodeChangeAuthorizationClear)
		return nil
	}

	// Otherwise install delegation to auth.Address.
	st.state.SetCode(authority, types.AddressToDelegation(auth.Address), tracing.CodeChangeAuthorization)

	return nil
}

// calcRefund computes refund counter, capped to a refund quotient.
func (st *stateTransition) calcRefund() vm.GasBudget {
	refund := st.gasUsed() / params.RefundQuotient
	if refund > st.state.GetRefund() {
		refund = st.state.GetRefund()
	}
	if st.evm.Config.Tracer != nil && st.evm.Config.Tracer.OnGasChange != nil && refund > 0 {
		st.evm.Config.Tracer.OnGasChange(st.gasRemaining.RegularGas, st.gasRemaining.RegularGas+refund, tracing.GasChangeTxRefunds)
	}
	return vm.NewGasBudget(refund)
}

// returnGas returns the default chain token for remaining gas,
// exchanged at the converted rate.
func (st *stateTransition) returnGas() error {
	defaultTokenID := qkccommon.DefaultTokenID
	return st.settle(defaultTokenID, st.convertedGasPrice, st.refundRate, st.gasUsed())
}

// gasUsed returns the amount of gas used up by the state transition.
func (st *stateTransition) gasUsed() uint64 {
	return st.gasRemaining.Used(st.initialBudget)
}

// blobGasUsed returns the amount of blob gas used by the message.
func (st *stateTransition) blobGasUsed() uint64 {
	return uint64(len(st.msg.BlobHashes) * params.BlobTxBlobGasPerBlob)
}
