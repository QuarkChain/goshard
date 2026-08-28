// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	qkcstate "github.com/ethereum/go-ethereum/qkc/state"
	"github.com/holiman/uint256"
)

// The two functions of the general native token manager the execution layer
// calls, by selector (messages.py:815-840).
var (
	calculateGasPriceSelector = []byte{0xce, 0x9e, 0x8c, 0x47}
	payAsGasSelector          = []byte{0x5a, 0xe8, 0xf7, 0xf1}
)

// generalNativeTokenCallGas is the gas the manager call is given
// (messages.py:801). It is a mock allowance: the call is not paid for by
// anyone, and the amount only has to be enough for the contract to finish.
const generalNativeTokenCallGas = uint64(1000000)

// payNativeTokenAsGas is pay_native_token_as_gas (messages.py:827): the
// general native token manager is asked to sell gas for a foreign token. It
// answers with the refund rate the transaction will be settled at and the price
// of that gas in the genesis token.
//
// A zero converted price is the manager's way of saying no — the token has no
// price, or the contract is not deployed — and every caller treats it as a
// refusal rather than as free gas.
//
// The call moves balances: the manager takes the native token and gives up
// genesis token. Callers that only want the quote run it inside a snapshot.
func payNativeTokenAsGas(ctx *ExecutionContext, state *qkcstate.EvmState, tokenID, gas uint64, gasPriceInNativeToken *big.Int) (uint64, *big.Int, []common.Address, error) {
	data := make([]byte, 0, 4+96)
	data = append(data, payAsGasSelector...)
	data = append(data, common.BigToHash(new(big.Int).SetUint64(tokenID)).Bytes()...)
	data = append(data, common.BigToHash(new(big.Int).SetUint64(gas)).Bytes()...)
	data = append(data, common.BigToHash(gasPriceInNativeToken).Bytes()...)
	return callGeneralNativeTokenManager(ctx, state, data)
}

// gasUtilityInfo is get_gas_utility_info (messages.py:815): the same quote
// without the sale. Only the transaction pool and the miner read it, so it is
// not on any consensus path.
func gasUtilityInfo(ctx *ExecutionContext, state *qkcstate.EvmState, tokenID uint64, gasPriceInNativeToken *big.Int) (uint64, *big.Int, []common.Address, error) {
	data := make([]byte, 0, 4+64)
	data = append(data, calculateGasPriceSelector...)
	data = append(data, common.BigToHash(new(big.Int).SetUint64(tokenID)).Bytes()...)
	data = append(data, common.BigToHash(gasPriceInNativeToken).Bytes()...)
	return callGeneralNativeTokenManager(ctx, state, data)
}

// callGeneralNativeTokenManager is _call_general_native_token_manager
// (messages.py:790): a message the chain sends to itself.
//
// The sender is the contract, which is what the contract checks for — no
// account outside it can ask for the sale. The message pays no gas and carries
// no value, so nothing here depends on who sent the transaction that triggered
// it.
func callGeneralNativeTokenManager(ctx *ExecutionContext, state *qkcstate.EvmState, data []byte) (uint64, *big.Int, []common.Address, error) {
	contract := generalNativeTokenContractAddress
	if len(state.GetCode(contract)) == 0 {
		return 0, new(big.Int), nil, nil
	}

	evm, err := newEVM(ctx, state, contract, new(big.Int), common.Hash{}, 0, 0)
	if err != nil {
		return 0, nil, nil, err
	}
	// The message's transfer token is left at zero, as vm.Message's default
	// (vm.py:94). It is never moved: the value is zero, which is also what
	// keeps the un-queried non-default token rule from firing.
	output, _, vmErr := evm.QKCApplyMessage(contract, contract, data,
		vm.NewGasBudget(generalNativeTokenCallGas), new(uint256.Int), 0, 0)
	if err := profileError(evm, state, vmErr); err != nil {
		return 0, nil, nil, err
	}
	// The marks the manager made are the transaction's, not this call's: the
	// state they were made against is shared.
	suicides := evm.QKCSuicides()
	if vmErr != nil || len(output) < 64 {
		// A failed call is a refusal, not an error: apply_msg's zero result
		// makes _call_general_native_token_manager return (0, 0).
		return 0, new(big.Int), suicides, nil
	}
	refundRate := new(big.Int).SetBytes(output[:32])
	convertedGasPrice := new(big.Int).SetBytes(output[32:64])
	if !refundRate.IsUint64() {
		return 0, nil, nil, fmt.Errorf("%w: refund rate %s does not fit in 64 bits",
			ErrInvalidTransaction, refundRate)
	}
	return refundRate.Uint64(), convertedGasPrice, suicides, nil
}

// quoteNativeTokenGas runs the sale inside a snapshot and unwinds it, which is
// how validate_transaction asks the price without paying it (messages.py:236).
func quoteNativeTokenGas(ctx *ExecutionContext, state *qkcstate.EvmState, tokenID, gas uint64, gasPriceInNativeToken *big.Int) (uint64, *big.Int, error) {
	snapshot := state.Snapshot()
	// Whatever the quote marked goes away with the snapshot, as state.revert
	// restores the suicide list along with everything else (state.py:520).
	refundRate, converted, _, err := payNativeTokenAsGas(ctx, state, tokenID, gas, gasPriceInNativeToken)
	state.RevertToSnapshot(snapshot)
	return refundRate, converted, err
}
