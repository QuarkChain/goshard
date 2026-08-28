// Copyright 2026-2027, QuarkChain.
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

package vm

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

// The three QuarkChain precompiles (quarkchain/evm/specials.py:239-330). They
// sit at 0x…514b430001-03 and are observable throughout the supported window,
// so none of them may be stubbed out. Unlike the Ethereum eight they re-enter
// the message layer, which is why they take the EVM and the whole message
// rather than a byte slice.
//
// pyquarkchain reports failure as (0, 0, []) — the frame is over and its gas is
// gone. Here that is an error with an exhausted budget, which qkcApplyMsg then
// treats the same way.

// qkcNonReservedNativeTokenIndex is SystemContract.NON_RESERVED_NATIVE_TOKEN:
// the auction contract, and the only account allowed to mint.
const qkcNonReservedNativeTokenIndex = uint64(2)

// qkcTokenIDMax is TOKEN_ID_MAX: the largest id token_id_encode can produce
// (utils.py), and the bound both token-aware precompiles check against.
const qkcTokenIDMax = uint64(4873763662273663091)

// precompileFailed is the (0, 0, []) return: no output, no gas, failure.
func precompileFailed() ([]byte, GasBudget, error) {
	return nil, GasBudget{}, ErrQKCPrecompileFailed
}

// qkcCallDataWord is CallData.extract32 (vm.py:59), which is CALLDATALOAD's
// rule: the word past the end of the calldata reads as zero, and a word that
// straddles the end is padded with zeros on the right.
//
// Reading the arguments this way rather than refusing a short call is
// consensus. Only proc_transfer_mnt checks its calldata length (specials.py:242);
// the others just read, so a call with too few bytes succeeds upstream against
// zeros, and refusing it here would burn a frame's gas that pyquarkchain hands
// back.
func qkcCallDataWord(input []byte, offset int) *uint256.Int {
	if offset >= len(input) {
		return new(uint256.Int)
	}
	var word [32]byte
	copy(word[:], input[offset:])
	return new(uint256.Int).SetBytes32(word[:])
}

// qkcCallDataAddress is int_to_addr over the word at offset: the low 20 bytes.
func qkcCallDataAddress(input []byte, offset int) common.Address {
	word := qkcCallDataWord(input, offset).Bytes32()
	return common.BytesToAddress(word[:])
}

// qkcProcCurrentMntID returns the token the current frame is moving
// (specials.py:239). Calling it is also what marks the *calling* frame as
// having asked — that half is done at the call site, since the flag belongs to
// the caller, not to this frame.
func qkcProcCurrentMntID(evm *EVM, msg *qkcMessage) ([]byte, GasBudget, error) {
	const gasCost = 3
	gas := msg.gas
	if _, ok := gas.Charge(GasCosts{RegularGas: gasCost}); !ok {
		return precompileFailed()
	}
	out := new(uint256.Int).SetUint64(msg.transferTokenID).Bytes32()
	return out[:], gas, nil
}

// qkcProcTransferMnt sends an arbitrary token to an address
// (specials.py:247). Its calldata is (address, token id, value, data) with the
// tail passed on to the recipient.
func qkcProcTransferMnt(evm *EVM, msg *qkcMessage) ([]byte, GasBudget, error) {
	// Under 96 bytes there is no message to build, and a static frame may not
	// move value.
	if len(msg.input) < 96 || msg.static {
		return precompileFailed()
	}
	to := common.BytesToAddress(msg.input[12:32])
	tokenID := new(uint256.Int).SetBytes(msg.input[32:64])
	value := new(uint256.Int).SetBytes(msg.input[64:96])
	data := common.CopyBytes(msg.input[96:])

	if !tokenID.IsUint64() || tokenID.Uint64() > qkcTokenIDMax {
		return precompileFailed()
	}
	// Recursing into itself is refused outright.
	if to == QKCTransferMntAddress {
		return precompileFailed()
	}

	var surcharge uint64
	if !value.IsZero() {
		surcharge = params.CallValueTransferGas
		if evm.StateDB.Empty(to) {
			surcharge += params.CallNewAccountGas
		}
	}
	gas := msg.gas
	if _, ok := gas.Charge(GasCosts{RegularGas: surcharge}); !ok {
		return precompileFailed()
	}
	// Too little balance or too deep a stack: the surcharge is spent but the
	// rest of the gas survives, as a failed CALL's would.
	//
	// The depth bound is >= where the CALL opcode's is >, and the difference is
	// real. evm.depth counts frames that ran the interpreter, so it is one ahead
	// of the depth pyquarkchain hands a frame — except here, because a precompile
	// runs no interpreter and so does not advance it, while the child message
	// python builds for the precompile carries msg.depth + 1 (specials.py:265).
	// At that point the two counters agree, and the comparison has to agree too.
	if evm.QKC.state.GetBalanceByTokenID(msg.sender, tokenID.Uint64()).Lt(value) ||
		evm.depth >= int(params.CallCreateDepth) {
		return nil, gas, ErrQKCPrecompileFailed
	}
	if !value.IsZero() {
		gas.RegularGas += params.CallStipend
	}
	// The child message is built without a shard key (specials.py:265), and
	// that omission is consensus: a CREATE inside the callee then derives its
	// address the Ethereum way rather than the QuarkChain way.
	return evm.qkcApplyMsg(&qkcMessage{
		sender:          msg.sender,
		to:              to,
		codeAddress:     to,
		value:           value,
		gas:             gas,
		input:           data,
		transfersValue:  true,
		transferTokenID: tokenID.Uint64(),
	})
}

// qkcProcDeploySystemContract deploys one of the fixed system contracts at its
// fixed address (specials.py:290). The contract index comes from the first
// calldata word, with 0 meaning 1.
func qkcProcDeploySystemContract(evm *EVM, msg *qkcMessage) ([]byte, GasBudget, error) {
	const gasCost = 3
	gas := msg.gas
	if _, ok := gas.Charge(GasCosts{RegularGas: gasCost}); !ok {
		return precompileFailed()
	}
	word := qkcCallDataWord(msg.input, 0)
	index := uint64(1)
	if !word.IsZero() {
		if !word.IsUint64() {
			return precompileFailed()
		}
		index = word.Uint64()
	}
	contract, ok := evm.QKC.SystemContracts[index]
	if !ok {
		return precompileFailed()
	}
	if contract.Chain0Only && evm.QKC.ChainID != 0 {
		return precompileFailed()
	}
	// Deployment is gated on a non-strict comparison, unlike the strict one
	// that decided whether this precompile could be called at all.
	if evm.Context.Time < contract.EnableTs {
		return precompileFailed()
	}
	addr := contract.Address
	// create_contract answers with the address it deployed to, not with the
	// deployed code (messages.py:783), and the caller can read that off the
	// return data.
	_, deployed, gas, err := evm.qkcCreateContract(msg.to, contract.Code, gas, new(uint256.Int),
		msg.transferTokenID, msg.toFullShardKey, &addr, nil)
	if err != nil {
		return nil, gas, err
	}
	return deployed.Bytes(), gas, nil
}

// qkcProcMintMnt credits a newly minted native token (specials.py:331). It is
// the only way tokens come into existence, and the restrictions are what make
// that safe: only chain 0, never the chain's own token, and only when the call
// comes from the non-reserved native token contract, which is the auction that
// sold the token.
func qkcProcMintMnt(evm *EVM, msg *qkcMessage) ([]byte, GasBudget, error) {
	minter := qkcCallDataAddress(msg.input, 0)
	tokenID := qkcCallDataWord(msg.input, 32)
	amount := qkcCallDataWord(msg.input, 64)
	// A zero mint is refused before any charge, so it costs the frame its gas
	// without touching an account.
	if amount.IsZero() {
		return precompileFailed()
	}

	gasCost := params.CallValueTransferGas
	if evm.StateDB.Empty(minter) {
		gasCost += params.CallNewAccountGas
	}
	gas := msg.gas
	if _, ok := gas.Charge(GasCosts{RegularGas: gasCost}); !ok {
		return precompileFailed()
	}
	if !tokenID.IsUint64() || tokenID.Uint64() > qkcTokenIDMax {
		return precompileFailed()
	}
	if evm.QKC.ChainID != 0 {
		return precompileFailed()
	}
	if tokenID.Uint64() == evm.QKC.DefaultChainToken {
		return precompileFailed()
	}
	if msg.sender != evm.QKC.SystemContracts[qkcNonReservedNativeTokenIndex].Address {
		return precompileFailed()
	}

	evm.QKC.state.AddBalanceByTokenID(minter, amount, tokenID.Uint64(), tracing.BalanceChangeTransfer)
	out := new(uint256.Int).SetUint64(1).Bytes32()
	return out[:], gas, nil
}

// qkcProcBalanceMnt reads any token's balance for any address
// (specials.py:365). The flat charge is what pyquarkchain charges, whatever the
// account holds.
func qkcProcBalanceMnt(evm *EVM, msg *qkcMessage) ([]byte, GasBudget, error) {
	address := qkcCallDataAddress(msg.input, 0)
	tokenID := qkcCallDataWord(msg.input, 32)

	const gasCost = 400
	gas := msg.gas
	if _, ok := gas.Charge(GasCosts{RegularGas: gasCost}); !ok {
		return precompileFailed()
	}
	if !tokenID.IsUint64() || tokenID.Uint64() > qkcTokenIDMax {
		return precompileFailed()
	}
	out := evm.QKC.state.GetBalanceByTokenID(address, tokenID.Uint64()).Bytes32()
	return out[:], gas, nil
}
