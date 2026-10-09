// Copyright 2026-2027, QuarkChain.

package vm

import "github.com/ethereum/go-ethereum/core/tracing"

// TODO: QKC EVM-aware precompile implementations and embedded system-contract
// bytecode will be implemented in follow-up PRs.
func (evm *EVM) runPrecompiledContractWithEVM(precompile PrecompiledContractWithEVM, input []byte, contract *Contract) ([]byte, error) {
	// Pyquarkchain passes the effective static flag and current message depth to
	// special contracts through its Message object. Contract is the equivalent
	// call-frame object for EVM-aware precompiles in Go.
	contract.IsStatic = evm.readOnly
	contract.Depth = evm.depth
	// TODO: If QKC enables Amsterdam/EIP-7928 block-level access lists, add
	// the standard precompile Touch here in a follow-up PR. Current QKC shard
	// rules are Petersburg-only, so Amsterdam cannot be active.
	//
	// MNT precompiles that create child messages must propagate Depth+1. Their
	// nested-call behavior will be implemented with those precompiles in a
	// follow-up PR.
	gasCost := precompile.RequiredGas(input)
	if !contract.UseGas(GasCosts{RegularGas: gasCost}, evm.Config.Tracer, tracing.GasChangeCallPrecompiledContract) {
		return nil, ErrOutOfGas
	}
	return precompile.RunWithEVM(input, evm, contract)
}
