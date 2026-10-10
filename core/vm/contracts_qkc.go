// Copyright 2026-2027, QuarkChain.

package vm

import (
	"encoding/binary"
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/params"
	qkcconfig "github.com/ethereum/go-ethereum/qkc/config"
)

var (
	// currentMntIDAddr is the first QKC EVM-aware precompile implemented in
	// this PR. It provides the representative implementation used to validate
	// the common EVM-aware precompile dispatch.
	currentMntIDAddr     = common.HexToAddress("0x000000000000000000000000000000514b430001")
	errQKCSpecialFailure = errors.New("qkc special call failed")
)

const currentMntIDGas uint64 = 3

var qkcEVMPrecompiledContracts = PrecompiledContracts{
	currentMntIDAddr: &currentMntID{},
}

// TODO: Add the other four QKC MNT precompiles in follow-up PRs. To match
// pyquarkchain, transferMnt and deploySystemContract must use the strict
// timestamp > EnableEvmTimeStamp boundary, while mintMNT and balanceMNT must
// use timestamp > EnableNonReservedNativeTokenTimestamp.

// activeQKCPrecompiledContracts adds QKC specials only after their configured
// activation timestamp. currentMntID uses a strict timestamp >
// EnableEvmTimeStamp boundary, matching pyquarkchain. A nil QKC config keeps
// standard EVM behavior and avoids copying the standard precompile registry.
func activeQKCPrecompiledContracts(rules params.Rules, timestamp uint64, config *qkcconfig.QuarkChainConfig) PrecompiledContracts {
	if config == nil || timestamp <= config.EnableEvmTimeStamp {
		return activePrecompiledContracts(rules)
	}
	contracts := ActivePrecompiledContracts(rules)
	for addr, precompile := range qkcEVMPrecompiledContracts {
		contracts[addr] = precompile
	}
	return contracts
}

// TODO: transferMnt and the remaining QKC MNT precompiles, plus embedded
// system-contract bytecode, will be implemented in follow-up PRs.
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
	if gasCost != 0 && !contract.UseGas(GasCosts{RegularGas: gasCost}, evm.Config.Tracer, tracing.GasChangeCallPrecompiledContract) {
		return nil, ErrOutOfGas
	}
	return precompile.RunWithEVM(input, evm, contract)
}

type currentMntID struct{}

func (*currentMntID) RequiredGas([]byte) uint64  { return currentMntIDGas }
func (*currentMntID) Run([]byte) ([]byte, error) { return nil, errQKCSpecialFailure }
func (*currentMntID) Name() string               { return "CURRENT_MNT_ID" }

func (*currentMntID) RunWithEVM(_ []byte, _ *EVM, contract *Contract) ([]byte, error) {
	contract.TokenIDQueried = true
	output := make([]byte, 32)
	binary.BigEndian.PutUint64(output[24:], contract.TransferTokenID)
	return output, nil
}
