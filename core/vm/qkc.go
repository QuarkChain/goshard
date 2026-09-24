// Copyright 2026-2027, QuarkChain.

package vm

import (
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

var (
	qkcCurrentMNTIDAddress         = common.HexToAddress("0x000000000000000000000000000000514b430001")
	qkcTransferMNTAddress          = common.HexToAddress("0x000000000000000000000000000000514b430002")
	qkcDeploySystemContractAddress = common.HexToAddress("0x000000000000000000000000000000514b430003")
	qkcMintMNTAddress              = common.HexToAddress("0x000000000000000000000000000000514b430004")
	qkcBalanceMNTAddress           = common.HexToAddress("0x000000000000000000000000000000514b430005")
)

var (
	// ErrQKCSenderDisallowed rejects a transfer that would spend locked PoSW stake.
	ErrQKCSenderDisallowed = errors.New("qkc: sender barred by proof-of-staked-work")
	// ErrQKCUnsupportedMNT abandons a block that reaches multi-native-token execution.
	ErrQKCUnsupportedMNT = errors.New("qkc: multi-native-token execution is unsupported")
)

func (evm *EVM) qkcPOSWDisallows(sender common.Address, value *uint256.Int) bool {
	locked, ok := evm.Context.SenderDisallowMap[sender]
	if !ok {
		return false
	}
	required, overflow := new(uint256.Int).AddOverflow(value, locked)
	return overflow || required.Gt(evm.StateDB.GetBalance(sender))
}

func (evm *EVM) qkcSpecial(addr common.Address) (PrecompiledContract, bool, error) {
	var enableTimestamp uint64
	switch addr {
	case qkcCurrentMNTIDAddress, qkcTransferMNTAddress, qkcDeploySystemContractAddress:
		enableTimestamp = evm.Context.EVMEnableTimestamp
	case qkcMintMNTAddress, qkcBalanceMNTAddress:
		enableTimestamp = evm.Context.MNTEnableTimestamp
	default:
		precompile, ok := evm.precompile(addr)
		return precompile, ok && evm.Context.Time > 0, nil
	}
	if evm.Context.Time > enableTimestamp {
		evm.qkcUnsupportedMNT = ErrQKCUnsupportedMNT
		return nil, true, ErrQKCUnsupportedMNT
	}
	return nil, false, nil
}

// QKCApplyMessage enters the normal VM message path for a top-level QKC call.
// The transaction layer uses the same path after it has performed admission.
func (evm *EVM) QKCApplyMessage(sender, to common.Address, input []byte, gas GasBudget, value *uint256.Int, transferTokenID uint64, toFullShardKey uint32) ([]byte, GasBudget, error) {
	if transferTokenID != qkccommon.DefaultTokenID {
		return nil, gas, ErrQKCUnsupportedMNT
	}
	txContext := evm.TxContext
	txContext.Origin = sender
	txContext.ToFullShardKey = toFullShardKey
	evm.SetTxContext(txContext)
	evm.qkcExecution = true
	return evm.Call(sender, to, input, gas, value)
}

// QKCCreateContract enters contract creation through the same normal CREATE
// path used by the interpreter.
func (evm *EVM) QKCCreateContract(caller common.Address, code []byte, gas GasBudget, value *uint256.Int, transferTokenID uint64, toFullShardKey uint32, recipient *common.Address) ([]byte, common.Address, GasBudget, error) {
	if transferTokenID != qkccommon.DefaultTokenID {
		return nil, common.Address{}, gas, ErrQKCUnsupportedMNT
	}
	txContext := evm.TxContext
	txContext.Origin = caller
	txContext.ToFullShardKey = toFullShardKey
	evm.SetTxContext(txContext)
	evm.qkcExecution = true
	if recipient == nil {
		return evm.Create(caller, code, gas, value)
	}
	return evm.create(caller, code, gas, value, *recipient, CREATE)
}

// QKCContractAddress is pyquarkchain's mk_contract_address.
func QKCContractAddress(sender common.Address, fullShardKey uint32, nonce uint64) common.Address {
	encoded, _ := rlp.EncodeToBytes([]any{sender, fullShardKey, nonce})
	return common.BytesToAddress(crypto.Keccak256(encoded)[12:])
}
