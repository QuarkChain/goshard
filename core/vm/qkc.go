// Copyright 2026-2027, QuarkChain.

package vm

import (
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
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
	// ErrQKCUnsupportedMNT signals unsupported multi-native-token execution.
	ErrQKCUnsupportedMNT = errors.New("qkc: multi-native-token execution is unsupported")

	errQKCConfigMissing = errors.New("qkc: missing chain configuration")
)

func (evm *EVM) qkcPOSWDisallows(sender common.Address, value *uint256.Int) bool {
	if evm.Config.QKCConfig == nil {
		return false
	}
	locked, ok := evm.Context.SenderDisallowMap[sender]
	if !ok {
		return false
	}
	required, overflow := new(uint256.Int).AddOverflow(value, locked)
	return overflow || required.Gt(evm.StateDB.GetBalance(sender))
}

// qkcPrecompile resolves addr using QuarkChain's precompile activation rules.
// On QuarkChain, Ethereum precompiles are inactive at timestamp zero. System
// precompiles remain inactive through their enable timestamp. Active system
// precompiles return an error because they are not implemented yet.
func (evm *EVM) qkcPrecompile(addr common.Address) (PrecompiledContract, bool, error) {
	if evm.Config.QKCConfig == nil {
		precompile, ok := evm.precompile(addr)
		return precompile, ok, nil
	}
	var enableTimestamp uint64
	switch addr {
	case qkcCurrentMNTIDAddress, qkcTransferMNTAddress, qkcDeploySystemContractAddress:
		enableTimestamp = evm.Config.QKCConfig.EnableEvmTimeStamp
	case qkcMintMNTAddress, qkcBalanceMNTAddress:
		enableTimestamp = evm.Config.QKCConfig.EnableNonReservedNativeTokenTimestamp
	default:
		precompile, ok := evm.precompile(addr)
		return precompile, ok && evm.Context.Time > 0, nil
	}
	if evm.Context.Time > enableTimestamp {
		// TODO: Implement the active QKC system precompiles.
		log.Error("QKC precompile is active but not implemented", "address", addr, "block", evm.Context.BlockNumber)
		return nil, true, ErrQKCUnsupportedMNT
	}
	return nil, false, nil
}

// QKCApplyMessage enters the normal VM message path for a top-level QKC call.
// The transaction layer uses the same path after it has performed admission.
func (evm *EVM) QKCApplyMessage(sender, to common.Address, input []byte, gas GasBudget, value *uint256.Int, transferTokenID uint64, toFullShardKey uint32) ([]byte, GasBudget, error) {
	if evm.Config.QKCConfig == nil {
		return nil, gas, errQKCConfigMissing
	}
	if transferTokenID != qkccommon.DefaultTokenID {
		return nil, gas, ErrQKCUnsupportedMNT
	}
	txContext := evm.TxContext
	txContext.Origin = sender
	txContext.ToFullShardKey = toFullShardKey
	evm.SetTxContext(txContext)
	return evm.Call(sender, to, input, gas, value)
}

// QKCCreateContract enters contract creation through the same normal CREATE
// path used by the interpreter. Admission must have incremented the sender's
// nonce already, unless recipient supplies a cross-shard contract address.
func (evm *EVM) QKCCreateContract(caller common.Address, code []byte, gas GasBudget, value *uint256.Int, transferTokenID uint64, toFullShardKey uint32, recipient *common.Address) ([]byte, common.Address, GasBudget, error) {
	if evm.Config.QKCConfig == nil {
		return nil, common.Address{}, gas, errQKCConfigMissing
	}
	if transferTokenID != qkccommon.DefaultTokenID {
		return nil, common.Address{}, gas, ErrQKCUnsupportedMNT
	}
	txContext := evm.TxContext
	txContext.Origin = caller
	txContext.ToFullShardKey = toFullShardKey
	evm.SetTxContext(txContext)
	var address common.Address
	if recipient == nil {
		nonce := evm.StateDB.GetNonce(caller)
		if nonce == 0 {
			return nil, common.Address{}, gas, ErrNonceUintOverflow
		}
		address = QKCContractAddress(caller, toFullShardKey, nonce-1)
	} else {
		address = *recipient
	}
	return evm.create(caller, code, gas, value, address, CREATE, false)
}

// QKCContractAddress is pyquarkchain's mk_contract_address.
func QKCContractAddress(sender common.Address, fullShardKey uint32, nonce uint64) common.Address {
	encoded, _ := rlp.EncodeToBytes([]any{sender, fullShardKey, nonce})
	return common.BytesToAddress(crypto.Keccak256(encoded)[12:])
}
