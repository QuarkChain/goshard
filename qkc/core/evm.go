// Copyright 2026-2027, QuarkChain.

package core

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	ethparams "github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	qkcparams "github.com/ethereum/go-ethereum/qkc/params"
	qkcstate "github.com/ethereum/go-ethereum/qkc/state"
	"github.com/holiman/uint256"
)

// systemContractIndex mirrors specials.SystemContract: the value
// proc_deploy_system_contract reads out of its calldata.
const (
	systemContractRootChainPoSW        = uint64(1)
	systemContractNonReservedNativeTok = uint64(2)
	systemContractGeneralNativeToken   = uint64(3)
)

var (
	rootChainPoSWContractAddress       = common.HexToAddress("0x514b430000000000000000000000000000000001")
	nonReservedNativeTokenContractAddr = common.HexToAddress("0x514b430000000000000000000000000000000002")
	generalNativeTokenContractAddress  = common.HexToAddress("0x514b430000000000000000000000000000000003")
)

// newEVM builds the interpreter for one message.
//
// The chain configuration is pinned to Constantinople with Petersburg's SSTORE
// pricing (qkc/params.DefaultConstantinople), which is the rule set
// pyquarkchain's EVM implements; nothing here follows go-ethereum's own fork
// schedule. The QuarkChain profile is then attached, and every divergence from
// stock geth hangs off it.
func newEVM(ctx *ExecutionContext, state *qkcstate.EvmState, sender account.Recipient, gasPrice *big.Int, txHash common.Hash, fromFullShardKey, toFullShardKey uint32) (*vm.EVM, error) {
	blockNumber := new(big.Int).SetUint64(state.BlockNumber())
	blockCtx := vm.BlockContext{
		// Value moves inside the profile go through the token-aware path on
		// QKCContext; these two are geth's fallbacks and are written in the
		// chain's default token so they cannot disagree with it.
		CanTransfer: func(db vm.StateDB, addr common.Address, amount *uint256.Int) bool {
			return state.GetBalance(addr, state.DefaultChainTokenID()).Cmp(amount) >= 0
		},
		Transfer: func(db vm.StateDB, from, to common.Address, amount *uint256.Int, _ *ethparams.Rules) {
			token := state.DefaultChainTokenID()
			state.DeltaTokenBalance(from, token, new(big.Int).Neg(amount.ToBig()))
			state.DeltaTokenBalance(to, token, amount.ToBig())
		},
		GetHash: func(n uint64) common.Hash {
			// BLOCKHASH asks for an absolute height; the window is indexed
			// backwards from the parent (state.py:346, VMExt.block_hash).
			height := state.BlockNumber()
			if n >= height {
				return common.Hash{}
			}
			return state.GetBlockHash(height - n - 1)
		},
		Coinbase:    state.BlockCoinbase(),
		GasLimit:    state.GasLimit(),
		BlockNumber: blockNumber,
		Time:        state.Timestamp(),
		Difficulty:  state.BlockDifficulty(),
		BaseFee:     new(big.Int),
		BlobBaseFee: new(big.Int),
	}
	chainConfig := qkcparams.DefaultConstantinople
	evm := vm.NewEVM(blockCtx, state.StateDB, &chainConfig, vm.Config{})

	price, overflow := uint256.FromBig(gasPrice)
	if overflow {
		return nil, ErrInvalidTransaction
	}
	evm.SetTxContext(vm.TxContext{Origin: sender, GasPrice: price})

	disallow := make(map[common.Address]*uint256.Int, len(state.SenderDisallowMap()))
	for addr, locked := range state.SenderDisallowMap() {
		value, overflow := uint256.FromBig(locked)
		if overflow {
			// A stake beyond 256 bits bars the address outright, which is what
			// an unrepresentable requirement means.
			value = new(uint256.Int).SetAllOne()
		}
		disallow[addr] = value
	}

	qkcCtx := &vm.QKCContext{
		DefaultChainToken: state.DefaultChainTokenID(),
		GasTokenID:        state.GenesisTokenID(),
		TxHash:            txHash,
		FromFullShardKey:  fromFullShardKey,
		ChainID:           toFullShardKey >> 16,
		SenderDisallowMap: disallow,
		EvmEnableTs:       ctx.QKCConfig.EnableEvmTimeStamp,
		// The two native-token precompiles answer to the non-reserved switch
		// alone (env.py:63-76); the general one only decides when its own
		// system contract may be deployed.
		MntEnableTs:     ctx.QKCConfig.EnableNonReservedNativeTokenTimestamp,
		SystemContracts: systemContracts(ctx.QKCConfig),
	}
	if err := evm.SetQKCContext(qkcCtx); err != nil {
		return nil, err
	}
	return evm, nil
}

// systemContracts is specials._system_contracts after Env.cluster_config has
// rewritten the enable timestamps (specials.py:376). Each carries the bytecode
// proc_deploy_system_contract writes, so its code hash lands in the account
// leaf exactly as pyquarkchain's does.
func systemContracts(cfg *config.QuarkChainConfig) map[uint64]vm.QKCSystemContract {
	return map[uint64]vm.QKCSystemContract{
		systemContractRootChainPoSW: {
			Address: rootChainPoSWContractAddress,
			Code:    rootChainPoSWContractCode,
			// Enabled from the moment the QuarkChain precompiles are.
			EnableTs: 0,
		},
		systemContractNonReservedNativeTok: {
			Address:    nonReservedNativeTokenContractAddr,
			Code:       nonReservedNativeTokenContractCode,
			EnableTs:   cfg.EnableNonReservedNativeTokenTimestamp,
			Chain0Only: true,
		},
		systemContractGeneralNativeToken: {
			Address:  generalNativeTokenContractAddress,
			Code:     generalNativeTokenContractCode,
			EnableTs: cfg.EnableGeneralNativeTokenTimestamp,
		},
	}
}
