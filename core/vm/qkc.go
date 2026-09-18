// Copyright 2026-2027, QuarkChain.

package vm

import (
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

var (
	qkcCurrentMntIDAddress         = common.HexToAddress("0x000000000000000000000000000000514b430001")
	qkcTransferMntAddress          = common.HexToAddress("0x000000000000000000000000000000514b430002")
	qkcDeploySystemContractAddress = common.HexToAddress("0x000000000000000000000000000000514b430003")
	qkcMintMntAddress              = common.HexToAddress("0x000000000000000000000000000000514b430004")
	qkcBalanceMntAddress           = common.HexToAddress("0x000000000000000000000000000000514b430005")
)

var (
	// ErrQKCSenderDisallowed rejects a transfer that would spend locked PoSW stake.
	ErrQKCSenderDisallowed = errors.New("qkc: sender barred by proof-of-staked-work")
	// ErrQKCTransferFailed reports a value transfer not covered by the token balance.
	ErrQKCTransferFailed = errors.New("qkc: token transfer failed")
	// ErrQKCUnsupportedMNT abandons a block that reaches multi-native-token execution.
	ErrQKCUnsupportedMNT = errors.New("qkc: multi-native-token execution is unsupported")
)

// qkcStateDB is the state surface needed by the QuarkChain execution profile.
// SetQKCContext checks the interface once so an attached profile cannot silently
// fall back to geth's scalar balance.
type qkcStateDB interface {
	GetBalanceByTokenID(common.Address, uint64) *uint256.Int
	AddBalanceByTokenID(common.Address, *uint256.Int, uint64, tracing.BalanceChangeReason)
	SubBalanceByTokenID(common.Address, *uint256.Int, uint64, tracing.BalanceChangeReason)
	SetFullShardKey(uint32)
}

// QKCContext holds the QuarkChain policy that does not belong in geth's block
// or transaction context. One context is used for one top-level message.
type QKCContext struct {
	DefaultChainToken uint64
	FromFullShardKey  uint32
	SenderDisallowMap map[common.Address]*uint256.Int

	// Calls to QuarkChain's native-token precompiles are active only after the
	// corresponding timestamp. MNT execution is deliberately unsupported in
	// this delivery, so an active call records ErrQKCUnsupportedMNT.
	EvmEnableTs uint64
	MntEnableTs uint64

	state          qkcStateDB
	frame          qkcFrame
	unsupportedErr error
}

type qkcFrame struct {
	transferTokenID uint64
	toFullShardKey  *uint32
}

// SetQKCContext attaches the QuarkChain profile. A nil profile is the default,
// and leaves every geth execution path unchanged.
func (evm *EVM) SetQKCContext(ctx *QKCContext) error {
	if ctx == nil {
		evm.QKC = nil
		return nil
	}
	state, ok := evm.StateDB.(qkcStateDB)
	if !ok {
		return errors.New("qkc: state database does not support token-indexed balances")
	}
	ctx.state = state
	fromKey := ctx.FromFullShardKey
	ctx.frame = qkcFrame{
		transferTokenID: ctx.DefaultChainToken,
		toFullShardKey:  &fromKey,
	}
	ctx.unsupportedErr = nil
	evm.QKC = ctx
	return nil
}

func (ctx *QKCContext) enterFrame(frame qkcFrame) qkcFrame {
	previous := ctx.frame
	ctx.frame = frame
	return previous
}

func (ctx *QKCContext) defaultBalance(addr common.Address) *uint256.Int {
	return ctx.state.GetBalanceByTokenID(addr, ctx.DefaultChainToken)
}

func (ctx *QKCContext) canTransfer(addr common.Address, value *uint256.Int) bool {
	return ctx.defaultBalance(addr).Cmp(value) >= 0
}

func (ctx *QKCContext) transferValue(from, to common.Address, tokenID uint64, value *uint256.Int) bool {
	if ctx.state.GetBalanceByTokenID(from, tokenID).Cmp(value) < 0 {
		return false
	}
	ctx.state.SubBalanceByTokenID(from, value, tokenID, tracing.BalanceChangeTransfer)
	ctx.state.AddBalanceByTokenID(to, value, tokenID, tracing.BalanceChangeTransfer)
	return true
}

func (ctx *QKCContext) poswDisallows(sender common.Address, value *uint256.Int) bool {
	locked, ok := ctx.SenderDisallowMap[sender]
	if !ok {
		return false
	}
	required, overflow := new(uint256.Int).AddOverflow(value, locked)
	return overflow || required.Gt(ctx.defaultBalance(sender))
}

func (ctx *QKCContext) markUnsupported() error {
	ctx.unsupportedErr = ErrQKCUnsupportedMNT
	return ctx.unsupportedErr
}

// QKCPOSWDisallows exposes the source-side transfer gate without entering an
// EVM frame.
func (evm *EVM) QKCPOSWDisallows(sender common.Address, value *uint256.Int) bool {
	return evm.QKC != nil && evm.QKC.poswDisallows(sender, value)
}

func (evm *EVM) qkcSelfdestruct(scope *ScopeContext) ([]byte, error) {
	ctx := evm.QKC
	from := scope.Contract.Address()
	beneficiaryWord := scope.Stack.pop()
	beneficiary := common.Address(beneficiaryWord.Bytes20())
	balance := ctx.defaultBalance(from)

	if from != beneficiary {
		ctx.state.AddBalanceByTokenID(beneficiary, balance, ctx.DefaultChainToken, tracing.BalanceIncreaseSelfdestruct)
	}
	ctx.state.SubBalanceByTokenID(from, balance, ctx.DefaultChainToken, tracing.BalanceDecreaseSelfdestruct)
	evm.StateDB.SelfDestruct(from)

	if tracer := evm.Config.Tracer; tracer != nil {
		if tracer.OnEnter != nil {
			tracer.OnEnter(evm.depth, byte(SELFDESTRUCT), from, beneficiary, nil, 0, balance.ToBig())
		}
		if tracer.OnExit != nil {
			tracer.OnExit(evm.depth, nil, 0, nil, false)
		}
	}
	return nil, errStopToken
}

type qkcMessage struct {
	sender      common.Address
	to          common.Address
	codeAddress common.Address
	value       *uint256.Int
	gas         GasBudget
	input       []byte

	transfersValue bool
	static         bool

	transferTokenID uint64
	toFullShardKey  *uint32
	isCreate        bool
	code            []byte
}

type qkcSpecialFunc func(*EVM, *qkcMessage) ([]byte, GasBudget, error)

func wrapQKCEthereumPrecompile(precompile PrecompiledContract, addr common.Address) qkcSpecialFunc {
	return func(evm *EVM, msg *qkcMessage) ([]byte, GasBudget, error) {
		return RunPrecompiledContract(evm.StateDB, precompile, addr, msg.input, msg.gas, evm.Config.Tracer, evm.chainRules)
	}
}

func qkcUnsupportedPrecompile(evm *EVM, msg *qkcMessage) ([]byte, GasBudget, error) {
	return nil, msg.gas, evm.QKC.markUnsupported()
}

func (evm *EVM) qkcSpecial(addr common.Address) (qkcSpecialFunc, uint64, bool) {
	switch addr {
	case qkcCurrentMntIDAddress, qkcTransferMntAddress, qkcDeploySystemContractAddress:
		return qkcUnsupportedPrecompile, evm.QKC.EvmEnableTs, true
	case qkcMintMntAddress, qkcBalanceMntAddress:
		return qkcUnsupportedPrecompile, evm.QKC.MntEnableTs, true
	}
	// Pyquarkchain exposes only the first eight Ethereum precompiles. Their
	// enable timestamp is zero and the activation comparison is strict.
	if addr.Big().BitLen() <= 8 {
		last := addr[common.AddressLength-1]
		if last >= 1 && last <= 8 {
			if precompile, ok := evm.precompile(addr); ok {
				return wrapQKCEthereumPrecompile(precompile, addr), 0, true
			}
		}
	}
	return nil, 0, false
}

// qkcApplyMsg is pyquarkchain's _apply_msg: it applies the PoSW gate and value
// transfer, runs one frame, and rolls the frame back on failure.
func (evm *EVM) qkcApplyMsg(msg *qkcMessage) (ret []byte, leftOver GasBudget, err error) {
	ctx := evm.QKC
	if msg.transferTokenID != ctx.DefaultChainToken {
		return nil, msg.gas, ctx.markUnsupported()
	}
	if ctx.poswDisallows(msg.sender, msg.value) {
		return nil, GasBudget{}, ErrQKCSenderDisallowed
	}

	snapshot := evm.StateDB.Snapshot()
	if msg.transfersValue && !ctx.transferValue(msg.sender, msg.to, msg.transferTokenID, msg.value) {
		return nil, GasBudget{}, ErrQKCTransferFailed
	}

	previousFrame := ctx.enterFrame(qkcFrame{
		transferTokenID: msg.transferTokenID,
		toFullShardKey:  msg.toFullShardKey,
	})
	defer func() { ctx.frame = previousFrame }()

	code := msg.code
	if !msg.isCreate {
		code = evm.StateDB.GetCode(msg.codeAddress)
	}
	gas := msg.gas
	if special, enableTs, ok := evm.qkcSpecial(msg.codeAddress); ok && evm.Context.Time > enableTs {
		ret, gas, err = special(evm, msg)
	} else if len(code) == 0 {
		ret, err = nil, nil
	} else {
		contract := NewContract(msg.sender, msg.to, msg.value, msg.gas, evm.jumpDests)
		contract.IsDeployment = msg.isCreate
		if msg.isCreate {
			contract.SetCallCode(common.Hash{}, code)
		} else {
			contract.SetCallCode(evm.StateDB.GetCodeHash(msg.codeAddress), code)
		}
		ret, err = evm.Run(contract, msg.input, msg.static)
		gas = contract.Gas
	}
	if ctx.unsupportedErr != nil {
		err = ctx.unsupportedErr
	}
	if err != nil {
		evm.StateDB.RevertToSnapshot(snapshot)
		if err != ErrExecutionReverted && !errors.Is(err, ErrQKCUnsupportedMNT) {
			if evm.Config.Tracer != nil && evm.Config.Tracer.OnGasChange != nil {
				evm.Config.Tracer.OnGasChange(gas.RegularGas, 0, tracing.GasChangeCallFailedExecution)
			}
			gas.Exhaust()
		}
	}
	return ret, gas, err
}

// QKCApplyMessage enters the profile from a transaction or cross-shard
// deposit. It intentionally skips the CALL opcode's balance pre-check.
func (evm *EVM) QKCApplyMessage(sender, to common.Address, input []byte, gas GasBudget, value *uint256.Int, transferTokenID uint64, toFullShardKey uint32) ([]byte, GasBudget, error) {
	evm.QKC.state.SetFullShardKey(toFullShardKey)
	return evm.qkcApplyMsg(&qkcMessage{
		sender:          sender,
		to:              to,
		codeAddress:     to,
		value:           value,
		gas:             gas,
		input:           input,
		transfersValue:  true,
		transferTokenID: transferTokenID,
		toFullShardKey:  &toFullShardKey,
	})
}

func (evm *EVM) qkcCall(msg *qkcMessage) ([]byte, GasBudget, error) {
	if evm.depth > int(params.CallCreateDepth) {
		return nil, msg.gas, ErrDepth
	}
	if msg.transfersValue && !evm.QKC.canTransfer(msg.sender, msg.value) {
		return nil, msg.gas, ErrInsufficientBalance
	}
	return evm.qkcApplyMsg(msg)
}

func (evm *EVM) qkcCreateContract(caller common.Address, code []byte, gas GasBudget, value *uint256.Int, tokenID uint64, shardKey *uint32, recipient *common.Address, salt *common.Hash) (ret []byte, address common.Address, leftOver GasBudget, err error) {
	ctx := evm.QKC
	if tokenID != ctx.DefaultChainToken {
		return nil, common.Address{}, gas, ctx.markUnsupported()
	}
	if evm.depth > int(params.CallCreateDepth) {
		return nil, common.Address{}, gas, ErrDepth
	}
	if evm.TxContext.Origin != caller {
		nonce := evm.StateDB.GetNonce(caller)
		if nonce+1 < nonce {
			return nil, common.Address{}, gas, ErrNonceUintOverflow
		}
		evm.StateDB.SetNonce(caller, nonce+1, tracing.NonceChangeContractCreator)
	}

	switch {
	case recipient != nil:
		address = *recipient
	case salt != nil:
		address = crypto.CreateAddress2(caller, *salt, crypto.Keccak256(code))
	default:
		address = qkcContractAddress(caller, shardKey, evm.StateDB.GetNonce(caller)-1)
	}
	codeHash := evm.StateDB.GetCodeHash(address)
	if evm.StateDB.GetNonce(address) != 0 || (codeHash != (common.Hash{}) && codeHash != types.EmptyCodeHash) {
		gas.Exhaust()
		return nil, common.Address{}, gas, ErrContractAddressCollision
	}
	snapshot := evm.StateDB.Snapshot()
	if !evm.StateDB.Exist(address) {
		evm.StateDB.CreateAccount(address)
	}
	evm.StateDB.CreateContract(address)
	evm.StateDB.SetNonce(address, 1, tracing.NonceChangeNewContract)

	ret, gas, err = evm.qkcApplyMsg(&qkcMessage{
		sender:          caller,
		to:              address,
		codeAddress:     address,
		value:           value,
		gas:             gas,
		transfersValue:  true,
		transferTokenID: tokenID,
		toFullShardKey:  shardKey,
		isCreate:        true,
		code:            code,
	})
	if err != nil {
		evm.StateDB.RevertToSnapshot(snapshot)
		return ret, common.Address{}, gas, err
	}
	if len(ret) == 0 {
		return nil, address, gas, nil
	}
	storeCost := uint64(len(ret)) * params.CreateDataGas
	if _, ok := gas.Charge(GasCosts{RegularGas: storeCost}); !ok || uint64(len(ret)) > params.MaxCodeSize {
		evm.StateDB.RevertToSnapshot(snapshot)
		gas.Exhaust()
		return nil, common.Address{}, gas, ErrCodeStoreOutOfGas
	}
	evm.StateDB.SetCode(address, ret, tracing.CodeChangeContractCreation)
	return ret, address, gas, nil
}

// QKCCreateContract enters contract creation from outside the interpreter.
func (evm *EVM) QKCCreateContract(caller common.Address, code []byte, gas GasBudget, value *uint256.Int, transferTokenID uint64, toFullShardKey uint32, recipient *common.Address) ([]byte, common.Address, GasBudget, error) {
	evm.QKC.state.SetFullShardKey(toFullShardKey)
	return evm.qkcCreateContract(caller, code, gas, value, transferTokenID, &toFullShardKey, recipient, nil)
}

// QKCContractAddress is pyquarkchain's mk_contract_address.
func QKCContractAddress(sender common.Address, fullShardKey uint32, nonce uint64) common.Address {
	return qkcContractAddress(sender, &fullShardKey, nonce)
}

func qkcContractAddress(sender common.Address, fullShardKey *uint32, nonce uint64) common.Address {
	var value any
	if fullShardKey == nil {
		value = []any{sender, nonce}
	} else {
		value = []any{sender, *fullShardKey, nonce}
	}
	encoded, _ := rlp.EncodeToBytes(value)
	return common.BytesToAddress(crypto.Keccak256(encoded)[12:])
}
