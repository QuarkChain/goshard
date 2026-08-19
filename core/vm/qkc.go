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
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

// QKC: the QuarkChain execution profile.
//
// The profile is off unless EVM.QKC is set, and every divergence in this file
// and at the QKC branches in evm.go is behind that check, so a stock geth EVM
// runs stock geth rules. With it on, the message layer follows
// quarkchain/evm/messages.py rather than geth's:
//
//   - balances are indexed by token id, and the token travels with the frame
//   - a call that cannot move its value burns the frame's gas instead of
//     returning it, because _apply_msg reports (0, 0, []) (messages.py:656)
//   - a contract address is keccak(rlp([sender, fullShardKey, nonce]))[12:]
//     and the creator's nonce moves only for a nested CREATE (messages.py:704)
//   - the sender may be barred from spending by the POSW disallow map
//   - three QuarkChain precompiles live alongside the Ethereum eight, gated on
//     a strict timestamp comparison
//
// The interpreter itself is shared: opcodes, gas table and tracing are geth's.

var (
	// QKCCurrentMntIDAddress is proc_current_mnt_id (specials.py:239).
	QKCCurrentMntIDAddress = common.HexToAddress("0x000000000000000000000000000000514b430001")
	// QKCTransferMntAddress is proc_transfer_mnt (specials.py:247).
	QKCTransferMntAddress = common.HexToAddress("0x000000000000000000000000000000514b430002")
	// QKCDeploySystemContractAddress is proc_deploy_system_contract (specials.py:290).
	QKCDeploySystemContractAddress = common.HexToAddress("0x000000000000000000000000000000514b430003")
	// QKCMintMntAddress and QKCBalanceMntAddress are the two native-token
	// precompiles. Both are gated on ENABLE_NON_RESERVED_NATIVE_TOKEN_TIMESTAMP
	// alone (env.py:63-76) — the general native token switch only decides when
	// its own system contract may be deployed.
	QKCMintMntAddress    = common.HexToAddress("0x000000000000000000000000000000514b430004")
	QKCBalanceMntAddress = common.HexToAddress("0x000000000000000000000000000000514b430005")
)

// QuarkChain execution errors. They are message-layer outcomes, not geth VM
// errors: each one means the frame reported failure, and the surrounding code
// decides whether that costs the frame its gas.
var (
	// ErrQKCSenderDisallowed is transfer_failure_by_posw_balance_check
	// (messages.py:627): the sender is a recent block producer and the transfer
	// would dip into its locked stake.
	ErrQKCSenderDisallowed = errors.New("qkc: sender barred by proof-of-staked-work")
	// ErrQKCTransferFailed is a value transfer the balance did not cover. It
	// costs the frame everything (messages.py:656).
	ErrQKCTransferFailed = errors.New("qkc: token transfer failed")
	// ErrQKCTokenNotQueried is the guard at messages.py:684: a call that moved a
	// non-default token into code that never asked which token it was holding
	// fails, so a contract written before native tokens existed cannot be
	// tricked into treating them as QKC. Unlike the two above it keeps the gas
	// the frame had left.
	ErrQKCTokenNotQueried = errors.New("qkc: non-default token passed to code that did not query it")
	// ErrQKCPrecompileFailed is a QuarkChain precompile reporting failure.
	ErrQKCPrecompileFailed = errors.New("qkc: precompile failed")
)

// QKCStateDB is the state surface the profile needs on top of vm.StateDB. The
// running state is core/state.StateDB, which implements it; the assertion is
// made once, when the profile is attached.
type QKCStateDB interface {
	GetBalanceByTokenID(common.Address, uint64) *uint256.Int
	AddBalanceByTokenID(common.Address, *uint256.Int, uint64, tracing.BalanceChangeReason)
	SubBalanceByTokenID(common.Address, *uint256.Int, uint64, tracing.BalanceChangeReason)
	SetFullShardKey(uint32)
	GetFullShardKey(common.Address) uint32
	ResetStorage(common.Address)
}

// QKCSystemContract is one entry of specials._system_contracts: where a system
// contract is deployed, what it deploys, and when it may be deployed.
type QKCSystemContract struct {
	Address common.Address
	Code    []byte
	// EnableTs gates deployment with a non-strict comparison
	// (`block_timestamp < enable_ts` refuses), unlike the strict one that gates
	// the precompile call itself.
	EnableTs uint64
	// Chain0Only reproduces SYSTEM_CONTRACT_SCOPE_MAP: two of the three may
	// only be deployed on chain 0.
	Chain0Only bool
}

// QKCContext carries everything the profile needs that geth's BlockContext and
// TxContext have no field for. It lives for one transaction.
type QKCContext struct {
	// DefaultChainToken is shard_config.default_chain_token: the token a
	// balance query without an id means, and the one a frame may move without
	// the receiving code having to ask.
	DefaultChainToken uint64
	// GasTokenID is the token gas is priced in. Inside the supported profile it
	// is always the genesis token.
	GasTokenID uint64
	// TxHash is the QuarkChain transaction hash, which the EVM only carries
	// (vm.py:92) rather than reads.
	TxHash common.Hash
	// FromFullShardKey and ChainID describe where execution is happening.
	// ChainID gates the chain-0-only system contracts (specials.py:313).
	FromFullShardKey uint32
	ChainID          uint32

	// SenderDisallowMap is the POSW map, keyed by recent block producer.
	SenderDisallowMap map[common.Address]*uint256.Int

	// EvmEnableTs gates the three QuarkChain precompiles and MntEnableTs the
	// two native-token ones. Both comparisons are strict: `special_proc and
	// ext.block_timestamp > enable_ts` (messages.py:672), so the second in
	// which a precompile switches on still runs the address as an empty
	// account.
	EvmEnableTs uint64
	MntEnableTs uint64

	// SystemContracts is keyed by the index proc_deploy_system_contract reads
	// out of its calldata.
	SystemContracts map[uint64]QKCSystemContract

	// suicides is state.suicides (state.py:60): the accounts SELFDESTRUCT
	// marked during this message. QuarkChain does not delete them at the
	// opcode — del_account runs over this list once the message is over
	// (messages.py:351), and whether the account then survives is decided by
	// the end-of-block emptiness sweep. A reverted frame drops the entries it
	// added, as state.revert restores the list.
	suicides []common.Address

	// gasUnderflow records that a frame reached a point where pyquarkchain's
	// gas counter would have gone below zero. See QKCGasUnderflow.
	gasUnderflow bool

	// state is EVM.StateDB with the QuarkChain surface, asserted once.
	state QKCStateDB
	// frame is the message currently executing. Nested calls save and restore
	// it, which is how transfer_token_id rides down the stack and how each
	// frame gets its own token_id_queried.
	frame qkcFrame
}

// qkcFrame is the part of vm.Message that outlives the call into the
// interpreter: the token being moved, the shard key a CREATE inside this frame
// would use, and whether this frame has asked which token it is holding.
//
// toFullShardKey is nil where vm.Message leaves it unset (vm.py:90). That is
// not the same as zero: mk_contract_address falls back to Ethereum's
// rlp([sender, nonce]) when there is no key (messages.py:704), so a CREATE
// under such a frame deploys to a different address.
type qkcFrame struct {
	transferTokenID uint64
	toFullShardKey  *uint32
	tokenIDQueried  bool
}

// SetQKCContext attaches the profile. It must be called before execution and
// fails if the state cannot serve token-indexed balances, since silently
// running QuarkChain rules against single-balance state would produce a wrong
// root rather than an error.
func (evm *EVM) SetQKCContext(ctx *QKCContext) error {
	state, ok := evm.StateDB.(QKCStateDB)
	if !ok {
		return errors.New("qkc: state database does not support token-indexed balances")
	}
	ctx.state = state
	fromKey := ctx.FromFullShardKey
	ctx.frame = qkcFrame{
		transferTokenID: ctx.DefaultChainToken,
		toFullShardKey:  &fromKey,
	}
	evm.QKC = ctx
	return nil
}

// TransferTokenID is the token the frame currently executing is moving.
func (ctx *QKCContext) TransferTokenID() uint64 { return ctx.frame.transferTokenID }

// QKCSuicides is the list del_account must be run over once the message is
// finished. It is per message: the caller builds one EVM per message.
func (evm *EVM) QKCSuicides() []common.Address { return evm.QKC.suicides }

// QKCAdoptSuicides takes over marks made by another EVM on the same state.
// pyquarkchain keeps the list on the state, so a message the chain sends to
// itself mid-transaction — pricing a foreign gas token through the general
// native token manager — contributes to the transaction's list rather than to
// one of its own (messages.py:790).
func (evm *EVM) QKCAdoptSuicides(marks []common.Address) {
	evm.QKC.suicides = append(evm.QKC.suicides, marks...)
}

// QKCGasUnderflow reports whether execution reached a point where
// pyquarkchain's gas counter would have gone below zero.
//
// pyquarkchain subtracts CREATE2's per-word charge without checking it against
// the gas on hand (vm.py:689), and mem_extend — the next thing that would have
// noticed — only checks when it has to grow the memory. A frame whose memory
// already spans the init code therefore carries a negative counter onwards.
// What happens next is not a different state root but no result at all:
// apply_transaction trips `assert gas_remained >= 0` (messages.py:305), so
// pyquarkchain cannot process the transaction, and no block containing one
// exists. Charging cleanly here instead would let this fork accept a block
// every reference node refuses, so the flag is sticky and the execution layer
// turns it into an abandoned block.
//
// It is not cleared by a revert. The frame that set it is gone either way, and
// the transaction is already unprocessable upstream.
func (evm *EVM) QKCGasUnderflow() bool { return evm.QKC != nil && evm.QKC.gasUnderflow }

// addSuicide is ext.add_suicide (vm.py:888).
func (ctx *QKCContext) addSuicide(addr common.Address) {
	ctx.suicides = append(ctx.suicides, addr)
}

// hasSuicided reports whether this message already marked addr, which is what
// makes the refund count distinct accounts (messages.py:322).
func (ctx *QKCContext) hasSuicided(addr common.Address) bool {
	for _, marked := range ctx.suicides {
		if marked == addr {
			return true
		}
	}
	return false
}

// enterFrame installs a frame and returns the one it displaced.
func (ctx *QKCContext) enterFrame(f qkcFrame) qkcFrame {
	prev := ctx.frame
	ctx.frame = f
	return prev
}

// canTransfer is the balance test the CALL and CREATE opcodes make before
// building a message (vm.py:697, vm.py:775). It reads the *default chain
// token*, whatever token the message is actually going to move — a quirk worth
// keeping, because it decides whether the caller merely loses the call's
// surcharge or the whole frame's gas.
func (ctx *QKCContext) canTransfer(addr common.Address, value *uint256.Int) bool {
	return ctx.state.GetBalanceByTokenID(addr, ctx.DefaultChainToken).Cmp(value) >= 0
}

// transferValue is transfer_value (state.py:544), which reports rather than
// raises so that _apply_msg can turn a shortfall into a failed frame.
func (ctx *QKCContext) transferValue(from, to common.Address, tokenID uint64, value *uint256.Int) bool {
	if ctx.state.GetBalanceByTokenID(from, tokenID).Cmp(value) < 0 {
		return false
	}
	ctx.state.SubBalanceByTokenID(from, value, tokenID, tracing.BalanceChangeTransfer)
	ctx.state.AddBalanceByTokenID(to, value, tokenID, tracing.BalanceChangeTransfer)
	return true
}

// poswDisallows is transfer_failure_by_posw_balance_check (messages.py:627).
// The map holds the stake a recent block producer must leave untouched; a
// transfer that would eat into it fails before anything else happens. An absent
// sender is unconstrained, and an empty map disables the rule — which is why
// forgetting to build the map is a silent consensus divergence rather than an
// error, and why Process computes it itself.
func (ctx *QKCContext) poswDisallows(sender common.Address, value *uint256.Int) bool {
	locked, ok := ctx.SenderDisallowMap[sender]
	if !ok {
		return false
	}
	needed := new(uint256.Int).Add(value, locked)
	if needed.Lt(value) { // wrapped, so the requirement is beyond any balance
		return true
	}
	return needed.Gt(ctx.state.GetBalanceByTokenID(sender, ctx.DefaultChainToken))
}

// defaultBalance is ext.get_balance: an account's balance in the chain's
// default token, which is what the opcodes that take no token id read.
func (ctx *QKCContext) defaultBalance(addr common.Address) *uint256.Int {
	return ctx.state.GetBalanceByTokenID(addr, ctx.DefaultChainToken)
}

// qkcSelfdestruct is the SUICIDE opcode under QuarkChain rules (vm.py:875).
//
// Two things differ from Ethereum's, and both change the state root. The
// balance moved is the chain's default token rather than a single scalar. And
// the account is not destroyed here: it is added to the message's suicide list,
// which del_account is run over once the message is over (messages.py:351), so
// an account that is paid again later in the same block survives — where
// geth's self-destruct flag would delete it whatever happened afterwards.
func (evm *EVM) qkcSelfdestruct(scope *ScopeContext) ([]byte, error) {
	ctx := evm.QKC
	this := scope.Contract.Address()
	top := scope.Stack.pop()
	beneficiary := common.Address(top.Bytes20())
	balance := ctx.defaultBalance(this)

	if this != beneficiary {
		ctx.state.AddBalanceByTokenID(beneficiary, balance, ctx.DefaultChainToken,
			tracing.BalanceIncreaseSelfdestruct)
	}
	ctx.state.SubBalanceByTokenID(this, balance, ctx.DefaultChainToken,
		tracing.BalanceDecreaseSelfdestruct)
	ctx.addSuicide(this)

	if tracer := evm.Config.Tracer; tracer != nil {
		if tracer.OnEnter != nil {
			tracer.OnEnter(evm.depth, byte(SELFDESTRUCT), this, beneficiary, []byte{}, 0, balance.ToBig())
		}
		if tracer.OnExit != nil {
			tracer.OnExit(evm.depth, []byte{}, 0, nil, false)
		}
	}
	return nil, errStopToken
}

// QKCPOSWDisallows reports whether the proof-of-staked-work map bars sender
// from moving value. The cross-shard source path needs the answer without
// entering a frame, since a barred sender burns its gas and sends nothing
// (messages.py:490).
func (evm *EVM) QKCPOSWDisallows(sender common.Address, value *uint256.Int) bool {
	return evm.QKC.poswDisallows(sender, value)
}

// special resolves the precompile at addr along with the timestamp it switches
// on at. The Ethereum eight are enabled from timestamp 1, as pyquarkchain's
// specials table gives them enable_ts 0 under a strict comparison.
func (evm *EVM) qkcSpecial(addr common.Address) (qkcSpecialFunc, uint64, bool) {
	switch addr {
	case QKCCurrentMntIDAddress:
		return qkcProcCurrentMntID, evm.QKC.EvmEnableTs, true
	case QKCTransferMntAddress:
		return qkcProcTransferMnt, evm.QKC.EvmEnableTs, true
	case QKCDeploySystemContractAddress:
		return qkcProcDeploySystemContract, evm.QKC.EvmEnableTs, true
	case QKCMintMntAddress:
		return qkcProcMintMnt, evm.QKC.MntEnableTs, true
	case QKCBalanceMntAddress:
		return qkcProcBalanceMnt, evm.QKC.MntEnableTs, true
	}
	if p, ok := evm.precompile(addr); ok {
		return wrapEthPrecompile(p, addr), 0, true
	}
	return nil, 0, false
}

// qkcSpecialFunc is a precompile with the reach messages.py gives one: the two
// QuarkChain ones re-enter the message layer, so they need the EVM and the
// whole message, not just the calldata.
type qkcSpecialFunc func(evm *EVM, msg *qkcMessage) ([]byte, GasBudget, error)

func wrapEthPrecompile(p PrecompiledContract, addr common.Address) qkcSpecialFunc {
	return func(evm *EVM, msg *qkcMessage) ([]byte, GasBudget, error) {
		return RunPrecompiledContract(evm.StateDB, p, addr, msg.input, msg.gas, evm.Config.Tracer, evm.chainRules)
	}
}

// qkcMessage is vm.Message (vm.py:77) reduced to the fields the Go side needs.
type qkcMessage struct {
	sender      common.Address
	to          common.Address
	codeAddress common.Address
	value       *uint256.Int
	gas         GasBudget
	input       []byte

	// transfersValue is false only for DELEGATECALL, which runs another
	// account's code without moving anything.
	transfersValue bool
	static         bool

	transferTokenID uint64
	toFullShardKey  *uint32

	// isCreate and code mark a deployment frame, where the code to run is the
	// init code handed in rather than whatever is stored at codeAddress.
	isCreate bool
	code     []byte
}

// qkcApplyMsg is _apply_msg (messages.py:640): the whole of one call frame,
// from the proof-of-staked-work gate to the token check on the way out.
func (evm *EVM) qkcApplyMsg(msg *qkcMessage) (ret []byte, leftOver GasBudget, err error) {
	ctx := evm.QKC

	// The sender may not be allowed to move anything at all. The frame never
	// starts, and its gas is gone.
	if ctx.poswDisallows(msg.sender, msg.value) {
		return nil, GasBudget{}, ErrQKCSenderDisallowed
	}

	snapshot := evm.StateDB.Snapshot()
	suicideMark := len(ctx.suicides)
	if msg.transfersValue {
		if !ctx.transferValue(msg.sender, msg.to, msg.transferTokenID, msg.value) {
			return nil, GasBudget{}, ErrQKCTransferFailed
		}
	}

	prev := ctx.enterFrame(qkcFrame{
		transferTokenID: msg.transferTokenID,
		toFullShardKey:  msg.toFullShardKey,
	})
	defer func() { ctx.frame = prev }()

	code := msg.code
	if !msg.isCreate {
		code = evm.StateDB.GetCode(msg.codeAddress)
	}

	gas := msg.gas
	if proc, enableTs, ok := evm.qkcSpecial(msg.codeAddress); ok && evm.Context.Time > enableTs {
		ret, gas, err = proc(evm, msg)
	} else if len(code) == 0 {
		ret, err = nil, nil
	} else {
		contract := NewContract(msg.sender, msg.to, msg.value, msg.gas, evm.jumpDests)
		contract.IsDeployment = msg.isCreate
		if msg.isCreate {
			// Init code is not addressable, so it must not be cached against a
			// stored code hash.
			contract.SetCallCode(common.Hash{}, code)
		} else {
			contract.SetCallCode(evm.StateDB.GetCodeHash(msg.codeAddress), code)
		}
		ret, err = evm.Run(contract, msg.input, msg.static)
		gas = contract.Gas
	}

	// Code that ran without ever asking which token it was handed may not be
	// given a non-default one. The frame keeps its remaining gas: this is a
	// refusal, not a fault.
	if err == nil && len(code) != 0 && msg.transferTokenID != ctx.DefaultChainToken &&
		!ctx.frame.tokenIDQueried && !msg.value.IsZero() {
		err = ErrQKCTokenNotQueried
	}

	if err != nil {
		evm.StateDB.RevertToSnapshot(snapshot)
		ctx.suicides = ctx.suicides[:suicideMark]
		// A precompile decides for itself what the failure costs: it returns
		// the gas that survives it, and most return none. _apply_msg never
		// takes the rest away (messages.py:697), so neither may this.
		if err != ErrExecutionReverted && err != ErrQKCTokenNotQueried && err != ErrQKCPrecompileFailed {
			if evm.Config.Tracer != nil && evm.Config.Tracer.OnGasChange != nil {
				evm.Config.Tracer.OnGasChange(gas.RegularGas, 0, tracing.GasChangeCallFailedExecution)
			}
			gas.Exhaust()
		}
	}
	return ret, gas, err
}

// QKCApplyMessage is apply_msg (messages.py:620): a frame entered from outside
// the interpreter, which is how a transaction's top-level call and a
// cross-shard deposit both start. It skips the balance pre-check the CALL
// opcode makes, exactly as pyquarkchain does.
func (evm *EVM) QKCApplyMessage(sender, to common.Address, input []byte, gas GasBudget, value *uint256.Int, transferTokenID uint64, toFullShardKey uint32) ([]byte, GasBudget, error) {
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

// qkcCall is the CALL family's shared path: the depth and balance guards the
// opcode makes (vm.py:775), then the frame.
func (evm *EVM) qkcCall(msg *qkcMessage) ([]byte, GasBudget, error) {
	if evm.depth > int(params.CallCreateDepth) {
		return nil, msg.gas, ErrDepth
	}
	// Asking the current token id is what lets a frame accept a non-default
	// one. The flag belongs to the frame making the call, so it is set before
	// the child frame is pushed (vm.py:849).
	if msg.codeAddress == QKCCurrentMntIDAddress {
		evm.QKC.frame.tokenIDQueried = true
	}
	if !evm.QKC.canTransfer(msg.sender, msg.value) {
		return nil, msg.gas, ErrInsufficientBalance
	}
	return evm.qkcApplyMsg(msg)
}

// qkcCreateContract is create_contract (messages.py:700). recipient pins the
// address instead of deriving it, which a cross-shard deployment and the system
// contract precompile both need; salt selects CREATE2.
func (evm *EVM) qkcCreateContract(caller common.Address, code []byte, gas GasBudget, value *uint256.Int, tokenID uint64, shardKey *uint32, recipient *common.Address, salt *common.Hash) (ret []byte, address common.Address, leftOver GasBudget, err error) {
	ctx := evm.QKC

	// A contract may only be created with the default token. pyquarkchain
	// leaves the gas untouched here, so the caller gets it all back.
	if tokenID != ctx.DefaultChainToken {
		return nil, common.Address{}, gas, ErrQKCTransferFailed
	}
	if evm.depth > int(params.CallCreateDepth) {
		return nil, common.Address{}, gas, ErrDepth
	}

	// A nested CREATE moves the creator's nonce; a transaction-level one does
	// not, because apply_transaction already did (messages.py:711).
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
		// The nonce is read back after the increment above and stepped down
		// again, so a nested CREATE derives from the value the creator had
		// before it (messages.py:738).
		address = qkcContractAddressOpt(caller, shardKey, evm.StateDB.GetNonce(caller)-1)
	}

	// Deploying onto something already alive fails and costs everything.
	codeHash := evm.StateDB.GetCodeHash(address)
	hasCode := codeHash != (common.Hash{}) && codeHash != types.EmptyCodeHash
	if evm.StateDB.GetNonce(address) != 0 || hasCode {
		gas.Exhaust()
		return nil, common.Address{}, gas, ErrContractAddressCollision
	}
	// An address that exists only because it holds a balance is cleared out
	// first: the funds stay, everything else goes.
	if !evm.StateDB.Empty(address) {
		evm.StateDB.SetNonce(address, 0, tracing.NonceChangeUnspecified)
		evm.StateDB.SetCode(address, nil, tracing.CodeChangeUnspecified)
		ctx.state.ResetStorage(address)
	}

	snapshot := evm.StateDB.Snapshot()
	if !evm.StateDB.Exist(address) {
		evm.StateDB.CreateAccount(address)
	}
	evm.StateDB.CreateContract(address)
	evm.StateDB.SetNonce(address, 1, tracing.NonceChangeNewContract)
	ctx.state.ResetStorage(address)

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
	// Paying for the deployed code is all-or-nothing, and failing to pay costs
	// the rest of the gas as well as the contract (messages.py:790).
	storeCost := uint64(len(ret)) * params.CreateDataGas
	if _, ok := gas.Charge(GasCosts{RegularGas: storeCost}); !ok || uint64(len(ret)) > params.MaxCodeSize {
		evm.StateDB.RevertToSnapshot(snapshot)
		gas.Exhaust()
		return nil, common.Address{}, gas, ErrCodeStoreOutOfGas
	}
	evm.StateDB.SetCode(address, ret, tracing.CodeChangeContractCreation)
	return ret, address, gas, nil
}

// QKCCreateContract deploys code from outside the interpreter: a transaction
// whose recipient is empty, or a cross-shard deposit that carries the address
// the source shard already derived.
func (evm *EVM) QKCCreateContract(caller common.Address, code []byte, gas GasBudget, value *uint256.Int, transferTokenID uint64, toFullShardKey uint32, recipient *common.Address) ([]byte, common.Address, GasBudget, error) {
	return evm.qkcCreateContract(caller, code, gas, value, transferTokenID, &toFullShardKey, recipient, nil)
}

// QKCContractAddress is mk_contract_address (messages.py:729): the shard key
// sits between the sender and the nonce, so the same account on two shards
// deploys to two different addresses.
func QKCContractAddress(sender common.Address, fullShardKey uint32, nonce uint64) common.Address {
	data, _ := rlp.EncodeToBytes([]any{sender, fullShardKey, nonce})
	return common.BytesToAddress(crypto.Keccak256(data)[12:])
}

// qkcContractAddressOpt is mk_contract_address in full (messages.py:704). A
// frame with no shard key at all falls back to Ethereum's derivation — which
// is reachable, because the transfer-token precompile builds its child message
// without one.
func qkcContractAddressOpt(sender common.Address, fullShardKey *uint32, nonce uint64) common.Address {
	if fullShardKey == nil {
		data, _ := rlp.EncodeToBytes([]any{sender, nonce})
		return common.BytesToAddress(crypto.Keccak256(data)[12:])
	}
	return QKCContractAddress(sender, *fullShardKey, nonce)
}
