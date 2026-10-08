// Copyright 2026-2027, QuarkChain.

package vm

import (
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/crypto"
	qkcconfig "github.com/ethereum/go-ethereum/qkc/config"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

// stateDBWithoutMandatoryHookSuspension deliberately hides the embedded
// method with an incompatible signature. The assertion ensures payAsGas trace
// suppression remains an optional core concern instead of a VM StateDB API.
type stateDBWithoutMandatoryHookSuspension struct {
	StateDB
}

func (*stateDBWithoutMandatoryHookSuspension) SuspendQKCStateHooks() {}

var _ StateDB = (*stateDBWithoutMandatoryHookSuspension)(nil)

type fullShardKeyProbe struct {
	seen []*uint32
}

func (*fullShardKeyProbe) RequiredGas([]byte) uint64  { return 0 }
func (*fullShardKeyProbe) Run([]byte) ([]byte, error) { return nil, errQKCSpecialFailure }
func (*fullShardKeyProbe) Name() string               { return "FULL_SHARD_KEY_PROBE" }

func (p *fullShardKeyProbe) RunWithEVM(_ []byte, _ *EVM, contract *Contract) ([]byte, error) {
	p.seen = append(p.seen, contract.ToFullShardKey)
	return nil, nil
}

func callProbeCode(op OpCode, target common.Address) []byte {
	argumentCount := 5
	if op == DELEGATECALL || op == STATICCALL {
		argumentCount = 4
	}
	code := make([]byte, 0, argumentCount*2+23)
	for range argumentCount {
		code = append(code, byte(PUSH1), 0)
	}
	code = append(code, byte(PUSH20))
	code = append(code, target.Bytes()...)
	return append(code, byte(GAS), byte(op), byte(STOP))
}

func createProbeCode(target common.Address) []byte {
	initCode := callProbeCode(CALL, target)
	code := []byte{
		byte(PUSH1), byte(len(initCode)),
		byte(PUSH1), 15,
		byte(PUSH1), 0,
		byte(CODECOPY),
		byte(PUSH1), byte(len(initCode)),
		byte(PUSH1), 0,
		byte(PUSH1), 0,
		byte(CREATE),
		byte(STOP),
	}
	return append(code, initCode...)
}

func TestQKCFullShardKeyIsScopedToCallFrame(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	probeAddress := common.HexToAddress("0xf001")

	for _, op := range []OpCode{CALL, CALLCODE, DELEGATECALL, STATICCALL} {
		t.Run(op.String(), func(t *testing.T) {
			evm, db := newQKCTestEVM(t, 1, config)
			defer evm.Release()
			probe := new(fullShardKeyProbe)
			evm.precompiles[probeAddress] = probe
			caller := common.HexToAddress("0xf100")
			target := common.HexToAddress("0xf200")
			db.SetCode(target, callProbeCode(op, probeAddress), tracing.CodeChangeUnspecified)
			key := uint32(0x12340001)
			evm.SetTxContext(TxContext{
				Origin:          caller,
				GasPrice:        new(uint256.Int),
				GasTokenID:      config.GetDefaultChainTokenID(),
				TransferTokenID: config.GetDefaultChainTokenID(),
				ToFullShardKey:  &key,
			})

			_, _, err := evm.Call(caller, target, nil, NewGasBudget(100_000), new(uint256.Int))
			require.NoError(t, err)
			require.Len(t, probe.seen, 1)
			require.NotNil(t, probe.seen[0])
			require.Equal(t, uint32(0x12340001), *probe.seen[0])

			key = 0
			require.Equal(t, uint32(0x12340001), *probe.seen[0], "call frames must own a cloned shard key")
		})
	}
}

func TestQKCTransferMNTClearsChildFullShardKey(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	evm, _ := newQKCTestEVM(t, 1, config)
	defer evm.Release()
	probeAddress := common.HexToAddress("0xf003")
	probe := new(fullShardKeyProbe)
	evm.precompiles[probeAddress] = probe
	key := uint32(0x12340001)
	evm.SetTxContext(TxContext{
		Origin:          common.HexToAddress("0xf300"),
		GasPrice:        new(uint256.Int),
		GasTokenID:      config.GetDefaultChainTokenID(),
		TransferTokenID: config.GetDefaultChainTokenID(),
		ToFullShardKey:  &key,
	})
	input := make([]byte, 96)
	copy(input[12:32], probeAddress.Bytes())
	binary.BigEndian.PutUint64(input[56:64], config.GetDefaultChainTokenID())

	_, _, err := evm.Call(evm.TxContext.Origin, transferMntAddr, input, NewGasBudget(100_000), new(uint256.Int))
	require.NoError(t, err)
	require.Equal(t, []*uint32{nil}, probe.seen)
}

func TestQKCTransferMNTCreatedChildKeepsFullShardKeyCleared(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	evm, db := newQKCTestEVM(t, 1, config)
	defer evm.Release()
	probeAddress := common.HexToAddress("0xf006")
	probe := new(fullShardKeyProbe)
	evm.precompiles[probeAddress] = probe
	target := common.HexToAddress("0xf600")
	db.SetCode(target, createProbeCode(probeAddress), tracing.CodeChangeUnspecified)
	key := uint32(0x12340001)
	evm.SetTxContext(TxContext{
		Origin:          common.HexToAddress("0xf601"),
		GasPrice:        new(uint256.Int),
		GasTokenID:      config.GetDefaultChainTokenID(),
		TransferTokenID: config.GetDefaultChainTokenID(),
		ToFullShardKey:  &key,
	})
	input := make([]byte, 96)
	copy(input[12:32], target.Bytes())
	binary.BigEndian.PutUint64(input[56:64], config.GetDefaultChainTokenID())

	_, _, err := evm.Call(evm.TxContext.Origin, transferMntAddr, input, NewGasBudget(200_000), new(uint256.Int))
	require.NoError(t, err)
	require.Equal(t, []*uint32{nil}, probe.seen)
}

func TestQKCSystemContractCreationInheritsFullShardKey(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	evm, _ := newQKCTestEVM(t, 1, config)
	defer evm.Release()
	probeAddress := common.HexToAddress("0xf004")
	probe := new(fullShardKeyProbe)
	evm.precompiles[probeAddress] = probe
	parent := NewContract(
		common.HexToAddress("0xf400"),
		common.HexToAddress("0xf401"),
		new(uint256.Int),
		NewGasBudget(100_000),
		evm.jumpDests,
	)
	parent.TransferTokenID = config.GetDefaultChainTokenID()
	key := uint32(0x12340001)
	parent.ToFullShardKey = &key

	_, _, _, err := evm.createSystemContract(
		parent.Address(),
		callProbeCode(CALL, probeAddress),
		NewGasBudget(100_000),
		new(uint256.Int),
		common.HexToAddress("0xf402"),
		CREATE,
		parent,
		false,
	)
	require.NoError(t, err)
	require.Len(t, probe.seen, 1)
	require.NotNil(t, probe.seen[0])
	require.Equal(t, uint32(0x12340001), *probe.seen[0])
}

func TestQKCCreateFramesInheritFullShardKey(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	probeAddress := common.HexToAddress("0xf005")
	tests := []struct {
		name string
		run  func(*EVM, common.Address, []byte) error
	}{
		{
			name: "CREATE",
			run: func(evm *EVM, caller common.Address, code []byte) error {
				_, _, _, err := evm.Create(caller, code, NewGasBudget(100_000), new(uint256.Int))
				return err
			},
		},
		{
			name: "CREATE2",
			run: func(evm *EVM, caller common.Address, code []byte) error {
				_, _, _, err := evm.Create2(caller, code, NewGasBudget(100_000), new(uint256.Int), uint256.NewInt(1))
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evm, _ := newQKCTestEVM(t, 1, config)
			defer evm.Release()
			probe := new(fullShardKeyProbe)
			evm.precompiles[probeAddress] = probe
			caller := common.HexToAddress("0xf500")
			key := uint32(0x12340001)
			evm.SetTxContext(TxContext{
				Origin:          caller,
				GasPrice:        new(uint256.Int),
				GasTokenID:      config.GetDefaultChainTokenID(),
				TransferTokenID: config.GetDefaultChainTokenID(),
				ToFullShardKey:  &key,
			})

			err := test.run(evm, caller, callProbeCode(CALL, probeAddress))
			require.NoError(t, err)
			require.Len(t, probe.seen, 1)
			require.NotNil(t, probe.seen[0])
			require.Equal(t, uint32(0x12340001), *probe.seen[0])
		})
	}
}

func TestQKCTransferCreditOverflowFailsWithoutPoisoningState(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	evm, db := newQKCTestEVM(t, 1, config)
	defer evm.Release()
	sender := common.HexToAddress("0x3100")
	recipient := common.HexToAddress("0x3200")
	tokenID := uint64(1)
	db.AddBalanceByTokenID(sender, uint256.NewInt(1), tokenID, tracing.BalanceChangeUnspecified)
	db.AddBalanceByTokenID(recipient, new(uint256.Int).SetAllOne(), tokenID, tracing.BalanceChangeUnspecified)
	evm.SetTxContext(TxContext{GasPrice: new(uint256.Int), GasTokenID: config.GetDefaultChainTokenID(), TransferTokenID: tokenID})

	_, gas, err := evm.Call(sender, recipient, nil, NewGasBudget(50_000), uint256.NewInt(1))
	require.Error(t, err)
	require.Zero(t, gas.RegularGas)
	require.Equal(t, uint64(1), db.GetBalanceByTokenID(sender, tokenID).Uint64())
	require.True(t, db.GetBalanceByTokenID(recipient, tokenID).Eq(new(uint256.Int).SetAllOne()))
	require.NoError(t, db.Error())
}

func TestQKCNestedCallRequiresDefaultAndTransferTokenBalances(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	tokenID := uint64(1)
	target := common.HexToAddress("0x5000")
	recipient := common.HexToAddress("0x6000")
	code := common.FromHex("0x6000600060006000600173")
	code = append(code, recipient.Bytes()...)
	code = append(code, byte(GAS), byte(CALL), byte(PUSH1), 0, byte(SSTORE), byte(STOP))

	for _, test := range []struct {
		name        string
		defaultFund uint64
		wantSuccess uint64
	}{
		{name: "missing default balance", wantSuccess: 0},
		{name: "both balances present", defaultFund: 1, wantSuccess: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			evm, db := newQKCTestEVM(t, 1, config)
			defer evm.Release()
			db.SetCode(target, code, tracing.CodeChangeUnspecified)
			db.AddBalanceByTokenID(target, uint256.NewInt(1), tokenID, tracing.BalanceChangeUnspecified)
			if test.defaultFund != 0 {
				db.AddBalanceByTokenID(target, uint256.NewInt(test.defaultFund), config.GetDefaultChainTokenID(), tracing.BalanceChangeUnspecified)
			}
			evm.SetTxContext(TxContext{GasPrice: new(uint256.Int), GasTokenID: config.GetDefaultChainTokenID(), TransferTokenID: tokenID})

			_, _, err := evm.Call(common.HexToAddress("0x7000"), target, nil, NewGasBudget(200_000), new(uint256.Int))
			require.NoError(t, err)
			require.Equal(t, test.wantSuccess, db.GetState(target, common.Hash{}).Big().Uint64())
			require.Equal(t, test.wantSuccess, db.GetBalanceByTokenID(recipient, tokenID).Uint64())
		})
	}
}

func TestQKCCallCodeDoesNotAcknowledgeTransferToken(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	evm, db := newQKCTestEVM(t, 1, config)
	defer evm.Release()
	caller := common.HexToAddress("0x8000")
	target := common.HexToAddress("0x9000")
	tokenID := uint64(1)
	code := common.FromHex("0x6000600060006000600073")
	code = append(code, currentMntIDAddr.Bytes()...)
	code = append(code, byte(GAS), byte(CALLCODE), byte(STOP))
	db.SetCode(target, code, tracing.CodeChangeUnspecified)
	db.AddBalanceByTokenID(caller, uint256.NewInt(2), tokenID, tracing.BalanceChangeUnspecified)
	evm.SetTxContext(TxContext{Origin: caller, GasPrice: new(uint256.Int), GasTokenID: config.GetDefaultChainTokenID(), TransferTokenID: tokenID})

	_, _, err := evm.Call(caller, target, nil, NewGasBudget(100_000), uint256.NewInt(1))
	require.ErrorIs(t, err, ErrExecutionReverted)
	require.Equal(t, uint64(2), db.GetBalanceByTokenID(caller, tokenID).Uint64())
	require.Zero(t, db.GetBalanceByTokenID(target, tokenID).Uint64())
}

func TestQKCFailedOpcodePrecheckDoesNotAcknowledgeTransferToken(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	evm, db := newQKCTestEVM(t, 1, config)
	defer evm.Release()
	caller := common.HexToAddress("0x8100")
	target := common.HexToAddress("0x9100")
	tokenID := uint64(1)
	// The nested CALL asks to transfer value 1 to currentMntID. The target has
	// the MNT balance received by the top-level call, but no default-QKC balance,
	// so pyquarkchain fails the opcode precheck before marking acknowledgement.
	code := common.FromHex("0x6000600060006000600173")
	code = append(code, currentMntIDAddr.Bytes()...)
	code = append(code, byte(GAS), byte(CALL), byte(STOP))
	db.SetCode(target, code, tracing.CodeChangeUnspecified)
	db.AddBalanceByTokenID(caller, uint256.NewInt(2), tokenID, tracing.BalanceChangeUnspecified)
	evm.SetTxContext(TxContext{Origin: caller, GasPrice: new(uint256.Int), GasTokenID: config.GetDefaultChainTokenID(), TransferTokenID: tokenID})

	_, _, err := evm.Call(caller, target, nil, NewGasBudget(100_000), uint256.NewInt(1))
	require.ErrorIs(t, err, ErrExecutionReverted)
	require.Equal(t, uint64(2), db.GetBalanceByTokenID(caller, tokenID).Uint64())
	require.Zero(t, db.GetBalanceByTokenID(target, tokenID).Uint64())
}

func TestQKCCreateUsesStandardAddressFormula(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	evm, db := newQKCTestEVM(t, 1, config)
	defer evm.Release()
	sender := common.HexToAddress("0xa100")
	key := uint32(0x12340001)
	evm.SetTxContext(TxContext{
		Origin:          sender,
		GasPrice:        new(uint256.Int),
		GasTokenID:      config.GetDefaultChainTokenID(),
		TransferTokenID: config.GetDefaultChainTokenID(),
		ToFullShardKey:  &key,
	})
	want := crypto.CreateAddress(sender, 0)

	_, address, _, err := evm.Create(sender, []byte{byte(STOP)}, NewGasBudget(100_000), new(uint256.Int))
	require.NoError(t, err)
	require.Equal(t, want, address)
	require.Equal(t, uint64(1), db.GetNonce(sender))
}

func TestQKCCreate2UsesStandardAddressFormula(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	evm, db := newQKCTestEVM(t, 1, config)
	defer evm.Release()
	creator := common.HexToAddress("0xb200")
	key := uint32(0x12340001)
	salt := uint256.NewInt(9)
	initCode := []byte{byte(STOP)}
	evm.SetTxContext(TxContext{
		Origin:          creator,
		GasPrice:        new(uint256.Int),
		GasTokenID:      config.GetDefaultChainTokenID(),
		TransferTokenID: config.GetDefaultChainTokenID() + 1,
		ToFullShardKey:  &key,
	})
	wantHash := crypto.Keccak256Hash(initCode)
	want := crypto.CreateAddress2(creator, salt.Bytes32(), wantHash[:])

	_, address, _, err := evm.Create2(creator, initCode, NewGasBudget(100_000), new(uint256.Int), salt)
	require.NoError(t, err)
	require.Equal(t, want, address)
	require.Equal(t, uint64(1), db.GetNonce(creator))
}

func TestQKCCreateIgnoresQKCTransferTokenContext(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	evm, db := newQKCTestEVM(t, 1, config)
	defer evm.Release()
	sender := common.HexToAddress("0xc100")
	db.SetNonce(sender, 7, tracing.NonceChangeUnspecified)
	evm.SetTxContext(TxContext{
		Origin:          sender,
		GasPrice:        new(uint256.Int),
		GasTokenID:      config.GetDefaultChainTokenID(),
		TransferTokenID: config.GetDefaultChainTokenID() + 1,
	})
	want := crypto.CreateAddress(sender, 7)

	_, address, _, err := evm.Create(sender, []byte{byte(STOP)}, NewGasBudget(100_000), new(uint256.Int))
	require.NoError(t, err)
	require.Equal(t, want, address)
	require.Equal(t, uint64(8), db.GetNonce(sender))
}

func TestQKCCreateOpcodesUseStandardCreatePath(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	for _, test := range []struct {
		name    string
		code    []byte
		wantFor func(common.Address) common.Address
	}{
		{
			name: "CREATE",
			code: common.FromHex("0x600060006000f060005500"),
			wantFor: func(creator common.Address) common.Address {
				return crypto.CreateAddress(creator, 0)
			},
		},
		{
			name: "CREATE2",
			code: common.FromHex("0x6009600060006000f560005500"),
			wantFor: func(creator common.Address) common.Address {
				hash := crypto.Keccak256Hash(nil)
				return crypto.CreateAddress2(creator, uint256.NewInt(9).Bytes32(), hash[:])
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			evm, db := newQKCTestEVM(t, 1, config)
			defer evm.Release()
			origin := common.HexToAddress("0xd100")
			creator := common.HexToAddress("0xd200")
			db.SetCode(creator, test.code, tracing.CodeChangeUnspecified)
			evm.SetTxContext(TxContext{
				Origin:          origin,
				GasPrice:        new(uint256.Int),
				GasTokenID:      config.GetDefaultChainTokenID(),
				TransferTokenID: config.GetDefaultChainTokenID() + 1,
			})

			_, _, err := evm.Call(origin, creator, nil, NewGasBudget(200_000), new(uint256.Int))
			require.NoError(t, err)
			require.Equal(t, test.wantFor(creator), common.BytesToAddress(db.GetState(creator, common.Hash{}).Bytes()))
			require.Equal(t, uint64(1), db.GetNonce(creator))
		})
	}
}
