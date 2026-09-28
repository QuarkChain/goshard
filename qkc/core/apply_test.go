// Copyright 2026-2027, QuarkChain.

package core

import (
	"crypto/ecdsa"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	ethcore "github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	qkcparams "github.com/ethereum/go-ethereum/qkc/params"
	qkctypes "github.com/ethereum/go-ethereum/qkc/types"
	"github.com/holiman/uint256"
)

const (
	testShardKey  = uint32(0x00000002)
	testBlockGas  = uint64(1_000_000)
	testBalance   = uint64(1_000_000_000)
	testBlockTime = uint64(100)
)

type applyHarness struct {
	ctx      *ExecutionContext
	state    *state.StateDB
	evm      *vm.EVM
	gasPool  *ethcore.GasPool
	key      *ecdsa.PrivateKey
	sender   common.Address
	coinbase common.Address
}

func newApplyHarness(t *testing.T) *applyHarness {
	t.Helper()

	qkcConfig := config.NewQuarkChainConfig()
	qkcConfig.EnableTxTimeStamp = 0
	qkcConfig.EnableEvmTimeStamp = 0
	shardConfig := qkcConfig.GetShardConfigByFullShardID(testShardKey)
	if shardConfig == nil {
		t.Fatalf("missing shard %#x", testShardKey)
	}
	db := rawdb.NewMemoryDatabase()
	statedb, err := state.NewQKC(coretypes.EmptyRootHash, state.NewQKCDatabase(db))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	coinbase := common.HexToAddress("0x00000000000000000000000000000000000000cc")
	statedb.SetFullShardKey(testShardKey)
	statedb.AddBalance(sender, uint256.NewInt(testBalance), tracing.BalanceChangeUnspecified)

	blockContext := vm.BlockContext{
		CanTransfer:       ethcore.CanTransfer,
		Transfer:          ethcore.Transfer,
		GetHash:           func(uint64) common.Hash { return common.Hash{} },
		Coinbase:          coinbase,
		GasLimit:          testBlockGas,
		BlockNumber:       big.NewInt(1),
		Time:              testBlockTime,
		Difficulty:        big.NewInt(1),
		BaseFee:           new(big.Int),
		BlobBaseFee:       new(big.Int),
		SenderDisallowMap: make(map[common.Address]*uint256.Int),
	}
	return &applyHarness{
		ctx: &ExecutionContext{
			QKCConfig:   qkcConfig,
			ShardConfig: shardConfig,
		},
		state:    statedb,
		evm:      vm.NewEVM(blockContext, statedb, &qkcparams.DefaultConstantinople, vm.Config{}),
		gasPool:  ethcore.NewGasPool(testBlockGas),
		key:      key,
		sender:   sender,
		coinbase: coinbase,
	}
}

func (h *applyHarness) sign(t *testing.T, tx *qkctypes.Transaction) *qkctypes.Transaction {
	t.Helper()
	signed, err := qkctypes.SignTx(tx, qkctypes.MakeSigner(h.ctx.QKCConfig.NetworkID, h.ctx.ShardConfig.EthChainID), h.key)
	if err != nil {
		t.Fatalf("sign transaction: %v", err)
	}
	return signed
}

func (h *applyHarness) transfer(t *testing.T, nonce uint64, to common.Address, value, gas, gasPrice uint64, data []byte) *qkctypes.Transaction {
	t.Helper()
	return h.sign(t, qkctypes.NewEvmTransaction(
		nonce, to, new(big.Int).SetUint64(value), gas, new(big.Int).SetUint64(gasPrice),
		testShardKey, testShardKey, h.ctx.QKCConfig.NetworkID, 0, data,
		qkccommon.DefaultTokenID, qkccommon.DefaultTokenID,
	))
}

func TestApplyTransactionTransfersAndSettlesGas(t *testing.T) {
	h := newApplyHarness(t)
	to := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	tx := h.transfer(t, 0, to, 123, 50_000, 2, nil)

	receipt, output, err := ApplyTransaction(h.ctx, h.evm, h.gasPool, h.state, tx, 0)
	if err != nil {
		t.Fatalf("apply transaction: %v", err)
	}
	if receipt.Status != qkctypes.ReceiptStatusSuccessful {
		t.Fatalf("receipt status = %d, want success", receipt.Status)
	}
	if receipt.GasUsed != txGas || receipt.CumulativeGasUsed != txGas {
		t.Fatalf("receipt gas = (%d, %d), want (%d, %d)", receipt.GasUsed, receipt.CumulativeGasUsed, txGas, txGas)
	}
	if len(output) != 0 {
		t.Fatalf("transfer output = %x, want empty", output)
	}
	if got := h.state.GetNonce(h.sender); got != 1 {
		t.Fatalf("sender nonce = %d, want 1", got)
	}
	if got := h.state.GetBalance(to).Uint64(); got != 123 {
		t.Fatalf("recipient balance = %d, want 123", got)
	}
	wantSender := testBalance - 123 - 2*txGas
	if got := h.state.GetBalance(h.sender).Uint64(); got != wantSender {
		t.Fatalf("sender balance = %d, want %d", got, wantSender)
	}
	// The default configuration taxes half of the transaction fee to the root chain.
	if got := h.state.GetBalance(h.coinbase).Uint64(); got != txGas {
		t.Fatalf("coinbase balance = %d, want %d", got, txGas)
	}
	if got := h.gasPool.Gas(); got != testBlockGas-txGas {
		t.Fatalf("remaining block gas = %d, want %d", got, testBlockGas-txGas)
	}
}

func TestApplyTransactionCreatesQKCContract(t *testing.T) {
	h := newApplyHarness(t)
	// PUSH1 0 PUSH1 0 RETURN deploys empty runtime code.
	initCode := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	tx := h.sign(t, qkctypes.NewEvmContractCreation(
		0, new(big.Int), 200_000, big.NewInt(1), testShardKey, testShardKey,
		h.ctx.QKCConfig.NetworkID, 0, initCode, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID,
	))

	receipt, output, err := ApplyTransaction(h.ctx, h.evm, h.gasPool, h.state, tx, 0)
	if err != nil {
		t.Fatalf("create contract: %v", err)
	}
	want := vm.QKCContractAddress(h.sender, testShardKey, 0)
	if receipt.Status != qkctypes.ReceiptStatusSuccessful {
		t.Fatalf("receipt status = %d, want success", receipt.Status)
	}
	if receipt.ContractAddress != want || receipt.ContractFullShardKey != testShardKey {
		t.Fatalf("contract receipt = (%s, %#x), want (%s, %#x)", receipt.ContractAddress, receipt.ContractFullShardKey, want, testShardKey)
	}
	if got := common.BytesToAddress(output); got != want {
		t.Fatalf("creation output = %s, want %s", got, want)
	}
	if got := h.state.GetNonce(h.sender); got != 1 {
		t.Fatalf("sender nonce = %d, want 1", got)
	}
	if got := h.state.GetNonce(want); got != 1 {
		t.Fatalf("contract nonce = %d, want 1", got)
	}
}

func TestApplyTransactionReturnsFailedReceiptForRevert(t *testing.T) {
	h := newApplyHarness(t)
	contract := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	// PUSH1 0 PUSH1 0 REVERT burns six execution gas and returns the rest.
	h.state.SetCode(contract, []byte{0x60, 0x00, 0x60, 0x00, byte(vm.REVERT)}, tracing.CodeChangeUnspecified)
	tx := h.transfer(t, 0, contract, 1, 200_000, 1, nil)

	receipt, output, err := ApplyTransaction(h.ctx, h.evm, h.gasPool, h.state, tx, 0)
	if err != nil {
		t.Fatalf("apply failed message: %v", err)
	}
	if receipt.Status != qkctypes.ReceiptStatusFailed {
		t.Fatalf("receipt status = %d, want failed", receipt.Status)
	}
	if receipt.GasUsed != txGas+6 {
		t.Fatalf("gas used = %d, want %d", receipt.GasUsed, txGas+6)
	}
	if len(output) != 0 {
		t.Fatalf("failed output = %x, want empty", output)
	}
	if got := h.state.GetBalance(contract).Uint64(); got != 0 {
		t.Fatalf("reverted transfer left contract balance %d", got)
	}
	if got := h.state.GetNonce(h.sender); got != 1 {
		t.Fatalf("sender nonce = %d, want 1", got)
	}
	if got := h.state.GetBalance(h.sender).Uint64(); got != testBalance-(txGas+6) {
		t.Fatalf("sender balance = %d, want %d", got, testBalance-(txGas+6))
	}
}

func TestApplyTransactionRejectsAdmissionWithoutMutation(t *testing.T) {
	tests := []struct {
		name string
		make func(*testing.T, *applyHarness) *qkctypes.Transaction
		want error
	}{
		{
			name: "wrong nonce",
			make: func(t *testing.T, h *applyHarness) *qkctypes.Transaction {
				return h.transfer(t, 1, common.Address{1}, 0, 30_000, 1, nil)
			},
			want: ErrInvalidNonce,
		},
		{
			name: "intrinsic gas",
			make: func(t *testing.T, h *applyHarness) *qkctypes.Transaction {
				return h.transfer(t, 0, common.Address{1}, 0, txGas-1, 1, nil)
			},
			want: ErrInsufficientStartGas,
		},
		{
			name: "insufficient balance",
			make: func(t *testing.T, h *applyHarness) *qkctypes.Transaction {
				return h.transfer(t, 0, common.Address{1}, testBalance, 30_000, 1, nil)
			},
			want: ErrInsufficientBalance,
		},
		{
			name: "block gas",
			make: func(t *testing.T, h *applyHarness) *qkctypes.Transaction {
				h.gasPool = ethcore.NewGasPool(29_999)
				return h.transfer(t, 0, common.Address{1}, 0, 30_000, 1, nil)
			},
			want: ErrBlockGasLimitReached,
		},
		{
			name: "cross shard",
			make: func(t *testing.T, h *applyHarness) *qkctypes.Transaction {
				return h.sign(t, qkctypes.NewEvmTransaction(
					0, common.Address{1}, new(big.Int), 30_000, big.NewInt(1),
					testShardKey, testShardKey+1, h.ctx.QKCConfig.NetworkID, 0, nil,
					qkccommon.DefaultTokenID, qkccommon.DefaultTokenID,
				))
			},
			want: ErrInvalidTransaction,
		},
		{
			name: "transactions disabled",
			make: func(t *testing.T, h *applyHarness) *qkctypes.Transaction {
				h.ctx.QKCConfig.EnableTxTimeStamp = testBlockTime + 1
				return h.transfer(t, 0, common.Address{1}, 0, 30_000, 1, nil)
			},
			want: ErrInvalidTransaction,
		},
		{
			name: "contracts disabled",
			make: func(t *testing.T, h *applyHarness) *qkctypes.Transaction {
				h.ctx.QKCConfig.EnableEvmTimeStamp = testBlockTime + 1
				return h.transfer(t, 0, common.Address{1}, 0, 30_000, 1, []byte{0})
			},
			want: ErrInvalidTransaction,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newApplyHarness(t)
			tx := test.make(t, h)
			beforeGas := h.gasPool.Gas()
			beforeBalance := h.state.GetBalance(h.sender).Uint64()

			receipt, output, err := ApplyTransaction(h.ctx, h.evm, h.gasPool, h.state, tx, 0)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if receipt != nil || output != nil {
				t.Fatalf("rejected result = (%v, %x), want nil", receipt, output)
			}
			if got := h.state.GetNonce(h.sender); got != 0 {
				t.Fatalf("rejected transaction moved nonce to %d", got)
			}
			if got := h.state.GetBalance(h.sender).Uint64(); got != beforeBalance {
				t.Fatalf("rejected transaction changed balance to %d, was %d", got, beforeBalance)
			}
			if got := h.gasPool.Gas(); got != beforeGas {
				t.Fatalf("rejected transaction changed gas pool to %d, was %d", got, beforeGas)
			}
		})
	}
}

func TestApplyTransactionAllowsWhitelistedSenderBeforeEnableTime(t *testing.T) {
	h := newApplyHarness(t)
	h.ctx.QKCConfig.EnableTxTimeStamp = testBlockTime + 1
	h.ctx.QKCConfig.TxWhitelistSenders = []string{strings.TrimPrefix(h.sender.Hex(), "0x")}
	tx := h.transfer(t, 0, common.Address{1}, 0, 30_000, 1, nil)

	if _, _, err := ApplyTransaction(h.ctx, h.evm, h.gasPool, h.state, tx, 0); err != nil {
		t.Fatalf("apply whitelisted transaction: %v", err)
	}
}

func TestApplyTransactionVersion2Admission(t *testing.T) {
	h := newApplyHarness(t)
	const ethChainID = uint32(100_001)
	h.ctx.QKCConfig.BaseEthChainID = ethChainID - 1
	h.ctx.QKCConfig.EnableEIP155SignerTimestamp = 0
	h.ctx.QKCConfig.Chains[0].EthChainID = ethChainID
	h.ctx.ShardConfig.EthChainID = ethChainID

	to := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	tx := qkctypes.NewEvmTransaction(
		0, to, big.NewInt(1), 30_000, big.NewInt(1), 0, 0, ethChainID, 2, nil,
		qkccommon.DefaultTokenID, qkccommon.DefaultTokenID,
	)
	tx = h.sign(t, tx)
	if _, _, err := ApplyTransaction(h.ctx, h.evm, h.gasPool, h.state, tx, 0); err != nil {
		t.Fatalf("apply version 2 transaction: %v", err)
	}

	foreign := qkccommon.TokenIDEncode("QETC")
	bad := qkctypes.NewEvmTransaction(
		1, to, new(big.Int), 30_000, big.NewInt(1), 0, 0, ethChainID, 2, nil,
		foreign, qkccommon.DefaultTokenID,
	)
	if _, _, err := ApplyTransaction(h.ctx, h.evm, h.gasPool, h.state, bad, 1); !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("version 2 non-default token error = %v, want invalid transaction", err)
	}
}

func TestApplyTransactionRejectsUnsupportedMNTAndRestoresState(t *testing.T) {
	h := newApplyHarness(t)
	to := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	foreign := qkccommon.TokenIDEncode("QETC")
	tx := h.sign(t, qkctypes.NewEvmTransaction(
		0, to, big.NewInt(1), 30_000, big.NewInt(1), testShardKey, testShardKey,
		h.ctx.QKCConfig.NetworkID, 0, nil, qkccommon.DefaultTokenID, foreign,
	))
	beforeGas := h.gasPool.Gas()
	beforeBalance := h.state.GetBalance(h.sender).Uint64()

	receipt, output, err := ApplyTransaction(h.ctx, h.evm, h.gasPool, h.state, tx, 0)
	if !errors.Is(err, vm.ErrQKCUnsupportedMNT) {
		t.Fatalf("error = %v, want MNT sentinel", err)
	}
	if receipt != nil || output != nil {
		t.Fatalf("MNT result = (%v, %x), want nil", receipt, output)
	}
	if got := h.state.GetNonce(h.sender); got != 0 {
		t.Fatalf("MNT rejection moved nonce to %d", got)
	}
	if got := h.state.GetBalance(h.sender).Uint64(); got != beforeBalance {
		t.Fatalf("MNT rejection changed balance to %d, was %d", got, beforeBalance)
	}
	if got := h.gasPool.Gas(); got != beforeGas {
		t.Fatalf("MNT rejection changed gas pool to %d, was %d", got, beforeGas)
	}
}

func TestApplyTransactionRollsBackActiveMNTPrecompile(t *testing.T) {
	h := newApplyHarness(t)
	precompile := common.HexToAddress("0x000000000000000000000000000000514b430001")
	tx := h.transfer(t, 0, precompile, 1, 30_000, 1, nil)
	beforeGas := h.gasPool.Gas()
	beforeBalance := h.state.GetBalance(h.sender).Uint64()

	receipt, output, err := ApplyTransaction(h.ctx, h.evm, h.gasPool, h.state, tx, 0)
	if !errors.Is(err, vm.ErrQKCUnsupportedMNT) {
		t.Fatalf("error = %v, want MNT sentinel", err)
	}
	if receipt != nil || output != nil {
		t.Fatalf("MNT result = (%v, %x), want nil", receipt, output)
	}
	if got := h.state.GetNonce(h.sender); got != 0 {
		t.Fatalf("MNT rollback left nonce %d", got)
	}
	if got := h.state.GetBalance(h.sender).Uint64(); got != beforeBalance {
		t.Fatalf("MNT rollback left balance %d, was %d", got, beforeBalance)
	}
	if got := h.state.GetBalance(precompile).Uint64(); got != 0 {
		t.Fatalf("MNT rollback left precompile balance %d", got)
	}
	if got := h.gasPool.Gas(); got != beforeGas {
		t.Fatalf("MNT rollback left gas pool %d, was %d", got, beforeGas)
	}
}
