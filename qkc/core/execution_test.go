// Copyright 2026-2027, QuarkChain.

package core

import (
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	qkcstate "github.com/ethereum/go-ethereum/qkc/state"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// The mainnet switches are what make these tests worth writing against: the
// EVM, transactions and the native-token fork all turn on at different real
// timestamps, so a test can sit on either side of each one.
const (
	mainnetEnableTx    = uint64(1561791600)
	mainnetEnableEVM   = uint64(1569567600)
	mainnetNativeFork  = uint64(1588291200)
	mainnetChain0Shard = uint32(0x00000001) // chain 0, shard size 1, shard 0
	mainnetChain1Shard = uint32(0x00010001)
	postEVMTimestamp   = mainnetEnableEVM + 1
	beforeEVMTimestamp = mainnetEnableEVM - 1
)

type harness struct {
	t     *testing.T
	ctx   *ExecutionContext
	state *qkcstate.EvmState
	key   *ecdsa.PrivateKey
	from  account.Recipient
}

// newHarness sets up one shard of mainnet at the given timestamp, with a funded
// sender. Mainnet rather than devnet because devnet has every switch at zero,
// so a devnet test cannot sit on one side of a switch — and which side a block
// is on is what most of these tests are about.
func newHarness(t *testing.T, fullShardID uint32, timestamp uint64) *harness {
	t.Helper()
	cluster := network(t, "mainnet")
	shard := cluster.Quarkchain.GetShardConfigByFullShardID(fullShardID)
	if shard == nil {
		t.Fatalf("mainnet has no shard %#x", fullShardID)
	}

	db := rawdb.NewMemoryDatabase()
	state, err := qkcstate.New(coretypes.EmptyRootHash, db, qkcstate.NewDatabase(db))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	state.SetConfig(cluster.Quarkchain, shard)

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	from := crypto.PubkeyToAddress(key.PublicKey)

	state.SetFullShardKey(fullShardID)
	state.DeltaTokenBalance(from, qkccommon.DefaultTokenID, new(big.Int).Mul(big.NewInt(1e18), big.NewInt(100)))
	if _, err := state.Commit(0); err != nil {
		t.Fatalf("commit allocation: %v", err)
	}

	state.SetTimestamp(timestamp)
	state.SetGasLimit(12000000)
	state.SetBlockNumber(1)
	state.SetBlockCoinbase(common.HexToAddress("0x00000000000000000000000000000000000000cc"))
	state.SetBlockDifficulty(big.NewInt(1))

	return &harness{
		t:     t,
		state: state,
		key:   key,
		from:  from,
		ctx: &ExecutionContext{
			QKCConfig:   cluster.Quarkchain,
			ShardConfig: shard,
		},
	}
}

func (h *harness) sign(tx *types.Transaction) *types.Transaction {
	h.t.Helper()
	signer := types.MakeSigner(h.ctx.QKCConfig.NetworkID, h.ctx.ShardConfig.EthChainID)
	signed, err := types.SignTx(tx, signer, h.key)
	if err != nil {
		h.t.Fatalf("sign: %v", err)
	}
	return signed
}

// call builds and applies a plain in-shard transaction.
func (h *harness) call(nonce uint64, to *account.Recipient, value *big.Int, gas uint64, data []byte) (bool, []byte, error) {
	h.t.Helper()
	shardKey := h.ctx.ShardConfig.GetFullShardId()
	var tx *types.Transaction
	if to == nil {
		tx = types.NewEvmContractCreation(nonce, value, gas, big.NewInt(1), shardKey, shardKey,
			h.ctx.QKCConfig.NetworkID, 0, data, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
	} else {
		tx = types.NewEvmTransaction(nonce, *to, value, gas, big.NewInt(1), shardKey, shardKey,
			h.ctx.QKCConfig.NetworkID, 0, data, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
	}
	signed := h.sign(tx)
	return ApplyTransaction(h.ctx, h.state, signed, signed.Hash(), 0)
}

func (h *harness) balance(addr account.Recipient) *big.Int {
	return h.state.GetBalance(addr, qkccommon.DefaultTokenID).ToBig()
}

// TestQKCContractAddressUsesShardKey pins mk_contract_address
// (messages.py:729). The shard key is part of the preimage, so the same sender
// and nonce deploy to different addresses on different shards — the whole point
// of the derivation, and the thing an Ethereum-shaped implementation gets wrong.
func TestQKCContractAddressUsesShardKey(t *testing.T) {
	sender := common.HexToAddress("0x1111111111111111111111111111111111111111")
	a := vm.QKCContractAddress(sender, 1, 0)
	b := vm.QKCContractAddress(sender, 2, 0)
	if a == b {
		t.Fatal("the shard key does not reach the derived address")
	}
	if eth := crypto.CreateAddress(sender, 0); a == eth {
		t.Fatal("the derivation collapsed to Ethereum's")
	}
	// Distinct nonces must still separate, shard key held constant.
	if vm.QKCContractAddress(sender, 1, 1) == a {
		t.Fatal("the nonce does not reach the derived address")
	}
}

// TestTopLevelCreateDoesNotDoubleIncrementNonce covers the nonce timing at
// messages.py:711. apply_transaction has already moved the sender's nonce, so
// create_contract must not move it again — and it derives the address from the
// value *before* that single increment.
func TestTopLevelCreateDoesNotDoubleIncrementNonce(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)

	// PUSH1 0x00 PUSH1 0x00 RETURN: deploys empty code, succeeds.
	initCode := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	success, output, err := h.call(0, nil, big.NewInt(0), 200000, initCode)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !success {
		t.Fatal("create failed")
	}
	if got := h.state.GetNonce(h.from); got != 1 {
		t.Errorf("sender nonce = %d, want 1", got)
	}
	want := vm.QKCContractAddress(h.from, h.ctx.ShardConfig.GetFullShardId(), 0)
	if got := common.BytesToAddress(output); got != want {
		t.Errorf("deployed at %s, want %s", got.Hex(), want.Hex())
	}
	if got := h.state.GetNonce(want); got != 1 {
		t.Errorf("contract nonce = %d, want 1", got)
	}
}

// TestReceiptRecordsContractAddressAndShardKey: the receipt carries the created
// address and the transaction's destination shard key (messages.py:107), two
// fields Ethereum receipts do not have and which the receipt root commits to.
func TestReceiptRecordsContractAddressAndShardKey(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	initCode := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	if _, _, err := h.call(0, nil, big.NewInt(0), 200000, initCode); err != nil {
		t.Fatalf("create: %v", err)
	}
	receipts := h.state.Receipts()
	if len(receipts) != 1 {
		t.Fatalf("%d receipts, want 1", len(receipts))
	}
	want := vm.QKCContractAddress(h.from, h.ctx.ShardConfig.GetFullShardId(), 0)
	if receipts[0].ContractAddress != want {
		t.Errorf("receipt contract address = %s, want %s", receipts[0].ContractAddress.Hex(), want.Hex())
	}
	if receipts[0].ContractFullShardKey != h.ctx.ShardConfig.GetFullShardId() {
		t.Errorf("receipt shard key = %d, want %d", receipts[0].ContractFullShardKey, h.ctx.ShardConfig.GetFullShardId())
	}
}

// TestZeroBalanceNativeTokenIsRefusedBeforeAnyMutation covers
// validate_transaction (4): a foreign token the sender holds none of is
// refused, where the chain's own token is spendable from empty. The refusal has
// to happen before the nonce moves, or a rejected transaction would still be
// visible in the state root.
func TestZeroBalanceNativeTokenIsRefusedBeforeAnyMutation(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	before, err := h.state.Commit(0)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	shardKey := h.ctx.ShardConfig.GetFullShardId()
	foreign := qkccommon.TokenIDEncode("QETC")
	tx := h.sign(types.NewEvmTransaction(0, to, big.NewInt(1), 30000, big.NewInt(1), shardKey, shardKey,
		h.ctx.QKCConfig.NetworkID, 0, nil, qkccommon.DefaultTokenID, foreign))

	if _, _, err := ApplyTransaction(h.ctx, h.state, tx, tx.Hash(), 0); err == nil {
		t.Fatal("a transfer token the sender does not hold was accepted")
	} else if !errorsIs(err, ErrInvalidNativeToken) {
		t.Fatalf("refused with %v, want an invalid-native-token error", err)
	}
	if got := h.state.GetNonce(h.from); got != 0 {
		t.Errorf("nonce moved to %d on a refused transaction", got)
	}
	after, err := h.state.Commit(0)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if after != before {
		t.Errorf("a refused transaction changed the state root: %s, was %s", after, before)
	}
}

func isUnsupportedNativeToken(err error) bool {
	return err != nil && errorsIs(err, ErrUnsupportedNativeToken)
}

// TestCrossShardSourceProducesDeposit covers messages.py:520-543: the value
// leaves, a deposit is queued with the whole remaining allowance reserved for
// the target, and the source's own fee excludes the cross-shard surcharge.
func TestCrossShardSourceProducesDeposit(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	to := common.HexToAddress("0x3333333333333333333333333333333333333333")
	fromKey := h.ctx.ShardConfig.GetFullShardId()
	toKey := mainnetChain1Shard

	tx := h.sign(types.NewEvmTransaction(0, to, big.NewInt(1000), 60000, big.NewInt(2), fromKey, toKey,
		h.ctx.QKCConfig.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID))
	if !tx.IsCrossShard() {
		t.Fatal("the transaction is not cross-shard")
	}

	success, _, err := ApplyTransaction(h.ctx, h.state, tx, tx.Hash(), 0)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !success {
		t.Fatal("the cross-shard source leg failed")
	}

	deposits := h.state.XShardList()
	if len(deposits) != 1 {
		t.Fatalf("%d deposits, want 1", len(deposits))
	}
	deposit := deposits[0]
	intrinsic := IntrinsicGas(tx)
	if want := new(big.Int).SetUint64(tx.Gas() - intrinsic); deposit.GasRemained.Value.Cmp(want) != 0 {
		t.Errorf("gas reserved for the target = %s, want %s", deposit.GasRemained.Value, want)
	}
	if deposit.Value.Value.Cmp(big.NewInt(1000)) != 0 {
		t.Errorf("deposit value = %s, want 1000", deposit.Value.Value)
	}
	if deposit.To.FullShardKey != toKey || deposit.From.FullShardKey != fromKey {
		t.Errorf("deposit shard keys = (%d, %d), want (%d, %d)",
			deposit.From.FullShardKey, deposit.To.FullShardKey, fromKey, toKey)
	}
	if deposit.CreateContract || deposit.IsFromRootChain {
		t.Error("a plain cross-shard transfer is neither a deployment nor a root-chain payout")
	}

	// The source pays for the intrinsic gas minus the surcharge, which is the
	// target shard's to collect.
	if got, want := h.state.GasUsed(), intrinsic-GTXXShardCost; got != want {
		t.Errorf("gas used = %d, want %d", got, want)
	}
	wantFee := localFee(h.state, big.NewInt(2), intrinsic-GTXXShardCost)
	if got := h.state.BlockFeeTokens()[qkccommon.DefaultTokenID]; got == nil || got.ToBig().Cmp(wantFee) != 0 {
		t.Errorf("block fee = %v, want %s", got, wantFee)
	}
}

// TestCrossShardSourceContractCreationReservesGasBeforeEVM: the two cross-shard
// branches reserve gas differently (messages.py:496 against 521). A deployment
// reserves unconditionally; a plain transfer reserves nothing until the EVM is
// switched on. Writing them symmetrically breaks every pre-EVM cross-shard
// deployment.
func TestCrossShardSourceContractCreationReservesGasBeforeEVM(t *testing.T) {
	toKey := mainnetChain1Shard

	deploy := func(timestamp uint64, recipient *account.Recipient) *types.CrossShardTransactionDeposit {
		t.Helper()
		h := newHarness(t, mainnetChain0Shard, timestamp)
		fromKey := h.ctx.ShardConfig.GetFullShardId()
		var tx *types.Transaction
		if recipient == nil {
			tx = types.NewEvmContractCreation(0, big.NewInt(0), 100000, big.NewInt(1), fromKey, toKey,
				h.ctx.QKCConfig.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
		} else {
			tx = types.NewEvmTransaction(0, *recipient, big.NewInt(0), 100000, big.NewInt(1), fromKey, toKey,
				h.ctx.QKCConfig.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
		}
		signed := h.sign(tx)
		if _, _, err := ApplyTransaction(h.ctx, h.state, signed, signed.Hash(), 0); err != nil {
			t.Fatalf("apply: %v", err)
		}
		deposits := h.state.XShardList()
		if len(deposits) != 1 {
			t.Fatalf("%d deposits, want 1", len(deposits))
		}
		return deposits[0]
	}

	to := common.HexToAddress("0x4444444444444444444444444444444444444444")
	if got := deploy(beforeEVMTimestamp, &to).GasRemained.Value; got.Sign() != 0 {
		t.Errorf("a pre-EVM plain transfer reserved %s for the target, want 0", got)
	}
	if got := deploy(beforeEVMTimestamp, nil).GasRemained.Value; got.Sign() == 0 {
		t.Error("a pre-EVM cross-shard deployment reserved nothing for the target")
	}
}

// TestCrossShardTargetPreEVMCreditsRecipientDirectly covers
// shard_state.py:1591-1611, the path that exists only before the EVM switch.
// The recipient is credited straight, its code is not run, no deposit receipt
// appears, and the fee is a flat one.
func TestCrossShardTargetPreEVMCreditsRecipientDirectly(t *testing.T) {
	h := newHarness(t, mainnetChain1Shard, beforeEVMTimestamp)

	// A recipient with code that would revert if it ever ran.
	contract := common.HexToAddress("0x5555555555555555555555555555555555555555")
	h.state.SetFullShardKey(mainnetChain1Shard)
	h.state.SetCode(contract, []byte{0xfd}) // REVERT
	h.state.SetNonce(contract, 1)

	deposit := &types.CrossShardTransactionDeposit{
		TxHash:          common.HexToHash("0xaa"),
		From:            account.NewAddress(h.from, mainnetChain0Shard),
		To:              account.NewAddress(contract, mainnetChain1Shard),
		Value:           &serialize.Uint256{Value: big.NewInt(777)},
		GasPrice:        &serialize.Uint256{Value: big.NewInt(3)},
		GasTokenID:      qkccommon.DefaultTokenID,
		TransferTokenID: qkccommon.DefaultTokenID,
		GasRemained:     &serialize.Uint256{Value: big.NewInt(0)},
		RefundRate:      100,
	}
	if err := RunOneXShardTx(h.ctx, h.state, deposit, true, 0); err != nil {
		t.Fatalf("run deposit: %v", err)
	}

	if got := h.balance(contract); got.Cmp(big.NewInt(777)) != 0 {
		t.Errorf("recipient balance = %s, want 777 credited directly", got)
	}
	if len(h.state.XShardDepositReceipts()) != 0 {
		t.Error("a pre-EVM deposit produced a receipt")
	}
	if got, want := h.state.GasUsed(), GTXXShardCost; got != want {
		t.Errorf("gas used = %d, want the flat %d", got, want)
	}
	wantFee := localFee(h.state, big.NewInt(3), GTXXShardCost)
	if got := h.state.BlockFeeTokens()[qkccommon.DefaultTokenID]; got == nil || got.ToBig().Cmp(wantFee) != 0 {
		t.Errorf("fee = %v, want the flat %s", got, wantFee)
	}
}

// TestPreEVMDepositDoesNotRestampTheShardKey pins the FIXME at
// shard_state.py:1594. An account brought into existence by a pre-EVM
// cross-shard credit carries the shard key the state happened to hold, which
// through the cross-shard phase is the default zero — not the recipient's own.
// "Fixing" it changes the account leaf and so the state root.
func TestPreEVMDepositDoesNotRestampTheShardKey(t *testing.T) {
	h := newHarness(t, mainnetChain1Shard, beforeEVMTimestamp)
	h.state.SetFullShardKey(0)

	fresh := common.HexToAddress("0x0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b")
	deposit := &types.CrossShardTransactionDeposit{
		TxHash:          common.HexToHash("0xee"),
		From:            account.NewAddress(h.from, mainnetChain0Shard),
		To:              account.NewAddress(fresh, mainnetChain1Shard),
		Value:           &serialize.Uint256{Value: big.NewInt(9)},
		GasPrice:        &serialize.Uint256{Value: big.NewInt(1)},
		GasTokenID:      qkccommon.DefaultTokenID,
		TransferTokenID: qkccommon.DefaultTokenID,
		GasRemained:     &serialize.Uint256{Value: big.NewInt(0)},
		RefundRate:      100,
	}
	if err := RunOneXShardTx(h.ctx, h.state, deposit, true, 0); err != nil {
		t.Fatalf("run deposit: %v", err)
	}
	if got := h.state.GetFullShardKey(fresh); got != 0 {
		t.Errorf("the credited account's shard key = %d, want the state's default 0", got)
	}
}

// TestCrossShardTargetPostEVMStrandsFundsOnFailure is the semantics at
// messages.py:368-377 that must not be "optimised" into a direct credit: the
// value is given to the sender's account *on this shard* first, so when the
// message fails the funds stay there rather than going back to the source shard
// or to the intended recipient.
func TestCrossShardTargetPostEVMStrandsFundsOnFailure(t *testing.T) {
	h := newHarness(t, mainnetChain1Shard, postEVMTimestamp)

	contract := common.HexToAddress("0x6666666666666666666666666666666666666666")
	h.state.SetFullShardKey(mainnetChain1Shard)
	h.state.SetCode(contract, []byte{0xfd}) // REVERT
	h.state.SetNonce(contract, 1)

	stranded := common.HexToAddress("0x7777777777777777777777777777777777777777")
	deposit := &types.CrossShardTransactionDeposit{
		TxHash:          common.HexToHash("0xbb"),
		From:            account.NewAddress(stranded, mainnetChain0Shard),
		To:              account.NewAddress(contract, mainnetChain1Shard),
		Value:           &serialize.Uint256{Value: big.NewInt(555)},
		GasPrice:        &serialize.Uint256{Value: big.NewInt(1)},
		GasTokenID:      qkccommon.DefaultTokenID,
		TransferTokenID: qkccommon.DefaultTokenID,
		GasRemained:     &serialize.Uint256{Value: big.NewInt(50000)},
		RefundRate:      100,
	}
	if err := RunOneXShardTx(h.ctx, h.state, deposit, true, 0); err != nil {
		t.Fatalf("run deposit: %v", err)
	}

	if got := h.balance(contract); got.Sign() != 0 {
		t.Errorf("the recipient received %s from a failed deposit", got)
	}
	if got := h.balance(stranded); got.Cmp(big.NewInt(555)) != 0 {
		t.Errorf("sender's balance on the target shard = %s, want the stranded 555", got)
	}
	receipts := h.state.XShardDepositReceipts()
	if len(receipts) != 1 {
		t.Fatalf("%d deposit receipts, want 1", len(receipts))
	}
	if receipts[0].Status != types.ReceiptStatusFailed {
		t.Error("the deposit receipt reports success for a reverted message")
	}
}

// TestCrossShardTargetEVMSwitchBoundary walks the three timestamps around
// ENABLE_EVM_TIMESTAMP. The gate is `<`, unlike the strict `>` that gates the
// precompiles, so the switch second already takes the post-EVM path.
func TestCrossShardTargetEVMSwitchBoundary(t *testing.T) {
	for _, tc := range []struct {
		name        string
		timestamp   uint64
		wantReceipt int
	}{
		{"one second before", mainnetEnableEVM - 1, 0},
		{"exactly at", mainnetEnableEVM, 1},
		{"one second after", mainnetEnableEVM + 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, mainnetChain1Shard, tc.timestamp)
			to := common.HexToAddress("0x8888888888888888888888888888888888888888")
			deposit := &types.CrossShardTransactionDeposit{
				TxHash:          common.HexToHash("0xcc"),
				From:            account.NewAddress(h.from, mainnetChain0Shard),
				To:              account.NewAddress(to, mainnetChain1Shard),
				Value:           &serialize.Uint256{Value: big.NewInt(10)},
				GasPrice:        &serialize.Uint256{Value: big.NewInt(1)},
				GasTokenID:      qkccommon.DefaultTokenID,
				TransferTokenID: qkccommon.DefaultTokenID,
				GasRemained:     &serialize.Uint256{Value: big.NewInt(0)},
				RefundRate:      100,
			}
			if err := RunOneXShardTx(h.ctx, h.state, deposit, true, 0); err != nil {
				t.Fatalf("run deposit: %v", err)
			}
			if got := len(h.state.XShardDepositReceipts()); got != tc.wantReceipt {
				t.Errorf("%d deposit receipts, want %d", got, tc.wantReceipt)
			}
			if got := h.state.GetBalance(to, qkccommon.DefaultTokenID).ToBig(); got.Cmp(big.NewInt(10)) != 0 {
				t.Errorf("recipient balance = %s, want 10", got)
			}
		})
	}
}

// TestDepositWithForeignTokenAndPartialRefund: a deposit priced in a foreign
// gas token arrives with the refund rate the source shard's sale set. The
// recipient is credited in the transfer token, the unused gas is refunded at
// that rate, and the remainder is burned to the zero address (messages.py:268).
func TestDepositWithForeignTokenAndPartialRefund(t *testing.T) {
	h := newHarness(t, mainnetChain1Shard, postEVMTimestamp)
	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	foreign := qkccommon.TokenIDEncode("QETC")

	deposit := &types.CrossShardTransactionDeposit{
		TxHash:          common.HexToHash("0xdd"),
		From:            account.NewAddress(from, mainnetChain0Shard),
		To:              account.NewAddress(to, mainnetChain1Shard),
		Value:           &serialize.Uint256{Value: big.NewInt(1000)},
		GasPrice:        &serialize.Uint256{Value: big.NewInt(2)},
		GasTokenID:      qkccommon.DefaultTokenID,
		TransferTokenID: foreign,
		GasRemained:     &serialize.Uint256{Value: big.NewInt(30000)},
		RefundRate:      80,
	}

	if err := RunOneXShardTx(h.ctx, h.state, deposit, true, 0); err != nil {
		t.Fatalf("deposit refused: %v", err)
	}
	if got := h.state.GetBalance(to, foreign).ToBig(); got.Cmp(big.NewInt(1000)) != 0 {
		t.Errorf("recipient holds %s of the foreign token, want 1000", got)
	}
	// The whole 30000 is unspent: nothing but the transfer ran. At price 2 that
	// is 60000 to split, 80%% back to the sender and the rest burned.
	if got := h.state.GetBalance(from, qkccommon.DefaultTokenID).ToBig(); got.Cmp(big.NewInt(48000)) != 0 {
		t.Errorf("sender refunded %s, want 48000", got)
	}
	if got := h.state.GetBalance(common.Address{}, qkccommon.DefaultTokenID).ToBig(); got.Cmp(big.NewInt(12000)) != 0 {
		t.Errorf("burned %s, want 12000", got)
	}
	if len(h.state.XShardDepositReceipts()) != 1 {
		t.Errorf("%d deposit receipts, want 1", len(h.state.XShardDepositReceipts()))
	}
}

// TestPOSWDisallowMapBlocksTransfer: the map is the one rule that fails
// quietly if it is not built, so this checks it both ways — the same transfer
// succeeds with an empty map and fails with a populated one.
func TestPOSWDisallowMapBlocksTransfer(t *testing.T) {
	run := func(locked bool) bool {
		t.Helper()
		h := newHarness(t, mainnetChain1Shard, postEVMTimestamp)
		if locked {
			// Lock more than the sender holds.
			h.state.SetSenderDisallowMap(map[account.Recipient]*big.Int{
				h.from: new(big.Int).Mul(big.NewInt(1e18), big.NewInt(1000)),
			})
		}
		to := common.HexToAddress("0x9999999999999999999999999999999999999999")
		success, _, err := h.call(0, &to, big.NewInt(1000), 30000, nil)
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
		return success
	}
	if !run(false) {
		t.Fatal("the transfer failed with no stake locked")
	}
	if run(true) {
		t.Fatal("a locked sender was allowed to spend")
	}
}

// TestSenderDisallowMapCountsCandidateCoinbase: the block being produced counts
// towards its own producer's stake (shard_state.py:1994), so a shard with POSW
// on always bars its own coinbase by at least one block's worth.
func TestSenderDisallowMapCountsCandidateCoinbase(t *testing.T) {
	cluster := network(t, "mainnet")
	shard := cluster.Quarkchain.GetShardConfigByFullShardID(mainnetChain1Shard)
	coinbase := common.HexToAddress("0x00000000000000000000000000000000000000c1")
	parent := &types.MinorBlockHeader{
		Number:   0,
		Time:     postEVMTimestamp,
		Coinbase: account.NewAddress(common.HexToAddress("0x00000000000000000000000000000000000000c2"), mainnetChain1Shard),
	}
	ctx := &ExecutionContext{
		QKCConfig:   cluster.Quarkchain,
		ShardConfig: shard,
		MinorHeaderByHash: func(common.Hash) (*types.MinorBlockHeader, error) {
			return parent, nil
		},
	}
	got, err := SenderDisallowMap(ctx, parent, &coinbase)
	if err != nil {
		t.Fatalf("disallow map: %v", err)
	}
	if got[coinbase] == nil || got[coinbase].Cmp(shard.PoswConfig.TotalStakePerBlock) != 0 {
		t.Errorf("candidate coinbase locked %v, want one block's stake %s", got[coinbase], shard.PoswConfig.TotalStakePerBlock)
	}
	if got[parent.Coinbase.Recipient] == nil {
		t.Error("the parent's producer is not in the map")
	}
}

// TestPOSWDisabledShardHasEmptyMap: chain 0 has POSW off on mainnet, and an
// empty map is what lets its transfers through.
func TestPOSWDisabledShardHasEmptyMap(t *testing.T) {
	cluster := network(t, "mainnet")
	shard := cluster.Quarkchain.GetShardConfigByFullShardID(mainnetChain0Shard)
	parent := &types.MinorBlockHeader{Number: 0, Time: postEVMTimestamp}
	ctx := &ExecutionContext{QKCConfig: cluster.Quarkchain, ShardConfig: shard}
	got, err := SenderDisallowMap(ctx, parent, nil)
	if err != nil {
		t.Fatalf("disallow map: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("chain 0 produced a %d-entry map with POSW disabled", len(got))
	}
}

// TestValidateTxForBlockGates covers the switches __validate_tx reads
// (shard_state.py:501-518), each at the timestamp where its behaviour changes.
func TestValidateTxForBlockGates(t *testing.T) {
	t.Run("contract transactions refused before the EVM switch", func(t *testing.T) {
		h := newHarness(t, mainnetChain0Shard, beforeEVMTimestamp)
		shardKey := h.ctx.ShardConfig.GetFullShardId()
		tx := h.sign(types.NewEvmContractCreation(0, big.NewInt(0), 100000, big.NewInt(1), shardKey, shardKey,
			h.ctx.QKCConfig.NetworkID, 0, []byte{0x00}, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID))
		if _, err := ValidateTxForBlock(h.ctx, h.state, tx, 6000000); err == nil {
			t.Fatal("a deployment was accepted before the EVM was enabled")
		}
	})

	t.Run("unwhitelisted senders refused before transactions are enabled", func(t *testing.T) {
		h := newHarness(t, mainnetChain0Shard, mainnetEnableTx-1)
		to := common.HexToAddress("0xabababababababababababababababababababab")
		shardKey := h.ctx.ShardConfig.GetFullShardId()
		tx := h.sign(types.NewEvmTransaction(0, to, big.NewInt(1), 30000, big.NewInt(1), shardKey, shardKey,
			h.ctx.QKCConfig.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID))
		if _, err := ValidateTxForBlock(h.ctx, h.state, tx, 6000000); err == nil {
			t.Fatal("an unwhitelisted sender was accepted before transactions were enabled")
		}
	})

	t.Run("wrong network id refused", func(t *testing.T) {
		h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
		to := common.HexToAddress("0xabababababababababababababababababababab")
		shardKey := h.ctx.ShardConfig.GetFullShardId()
		tx := types.NewEvmTransaction(0, to, big.NewInt(1), 30000, big.NewInt(1), shardKey, shardKey,
			h.ctx.QKCConfig.NetworkID+1, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
		signer := types.MakeSigner(h.ctx.QKCConfig.NetworkID+1, h.ctx.ShardConfig.EthChainID)
		signed, err := types.SignTx(tx, signer, h.key)
		if err != nil {
			t.Fatalf("sign: %v", err)
		}
		if _, err := ValidateTxForBlock(h.ctx, h.state, signed, 6000000); err == nil {
			t.Fatal("a transaction for another network was accepted")
		}
	})

	t.Run("future nonce skips validate_transaction", func(t *testing.T) {
		// A nonce inside the future window returns early, so the balance is
		// never checked here — the transaction is left for apply_transaction to
		// reject. Collapsing the two passes into one would refuse it now.
		h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
		to := common.HexToAddress("0xabababababababababababababababababababab")
		shardKey := h.ctx.ShardConfig.GetFullShardId()
		huge := new(big.Int).Mul(big.NewInt(1e18), big.NewInt(1e6))
		tx := h.sign(types.NewEvmTransaction(1, to, huge, 30000, big.NewInt(1), shardKey, shardKey,
			h.ctx.QKCConfig.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID))
		if _, err := ValidateTxForBlock(h.ctx, h.state, tx, 6000000); err != nil {
			t.Fatalf("a future-nonce transaction was refused early: %v", err)
		}
		if _, _, err := ApplyTransaction(h.ctx, h.state, tx, tx.Hash(), 0); err == nil {
			t.Fatal("apply_transaction accepted it too")
		}
	})
}

// TestCrossShardGasLimitRefusesOversizedTransaction: shard_state.py:499 caps a
// cross-shard transaction's start gas at the block's cross-shard allowance.
func TestCrossShardGasLimitRefusesOversizedTransaction(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	to := common.HexToAddress("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd")
	tx := h.sign(types.NewEvmTransaction(0, to, big.NewInt(1), 5_000_000, big.NewInt(1),
		h.ctx.ShardConfig.GetFullShardId(), mainnetChain1Shard,
		h.ctx.QKCConfig.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID))
	if _, err := ValidateTxForBlock(h.ctx, h.state, tx, 1_000_000); err == nil {
		t.Fatal("a cross-shard transaction above the cross-shard limit was accepted")
	}
}

// TestIntrinsicGas pins transactions.py:213, including the cross-shard
// surcharge and the per-byte split that differs from post-Istanbul Ethereum.
func TestIntrinsicGas(t *testing.T) {
	shardKey := mainnetChain0Shard
	plain := types.NewEvmTransaction(0, common.HexToAddress("0x01"), big.NewInt(0), 0, big.NewInt(0),
		shardKey, shardKey, 1, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
	if got := IntrinsicGas(plain); got != GTXCost {
		t.Errorf("plain transfer = %d, want %d", got, GTXCost)
	}

	withData := types.NewEvmTransaction(0, common.HexToAddress("0x01"), big.NewInt(0), 0, big.NewInt(0),
		shardKey, shardKey, 1, 0, []byte{0x00, 0x01}, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
	if got, want := IntrinsicGas(withData), GTXCost+GTXDataZero+GTXDataNonZero; got != want {
		t.Errorf("with data = %d, want %d", got, want)
	}

	creation := types.NewEvmContractCreation(0, big.NewInt(0), 0, big.NewInt(0), shardKey, shardKey,
		1, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
	if got, want := IntrinsicGas(creation), GTXCost+CreateContractGas; got != want {
		t.Errorf("creation = %d, want %d", got, want)
	}

	crossShard := types.NewEvmTransaction(0, common.HexToAddress("0x01"), big.NewInt(0), 0, big.NewInt(0),
		shardKey, mainnetChain1Shard, 1, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
	if got, want := IntrinsicGas(crossShard), GTXCost+GTXXShardCost; got != want {
		t.Errorf("cross-shard = %d, want %d", got, want)
	}
}

// TestIsNeighbor pins neighbor.py:5, including the branch that makes every
// shard a neighbour at the sizes QuarkChain actually runs.
func TestIsNeighbor(t *testing.T) {
	b := func(chain, shard uint32) account.Branch {
		return account.NewBranch(chain<<16 | 64 | shard)
	}
	if !IsNeighbor(b(0, 0), b(7, 13), 32) {
		t.Error("at 32 shards every branch is a neighbour")
	}
	if !IsNeighbor(b(0, 0), b(0, 4), 64) {
		t.Error("same chain, shard distance 4 is a power of two")
	}
	if IsNeighbor(b(0, 0), b(0, 3), 64) {
		t.Error("same chain, shard distance 3 is not a power of two")
	}
	if IsNeighbor(b(0, 1), b(3, 2), 64) {
		t.Error("different chain and different shard are never neighbours")
	}
}

// TestLocalFeeTruncatesTaxAway: the root chain's share is not credited to any
// account, so the shard's fee is a truncating division and the remainder simply
// vanishes from the balance sheet (messages.py:336).
func TestLocalFeeTruncatesTaxAway(t *testing.T) {
	cluster := network(t, "mainnet")
	db := rawdb.NewMemoryDatabase()
	state, err := qkcstate.New(coretypes.EmptyRootHash, db, qkcstate.NewDatabase(db))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	state.SetConfig(cluster.Quarkchain, cluster.Quarkchain.GetShardConfigByFullShardID(mainnetChain0Shard))
	// The mainnet tax is one half, so an odd fee loses its remainder.
	if got := localFee(state, big.NewInt(1), 3); got.Cmp(big.NewInt(1)) != 0 {
		t.Errorf("local fee of 3 at price 1 = %s, want 1", got)
	}
}

// errorsIs keeps the test file from importing errors just for one call.
func errorsIs(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}

// TestTokenTrieBoundaryReachesTheCaller is the one representation this package
// does not implement: past the account leaf's sixteen-token list pyquarkchain
// switches the balances to a secure trie. The write has to abandon the block
// rather than commit a leaf no consumer can read, so the error must come back
// out of ApplyTransaction instead of turning into a failed receipt.
func TestTokenTrieBoundaryReachesTheCaller(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, mainnetNativeFork+1)
	recipient := common.HexToAddress("0x6666666666666666666666666666666666666666")
	// Fill the recipient's list, then send it one more token.
	for i := uint64(0); i < qkccommon.TokenTrieThreshold; i++ {
		h.state.DeltaTokenBalance(recipient, qkccommon.TokenIDEncode("QET")+i, big.NewInt(1))
	}
	overflowing := qkccommon.TokenIDEncode("QOVER")
	h.state.DeltaTokenBalance(h.from, overflowing, big.NewInt(100))

	shardKey := h.ctx.ShardConfig.GetFullShardId()
	tx := h.sign(types.NewEvmTransaction(0, recipient, big.NewInt(1), 30000, big.NewInt(1),
		shardKey, shardKey, h.ctx.QKCConfig.NetworkID, 0, nil,
		qkccommon.DefaultTokenID, overflowing))

	_, _, err := ApplyTransaction(h.ctx, h.state, tx, tx.Hash(), 0)
	if err == nil {
		t.Fatal("a seventeenth token was written")
	}
	if !isUnsupportedNativeToken(err) {
		t.Fatalf("refused with %v, want the token-trie boundary", err)
	}
}

// TestXShardDepositRefusesARefundRateAboveOneHundred marks the boundary the
// deposit's own byte allows but the profile does not.
//
// Upstream checks the rate at neither end: _refund would hand the sender more
// than it paid and burn a negative amount, which the account layer neither
// refuses nor represents. The general native token manager cannot answer with
// such a rate, so no deposit carrying one exists to replay, and refusing the
// block is what keeps this fork from being the only one that settles it.
func TestXShardDepositRefusesARefundRateAboveOneHundred(t *testing.T) {
	to := common.HexToAddress("0x00000000000000000000000000000000000000dd")
	deposit := func(rate uint8) *types.CrossShardTransactionDeposit {
		return &types.CrossShardTransactionDeposit{
			TxHash:          common.HexToHash("0x01"),
			From:            account.NewAddress(common.HexToAddress("0x01"), mainnetChain0Shard),
			To:              account.NewAddress(to, mainnetChain0Shard),
			Value:           &serialize.Uint256{Value: big.NewInt(1000)},
			GasPrice:        &serialize.Uint256{Value: big.NewInt(1)},
			GasTokenID:      qkccommon.DefaultTokenID,
			TransferTokenID: qkccommon.DefaultTokenID,
			GasRemained:     &serialize.Uint256{Value: big.NewInt(30000)},
			RefundRate:      rate,
		}
	}

	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	if _, _, err := ApplyXShardDeposit(h.ctx, h.state, deposit(101), 0, 0); err == nil {
		t.Fatal("a refund rate above 100 was settled")
	}
	if _, _, err := ApplyXShardDeposit(h.ctx, h.state, deposit(100), 0, 0); err != nil {
		t.Fatalf("the highest legal rate was refused: %v", err)
	}
}
