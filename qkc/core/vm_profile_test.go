// Copyright 2026-2027, QuarkChain.

package core

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/holiman/uint256"
)

// deployCode wraps runtime bytecode in the usual CODECOPY/RETURN preamble, so a
// test can name the code it wants deployed instead of hand-assembling init
// code each time. The preamble is twelve bytes, which is where the runtime
// starts.
func deployCode(runtime []byte) []byte {
	n := byte(len(runtime))
	preamble := []byte{
		0x60, n, // PUSH1 len
		0x60, 0x0c, // PUSH1 12 — offset of the runtime within this code
		0x60, 0x00, // PUSH1 0
		0x39,    // CODECOPY
		0x60, n, // PUSH1 len
		0x60, 0x00, // PUSH1 0
		0xf3, // RETURN
	}
	return append(preamble, runtime...)
}

// deploy puts runtime code on the shard and returns its address.
func (h *harness) deploy(nonce uint64, runtime []byte) account.Recipient {
	h.t.Helper()
	success, output, err := h.call(nonce, nil, big.NewInt(0), 500000, deployCode(runtime))
	if err != nil {
		h.t.Fatalf("deploy: %v", err)
	}
	if !success {
		h.t.Fatal("deployment failed")
	}
	addr := common.BytesToAddress(output)
	if len(h.state.GetCode(addr)) != len(runtime) {
		h.t.Fatalf("deployed %d bytes of code, want %d", len(h.state.GetCode(addr)), len(runtime))
	}
	return addr
}

// TestContractLogsReachTheReceiptAndBloom: a LOG has to land in the
// transaction's receipt and in the bloom the block header commits to.
func TestContractLogsReachTheReceiptAndBloom(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	// PUSH1 0x2a PUSH1 0x00 MSTORE PUSH1 0x20 PUSH1 0x00 LOG0 STOP
	logger := h.deploy(0, []byte{0x60, 0x2a, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xa0, 0x00})

	success, _, err := h.call(1, &logger, big.NewInt(0), 200000, nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !success {
		t.Fatal("the call failed")
	}
	receipts := h.state.Receipts()
	receipt := receipts[len(receipts)-1]
	if len(receipt.Logs) != 1 {
		t.Fatalf("%d logs on the receipt, want 1", len(receipt.Logs))
	}
	if receipt.Logs[0].Address != logger {
		t.Errorf("log address = %s, want %s", receipt.Logs[0].Address.Hex(), logger.Hex())
	}
	if receipt.Bloom == (types.Bloom{}) {
		t.Error("the receipt bloom is empty despite a log")
	}
	if h.state.Bloom() == (types.Bloom{}) {
		t.Error("the block bloom is empty despite a log")
	}
}

// TestSelfDestructRefundIsCappedAtHalfTheGas covers messages.py:322-327: the
// refund can only ever return half of what the transaction used, and the
// destroyed account is gone at the end of the block.
func TestSelfDestructRefundIsCappedAtHalfTheGas(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	beneficiary := common.HexToAddress("0x0505050505050505050505050505050505050505")
	// PUSH20 <beneficiary> SELFDESTRUCT
	runtime := append([]byte{0x73}, beneficiary.Bytes()...)
	runtime = append(runtime, 0xff)
	victim := h.deploy(0, runtime)

	// Fund the victim so the self-destruct has something to move.
	h.state.SetFullShardKey(h.ctx.ShardConfig.GetFullShardId())
	h.state.DeltaTokenBalance(victim, qkccommon.DefaultTokenID, big.NewInt(4242))

	gasBefore := h.state.GasUsed()
	success, _, err := h.call(1, &victim, big.NewInt(0), 200000, nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !success {
		t.Fatal("the self-destruct call failed")
	}
	used := h.state.GasUsed() - gasBefore
	// The refund is capped at half, so the transaction can never come out
	// below the intrinsic cost halved.
	if used < GTXCost/2 {
		t.Errorf("gas used %d fell below half the intrinsic cost, so the cap did not apply", used)
	}
	if got := h.state.GetBalance(beneficiary, qkccommon.DefaultTokenID).Uint64(); got != 4242 {
		t.Errorf("beneficiary balance = %d, want the victim's 4242", got)
	}
	if _, err := h.state.Commit(1); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if h.state.Exists(victim) {
		t.Error("the self-destructed contract survived the block")
	}
}

// TestNestedCreateIncrementsCreatorNonce is the other half of the nonce timing
// rule: a CREATE made from inside a contract *does* move the creator's nonce,
// because tx_origin is not the creator (messages.py:711).
func TestNestedCreateIncrementsCreatorNonce(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	// A factory whose runtime does: PUSH1 0 PUSH1 0 PUSH1 0 CREATE, then stops.
	// The child has empty init code, which deploys an empty contract.
	factory := h.deploy(0, []byte{0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0xf0, 0x00})
	if got := h.state.GetNonce(factory); got != 1 {
		t.Fatalf("a fresh contract's nonce = %d, want 1", got)
	}

	success, _, err := h.call(1, &factory, big.NewInt(0), 300000, nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !success {
		t.Fatal("the factory call failed")
	}
	if got := h.state.GetNonce(factory); got != 2 {
		t.Errorf("factory nonce = %d, want 2 after a nested CREATE", got)
	}
	// The child address is derived from the nonce *before* the increment, with
	// the frame's shard key.
	child := vm.QKCContractAddress(factory, h.ctx.ShardConfig.GetFullShardId(), 1)
	if got := h.state.GetNonce(child); got != 1 {
		t.Errorf("child contract nonce = %d, want 1 — the address derivation is off", got)
	}
}

// TestCreate2MatchesEthereum: CREATE2 is the one derivation QuarkChain did not
// change, so it must stay byte-identical to Ethereum's.
func TestCreate2MatchesEthereum(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	// PUSH1 0 (salt) PUSH1 0 (size) PUSH1 0 (offset) PUSH1 0 (value) CREATE2 STOP
	factory := h.deploy(0, []byte{0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0xf5, 0x00})
	if _, _, err := h.call(1, &factory, big.NewInt(0), 300000, nil); err != nil {
		t.Fatalf("call: %v", err)
	}
	want := crypto.CreateAddress2(factory, common.Hash{}, crypto.Keccak256(nil))
	if got := h.state.GetNonce(want); got != 1 {
		t.Errorf("no contract at the Ethereum CREATE2 address %s", want.Hex())
	}
}

// TestCurrentMntIDPrecompile pins proc_current_mnt_id (specials.py:239) and the
// timestamp comparison that gates it. The gate is strict — `block_timestamp >
// enable_ts` — so at exactly the enable second the address is still an ordinary
// empty account, and the call returns nothing at all.
func TestCurrentMntIDPrecompile(t *testing.T) {
	for _, tc := range []struct {
		name       string
		timestamp  uint64
		wantOutput bool
	}{
		{"one second before enabling", mainnetEnableEVM - 1, false},
		{"exactly at enabling", mainnetEnableEVM, false},
		{"one second after enabling", mainnetEnableEVM + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, mainnetChain0Shard, tc.timestamp)
			to := vm.QKCCurrentMntIDAddress
			success, output, err := h.call(0, &to, big.NewInt(0), 100000, nil)
			if err != nil {
				t.Fatalf("call: %v", err)
			}
			if !success {
				t.Fatal("the call failed")
			}
			if !tc.wantOutput {
				if len(output) != 0 {
					t.Errorf("the precompile answered %x before it was enabled", output)
				}
				return
			}
			want := new(uint256.Int).SetUint64(qkccommon.DefaultTokenID).Bytes32()
			if string(output) != string(want[:]) {
				t.Errorf("output = %x, want the current token id %x", output, want)
			}
		})
	}
}

// TestTransferMntPrecompileMovesANonDefaultToken covers proc_transfer_mnt
// (specials.py:247). Native tokens are closed at the public entry points, but
// not inside the VM: this precompile is observable throughout the supported
// window and has to move any token id the caller names.
func TestTransferMntPrecompileMovesANonDefaultToken(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	foreign := qkccommon.TokenIDEncode("QETC")
	h.state.SetFullShardKey(h.ctx.ShardConfig.GetFullShardId())
	h.state.SetTokenBalance(h.from, foreign, uint256.NewInt(5000))

	recipient := common.HexToAddress("0x0606060606060606060606060606060606060606")
	data := transferMntCalldata(recipient, foreign, 1500, nil)

	to := vm.QKCTransferMntAddress
	success, _, err := h.call(0, &to, big.NewInt(0), 200000, data)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !success {
		t.Fatal("the transfer precompile failed")
	}
	if got := h.state.GetBalance(recipient, foreign).Uint64(); got != 1500 {
		t.Errorf("recipient holds %d of the foreign token, want 1500", got)
	}
	if got := h.state.GetBalance(h.from, foreign).Uint64(); got != 3500 {
		t.Errorf("sender holds %d of the foreign token, want 3500", got)
	}
}

// TestNonDefaultTokenIntoUnawareCodeFails is the guard at messages.py:684. Code
// written before native tokens existed reads its value as if it were QKC, so a
// frame that moves a different token into code that never asked which token it
// is holding must fail.
func TestNonDefaultTokenIntoUnawareCodeFails(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	foreign := qkccommon.TokenIDEncode("QETC")
	h.state.SetFullShardKey(h.ctx.ShardConfig.GetFullShardId())
	h.state.SetTokenBalance(h.from, foreign, uint256.NewInt(5000))

	// A contract that does nothing but stop — it never queries the token id.
	unaware := h.deploy(0, []byte{0x00})
	data := transferMntCalldata(unaware, foreign, 100, nil)

	to := vm.QKCTransferMntAddress
	_, _, err := h.call(1, &to, big.NewInt(0), 200000, data)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := h.state.GetBalance(unaware, foreign).Uint64(); got != 0 {
		t.Errorf("token-unaware code received %d of a foreign token", got)
	}
}

// TestDeploySystemContractPrecompile covers proc_deploy_system_contract
// (specials.py:290) for the one system contract inside the supported window.
func TestDeploySystemContractPrecompile(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	to := vm.QKCDeploySystemContractAddress
	index := new(uint256.Int).SetUint64(systemContractRootChainPoSW).Bytes32()

	success, _, err := h.call(0, &to, big.NewInt(0), 3000000, index[:])
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !success {
		t.Fatal("deploying the root-chain proof-of-staked-work contract failed")
	}
	if len(h.state.GetCode(rootChainPoSWContractAddress)) == 0 {
		t.Error("no code at the root-chain proof-of-staked-work address")
	}
}

// TestDeploySystemContractRefusesFutureOnes: the other two system contracts
// only unlock at the native-token fork, so inside the window they must refuse
// rather than deploy something the rest of this package cannot execute against.
func TestDeploySystemContractRefusesFutureOnes(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	to := vm.QKCDeploySystemContractAddress
	index := new(uint256.Int).SetUint64(systemContractGeneralNativeToken).Bytes32()

	success, _, err := h.call(0, &to, big.NewInt(0), 3000000, index[:])
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if success {
		t.Fatal("a post-fork system contract was deployed inside the supported window")
	}
	if len(h.state.GetCode(generalNativeTokenContractAddress)) != 0 {
		t.Error("code appeared at the general-native-token address")
	}
}

// transferMntCalldata lays out proc_transfer_mnt's four arguments: recipient,
// token id, value, then the message body.
func transferMntCalldata(to account.Recipient, tokenID uint64, value uint64, body []byte) []byte {
	data := make([]byte, 96, 96+len(body))
	copy(data[12:32], to.Bytes())
	tokenWord := new(uint256.Int).SetUint64(tokenID).Bytes32()
	copy(data[32:64], tokenWord[:])
	valueWord := new(uint256.Int).SetUint64(value).Bytes32()
	copy(data[64:96], valueWord[:])
	return append(data, body...)
}
