// Copyright 2026-2027, QuarkChain.

package core

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/holiman/uint256"
)

// Cases ported from pyquarkchain's quarkchain/cluster/tests/test_shard_state.py
// that the rest of this package's tests did not already cover. Only the
// execution half is ported: the file's fork resolution, chain recovery,
// transaction queue and RPC cases belong to the chain layer.

// TestXShardSurchargeAcrossDDOSFix is test_xshard_tx_received_ddos_fix. The
// surcharge a received deposit pays used to depend on its gas price, so a
// zero-priced deposit was executed for free; from XSHARD_GAS_DDOS_FIX_ROOT_HEIGHT
// onwards every deposit that did not come from the root chain pays it
// (shard_state.py:1585).
func TestXShardSurchargeAcrossDDOSFix(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rootHeight uint32
		wantGas    uint64
	}{
		{"before the fix a free deposit costs nothing", 2, 0},
		{"from the fix onwards it pays the surcharge", 90000, GTXXShardCost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)
			recipient := common.HexToAddress("0x7777777777777777777777777777777777777777")

			// The traversal starts where the parent stopped, so the parent is
			// given a cursor just below the height under test rather than the
			// whole root chain being built up to it.
			start := tc.rootHeight - 1
			f.startCursorAt(start)

			// The sender's shard only has permission to send from a root height
			// above this shard's genesis root height, so the deposit rides on a
			// header pointing at the root block before the one that confirms it.
			previous := &types.RootBlockHeader{
				Number:          start,
				ParentHash:      f.src.roots[0].Header.Hash(),
				Coinbase:        account.NewAddress(common.HexToAddress("0x02"), mainnetChain1Shard),
				CoinbaseAmount:  qkccommon.NewEmptyTokenBalances(),
				Difficulty:      big.NewInt(1),
				TotalDifficulty: big.NewInt(2),
			}
			remote := &types.MinorBlockHeader{
				Branch:            account.NewBranch(mainnetChain1Shard),
				Number:            1,
				Coinbase:          account.NewAddress(common.HexToAddress("0x01"), mainnetChain1Shard),
				PrevRootBlockHash: previous.Hash(),
				GasLimit:          u256(12000000),
				Difficulty:        big.NewInt(1),
			}
			// Gas price zero is the whole point: before the fix that is what
			// let the surcharge be skipped.
			f.src.deposits[remote.Hash()] = []*types.CrossShardTransactionDeposit{
				newFixtureDeposit(recipient, 500, f.fullShard),
			}
			f.src.present[remote.Hash()] = true

			last := &types.RootBlockHeader{
				Number:          tc.rootHeight,
				ParentHash:      previous.Hash(),
				Coinbase:        account.NewAddress(common.HexToAddress("0x02"), mainnetChain1Shard),
				CoinbaseAmount:  qkccommon.NewEmptyTokenBalances(),
				Difficulty:      big.NewInt(1),
				TotalDifficulty: big.NewInt(3),
			}
			f.src.setRoot(&types.RootBlock{Header: previous})
			f.src.setRoot(&types.RootBlock{
				Header: last, MinorBlockHeaders: []*types.MinorBlockHeader{remote},
			})

			block := f.childBlock(postEVMTimestamp, nil)
			block.Header.PrevRootBlockHash = last.Hash()
			result, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src)
			if err != nil {
				t.Fatalf("process: %v", err)
			}
			if result.XShardReceiveGasUsed != tc.wantGas {
				t.Errorf("cross-shard receive gas = %d, want %d", result.XShardReceiveGasUsed, tc.wantGas)
			}
			if got := result.State.GetBalance(recipient, qkccommon.DefaultTokenID).Uint64(); got != 500 {
				t.Errorf("recipient balance = %d, want 500", got)
			}
		})
	}
}

// TestCursorSpansTwoRootBlocks is test_xshard_for_two_root_blocks: one minor
// block consumes the deposits of two root blocks in order, and the cursor ends
// past the second.
func TestCursorSpansTwoRootBlocks(t *testing.T) {
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)
	recipient := common.HexToAddress("0x8888888888888888888888888888888888888888")

	// Root block 1 carries nothing: it exists so the neighbour's headers can
	// point at a root height above this shard's genesis root height, which is
	// what gives that shard permission to send here.
	var lastRoot *types.RootBlockHeader
	for height := uint32(1); height <= 3; height++ {
		lastRoot = &types.RootBlockHeader{
			Number:          height,
			ParentHash:      f.src.roots[height-1].Header.Hash(),
			Coinbase:        account.NewAddress(common.HexToAddress("0x02"), mainnetChain1Shard),
			CoinbaseAmount:  qkccommon.NewEmptyTokenBalances(),
			Difficulty:      big.NewInt(1),
			TotalDifficulty: big.NewInt(int64(height) + 1),
		}
		root := &types.RootBlock{Header: lastRoot}
		if height > 1 {
			remote := &types.MinorBlockHeader{
				Branch:            account.NewBranch(mainnetChain1Shard),
				Number:            uint64(height),
				Coinbase:          account.NewAddress(common.HexToAddress("0x01"), mainnetChain1Shard),
				PrevRootBlockHash: f.src.roots[1].Header.Hash(),
				GasLimit:          u256(12000000),
				Difficulty:        big.NewInt(1),
			}
			f.src.deposits[remote.Hash()] = []*types.CrossShardTransactionDeposit{
				newFixtureDeposit(recipient, int64(height-1)*100, f.fullShard),
			}
			f.src.present[remote.Hash()] = true
			root.MinorBlockHeaders = []*types.MinorBlockHeader{remote}
		}
		f.src.roots = append(f.src.roots, root)
	}

	block := f.childBlock(postEVMTimestamp, nil)
	block.Header.PrevRootBlockHash = lastRoot.Hash()
	result, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := result.State.GetBalance(recipient, qkccommon.DefaultTokenID).Uint64(); got != 300 {
		t.Errorf("recipient balance = %d, want 300 from both root blocks", got)
	}
	if result.Cursor.RootBlockHeight != 4 {
		t.Errorf("cursor stopped at root height %d, want past the last root block",
			result.Cursor.RootBlockHeight)
	}
}

// TestFailedTransactionChargesWholeAllowance is test_failed_transaction_gas: a
// transaction whose code reverts is a successful block entry and a failed
// receipt. The sender pays for every unit it asked for, and the coinbase keeps
// the shard's share of it.
func TestFailedTransactionChargesWholeAllowance(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	contract := common.HexToAddress("0x4444444444444444444444444444444444444444")
	// jumpdest; jump(0) — the frame runs until the gas is gone, which is the
	// failure that costs the sender everything. A revert would hand the
	// remainder back.
	h.state.SetCode(contract, []byte{0x5b, 0x60, 0x00, 0x56})

	before := h.balance(h.from)
	tx := h.sign(types.NewEvmTransaction(0, contract, big.NewInt(1000), 100000, big.NewInt(2),
		h.ctx.ShardConfig.GetFullShardId(), h.ctx.ShardConfig.GetFullShardId(),
		h.ctx.QKCConfig.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID))

	success, _, err := ApplyTransaction(h.ctx, h.state, tx, tx.Hash(), 0)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if success {
		t.Fatal("a reverting call reported success")
	}
	spent := new(big.Int).Sub(before, h.balance(h.from))
	if want := big.NewInt(200000); spent.Cmp(want) != 0 {
		t.Errorf("sender paid %s, want the whole allowance %s", spent, want)
	}
	if got := h.balance(h.state.BlockCoinbase()); got.Cmp(big.NewInt(100000)) != 0 {
		t.Errorf("coinbase collected %s, want half the fee after tax", got)
	}
	if got := h.state.GetBalance(contract, qkccommon.DefaultTokenID).Sign(); got != 0 {
		t.Error("the value of a failed transfer stayed with the contract")
	}
}

// TestBlockHashWindow is test_blockhash_in_evm: BLOCKHASH reads the window of
// parent headers the state carries, and a height outside it is zero
// (state.py:346).
func TestBlockHashWindow(t *testing.T) {
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	h.state.SetPrevHeaders([]*types.MinorBlockHeader{f.parent})
	h.state.SetBlockNumber(f.parent.Number + 1)

	contract := common.HexToAddress("0x5555555555555555555555555555555555555555")
	// blockhash(calldataload(0)); mstore(0, it); return(0, 32)
	h.state.SetCode(contract, common.FromHex("0x6000354060005260206000f3"))

	read := func(height uint64) common.Hash {
		t.Helper()
		data := common.BigToHash(new(big.Int).SetUint64(height)).Bytes()
		_, output, err := h.call(h.state.GetNonce(h.from), &contract, big.NewInt(0), 100000, data)
		if err != nil {
			t.Fatalf("call: %v", err)
		}
		return common.BytesToHash(output)
	}

	if got := read(f.parent.Number); got != f.parent.Hash() {
		t.Errorf("blockhash(%d) = %s, want the parent %s", f.parent.Number, got, f.parent.Hash())
	}
	if got := read(f.parent.Number + 5); got != (common.Hash{}) {
		t.Errorf("blockhash outside the window = %s, want zero", got)
	}
}

// TestCoinbaseAmountDecaysByEpoch is test_shard_coinbase_decay: the reward is
// multiplied by the decay factor once per elapsed epoch, and only the final
// product is truncated (shard_state.py:2084).
func TestCoinbaseAmountDecaysByEpoch(t *testing.T) {
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)
	interval := f.ctx.ShardConfig.EpochInterval
	if interval == 0 {
		t.Skip("this shard has no epoch interval")
	}

	base := CoinbaseAmountMap(f.ctx, 0)[f.ctx.QKCConfig.GetDefaultChainTokenID()]
	factor := f.ctx.QKCConfig.BlockRewardDecayFactor
	for epoch := uint64(1); epoch <= 2; epoch++ {
		got := CoinbaseAmountMap(f.ctx, epoch*interval)[f.ctx.QKCConfig.GetDefaultChainTokenID()]
		want := new(big.Int).Mul(base, new(big.Int).Exp(factor.Num(), new(big.Int).SetUint64(epoch), nil))
		want.Div(want, new(big.Int).Exp(factor.Denom(), new(big.Int).SetUint64(epoch), nil))
		if got.Cmp(want) != 0 {
			t.Errorf("epoch %d reward = %s, want %s", epoch, got, want)
		}
	}
	// Inside an epoch nothing changes.
	if got := CoinbaseAmountMap(f.ctx, interval-1)[f.ctx.QKCConfig.GetDefaultChainTokenID()]; got.Cmp(base) != 0 {
		t.Errorf("reward changed inside the first epoch: %s, want %s", got, base)
	}
}

// TestValidateBlockResultRejectsEachMismatch is test_incorrect_coinbase_amount
// and test_add_block_receipt_root_not_match, generalized: every one of the
// seven values a block commits to has to be compared, so mutating any single
// one has to be caught.
func TestValidateBlockResultRejectsEachMismatch(t *testing.T) {
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)
	block := f.childBlock(postEVMTimestamp, nil)
	result, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	seal(block, result)
	if err := ValidateBlockResult(block, result); err != nil {
		t.Fatalf("a sealed block was refused: %v", err)
	}

	for name, mutate := range map[string]func(*types.MinorBlock){
		"state root": func(b *types.MinorBlock) {
			b.Meta.Root = common.HexToHash("0xdead")
		},
		"receipt root": func(b *types.MinorBlock) {
			b.Meta.ReceiptHash = common.HexToHash("0xdead")
		},
		"gas used": func(b *types.MinorBlock) {
			b.Meta.GasUsed = u256(12345)
		},
		"cross-shard gas used": func(b *types.MinorBlock) {
			b.Meta.CrossShardGasUsed = u256(12345)
		},
		"cursor": func(b *types.MinorBlock) {
			b.Meta.XShardTxCursor = types.XShardTxCursorInfo{RootBlockHeight: 99}
		},
		"coinbase amount": func(b *types.MinorBlock) {
			b.Header.CoinbaseAmount = qkccommon.NewEmptyTokenBalances()
		},
		"bloom": func(b *types.MinorBlock) {
			b.Header.Bloom[0] ^= 0xff
		},
	} {
		t.Run(name, func(t *testing.T) {
			mutated := types.NewMinorBlock(copyHeader(block.Header), copyMeta(block.Meta), nil, nil)
			mutate(mutated)
			if err := ValidateBlockResult(mutated, result); err == nil {
				t.Errorf("a block with a wrong %s was accepted", name)
			}
		})
	}
}

// TestEIP155ReplayOnAnotherChainIsRefused is test_eip155_signer_attack: a
// version 2 transaction is bound to the chain whose eth chain id it was signed
// with, so the same bytes replayed on a sibling chain must not recover a sender
// that chain will accept.
func TestEIP155ReplayOnAnotherChainIsRefused(t *testing.T) {
	origin := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	tx := origin.sign(types.NewEvmTransaction(0, common.HexToAddress("0x02"), big.NewInt(1), 30000,
		big.NewInt(1), 0, 0, origin.ctx.ShardConfig.EthChainID, 2, nil,
		qkccommon.DefaultTokenID, qkccommon.DefaultTokenID))

	elsewhere := newHarness(t, mainnetChain1Shard, postEVMTimestamp)
	if _, err := ValidateTxForBlock(elsewhere.ctx, elsewhere.state, tx, 6000000); err == nil {
		t.Fatal("a version 2 transaction was accepted on another chain")
	}
}

// TestCrossShardSourceConvertsNativeGasPrice is
// test_native_token_as_gas_cross_shard, source half: the deposit that leaves
// carries the converted genesis-token price and the rate the manager named, so
// the target settles at the same terms (messages.py:530).
func TestCrossShardSourceConvertsNativeGasPrice(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, mainnetNativeFork+1)
	foreign := qkccommon.TokenIDEncode("QETC")
	h.state.DeltaTokenBalance(h.from, foreign, big.NewInt(1e9))
	// mstore(0, 80); mstore(32, 2); return(0, 64) — the manager's two words.
	h.state.SetCode(generalNativeTokenContractAddress,
		common.FromHex("0x6050600052600260205260406000f3"))
	h.state.DeltaTokenBalance(generalNativeTokenContractAddress, qkccommon.DefaultTokenID, big.NewInt(1e18))

	tx := h.sign(types.NewEvmTransaction(0, common.HexToAddress("0x3333333333333333333333333333333333333333"),
		big.NewInt(1000), 60000, big.NewInt(3), mainnetChain0Shard, mainnetChain1Shard,
		h.ctx.QKCConfig.NetworkID, 0, nil, foreign, foreign))
	if !tx.IsCrossShard() {
		t.Fatal("the transaction is not cross-shard")
	}

	if _, _, err := ApplyTransaction(h.ctx, h.state, tx, tx.Hash(), 0); err != nil {
		t.Fatalf("apply: %v", err)
	}
	deposits := h.state.XShardList()
	if len(deposits) != 1 {
		t.Fatalf("%d deposits, want 1", len(deposits))
	}
	deposit := deposits[0]
	if got := deposit.GasPrice.Value; got.Cmp(big.NewInt(2)) != 0 {
		t.Errorf("deposit gas price = %s, want the converted 2", got)
	}
	if deposit.GasTokenID != h.state.GenesisTokenID() {
		t.Errorf("deposit gas token = %d, want the genesis token", deposit.GasTokenID)
	}
	if deposit.TransferTokenID != foreign {
		t.Errorf("deposit transfer token = %d, want the foreign one", deposit.TransferTokenID)
	}
	if deposit.RefundRate != 80 {
		t.Errorf("deposit refund rate = %d, want 80", deposit.RefundRate)
	}
}

func copyHeader(h *types.MinorBlockHeader) *types.MinorBlockHeader {
	out := *h
	return &out
}

func copyMeta(m *types.MinorBlockMeta) *types.MinorBlockMeta {
	out := *m
	return &out
}

// TestCursorRefusesAMissingRootBlockBelowTheTip: the traversal ends only past
// the block's own root tip. Below it, a root block the source cannot produce is
// missing data, and calling it the end of the stream would silently skip every
// deposit after it — producing a block that a node holding the data rejects.
func TestCursorRefusesAMissingRootBlockBelowTheTip(t *testing.T) {
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)

	root1 := &types.RootBlockHeader{
		Number:          1,
		ParentHash:      f.src.roots[0].Header.Hash(),
		Coinbase:        account.NewAddress(common.HexToAddress("0x02"), mainnetChain1Shard),
		CoinbaseAmount:  qkccommon.NewEmptyTokenBalances(),
		Difficulty:      big.NewInt(1),
		TotalDifficulty: big.NewInt(2),
	}
	tip := &types.RootBlockHeader{
		Number:          3,
		ParentHash:      root1.Hash(),
		Coinbase:        account.NewAddress(common.HexToAddress("0x02"), mainnetChain1Shard),
		CoinbaseAmount:  qkccommon.NewEmptyTokenBalances(),
		Difficulty:      big.NewInt(1),
		TotalDifficulty: big.NewInt(3),
	}
	f.src.setRoot(&types.RootBlock{Header: root1})
	f.src.setRoot(&types.RootBlock{Header: tip})
	// Height 2 is inside the range and absent.
	f.startCursorAt(2)

	block := f.childBlock(postEVMTimestamp, nil)
	block.Header.PrevRootBlockHash = tip.Hash()
	if _, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src); !errorsIs(err, ErrMissingRootBlock) {
		t.Fatalf("refused with %v, want a missing-root-block error", err)
	}
}

// TestCursorRefusesAnUnknownParentOrRootTip: the two lookups the traversal
// starts from have to produce something. Missing data must be an error rather
// than a nil dereference.
func TestCursorRefusesAnUnknownParentOrRootTip(t *testing.T) {
	t.Run("unknown root tip", func(t *testing.T) {
		f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)
		block := f.childBlock(postEVMTimestamp, nil)
		block.Header.PrevRootBlockHash = common.HexToHash("0xdead")
		if _, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src); err == nil {
			t.Fatal("a block naming an unknown root tip was executed")
		}
	})
	t.Run("unknown parent meta", func(t *testing.T) {
		f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)
		block := f.childBlock(postEVMTimestamp, nil)
		// The header resolves, the meta does not.
		delete(f.chain.metas, f.parent.Hash())
		if _, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src); err == nil {
			t.Fatal("a block whose parent meta is missing was executed")
		}
	})
}

// testShardWithDefaultToken copies a shard's configuration and gives it a
// different default chain token. The shipped networks all use QKC for
// everything, which hides every place that reads the wrong one.
func testShardWithDefaultToken(t *testing.T, base *config.ShardConfig, token string) *config.ShardConfig {
	t.Helper()
	shard := config.NewShardConfig(base.ChainConfig)
	shard.ShardID = base.ShardID
	shard.SetRootConfig(base.GetRootConfig())
	shard.DefaultChainToken = token
	return shard
}

// TestBalanceOpcodeReadsTheChainsDefaultToken: BALANCE takes no token id, and
// the one it means is the chain's default (messages.py:582) — not whichever
// token a single-balance state happens to keep.
func TestBalanceOpcodeReadsTheChainsDefaultToken(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	qeth := qkccommon.TokenIDEncode("QETH")
	shard := testShardWithDefaultToken(t, h.ctx.ShardConfig, "QETH")
	h.ctx.ShardConfig = shard
	h.state.SetConfig(h.ctx.QKCConfig, shard)

	target := common.HexToAddress("0x1212121212121212121212121212121212121212")
	h.state.DeltaTokenBalance(target, qkccommon.DefaultTokenID, big.NewInt(9))
	h.state.DeltaTokenBalance(target, qeth, big.NewInt(5))

	contract := common.HexToAddress("0x3434343434343434343434343434343434343434")
	// balance(target); mstore(0, it); return(0, 32)
	h.state.SetCode(contract, common.FromHex("0x73"+target.Hex()[2:]+"3160005260206000f3"))

	shardKey := shard.GetFullShardId()
	evm, err := newEVM(h.ctx, h.state, h.from, new(big.Int), common.Hash{}, shardKey, shardKey)
	if err != nil {
		t.Fatalf("evm: %v", err)
	}
	out, _, err := evm.QKCApplyMessage(h.from, contract, nil, vm.NewGasBudget(100000),
		new(uint256.Int), qeth, shardKey)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := new(big.Int).SetBytes(out); got.Cmp(big.NewInt(5)) != 0 {
		t.Errorf("BALANCE returned %s, want the default token's 5", got)
	}
}

// TestNativeTokenPrecompilesFollowTheNonReservedSwitch: both native-token
// precompiles are gated on ENABLE_NON_RESERVED_NATIVE_TOKEN_TIMESTAMP alone
// (env.py:63-76). The general native token switch only decides when its own
// system contract may be deployed, so an earlier one must not turn them on.
func TestNativeTokenPrecompilesFollowTheNonReservedSwitch(t *testing.T) {
	const (
		general     = uint64(50)
		nonReserved = uint64(100)
	)
	for _, tc := range []struct {
		name      string
		timestamp uint64
		active    bool
	}{
		{"after the general switch only", 75, false},
		{"after the non-reserved switch", nonReserved + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, mainnetChain0Shard, tc.timestamp)
			qkcConfig := *h.ctx.QKCConfig
			qkcConfig.EnableGeneralNativeTokenTimestamp = general
			qkcConfig.EnableNonReservedNativeTokenTimestamp = nonReserved
			h.ctx.QKCConfig = &qkcConfig
			h.state.SetConfig(&qkcConfig, h.ctx.ShardConfig)
			h.state.SetTimestamp(tc.timestamp)
			h.state.DeltaTokenBalance(h.from, qkccommon.DefaultTokenID, big.NewInt(77))

			// (address, token id) for the balance precompile.
			input := append(common.LeftPadBytes(h.from.Bytes(), 32),
				common.BigToHash(new(big.Int).SetUint64(qkccommon.DefaultTokenID)).Bytes()...)
			shardKey := h.ctx.ShardConfig.GetFullShardId()
			evm, err := newEVM(h.ctx, h.state, h.from, new(big.Int), common.Hash{}, shardKey, shardKey)
			if err != nil {
				t.Fatalf("evm: %v", err)
			}
			out, _, err := evm.QKCApplyMessage(h.from, vm.QKCBalanceMntAddress, input,
				vm.NewGasBudget(100000), new(uint256.Int), qkccommon.DefaultTokenID, shardKey)
			if err != nil {
				t.Fatalf("call: %v", err)
			}
			if tc.active {
				if got := new(big.Int).SetBytes(out); got.Sign() == 0 {
					t.Error("the balance precompile answered nothing after its switch")
				}
				return
			}
			if len(out) != 0 {
				t.Errorf("the precompile answered %x before its switch, want an empty account", out)
			}
		})
	}
}
