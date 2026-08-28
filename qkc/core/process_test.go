// Copyright 2026-2027, QuarkChain.

package core

import (
	"maps"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	qkcstate "github.com/ethereum/go-ethereum/qkc/state"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// fixtureSource is the chain-layer data the cursor traversal reads, supplied
// from a literal instead of a database. It is the whole reason XShardSource is
// an interface: a block can be executed without a chain behind it.
type fixtureSource struct {
	// roots is indexed by height. A gap, or a height past the end, is the end
	// of the root chain.
	roots    []*types.RootBlock
	deposits map[common.Hash][]*types.CrossShardTransactionDeposit
	// present distinguishes "sent an empty list" from "could not send", which
	// the traversal checks separately.
	present map[common.Hash]bool
}

// setRoot places a root block at its own height, growing the chain with unused
// placeholders when a case jumps to a height rather than building up to it.
func (s *fixtureSource) setRoot(block *types.RootBlock) {
	for uint64(len(s.roots)) <= uint64(block.Header.Number) {
		s.roots = append(s.roots, nil)
	}
	s.roots[block.Header.Number] = block
}

func (s *fixtureSource) RootBlockByHeight(_ common.Hash, height uint64) (*types.RootBlock, error) {
	if height >= uint64(len(s.roots)) {
		return nil, nil
	}
	return s.roots[height], nil
}

// eachRoot walks the roots a case actually placed, skipping the placeholders
// setRoot leaves behind.
func (s *fixtureSource) eachRoot(visit func(*types.RootBlock) bool) {
	for _, root := range s.roots {
		if root == nil {
			continue
		}
		if !visit(root) {
			return
		}
	}
}

func (s *fixtureSource) RootHeaderByHash(hash common.Hash) (*types.RootBlockHeader, error) {
	var found *types.RootBlockHeader
	s.eachRoot(func(root *types.RootBlock) bool {
		if root.Header.Hash() == hash {
			found = root.Header
			return false
		}
		return true
	})
	return found, nil
}

func (s *fixtureSource) DepositsByMinorBlockHash(hash common.Hash) ([]*types.CrossShardTransactionDeposit, bool, error) {
	if !s.present[hash] {
		return nil, false, nil
	}
	return s.deposits[hash], true, nil
}

// blockChain is the minimal shard history Process reads: headers by hash for
// the BLOCKHASH window and the proof-of-staked-work window, and metas by hash
// for the cursor to resume from.
type blockChain struct {
	headers map[common.Hash]*types.MinorBlockHeader
	metas   map[common.Hash]*types.MinorBlockMeta
}

func newBlockChain() *blockChain {
	return &blockChain{
		headers: map[common.Hash]*types.MinorBlockHeader{},
		metas:   map[common.Hash]*types.MinorBlockMeta{},
	}
}

func (c *blockChain) add(header *types.MinorBlockHeader, meta *types.MinorBlockMeta) {
	hash := header.Hash()
	c.headers[hash] = header
	c.metas[hash] = meta
}

func (c *blockChain) headerByHash(hash common.Hash) (*types.MinorBlockHeader, error) {
	return c.headers[hash], nil
}

func (c *blockChain) metaByHash(hash common.Hash) (*types.MinorBlockMeta, error) {
	meta, ok := c.metas[hash]
	if !ok {
		return nil, nil
	}
	return meta, nil
}

func u256(v int64) *serialize.Uint256 {
	return &serialize.Uint256{Value: big.NewInt(v)}
}

// blockFixture is one shard's genesis plus the machinery to execute the block
// that follows it.
type blockFixture struct {
	ctx        *ExecutionContext
	db         ethdb.Database
	tdb        *triedb.Database
	chain      *blockChain
	src        *fixtureSource
	parentRoot common.Hash
	parent     *types.MinorBlockHeader
	coinbase   account.Recipient
	fullShard  uint32
}

// newBlockFixture builds a genesis block on the given mainnet shard and leaves
// the fixture ready to execute its child. The root chain starts with one block
// whose coinbase belongs to this shard, so the very first thing the cursor sees
// is a root-chain payout.
// allocCode is variadic only so the callers that allocate plain balances stay
// as they are; at most one map is ever passed.
func newBlockFixture(t *testing.T, fullShardID uint32, timestamp uint64, alloc map[account.Recipient]*big.Int, withCode ...map[account.Recipient][]byte) *blockFixture {
	t.Helper()
	allocCode := map[account.Recipient][]byte{}
	for _, m := range withCode {
		maps.Copy(allocCode, m)
	}
	cluster := network(t, "mainnet")
	shard := cluster.Quarkchain.GetShardConfigByFullShardID(fullShardID)
	if shard == nil {
		t.Fatalf("mainnet has no shard %#x", fullShardID)
	}

	db := rawdb.NewMemoryDatabase()
	tdb := qkcstate.NewDatabase(db)
	state, err := qkcstate.New(coretypes.EmptyRootHash, db, tdb)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	state.SetConfig(cluster.Quarkchain, shard)
	state.SetFullShardKey(fullShardID)
	for addr, amount := range alloc {
		state.DeltaTokenBalance(addr, qkccommon.DefaultTokenID, amount)
	}
	for addr, code := range allocCode {
		state.SetCode(addr, code)
	}
	parentRoot, err := state.Commit(0)
	if err != nil {
		t.Fatalf("commit genesis: %v", err)
	}

	coinbase := common.HexToAddress("0x00000000000000000000000000000000000000cc")
	parent := &types.MinorBlockHeader{
		Version:    0,
		Branch:     account.NewBranch(fullShardID),
		Number:     0,
		Coinbase:   account.NewAddress(coinbase, fullShardID),
		GasLimit:   u256(12000000),
		Time:       timestamp - 10,
		Difficulty: big.NewInt(1),
	}
	// The parent has consumed nothing: root height 0, and the two indices at
	// the end-of-stream marker.
	parentMeta := &types.MinorBlockMeta{
		Root:              parentRoot,
		GasUsed:           u256(0),
		CrossShardGasUsed: u256(0),
		XShardGasLimit:    u256(6000000),
		XShardTxCursor:    types.XShardTxCursorInfo{RootBlockHeight: 0, MinorBlockIndex: 0, XShardDepositIndex: 0},
	}
	chain := newBlockChain()
	chain.add(parent, parentMeta)

	rootHeader := &types.RootBlockHeader{
		Number:          0,
		Coinbase:        account.NewAddress(coinbase, fullShardID),
		CoinbaseAmount:  qkccommon.NewEmptyTokenBalances(),
		Difficulty:      big.NewInt(1),
		TotalDifficulty: big.NewInt(1),
	}
	src := &fixtureSource{
		roots:    []*types.RootBlock{{Header: rootHeader}},
		deposits: map[common.Hash][]*types.CrossShardTransactionDeposit{},
		present:  map[common.Hash]bool{},
	}

	ctx := &ExecutionContext{
		QKCConfig:            cluster.Quarkchain,
		ShardConfig:          shard,
		ValidationRootTip:    rootHeader,
		MinorHeaderByHash:    chain.headerByHash,
		MinorBlockMetaByHash: chain.metaByHash,
	}
	return &blockFixture{
		ctx: ctx, db: db, tdb: tdb, chain: chain, src: src,
		parentRoot: parentRoot, parent: parent, coinbase: coinbase, fullShard: fullShardID,
	}
}

// startCursorAt rewrites the parent's cursor, so a case can begin the
// traversal at a given root height instead of walking the chain up to it.
func (f *blockFixture) startCursorAt(rootHeight uint32) {
	meta, err := f.chain.metaByHash(f.parent.Hash())
	if err != nil || meta == nil {
		panic("the fixture's parent has no meta")
	}
	updated := *meta
	updated.XShardTxCursor = types.XShardTxCursorInfo{RootBlockHeight: uint64(rootHeight)}
	f.chain.add(f.parent, &updated)
}

// childBlock builds the block that follows the fixture's parent. Its meta is
// filled with placeholders; ValidateBlockResult is only meaningful once the
// caller has copied the result into it, which sealWith does.
func (f *blockFixture) childBlock(timestamp uint64, txs []*types.Transaction) *types.MinorBlock {
	header := &types.MinorBlockHeader{
		Branch:            account.NewBranch(f.fullShard),
		Number:            f.parent.Number + 1,
		Coinbase:          account.NewAddress(f.coinbase, f.fullShard),
		CoinbaseAmount:    qkccommon.NewEmptyTokenBalances(),
		ParentHash:        f.parent.Hash(),
		PrevRootBlockHash: f.src.roots[len(f.src.roots)-1].Header.Hash(),
		GasLimit:          u256(12000000),
		Time:              timestamp,
		Difficulty:        big.NewInt(1),
	}
	meta := &types.MinorBlockMeta{
		GasUsed:           u256(0),
		CrossShardGasUsed: u256(0),
		XShardGasLimit:    u256(6000000),
	}
	return types.NewMinorBlock(header, meta, txs, nil)
}

// seal copies a result into the block, which is what a miner does. Every
// consensus field then matches by construction, so a test that mutates one is
// asking a precise question about that field.
func seal(block *types.MinorBlock, result *ProcessResult) {
	block.Meta.Root = result.StateRoot
	block.Meta.ReceiptHash = result.ReceiptRoot
	block.Meta.GasUsed = &serialize.Uint256{Value: new(big.Int).SetUint64(result.GasUsed)}
	block.Meta.CrossShardGasUsed = &serialize.Uint256{Value: new(big.Int).SetUint64(result.XShardReceiveGasUsed)}
	block.Meta.XShardTxCursor = result.Cursor
	block.Header.Bloom = result.Bloom
	block.Header.CoinbaseAmount = coinbaseBalances(result.CoinbaseAmountMap)
}

func coinbaseBalances(amounts map[uint64]*big.Int) *qkccommon.TokenBalances {
	out := qkccommon.NewEmptyTokenBalances()
	for token, amount := range amounts {
		value, _ := uint256.FromBig(amount)
		out.SetValue(value, token)
	}
	return out
}

// TestProcessRunsAcrossNativeTokenFork: the fork is a change of rules, not a
// wall. A block on either side of T executes, and the three timestamps around
// it are the whole point.
func TestProcessRunsAcrossNativeTokenFork(t *testing.T) {
	for _, tc := range []struct {
		name      string
		timestamp uint64
	}{
		{"one second before the fork", mainnetNativeFork - 1},
		{"exactly at the fork", mainnetNativeFork},
		{"one second after the fork", mainnetNativeFork + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBlockFixture(t, mainnetChain0Shard, tc.timestamp, nil)
			block := f.childBlock(tc.timestamp, nil)
			result, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src)
			if err != nil {
				t.Fatalf("block refused: %v", err)
			}
			if result == nil {
				t.Fatal("no result")
			}
		})
	}
}

// TestProcessCreditsRootChainCoinbase: the cursor's first stop is the root
// block's own payout, and it lands on the shard that owns the coinbase address.
func TestProcessCreditsRootChainCoinbase(t *testing.T) {
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)
	// Give the root block a payout in the genesis token.
	amount, _ := uint256.FromBig(big.NewInt(1234))
	f.src.roots[0].Header.CoinbaseAmount.SetValue(amount, qkccommon.DefaultTokenID)

	block := f.childBlock(postEVMTimestamp, nil)
	result, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(result.ConsumedDeposits) != 1 {
		t.Fatalf("consumed %d deposits, want the root payout", len(result.ConsumedDeposits))
	}
	deposit := result.ConsumedDeposits[0]
	if !deposit.IsFromRootChain {
		t.Error("the root payout is not marked as coming from the root chain")
	}
	if deposit.Value.Value.Cmp(big.NewInt(1234)) != 0 {
		t.Errorf("payout value = %s, want 1234", deposit.Value.Value)
	}
	// A root payout is free: gas price zero means no surcharge.
	if result.XShardReceiveGasUsed != 0 {
		t.Errorf("cross-shard receive gas = %d, want 0 for a free root payout", result.XShardReceiveGasUsed)
	}
	// End of stream: one root block, fully consumed.
	want := types.XShardTxCursorInfo{RootBlockHeight: 1, MinorBlockIndex: 0, XShardDepositIndex: 0}
	if result.Cursor != want {
		t.Errorf("cursor = %+v, want %+v", result.Cursor, want)
	}
}

// TestProcessCoinbaseIsRewardPlusFees: the coinbase a block declares is the
// decayed block reward *plus* every fee it collected (shard_state.py:983).
// Implementing only the reward half passes an empty block and fails the moment
// a transaction pays anything.
func TestProcessCoinbaseIsRewardPlusFees(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	sender := h.from

	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, map[account.Recipient]*big.Int{
		sender: new(big.Int).Mul(big.NewInt(1e18), big.NewInt(10)),
	})
	to := common.HexToAddress("0xdededededededededededededededededededede")
	shardKey := f.fullShard
	tx := types.NewEvmTransaction(0, to, big.NewInt(100), 30000, big.NewInt(1000000000),
		shardKey, shardKey, f.ctx.QKCConfig.NetworkID, 0, nil,
		qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
	signer := types.MakeSigner(f.ctx.QKCConfig.NetworkID, f.ctx.ShardConfig.EthChainID)
	signed, err := types.SignTx(tx, signer, h.key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	block := f.childBlock(postEVMTimestamp, []*types.Transaction{signed})
	result, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src)
	if err != nil {
		t.Fatalf("process: %v", err)
	}

	reward := CoinbaseAmountMap(f.ctx, block.Header.Number)[qkccommon.DefaultTokenID]
	fee := result.CoinbaseAmountMap[qkccommon.DefaultTokenID]
	if fee.Cmp(reward) <= 0 {
		t.Errorf("coinbase %s is not more than the bare reward %s, so the fees were dropped", fee, reward)
	}
	if result.GasUsed != GTXCost {
		t.Errorf("gas used = %d, want %d", result.GasUsed, GTXCost)
	}
	if len(result.Receipts) != 1 {
		t.Fatalf("%d receipts, want 1", len(result.Receipts))
	}
}

// TestExecuteAndValidateAcceptsASealedBlock and rejects each field on its own.
// The seven comparisons are the whole of add_block's result check, and a test
// that only asserts the state root would let six of them rot.
func TestExecuteAndValidateAcceptsASealedBlock(t *testing.T) {
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)
	block := f.childBlock(postEVMTimestamp, nil)
	result, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	seal(block, result)

	if _, err := ExecuteAndValidate(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src); err != nil {
		t.Fatalf("a correctly sealed block was rejected: %v", err)
	}

	for name, corrupt := range map[string]func(*types.MinorBlock){
		"state root": func(b *types.MinorBlock) {
			b.Meta.Root = common.HexToHash("0xdead")
		},
		"receipt root": func(b *types.MinorBlock) {
			b.Meta.ReceiptHash = common.HexToHash("0xdead")
		},
		"gas used": func(b *types.MinorBlock) {
			b.Meta.GasUsed = u256(999)
		},
		"cross-shard gas used": func(b *types.MinorBlock) {
			b.Meta.CrossShardGasUsed = u256(999)
		},
		"cursor": func(b *types.MinorBlock) {
			b.Meta.XShardTxCursor = types.XShardTxCursorInfo{RootBlockHeight: 42}
		},
		"coinbase": func(b *types.MinorBlock) {
			amount, _ := uint256.FromBig(big.NewInt(1))
			b.Header.CoinbaseAmount = qkccommon.NewEmptyTokenBalances()
			b.Header.CoinbaseAmount.SetValue(amount, qkccommon.DefaultTokenID)
		},
		"bloom": func(b *types.MinorBlock) {
			b.Header.Bloom[0] ^= 0xff
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)
			block := f.childBlock(postEVMTimestamp, nil)
			result, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src)
			if err != nil {
				t.Fatalf("process: %v", err)
			}
			seal(block, result)
			corrupt(block)
			if _, err := ExecuteAndValidate(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src); err == nil {
				t.Fatalf("a block with a wrong %s was accepted", name)
			}
		})
	}
}

// TestCursorConsumesNeighbourDepositsAndResumes: the cursor walks a root
// block's minor headers, consumes the deposits of the neighbouring shards, and
// stops on the cross-shard allowance — leaving a position the next block picks
// up from exactly.
func TestCursorConsumesNeighbourDepositsAndResumes(t *testing.T) {
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)

	// An intermediate root block. The neighbour's minor block has to point at a
	// root block *above* this shard's genesis root height, or the traversal
	// treats it as predating this shard and refuses to take anything from it.
	root1 := &types.RootBlockHeader{
		Number:          1,
		ParentHash:      f.src.roots[0].Header.Hash(),
		Coinbase:        account.NewAddress(common.HexToAddress("0x02"), mainnetChain1Shard),
		CoinbaseAmount:  qkccommon.NewEmptyTokenBalances(),
		Difficulty:      big.NewInt(1),
		TotalDifficulty: big.NewInt(2),
	}
	f.src.roots = append(f.src.roots, &types.RootBlock{Header: root1})

	remote := &types.MinorBlockHeader{
		Branch:            account.NewBranch(mainnetChain1Shard),
		Number:            1,
		Coinbase:          account.NewAddress(common.HexToAddress("0x01"), mainnetChain1Shard),
		PrevRootBlockHash: root1.Hash(),
		GasLimit:          u256(12000000),
		Difficulty:        big.NewInt(1),
	}
	recipient := common.HexToAddress("0xfefefefefefefefefefefefefefefefefefefefe")
	f.src.deposits[remote.Hash()] = []*types.CrossShardTransactionDeposit{
		newFixtureDeposit(recipient, 111, f.fullShard),
		newFixtureDeposit(recipient, 222, f.fullShard),
	}
	f.src.present[remote.Hash()] = true

	root2 := &types.RootBlockHeader{
		Number:          2,
		ParentHash:      root1.Hash(),
		Coinbase:        account.NewAddress(common.HexToAddress("0x03"), mainnetChain1Shard),
		CoinbaseAmount:  qkccommon.NewEmptyTokenBalances(),
		Difficulty:      big.NewInt(1),
		TotalDifficulty: big.NewInt(3),
	}
	f.src.roots = append(f.src.roots, &types.RootBlock{Header: root2, MinorBlockHeaders: []*types.MinorBlockHeader{remote}})

	block := f.childBlock(postEVMTimestamp, nil)
	block.Header.PrevRootBlockHash = root2.Hash()
	result, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src)
	if err != nil {
		t.Fatalf("process: %v", err)
	}

	// Three root payouts — every root block has one, whatever it is worth here
	// — and then both of the neighbour's deposits.
	if len(result.ConsumedDeposits) != 5 {
		t.Fatalf("consumed %d deposits, want 5", len(result.ConsumedDeposits))
	}
	if got := result.State.GetBalance(recipient, qkccommon.DefaultTokenID).Uint64(); got != 333 {
		t.Errorf("recipient balance = %d, want 333", got)
	}
	want := types.XShardTxCursorInfo{RootBlockHeight: 3, MinorBlockIndex: 0, XShardDepositIndex: 0}
	if result.Cursor != want {
		t.Errorf("cursor = %+v, want end of stream %+v", result.Cursor, want)
	}
}

// TestCursorSkipsOwnShardAndMissingLists: a shard never consumes its own
// deposits, and a minor block from before this shard existed must have no
// deposit list at all — an empty list would mean something different.
func TestCursorSkipsOwnShard(t *testing.T) {
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)

	own := &types.MinorBlockHeader{
		Branch:            account.NewBranch(f.fullShard),
		Number:            1,
		Coinbase:          account.NewAddress(common.HexToAddress("0x01"), f.fullShard),
		PrevRootBlockHash: f.src.roots[0].Header.Hash(),
		GasLimit:          u256(12000000),
		Difficulty:        big.NewInt(1),
	}
	// Deliberately present, and deliberately never consumed.
	f.src.deposits[own.Hash()] = []*types.CrossShardTransactionDeposit{
		newFixtureDeposit(common.HexToAddress("0x99"), 500, f.fullShard),
	}
	f.src.present[own.Hash()] = true

	root1 := &types.RootBlockHeader{
		Number:          1,
		ParentHash:      f.src.roots[0].Header.Hash(),
		Coinbase:        account.NewAddress(common.HexToAddress("0x02"), mainnetChain1Shard),
		CoinbaseAmount:  qkccommon.NewEmptyTokenBalances(),
		Difficulty:      big.NewInt(1),
		TotalDifficulty: big.NewInt(2),
	}
	f.src.roots = append(f.src.roots, &types.RootBlock{Header: root1, MinorBlockHeaders: []*types.MinorBlockHeader{own}})

	block := f.childBlock(postEVMTimestamp, nil)
	block.Header.PrevRootBlockHash = root1.Hash()
	result, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := result.State.GetBalance(common.HexToAddress("0x99"), qkccommon.DefaultTokenID).Uint64(); got != 0 {
		t.Errorf("a shard consumed its own deposit: balance %d", got)
	}
}

// TestProcessLeavesParentStateUntouchedOnFailure: a block that fails partway
// must not be visible in the caller's state. Process works on its own copy
// opened at the parent root, so the parent root still resolves to the same
// accounts afterwards.
func TestProcessLeavesParentStateUntouchedOnFailure(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, map[account.Recipient]*big.Int{
		h.from: big.NewInt(1000),
	})

	// A transaction the sender cannot afford: the block fails as a whole.
	to := common.HexToAddress("0xbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc")
	tx := types.NewEvmTransaction(0, to, big.NewInt(1e18), 30000, big.NewInt(1e9),
		f.fullShard, f.fullShard, f.ctx.QKCConfig.NetworkID, 0, nil,
		qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
	signer := types.MakeSigner(f.ctx.QKCConfig.NetworkID, f.ctx.ShardConfig.EthChainID)
	signed, err := types.SignTx(tx, signer, h.key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	block := f.childBlock(postEVMTimestamp, []*types.Transaction{signed})
	if _, err := Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src); err == nil {
		t.Fatal("a block containing an unaffordable transaction was executed")
	}

	reopened, err := qkcstate.New(f.parentRoot, f.db, f.tdb)
	if err != nil {
		t.Fatalf("reopen parent: %v", err)
	}
	if got := reopened.GetNonce(h.from); got != 0 {
		t.Errorf("the parent state moved: sender nonce = %d", got)
	}
	if got := reopened.GetBalance(h.from, qkccommon.DefaultTokenID).Uint64(); got != 1000 {
		t.Errorf("the parent state moved: sender balance = %d", got)
	}
}

// TestProcessAbandonsBlockOnGasUnderflow: the CREATE2 word charge is where
// pyquarkchain's gas counter goes below zero and its own assert stops it
// (messages.py:305). No such block exists upstream, so accepting one here would
// make this the only client that does. The message-level golden pins the
// refusal itself; what this pins is that the refusal reaches the block.
func TestProcessAbandonsBlockOnGasUnderflow(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	contract := common.HexToAddress("0x00000000000000000000000000000000000000a1")
	// mstore(0x0be0, 0) to span 3072 bytes, then create2 over all of it as the
	// last instruction. 32327 covers everything up to and including CREATE2's
	// 32000; the 576 of word charge on top is what there is not enough for.
	code := common.FromHex("0x6000610be0526000610c0060006000f5")
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp,
		map[account.Recipient]*big.Int{h.from: big.NewInt(1e18)},
		map[account.Recipient][]byte{contract: code})

	tx := types.NewEvmTransaction(0, contract, big.NewInt(0), 53500, big.NewInt(1),
		f.fullShard, f.fullShard, f.ctx.QKCConfig.NetworkID, 0, nil,
		qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
	signer := types.MakeSigner(f.ctx.QKCConfig.NetworkID, f.ctx.ShardConfig.EthChainID)
	signed, err := types.SignTx(tx, signer, h.key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	block := f.childBlock(postEVMTimestamp, []*types.Transaction{signed})
	_, err = Process(f.ctx, block, f.parentRoot, f.db, f.tdb, f.src)
	if err == nil {
		t.Fatal("a block pyquarkchain cannot execute was accepted")
	}
	if !errorsIs(err, ErrGasUnderflow) {
		t.Fatalf("abandoned with %v, want a gas-underflow refusal", err)
	}
}

// TestCursorCutsAtTheSoftLimitAndResumes: the cross-shard allowance is a soft
// limit — the deposit that crosses it still runs in full, and the position it
// leaves is where the next block picks up. Getting the resume point wrong by one
// either replays a deposit or drops it, and neither announces itself.
func TestCursorCutsAtTheSoftLimitAndResumes(t *testing.T) {
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, nil)

	root1 := &types.RootBlockHeader{
		Number:          1,
		ParentHash:      f.src.roots[0].Header.Hash(),
		Coinbase:        account.NewAddress(common.HexToAddress("0x02"), mainnetChain1Shard),
		CoinbaseAmount:  qkccommon.NewEmptyTokenBalances(),
		Difficulty:      big.NewInt(1),
		TotalDifficulty: big.NewInt(2),
	}
	f.src.roots = append(f.src.roots, &types.RootBlock{Header: root1})

	remote := &types.MinorBlockHeader{
		Branch:            account.NewBranch(mainnetChain1Shard),
		Number:            1,
		Coinbase:          account.NewAddress(common.HexToAddress("0x01"), mainnetChain1Shard),
		PrevRootBlockHash: root1.Hash(),
		GasLimit:          u256(12000000),
		Difficulty:        big.NewInt(1),
	}
	first := common.HexToAddress("0x1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a")
	second := common.HexToAddress("0x2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b")
	// A non-zero gas price is what makes a deposit cost the surcharge, and so
	// what lets one of them exhaust the allowance.
	depositA := newFixtureDeposit(first, 11, f.fullShard)
	depositA.GasPrice = u256(1)
	depositB := newFixtureDeposit(second, 22, f.fullShard)
	depositB.GasPrice = u256(1)
	f.src.deposits[remote.Hash()] = []*types.CrossShardTransactionDeposit{depositA, depositB}
	f.src.present[remote.Hash()] = true

	root2 := &types.RootBlockHeader{
		Number:          2,
		ParentHash:      root1.Hash(),
		Coinbase:        account.NewAddress(common.HexToAddress("0x03"), mainnetChain1Shard),
		CoinbaseAmount:  qkccommon.NewEmptyTokenBalances(),
		Difficulty:      big.NewInt(1),
		TotalDifficulty: big.NewInt(3),
	}
	f.src.roots = append(f.src.roots, &types.RootBlock{Header: root2, MinorBlockHeaders: []*types.MinorBlockHeader{remote}})

	// An allowance of exactly one deposit's surcharge.
	block1 := f.childBlock(postEVMTimestamp, nil)
	block1.Header.PrevRootBlockHash = root2.Hash()
	block1.Meta.XShardGasLimit = u256(int64(GTXXShardCost))
	result1, err := Process(f.ctx, block1, f.parentRoot, f.db, f.tdb, f.src)
	if err != nil {
		t.Fatalf("first block: %v", err)
	}
	if got := result1.State.GetBalance(first, qkccommon.DefaultTokenID).Uint64(); got != 11 {
		t.Errorf("first deposit credited %d, want 11", got)
	}
	if got := result1.State.GetBalance(second, qkccommon.DefaultTokenID).Uint64(); got != 0 {
		t.Errorf("the second deposit was consumed past the allowance: %d", got)
	}
	cut := types.XShardTxCursorInfo{RootBlockHeight: 2, MinorBlockIndex: 1, XShardDepositIndex: 0}
	if result1.Cursor != cut {
		t.Fatalf("cursor after the cut = %+v, want %+v", result1.Cursor, cut)
	}

	// The next block resumes from that position and takes the rest.
	seal(block1, result1)
	f.chain.add(block1.Header, block1.Meta)
	f.parent = block1.Header

	block2 := f.childBlock(postEVMTimestamp+10, nil)
	block2.Header.PrevRootBlockHash = root2.Hash()
	result2, err := Process(f.ctx, block2, result1.StateRoot, f.db, f.tdb, f.src)
	if err != nil {
		t.Fatalf("second block: %v", err)
	}
	if got := result2.State.GetBalance(second, qkccommon.DefaultTokenID).Uint64(); got != 22 {
		t.Errorf("second deposit credited %d, want 22 on resume", got)
	}
	if got := result2.State.GetBalance(first, qkccommon.DefaultTokenID).Uint64(); got != 11 {
		t.Errorf("the first deposit was replayed: balance %d", got)
	}
	end := types.XShardTxCursorInfo{RootBlockHeight: 3, MinorBlockIndex: 0, XShardDepositIndex: 0}
	if result2.Cursor != end {
		t.Errorf("cursor = %+v, want end of stream %+v", result2.Cursor, end)
	}
}

// TestChainedBlocksAdvanceState runs several blocks in a row against the state
// each left behind, which is the only way the parent-root plumbing and the
// per-block reset of the counters get exercised together.
func TestChainedBlocksAdvanceState(t *testing.T) {
	h := newHarness(t, mainnetChain0Shard, postEVMTimestamp)
	f := newBlockFixture(t, mainnetChain0Shard, postEVMTimestamp, map[account.Recipient]*big.Int{
		h.from: new(big.Int).Mul(big.NewInt(1e18), big.NewInt(10)),
	})
	signer := types.MakeSigner(f.ctx.QKCConfig.NetworkID, f.ctx.ShardConfig.EthChainID)
	to := common.HexToAddress("0x3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c")

	parentRoot := f.parentRoot
	for i := uint64(0); i < 3; i++ {
		tx := types.NewEvmTransaction(i, to, big.NewInt(100), 30000, big.NewInt(1000000000),
			f.fullShard, f.fullShard, f.ctx.QKCConfig.NetworkID, 0, nil,
			qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
		signed, err := types.SignTx(tx, signer, h.key)
		if err != nil {
			t.Fatalf("sign: %v", err)
		}
		block := f.childBlock(postEVMTimestamp+i, []*types.Transaction{signed})
		result, err := Process(f.ctx, block, parentRoot, f.db, f.tdb, f.src)
		if err != nil {
			t.Fatalf("block %d: %v", i+1, err)
		}
		seal(block, result)
		if _, err := ExecuteAndValidate(f.ctx, block, parentRoot, f.db, f.tdb, f.src); err != nil {
			t.Fatalf("block %d failed its own validation: %v", i+1, err)
		}
		if got := result.GasUsed; got != GTXCost {
			t.Errorf("block %d gas used = %d, want %d — the counter did not reset", i+1, got, GTXCost)
		}
		f.chain.add(block.Header, block.Meta)
		f.parent = block.Header
		parentRoot = result.StateRoot
	}

	final, err := qkcstate.New(parentRoot, f.db, f.tdb)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := final.GetNonce(h.from); got != 3 {
		t.Errorf("sender nonce after three blocks = %d, want 3", got)
	}
	if got := final.GetBalance(to, qkccommon.DefaultTokenID).Uint64(); got != 300 {
		t.Errorf("recipient balance = %d, want 300", got)
	}
}

func newFixtureDeposit(to account.Recipient, value int64, toShard uint32) *types.CrossShardTransactionDeposit {
	return &types.CrossShardTransactionDeposit{
		TxHash:          common.BigToHash(big.NewInt(value)),
		From:            account.NewAddress(common.HexToAddress("0x0a"), mainnetChain1Shard),
		To:              account.NewAddress(to, toShard),
		Value:           &serialize.Uint256{Value: big.NewInt(value)},
		GasPrice:        &serialize.Uint256{Value: new(big.Int)},
		GasTokenID:      qkccommon.DefaultTokenID,
		TransferTokenID: qkccommon.DefaultTokenID,
		GasRemained:     &serialize.Uint256{Value: new(big.Int)},
		RefundRate:      100,
	}
}

// TestCoinbaseComparisonCountsZeroEntries pins the shape of add_block's
// coinbase check (shard_state.py:983): it compares the two balance maps as
// dicts, and the header's map has been through a serializer that drops zeros
// (core.py:574). A computed entry worth zero therefore has nothing to match,
// and the block is refused. Reading the two as equal would make this fork
// accept a block every reference node rejects.
func TestCoinbaseComparisonCountsZeroEntries(t *testing.T) {
	token := qkccommon.DefaultTokenID
	empty := qkccommon.NewEmptyTokenBalances()
	declared := qkccommon.NewTokenBalancesWithMap(map[uint64]*uint256.Int{token: uint256.NewInt(7)})

	if err := compareCoinbase(map[uint64]*big.Int{token: new(big.Int)}, empty); err == nil {
		t.Error("a computed zero entry matched a header carrying no entry at all")
	}
	if err := compareCoinbase(map[uint64]*big.Int{}, empty); err != nil {
		t.Errorf("two empty maps did not match: %v", err)
	}
	if err := compareCoinbase(map[uint64]*big.Int{token: big.NewInt(7)}, declared); err != nil {
		t.Errorf("equal maps did not match: %v", err)
	}
	if err := compareCoinbase(map[uint64]*big.Int{token: big.NewInt(8)}, declared); err == nil {
		t.Error("a different amount matched")
	}
	if err := compareCoinbase(map[uint64]*big.Int{}, declared); err == nil {
		t.Error("a missing entry matched")
	}
}
