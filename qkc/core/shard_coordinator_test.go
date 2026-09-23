// Copyright 2026-2027, QuarkChain.

package core

import (
	"errors"
	"math/big"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// The concrete execution chain is supplied by a separate task. This double
// persists candidates in real rawdb and consumes the real execution cursor.
type minorChainStub struct {
	db                                  ethdb.Database
	current                             *types.MinorBlock
	outgoing                            []*types.CrossShardTransactionDeposit
	incoming                            []*types.CrossShardTransactionDeposit
	insertErr                           error
	insertions, canonicalChanges, stops int
	options                             InsertOptions
}

func newMinorChainStub(db ethdb.Database, genesis *types.MinorBlock) *minorChainStub {
	rawdb.WriteMinorBlock(db, genesis)
	return &minorChainStub{db: db, current: genesis}
}

func (s *minorChainStub) CurrentBlock() *types.MinorBlock { return s.current }
func (s *minorChainStub) GetBlock(hash common.Hash) *types.MinorBlock {
	return rawdb.ReadMinorBlock(s.db, hash)
}
func (s *minorChainStub) GetBlockByNumber(number uint64) *types.MinorBlock {
	for block := s.current; block != nil && block.NumberU64() >= number; block = s.GetBlock(block.ParentHash()) {
		if block.NumberU64() == number {
			return block
		}
	}
	return nil
}
func (s *minorChainStub) InsertBlockWithXShardInput(block *types.MinorBlock, cursor XShardDepositCursor, options InsertOptions) ([]*types.CrossShardTransactionDeposit, error) {
	s.insertions++
	s.options = options
	if s.insertErr != nil {
		return nil, s.insertErr
	}
	for {
		deposit, err := cursor.GetNextTx()
		if err != nil {
			return nil, err
		}
		if deposit == nil {
			break
		}
		s.incoming = append(s.incoming, deposit)
	}
	rawdb.WriteMinorBlock(s.db, block)
	return s.outgoing, nil
}
func (s *minorChainStub) SetCanonicalHead(hash common.Hash) error {
	block := s.GetBlock(hash)
	if block == nil {
		return errors.New("target is not persisted")
	}
	s.current = block
	s.canonicalChanges++
	return nil
}
func (s *minorChainStub) Stop() { s.stops++ }

type connManagerStub struct {
	stages       []string
	payloads     []XShardBroadcast
	headers      []*types.MinorBlock
	counts       []uint32
	tips         []*types.MinorBlockHeader
	rootTips     []*types.RootBlockHeader
	broadcastErr error
}

func (s *connManagerStub) BroadcastXShardTxList(payload XShardBroadcast) error {
	s.payloads = append(s.payloads, payload)
	s.stages = append(s.stages, "xshard")
	return s.broadcastErr
}
func (s *connManagerStub) SendMinorBlockHeaderToMaster(block *types.MinorBlock, count uint32) error {
	s.headers = append(s.headers, block)
	s.counts = append(s.counts, count)
	s.stages = append(s.stages, "header")
	return nil
}
func (s *connManagerStub) BroadcastNewTip(headers []*types.MinorBlockHeader, root *types.RootBlockHeader, branch uint32) error {
	if len(headers) != 1 || headers[0].Branch.Value != branch {
		return errors.New("wrong tip routing")
	}
	s.tips = append(s.tips, headers...)
	s.rootTips = append(s.rootTips, root)
	s.stages = append(s.stages, "tip")
	return nil
}

func coordinatorFixture(t *testing.T) (*ShardCoordinator, *minorChainStub, *connManagerStub, *types.RootBlock) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { db.Close() })
	root := testRootBlock(nil, 0, nil)
	genesis := testMinorBlock(nil, 0, root.Hash(), &types.XShardTxCursorInfo{})
	chain := newMinorChainStub(db, genesis)
	network := new(connManagerStub)
	qkcConfig := config.NewQuarkChainConfig()
	qkcConfig.Update(3, 1, 10, 3)
	coordinator, err := NewShardCoordinator(qkcConfig, qkcConfig.GetShardConfigByFullShardID(1), db, chain, network)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.InitFromRootBlock(root); err != nil {
		t.Fatal(err)
	}
	return coordinator, chain, network, root
}

func TestAddMinorBlockExecutesSetsCanonicalAndPublishes(t *testing.T) {
	c, chain, network, root := coordinatorFixture(t)
	network.stages = nil
	chain.outgoing = []*types.CrossShardTransactionDeposit{testDeposit(0x81)}
	block := testMinorBlock(chain.current, 1, root.Hash(), &types.XShardTxCursorInfo{RootBlockHeight: 1})
	if err := c.AddMinorBlock(block); err != nil {
		t.Fatal(err)
	}
	if chain.current.Hash() != block.Hash() || chain.GetBlock(block.Hash()) == nil {
		t.Fatal("minor candidate was not persisted and made canonical")
	}
	if chain.insertions != 1 || !chain.options.ForceInsert || chain.options.IsCheckDB {
		t.Fatal("execution layer did not receive an actual candidate import")
	}
	if len(chain.incoming) != 1 || !chain.incoming[0].IsFromRootChain || chain.incoming[0].TxHash != root.Hash() {
		t.Fatal("execution did not receive the genesis-root cursor input")
	}
	if !reflect.DeepEqual(network.stages, []string{"xshard", "header", "tip"}) {
		t.Fatalf("propagation order: %v", network.stages)
	}
	if payload := network.payloads[1]; payload.Block.Hash() != block.Hash() || len(payload.Deposits) != 1 {
		t.Fatal("outgoing deposits were not propagated with their source block")
	}
	if network.counts[1] != 1 || network.headers[1].Hash() != block.Hash() || network.tips[0].Hash() != block.Hash() || network.rootTips[0].Hash() != root.Hash() {
		t.Fatal("master header or peer tip is incorrect")
	}
	if err := c.AddMinorBlock(block); err != nil {
		t.Fatal(err)
	}
	if chain.insertions != 1 || len(network.stages) != 3 {
		t.Fatal("known canonical block was executed or republished")
	}
}

func TestAddMinorBlockFailures(t *testing.T) {
	for _, stage := range []string{"execution", "broadcast"} {
		t.Run(stage, func(t *testing.T) {
			c, chain, network, root := coordinatorFixture(t)
			failure := errors.New("injected failure")
			network.stages = nil
			if stage == "execution" {
				chain.insertErr = failure
			} else {
				network.broadcastErr = failure
			}
			genesis := chain.current
			block := testMinorBlock(genesis, 1, root.Hash(), &types.XShardTxCursorInfo{RootBlockHeight: 1})
			if err := c.AddMinorBlock(block); !errors.Is(err, failure) {
				t.Fatalf("lost %s error: %v", stage, err)
			}
			if stage == "execution" {
				if chain.GetBlock(block.Hash()) != nil || chain.current.Hash() != genesis.Hash() || len(network.stages) != 0 {
					t.Fatal("failed execution changed or published the chain")
				}
			} else if chain.GetBlock(block.Hash()) == nil || chain.current.Hash() != block.Hash() {
				t.Fatal("propagation failure rolled back the canonical block")
			}
		})
	}
}

func TestAddRootBlockUpdatesOnlyRootHead(t *testing.T) {
	c, chain, _, root := coordinatorFixture(t)
	block := testMinorBlock(chain.current, 1, root.Hash(), &types.XShardTxCursorInfo{RootBlockHeight: 1})
	if err := c.AddMinorBlock(block); err != nil {
		t.Fatal(err)
	}
	child := testRootBlock(root, 1, types.MinorBlockHeaders{chain.GetBlockByNumber(0).Header()})
	header := child.Header()
	header.TotalDifficulty = big.NewInt(0)
	header.Version = 10
	header.MinorHeaderHash = common.HexToHash("0xab")
	child = types.NewRootBlockWithHeader(header)
	if switched, err := c.AddRootBlock(child); err != nil || !switched {
		t.Fatalf("root must be directly selected without consensus validation: %t, %v", switched, err)
	}
	if chain.current.Hash() != block.Hash() || chain.canonicalChanges != 1 {
		t.Fatal("root import changed minor head")
	}
	if c.GetRootTip().Hash() != child.Hash() || rawdb.ReadRootHeadHash(c.db) != child.Hash() || c.RootBlockByHash(child.Hash()) == nil {
		t.Fatal("root block and head were not persisted")
	}
	if switched, err := c.AddRootBlock(child); err != nil || switched {
		t.Fatalf("duplicate root changed tip: %t, %v", switched, err)
	}
}

func TestInitializationRetriesFailedGenesisPublication(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	root := testRootBlock(nil, 0, nil)
	chain := newMinorChainStub(db, testMinorBlock(nil, 0, root.Hash(), &types.XShardTxCursorInfo{}))
	failure := errors.New("genesis propagation unavailable")
	network := &connManagerStub{broadcastErr: failure}
	qkcConfig := config.NewQuarkChainConfig()
	qkcConfig.Update(3, 1, 10, 3)
	c, err := NewShardCoordinator(qkcConfig, qkcConfig.GetShardConfigByFullShardID(1), db, chain, network)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.InitFromRootBlock(root); !errors.Is(err, failure) {
		t.Fatalf("lost failed publication: %v", err)
	}
	if c.GetRootTip() != nil || rawdb.ReadRootHeadHash(db) != (common.Hash{}) {
		t.Fatal("failed initialization committed the root head")
	}
	network.broadcastErr = nil
	network.stages = nil
	if err := c.InitFromRootBlock(root); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(network.stages, []string{"xshard", "header"}) {
		t.Fatalf("retry skipped unfinished genesis publication: %v", network.stages)
	}
}

func TestCoordinatorLifecycleAndSyncPanic(t *testing.T) {
	c, chain, _, root := coordinatorFixture(t)
	defer func() {
		if recover() == nil {
			t.Error("sync import must panic even for an empty list")
		}
	}()
	c.Stop()
	c.Stop()
	if chain.stops != 1 {
		t.Fatal("execution layer stopped more than once")
	}
	if _, err := c.AddRootBlock(root); !errors.Is(err, ErrChainStopped) {
		t.Fatalf("root import after stop: %v", err)
	}
	if err := c.AddXShardTxList(common.Hash{}, nil); !errors.Is(err, ErrChainStopped) {
		t.Fatalf("cross-shard write after stop: %v", err)
	}
	if err := c.AddMinorBlock(testMinorBlock(chain.current, 1, root.Hash(), &types.XShardTxCursorInfo{})); !errors.Is(err, ErrChainStopped) {
		t.Fatalf("minor import after stop: %v", err)
	}
	c.AddBlockListForSync(nil)
}

func testMinorBlock(parent *types.MinorBlock, number uint64, previousRoot common.Hash, cursor *types.XShardTxCursorInfo) *types.MinorBlock {
	meta := &types.MinorBlockMeta{
		Root:               common.BigToHash(new(big.Int).SetUint64(number + 1)),
		GasUsed:            &serialize.Uint256{Value: new(big.Int)},
		CrossShardGasUsed:  &serialize.Uint256{Value: new(big.Int)},
		XShardGasLimit:     &serialize.Uint256{Value: new(big.Int)},
		XShardTxCursorInfo: cursor,
	}
	header := &types.MinorBlockHeader{Number: number, Time: number, Branch: account.NewBranch(1), PrevRootBlockHash: previousRoot, MetaHash: meta.Hash(), Difficulty: big.NewInt(1), GasLimit: &serialize.Uint256{Value: big.NewInt(1_000_000)}, CoinbaseAmount: qkccommon.NewEmptyTokenBalances()}
	if parent != nil {
		header.ParentHash = parent.Hash()
	}
	return types.NewMinorBlockWithHeader(header, meta)
}

func testRootBlock(parent *types.RootBlock, number uint32, headers types.MinorBlockHeaders) *types.RootBlock {
	header := &types.RootBlockHeader{Number: number, Difficulty: big.NewInt(1), TotalDifficulty: new(big.Int).SetUint64(uint64(number) + 1), CoinbaseAmount: qkccommon.NewEmptyTokenBalances()}
	if parent != nil {
		header.ParentHash = parent.Hash()
	}
	return types.NewRootBlock(header, headers, nil)
}

func testDeposit(hash int64) *types.CrossShardTransactionDeposit {
	return &types.CrossShardTransactionDeposit{TxHash: common.BigToHash(big.NewInt(hash)), Value: &serialize.Uint256{Value: big.NewInt(hash)}, GasPrice: &serialize.Uint256{Value: new(big.Int)}, GasRemained: &serialize.Uint256{Value: new(big.Int)}, RefundRate: 100}
}
