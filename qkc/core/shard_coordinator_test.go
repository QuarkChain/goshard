// Copyright 2026-2027, QuarkChain.

package core

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/types"
)

type minorChainStub struct {
	current             *types.MinorBlock
	blocks              map[common.Hash]*types.MinorBlock
	outgoing            map[common.Hash][]*types.CrossShardTransactionDeposit
	inserted            []*types.MinorBlock
	inputs              []*XShardTxCursor
	insertOptions       []InsertOptions
	missingState        map[common.Hash]bool
	canonicalHeads      []common.Hash
	setCanonicalHeadErr error
	stopped             bool
}

func (s *minorChainStub) CurrentBlock() *types.MinorBlock { return s.current }

func (s *minorChainStub) GetBlock(hash common.Hash) *types.MinorBlock {
	return s.blocks[hash]
}

func (s *minorChainStub) GetBlockByNumber(number uint64) *types.MinorBlock {
	for block := s.current; block != nil && block.NumberU64() >= number; block = s.blocks[block.ParentHash()] {
		if block.NumberU64() == number {
			return block
		}
	}
	return nil
}

func (s *minorChainStub) HasState(root common.Hash) bool {
	return !s.missingState[root]
}

func (s *minorChainStub) InsertChainWithXShardInputs(chain []*types.MinorBlock, inputs []*XShardTxCursor, options InsertOptions) (int, [][]*types.CrossShardTransactionDeposit, error) {
	s.inputs = append(s.inputs, inputs...)
	s.insertOptions = append(s.insertOptions, options)
	outputs := make([][]*types.CrossShardTransactionDeposit, 0, len(chain))
	for _, block := range chain {
		s.inserted = append(s.inserted, block)
		s.blocks[block.Hash()] = block
		if s.current == nil || block.ParentHash() == s.current.Hash() || block.NumberU64() > s.current.NumberU64() {
			s.current = block
		}
		outputs = append(outputs, s.outgoing[block.Hash()])
	}
	return len(chain), outputs, nil
}

func (s *minorChainStub) SetCanonicalHead(hash common.Hash) error {
	if s.setCanonicalHeadErr != nil {
		err := s.setCanonicalHeadErr
		s.setCanonicalHeadErr = nil
		return err
	}
	block := s.blocks[hash]
	if block == nil {
		return ErrConfirmedMinorBlockUnavailable
	}
	s.current = block
	s.canonicalHeads = append(s.canonicalHeads, hash)
	return nil
}

func (s *minorChainStub) Stop() { s.stopped = true }

type connManagerStub struct {
	singleXShard []XShardBroadcast
	batchXShard  [][]XShardBroadcast
	singleHeader []*types.MinorBlock
	xShardCounts []uint32
	headerLists  [][]*types.MinorBlock
	tips         []*types.MinorBlockHeader
}

func (s *connManagerStub) SendMinorBlockHeaderToMaster(block *types.MinorBlock, xShardTxCount uint32) error {
	s.singleHeader = append(s.singleHeader, block)
	s.xShardCounts = append(s.xShardCounts, xShardTxCount)
	return nil
}
func (s *connManagerStub) SendMinorBlockHeaderListToMaster(blocks []*types.MinorBlock) error {
	s.headerLists = append(s.headerLists, blocks)
	return nil
}
func (s *connManagerStub) BroadcastXShardTxList(payload XShardBroadcast) error {
	s.singleXShard = append(s.singleXShard, payload)
	return nil
}
func (s *connManagerStub) BatchBroadcastXShardTxList(payloads []XShardBroadcast) error {
	s.batchXShard = append(s.batchXShard, payloads)
	return nil
}
func (s *connManagerStub) BroadcastNewTip(headers []*types.MinorBlockHeader, _ *types.RootBlockHeader, _ uint32) error {
	s.tips = append(s.tips, headers...)
	return nil
}
func (*connManagerStub) BroadcastTransactions(string, uint32, []*types.Transaction) error { return nil }
func (*connManagerStub) BroadcastMinorBlock(string, *types.MinorBlock) error              { return nil }
func (*connManagerStub) GetMinorBlocks([]common.Hash, string, uint32) ([]*types.MinorBlock, error) {
	return nil, nil
}
func (*connManagerStub) GetMinorBlockHeaderList(*wire.GetMinorBlockHeaderListWithSkipRequest) ([]*types.MinorBlockHeader, error) {
	return nil, nil
}

func TestNewShardCoordinatorValidatesDependencies(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	db := rawdb.NewMemoryDatabase()
	chain := newMinorChainStub(testMinorBlock(nil, 0, common.HexToHash("0x01"), common.Hash{}))

	tests := []struct {
		name        string
		qkcConfig   *config.QuarkChainConfig
		shardConfig *config.ShardConfig
		db          bool
		chain       MinorChain
		want        error
	}{
		{name: "nil qkc config", shardConfig: shardConfig, db: true, chain: chain, want: ErrQuarkChainConfigUnavailable},
		{name: "nil shard config", qkcConfig: qkcConfig, db: true, chain: chain, want: ErrShardConfigUnavailable},
		{name: "nil database", qkcConfig: qkcConfig, shardConfig: shardConfig, chain: chain, want: ErrDatabaseUnavailable},
		{name: "nil minor chain", qkcConfig: qkcConfig, shardConfig: shardConfig, db: true, want: ErrMinorChainUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var database = db
			if !test.db {
				database = nil
			}
			if _, err := NewShardCoordinator(test.qkcConfig, test.shardConfig, database, test.chain, new(connManagerStub)); !errors.Is(err, test.want) {
				t.Fatalf("NewShardCoordinator error = %v, want %v", err, test.want)
			}
		})
	}
	if _, err := NewShardCoordinator(qkcConfig, shardConfig, db, chain, nil); !errors.Is(err, ErrConnManagerUnavailable) {
		t.Fatalf("nil connection manager error = %v, want %v", err, ErrConnManagerUnavailable)
	}
	if coordinator, err := NewShardCoordinator(qkcConfig, shardConfig, db, chain, new(connManagerStub)); err != nil || coordinator == nil {
		t.Fatalf("valid NewShardCoordinator = %v, want success", err)
	}
}

func TestShardCoordinatorInitializesAtGenesisRoot(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x10"), rootGenesis.Hash())
	chain := newMinorChainStub(minorGenesis)
	db := rawdb.NewMemoryDatabase()
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, db, chain)

	if err := coordinator.InitFromRootBlock(rootGenesis); err != nil {
		t.Fatal(err)
	}
	if got := coordinator.GetRootTip(); got == nil || got.Hash() != rootGenesis.Hash() {
		t.Fatalf("root tip = %v, want %s", got, rootGenesis.Hash())
	}
	if coordinator.GetConfirmedMinorTip() != nil {
		t.Fatal("genesis root unexpectedly confirmed a minor block")
	}
	if got := rawdb.ReadRootHeadHash(db); got != rootGenesis.Hash() {
		t.Fatalf("stored root head = %s, want %s", got, rootGenesis.Hash())
	}
	if got := rawdb.ReadRootCanonicalHash(db, 0); got != rootGenesis.Hash() {
		t.Fatalf("canonical root at height 0 = %s, want %s", got, rootGenesis.Hash())
	}
	if err := coordinator.InitFromRootBlock(rootGenesis); !errors.Is(err, ErrRootChainAlreadyInitialized) {
		t.Fatalf("second initialization error = %v, want %v", err, ErrRootChainAlreadyInitialized)
	}
}

func TestAddRootBlockAdvancesConfirmedMinorTip(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x20"), rootGenesis.Hash())
	minorChild := testMinorBlock(minorGenesis, 1, common.HexToHash("0x21"), rootGenesis.Hash())
	minorGrandchild := testMinorBlock(minorChild, 2, common.HexToHash("0x22"), rootGenesis.Hash())
	rootChild := testRootBlock(rootGenesis, 1, types.MinorBlockHeaders{minorGenesis.Header(), minorChild.Header()})
	chain := newMinorChainStub(minorGenesis, minorChild, minorGrandchild)
	chain.current = minorGrandchild
	db := rawdb.NewMemoryDatabase()
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, db, chain)
	mustInitCoordinator(t, coordinator, rootGenesis)

	switched, err := coordinator.AddRootBlock(rootChild)
	if err != nil {
		t.Fatal(err)
	}
	if !switched {
		t.Fatal("higher-total-difficulty root did not become canonical")
	}
	if chain.current.Hash() != minorGrandchild.Hash() {
		t.Fatal("root confirmation rewound a compatible unconfirmed minor suffix")
	}
	if got := coordinator.GetConfirmedMinorTip(); got == nil || got.Hash() != minorChild.Hash() {
		t.Fatalf("confirmed minor tip = %v, want %s", got, minorChild.Hash())
	}
	if got := rawdb.ReadLastConfirmedMinorBlockHeaderAtRootBlock(db, rootChild.Hash()); got != minorChild.Hash() {
		t.Fatalf("stored confirmed minor = %s, want %s", got, minorChild.Hash())
	}
}

func TestAddRootBlockSwitchesOnlyToHigherDifficultyFork(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	canonical := testRootBlock(rootGenesis, 1, nil)
	sideHeader := canonical.Header()
	sideHeader.Nonce++
	side := types.NewRootBlock(sideHeader, nil, nil)
	sideChild := testRootBlock(side, 2, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x30"), rootGenesis.Hash())
	chain := newMinorChainStub(minorGenesis)
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, rawdb.NewMemoryDatabase(), chain)
	mustInitCoordinator(t, coordinator, rootGenesis)

	if switched, err := coordinator.AddRootBlock(canonical); err != nil || !switched {
		t.Fatalf("add canonical root = %t, %v", switched, err)
	}
	if switched, err := coordinator.AddRootBlock(side); err != nil || switched {
		t.Fatalf("add equal-difficulty side root = %t, %v", switched, err)
	}
	if coordinator.GetRootTip().Hash() != canonical.Hash() {
		t.Fatal("equal-difficulty side root replaced the root tip")
	}
	if switched, err := coordinator.AddRootBlock(sideChild); err != nil || !switched {
		t.Fatalf("add higher-difficulty side child = %t, %v", switched, err)
	}
	if coordinator.GetRootTip().Hash() != sideChild.Hash() {
		t.Fatal("higher-difficulty root fork did not replace the root tip")
	}
	if hash := rawdb.ReadRootCanonicalHash(coordinator.db, 1); hash != side.Hash() {
		t.Fatalf("canonical root at height 1 = %s, want %s", hash, side.Hash())
	}
	if hash := rawdb.ReadRootCanonicalHash(coordinator.db, 2); hash != sideChild.Hash() {
		t.Fatalf("canonical root at height 2 = %s, want %s", hash, sideChild.Hash())
	}
}

func TestAddRootBlockSelectsShorterHigherDifficultyFork(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	canonical := testRootBlock(rootGenesis, 1, nil)
	canonicalChild := testRootBlock(canonical, 2, nil)
	heavyHeader := canonical.Header()
	heavyHeader.Nonce++
	heavyHeader.Difficulty = big.NewInt(10)
	heavyHeader.TotalDifficulty = big.NewInt(10)
	heavy := types.NewRootBlock(heavyHeader, nil, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x33"), rootGenesis.Hash())
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, rawdb.NewMemoryDatabase(), newMinorChainStub(minorGenesis))
	mustInitCoordinator(t, coordinator, rootGenesis)

	if switched, err := coordinator.AddRootBlock(canonical); err != nil || !switched {
		t.Fatalf("add canonical root = %t, %v", switched, err)
	}
	if switched, err := coordinator.AddRootBlock(canonicalChild); err != nil || !switched {
		t.Fatalf("add canonical root child = %t, %v", switched, err)
	}
	if switched, err := coordinator.AddRootBlock(heavy); err != nil || !switched {
		t.Fatalf("add shorter higher-difficulty root = %t, %v", switched, err)
	}
	if got := coordinator.GetRootTip().Hash(); got != heavy.Hash() {
		t.Fatalf("root tip = %s, want %s", got, heavy.Hash())
	}
	if got := rawdb.ReadRootCanonicalHash(coordinator.db, 1); got != heavy.Hash() {
		t.Fatalf("canonical root at height 1 = %s, want %s", got, heavy.Hash())
	}
	if got := rawdb.ReadRootCanonicalHash(coordinator.db, 2); got != (common.Hash{}) {
		t.Fatalf("stale canonical root at height 2 = %s", got)
	}
}

func TestAddRootBlockRequiresGenesisAsFirstMinorConfirmation(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x35"), rootGenesis.Hash())
	minorChild := testMinorBlock(minorGenesis, 1, common.HexToHash("0x36"), rootGenesis.Hash())
	rootChild := testRootBlock(rootGenesis, 1, types.MinorBlockHeaders{minorChild.Header()})
	chain := newMinorChainStub(minorGenesis, minorChild)
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, rawdb.NewMemoryDatabase(), chain)
	mustInitCoordinator(t, coordinator, rootGenesis)

	if _, err := coordinator.AddRootBlock(rootChild); !errors.Is(err, ErrMinorNotConfirmedDescendant) {
		t.Fatalf("first minor confirmation error = %v, want %v", err, ErrMinorNotConfirmedDescendant)
	}
}

func TestAddRootBlockSelectsConfirmedMinorFork(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x40"), rootGenesis.Hash())
	canonicalRoot := testRootBlock(rootGenesis, 1, types.MinorBlockHeaders{minorGenesis.Header()})
	sideHeader := canonicalRoot.Header()
	sideHeader.Nonce++
	sideRoot := types.NewRootBlock(sideHeader, types.MinorBlockHeaders{minorGenesis.Header()}, nil)
	canonicalMinor := testMinorBlock(minorGenesis, 1, common.HexToHash("0x41"), canonicalRoot.Hash())
	sideMinor := testMinorBlock(minorGenesis, 1, common.HexToHash("0x42"), sideRoot.Hash())
	sideConfirmingRoot := testRootBlock(sideRoot, 2, types.MinorBlockHeaders{sideMinor.Header()})
	chain := newMinorChainStub(minorGenesis, canonicalMinor, sideMinor)
	chain.current = canonicalMinor
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, rawdb.NewMemoryDatabase(), chain)
	mustInitCoordinator(t, coordinator, rootGenesis)

	if _, err := coordinator.AddRootBlock(canonicalRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.AddRootBlock(sideRoot); err != nil {
		t.Fatal(err)
	}
	if switched, err := coordinator.AddRootBlock(sideConfirmingRoot); err != nil || !switched {
		t.Fatalf("add root confirming minor fork = %t, %v", switched, err)
	}
	if chain.CurrentBlock().Hash() != sideMinor.Hash() || coordinator.GetConfirmedMinorTip().Hash() != sideMinor.Hash() {
		t.Fatal("root fork did not select its confirmed minor fork")
	}
}

// This mirrors pyquarkchain's test_add_root_block_revert_header_tip.
func TestAddRootBlockRewindsMinorHeadFromOldRootFork(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x50"), rootGenesis.Hash())
	oldRoot := testRootBlock(rootGenesis, 1, types.MinorBlockHeaders{minorGenesis.Header()})
	sideHeader := oldRoot.Header()
	sideHeader.Nonce++
	sideRoot := types.NewRootBlock(sideHeader, types.MinorBlockHeaders{minorGenesis.Header()}, nil)
	minorChild := testMinorBlock(minorGenesis, 1, common.HexToHash("0x51"), rootGenesis.Hash())
	minorTip := testMinorBlock(minorChild, 2, common.HexToHash("0x52"), oldRoot.Hash())
	newRoot := testRootBlock(sideRoot, 2, types.MinorBlockHeaders{minorChild.Header()})
	chain := newMinorChainStub(minorGenesis, minorChild, minorTip)
	chain.current = minorChild
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, rawdb.NewMemoryDatabase(), chain)
	mustInitCoordinator(t, coordinator, rootGenesis)

	if switched, err := coordinator.AddRootBlock(oldRoot); err != nil || !switched {
		t.Fatalf("add old root = %t, %v", switched, err)
	}
	if switched, err := coordinator.AddRootBlock(sideRoot); err != nil || switched {
		t.Fatalf("add side root = %t, %v", switched, err)
	}
	chain.current = minorTip
	if switched, err := coordinator.AddRootBlock(newRoot); err != nil || !switched {
		t.Fatalf("add higher-difficulty side root = %t, %v", switched, err)
	}
	if chain.CurrentBlock().Hash() != minorChild.Hash() {
		t.Fatalf("minor head = %s, want %s", chain.CurrentBlock().Hash(), minorChild.Hash())
	}
	if len(chain.canonicalHeads) != 1 || chain.canonicalHeads[0] != minorChild.Hash() {
		t.Fatalf("canonical head updates = %v, want one rewind to %s", chain.canonicalHeads, minorChild.Hash())
	}
}

func TestAddRootBlockCanRetryCanonicalSwitch(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x55"), rootGenesis.Hash())
	canonicalMinor := testMinorBlock(minorGenesis, 1, common.HexToHash("0x56"), rootGenesis.Hash())
	confirmedMinor := testMinorBlock(minorGenesis, 1, common.HexToHash("0x57"), rootGenesis.Hash())
	rootChild := testRootBlock(rootGenesis, 1, types.MinorBlockHeaders{minorGenesis.Header(), confirmedMinor.Header()})
	chain := newMinorChainStub(minorGenesis, canonicalMinor, confirmedMinor)
	chain.current = canonicalMinor
	chain.setCanonicalHeadErr = errors.New("set canonical head failed")
	db := rawdb.NewMemoryDatabase()
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, db, chain)
	mustInitCoordinator(t, coordinator, rootGenesis)

	if switched, err := coordinator.AddRootBlock(rootChild); err == nil || switched {
		t.Fatalf("first root import = %t, %v; want canonical switch failure", switched, err)
	}
	if rawdb.ReadRootBlock(db, rootChild.Hash()) == nil {
		t.Fatal("failed canonical switch did not retain the imported root block")
	}
	if coordinator.GetRootTip().Hash() != rootGenesis.Hash() {
		t.Fatal("failed canonical switch changed the root tip")
	}
	if switched, err := coordinator.AddRootBlock(rootChild); err != nil || !switched {
		t.Fatalf("retry root import = %t, %v; want successful canonical switch", switched, err)
	}
	if chain.CurrentBlock().Hash() != confirmedMinor.Hash() {
		t.Fatalf("minor head after retry = %s, want %s", chain.CurrentBlock().Hash(), confirmedMinor.Hash())
	}
}

func TestAddRootBlockRequiresRemoteXShardList(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	rootParent := testRootBlock(rootGenesis, 1, nil)
	remote := testMinorBlock(nil, 1, common.HexToHash("0x60"), rootParent.Hash()).Header()
	remote.Branch = account.NewBranch(1<<16 | 1)
	root := testRootBlock(rootParent, 2, types.MinorBlockHeaders{remote})
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x61"), rootGenesis.Hash())
	chain := newMinorChainStub(minorGenesis)
	db := rawdb.NewMemoryDatabase()
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, db, chain)
	mustInitCoordinator(t, coordinator, rootGenesis)
	if _, err := coordinator.AddRootBlock(rootParent); err != nil {
		t.Fatal(err)
	}

	if _, err := coordinator.AddRootBlock(root); !errors.Is(err, ErrRemoteXShardTxListUnavailable) {
		t.Fatalf("missing remote list error = %v, want %v", err, ErrRemoteXShardTxListUnavailable)
	}
	rawdb.WriteCrossShardTxList(db, remote.Hash(), types.NewCrossShardTransactionList(nil))
	if switched, err := coordinator.AddRootBlock(root); err != nil || !switched {
		t.Fatalf("add root after remote list = %t, %v", switched, err)
	}
}

func TestAddRootBlockRejectsUnexpectedRemoteXShardList(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	remote := testMinorBlock(nil, 1, common.HexToHash("0x65"), rootGenesis.Hash()).Header()
	remote.Branch = account.NewBranch(1<<16 | 1)
	rootChild := testRootBlock(rootGenesis, 1, types.MinorBlockHeaders{remote})
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x66"), rootGenesis.Hash())
	db := rawdb.NewMemoryDatabase()
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, db, newMinorChainStub(minorGenesis))
	mustInitCoordinator(t, coordinator, rootGenesis)
	rawdb.WriteCrossShardTxList(db, remote.Hash(), types.NewCrossShardTransactionList(nil))

	if _, err := coordinator.AddRootBlock(rootChild); !errors.Is(err, ErrUnexpectedRemoteXShardTxList) {
		t.Fatalf("unexpected remote list error = %v, want %v", err, ErrUnexpectedRemoteXShardTxList)
	}
}

func TestShardCoordinatorRecoversConfirmedTips(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x70"), rootGenesis.Hash())
	minorChild := testMinorBlock(minorGenesis, 1, common.HexToHash("0x71"), rootGenesis.Hash())
	minorTip := testMinorBlock(minorChild, 2, common.HexToHash("0x72"), rootGenesis.Hash())
	rootChild := testRootBlock(rootGenesis, 1, types.MinorBlockHeaders{minorGenesis.Header(), minorChild.Header()})
	chain := newMinorChainStub(minorGenesis, minorChild, minorTip)
	chain.current = minorTip
	db := rawdb.NewMemoryDatabase()
	first := mustNewShardCoordinator(t, qkcConfig, shardConfig, db, chain)
	mustInitCoordinator(t, first, rootGenesis)
	if _, err := first.AddRootBlock(rootChild); err != nil {
		t.Fatal(err)
	}

	chain.current = minorTip
	restarted := mustNewShardCoordinator(t, qkcConfig, shardConfig, db, chain)
	if err := restarted.InitFromRootBlock(rootChild); err != nil {
		t.Fatal(err)
	}
	if chain.current.Hash() != minorChild.Hash() {
		t.Fatalf("recovered minor head = %s, want %s", chain.current.Hash(), minorChild.Hash())
	}
	if restarted.GetRootTip().Hash() != rootChild.Hash() || restarted.GetConfirmedMinorTip().Hash() != minorChild.Hash() {
		t.Fatal("coordinator did not recover root and confirmed-minor tips")
	}
}

func TestShardCoordinatorRecoveryRebuildsRootCanonicalIndexes(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	rootChild := testRootBlock(rootGenesis, 1, nil)
	rootTip := testRootBlock(rootChild, 2, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x75"), rootGenesis.Hash())
	db := rawdb.NewMemoryDatabase()
	rawdb.WriteRootBlock(db, rootGenesis)
	rawdb.WriteRootBlock(db, rootChild)
	rawdb.WriteRootBlock(db, rootTip)
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, db, newMinorChainStub(minorGenesis))

	if err := coordinator.InitFromRootBlock(rootTip); err != nil {
		t.Fatal(err)
	}
	for number, root := range []*types.RootBlock{rootGenesis, rootChild, rootTip} {
		if got := rawdb.ReadRootCanonicalHash(db, uint64(number)); got != root.Hash() {
			t.Fatalf("canonical root at height %d = %s, want %s", number, got, root.Hash())
		}
	}
}

func TestShardCoordinatorStopRejectsRootBlocks(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x80"), rootGenesis.Hash())
	chain := newMinorChainStub(minorGenesis)
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, rawdb.NewMemoryDatabase(), chain)
	mustInitCoordinator(t, coordinator, rootGenesis)
	coordinator.Stop()
	coordinator.Stop()

	if !chain.stopped {
		t.Fatal("coordinator did not stop its minor chain")
	}
	if _, err := coordinator.AddRootBlock(testRootBlock(rootGenesis, 1, nil)); !errors.Is(err, ErrChainStopped) {
		t.Fatalf("AddRootBlock after Stop error = %v, want %v", err, ErrChainStopped)
	}
}

func TestAddMinorBlockReplaysAndPropagates(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0x90"), rootGenesis.Hash())
	minorChild := testMinorBlock(minorGenesis, 1, common.HexToHash("0x91"), rootGenesis.Hash())
	outgoing := &types.CrossShardTransactionDeposit{TxHash: common.HexToHash("0x92")}
	chain := newMinorChainStub(minorGenesis)
	chain.outgoing[minorChild.Hash()] = []*types.CrossShardTransactionDeposit{outgoing}
	manager := new(connManagerStub)
	coordinator, err := NewShardCoordinator(qkcConfig, shardConfig, rawdb.NewMemoryDatabase(), chain, manager)
	if err != nil {
		t.Fatal(err)
	}
	mustInitCoordinator(t, coordinator, rootGenesis)

	if err := coordinator.AddMinorBlock(minorChild); err != nil {
		t.Fatal(err)
	}
	if len(chain.inserted) != 1 || chain.inserted[0] != minorChild || len(chain.inputs) != 1 {
		t.Fatal("minor block was not replayed with an x-shard cursor")
	}
	if len(chain.insertOptions) != 1 || !chain.insertOptions[0].ForceInsert {
		t.Fatal("minor block replay did not force insertion")
	}
	if len(manager.singleXShard) != 1 || manager.singleXShard[0].Block != minorChild || len(manager.singleXShard[0].Deposits) != 1 || manager.singleXShard[0].Deposits[0] != outgoing {
		t.Fatal("minor block x-shard output was not broadcast")
	}
	if len(manager.singleHeader) != 1 || manager.singleHeader[0] != minorChild {
		t.Fatal("minor block header was not sent to master")
	}
	if len(manager.xShardCounts) != 1 || manager.xShardCounts[0] != 1 {
		t.Fatalf("x-shard count sent to master = %v, want [1]", manager.xShardCounts)
	}
	if len(manager.tips) != 1 || manager.tips[0].Hash() != minorChild.Hash() {
		t.Fatal("new canonical minor tip was not broadcast")
	}
}

func TestAddBlockListForSyncReplaysAndPropagatesInBatches(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0xa0"), rootGenesis.Hash())
	first := testMinorBlock(minorGenesis, 1, common.HexToHash("0xa1"), rootGenesis.Hash())
	second := testMinorBlock(first, 2, common.HexToHash("0xa2"), rootGenesis.Hash())
	chain := newMinorChainStub(minorGenesis)
	manager := new(connManagerStub)
	coordinator, err := NewShardCoordinator(qkcConfig, shardConfig, rawdb.NewMemoryDatabase(), chain, manager)
	if err != nil {
		t.Fatal(err)
	}
	mustInitCoordinator(t, coordinator, rootGenesis)

	if err := coordinator.AddBlockListForSync([]*types.MinorBlock{first, second}); err != nil {
		t.Fatal(err)
	}
	if len(chain.inserted) != 2 || chain.inserted[0] != first || chain.inserted[1] != second {
		t.Fatal("sync blocks were not replayed in order")
	}
	for _, options := range chain.insertOptions {
		if !options.ForceInsert {
			t.Fatal("sync block replay did not force insertion")
		}
	}
	if len(manager.batchXShard) != 1 || len(manager.batchXShard[0]) != 2 {
		t.Fatal("sync x-shard outputs were not sent in one batch")
	}
	if len(manager.headerLists) != 1 || len(manager.headerLists[0]) != 2 {
		t.Fatal("sync headers were not sent to master in one batch")
	}
	if len(manager.tips) != 0 {
		t.Fatal("sync replay unexpectedly broadcast a peer tip")
	}
}

func TestAddBlockListForSyncRejectsOutOfOrderBlocks(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0xb0"), rootGenesis.Hash())
	first := testMinorBlock(minorGenesis, 1, common.HexToHash("0xb1"), rootGenesis.Hash())
	second := testMinorBlock(first, 2, common.HexToHash("0xb2"), rootGenesis.Hash())
	chain := newMinorChainStub(minorGenesis)
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, rawdb.NewMemoryDatabase(), chain)

	if err := coordinator.AddBlockListForSync([]*types.MinorBlock{second, first}); !errors.Is(err, ErrMinorBlockOrder) {
		t.Fatalf("AddBlockListForSync error = %v, want %v", err, ErrMinorBlockOrder)
	}
	if len(chain.inserted) != 0 {
		t.Fatal("out-of-order sync list was partially replayed")
	}
}

func TestXShardTxCursorStartsAfterParentCursor(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	rootParent := testRootBlock(rootGenesis, 1, nil)
	remote := testMinorBlock(nil, 1, common.HexToHash("0xc0"), rootParent.Hash()).Header()
	remote.Branch = account.NewBranch(1<<16 | 1)
	root := testRootBlock(rootParent, 2, types.MinorBlockHeaders{remote})
	db := rawdb.NewMemoryDatabase()
	rawdb.WriteRootBlock(db, rootGenesis)
	rawdb.WriteRootBlock(db, rootParent)
	rawdb.WriteRootBlock(db, root)
	deposits := []*types.CrossShardTransactionDeposit{
		{TxHash: common.HexToHash("0xc1")},
		{TxHash: common.HexToHash("0xc2")},
		{TxHash: common.HexToHash("0xc3")},
	}
	rawdb.WriteCrossShardTxList(db, remote.Hash(), types.NewCrossShardTransactionList(deposits))
	parent := testMinorBlock(nil, 1, common.HexToHash("0xc4"), root.Hash())
	parentMeta := parent.Meta()
	parentMeta.XShardTxCursorInfo = &types.XShardTxCursorInfo{RootBlockHeight: 2, MinorBlockIndex: 1, XShardDepositIndex: 0}
	parentHeader := parent.Header()
	parentHeader.MetaHash = parentMeta.Hash()
	parent = types.NewMinorBlockWithHeader(parentHeader, parentMeta)
	block := testMinorBlock(parent, 2, common.HexToHash("0xc5"), root.Hash())
	chain := newMinorChainStub(parent)
	coordinator, err := NewShardCoordinator(qkcConfig, shardConfig, db, chain, new(connManagerStub))
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := coordinator.xShardExecutionInput(block, parent)
	if err != nil {
		t.Fatal(err)
	}
	first, err := cursor.GetNextTx()
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || first.TxHash != deposits[1].TxHash {
		t.Fatalf("first x-shard deposit = %+v, want the deposit after the parent cursor", first)
	}
	remaining := collectXShardDeposits(t, cursor)
	if len(remaining) != 1 || remaining[0].TxHash != deposits[2].TxHash {
		t.Fatalf("remaining x-shard deposits = %+v", remaining)
	}
	if info := cursor.GetCursorInfo(); *info != (types.XShardTxCursorInfo{RootBlockHeight: 3}) {
		t.Fatalf("x-shard cursor = %+v, want root height 3 EOF", info)
	}
}

func TestAddXShardTxListPersistsDeposits(t *testing.T) {
	qkcConfig, shardConfig := testShardConfig(0)
	rootGenesis := testRootBlock(nil, 0, nil)
	minorGenesis := testMinorBlock(nil, 0, common.HexToHash("0xd0"), rootGenesis.Hash())
	db := rawdb.NewMemoryDatabase()
	coordinator := mustNewShardCoordinator(t, qkcConfig, shardConfig, db, newMinorChainStub(minorGenesis))
	hash := common.HexToHash("0xd1")
	deposit := &types.CrossShardTransactionDeposit{TxHash: common.HexToHash("0xd2")}

	if err := coordinator.AddXShardTxList(hash, []*types.CrossShardTransactionDeposit{deposit}); err != nil {
		t.Fatal(err)
	}
	stored := rawdb.ReadCrossShardTxList(db, hash)
	if stored == nil || len(stored.TXList) != 1 || stored.TXList[0].TxHash != deposit.TxHash {
		t.Fatal("x-shard transaction list was not persisted")
	}
}

func newMinorChainStub(blocks ...*types.MinorBlock) *minorChainStub {
	stub := &minorChainStub{
		blocks:       make(map[common.Hash]*types.MinorBlock),
		outgoing:     make(map[common.Hash][]*types.CrossShardTransactionDeposit),
		missingState: make(map[common.Hash]bool),
	}
	for _, block := range blocks {
		stub.blocks[block.Hash()] = block
		stub.current = block
	}
	return stub
}

func mustNewShardCoordinator(t *testing.T, qkcConfig *config.QuarkChainConfig, shardConfig *config.ShardConfig, db ethdb.Database, chain MinorChain) *ShardCoordinator {
	t.Helper()
	coordinator, err := NewShardCoordinator(qkcConfig, shardConfig, db, chain, new(connManagerStub))
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func mustInitCoordinator(t *testing.T, coordinator *ShardCoordinator, root *types.RootBlock) {
	t.Helper()
	if err := coordinator.InitFromRootBlock(root); err != nil {
		t.Fatal(err)
	}
}

func testShardConfig(genesisRootHeight uint32) (*config.QuarkChainConfig, *config.ShardConfig) {
	qkcConfig := config.NewQuarkChainConfig()
	qkcConfig.Update(3, 1, 10, 3)
	shardConfig := qkcConfig.GetShardConfigByFullShardID(1)
	shardConfig.Genesis.RootHeight = genesisRootHeight
	return qkcConfig, shardConfig
}

func testMinorBlock(parent *types.MinorBlock, number uint64, stateRoot, previousRoot common.Hash) *types.MinorBlock {
	parentHash := common.Hash{}
	if parent != nil {
		parentHash = parent.Hash()
	}
	meta := &types.MinorBlockMeta{
		Root:               stateRoot,
		GasUsed:            &serialize.Uint256{Value: new(big.Int)},
		CrossShardGasUsed:  &serialize.Uint256{Value: new(big.Int)},
		XShardTxCursorInfo: new(types.XShardTxCursorInfo),
		XShardGasLimit:     &serialize.Uint256{Value: new(big.Int)},
	}
	header := &types.MinorBlockHeader{
		Branch:            account.NewBranch(1),
		Number:            number,
		Time:              number,
		ParentHash:        parentHash,
		PrevRootBlockHash: previousRoot,
		CoinbaseAmount:    qkccommon.NewEmptyTokenBalances(),
		GasLimit:          &serialize.Uint256{Value: big.NewInt(1_000_000)},
		Difficulty:        big.NewInt(1),
		MetaHash:          meta.Hash(),
	}
	return types.NewMinorBlockWithHeader(header, meta)
}

func collectXShardDeposits(t *testing.T, cursor *XShardTxCursor) []*types.CrossShardTransactionDeposit {
	t.Helper()
	var deposits []*types.CrossShardTransactionDeposit
	for {
		deposit, err := cursor.GetNextTx()
		if err != nil {
			t.Fatal(err)
		}
		if deposit == nil {
			return deposits
		}
		deposits = append(deposits, deposit)
	}
}

func testRootBlock(parent *types.RootBlock, number uint32, headers types.MinorBlockHeaders) *types.RootBlock {
	parentHash := common.Hash{}
	if parent != nil {
		parentHash = parent.Hash()
	}
	header := &types.RootBlockHeader{
		Number:          number,
		ParentHash:      parentHash,
		CoinbaseAmount:  qkccommon.NewEmptyTokenBalances(),
		Difficulty:      big.NewInt(1),
		TotalDifficulty: new(big.Int).SetUint64(uint64(number + 1)),
	}
	return types.NewRootBlock(header, headers, nil)
}
