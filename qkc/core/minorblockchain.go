// Ported from github.com/QuarkChain/goquarkchain/core (structural subset, faithful
// where it does not require the execution layer).
// Modified from go-ethereum under GNU Lesser General Public License
//
// TODO(execution-issue): the deferred execution layer owns the parts that were
// removed from goquarkchain's MinorBlockChain here, namely:
//   - stateCache/currentEvmState/triegc (geth core/state wiring), State()/StateAt()
//   - the TxPool, PoSW calculator, gas-price oracle and cross-shard tx caches
//   - Process/ValidateState/WriteBlockWithState inside the import path
//   - reorg handling that re-executes side chains
//
// The structural import below validates blocks (header rules, roots recomputable
// from the block itself, root-chain anchoring) and maintains storage + the
// canonical head, exactly as a base for the execution layer to plug into.
package core

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/consensus"
	"github.com/ethereum/go-ethereum/qkc/core/rawdb"
	qkcParams "github.com/ethereum/go-ethereum/qkc/params"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/types"
)

const (
	blockCacheLimit     = 1024 // TODO really need 1024?
	receiptsCacheLimit  = 32
	maxFutureBlocks     = 32
	maxTimeFutureBlocks = 30

	maxCrossShardLimit       = 256
	maxRootBlockLimit        = 128
	maxLastConfirmLimit      = 256
	maxGasPriceCacheLimit    = 128
	maxCacheCountHeight2Hash = 1024

	statsReportLimit = 8 * time.Second

	ALLOWED_FUTURE_BLOCKS_TIME_VALIDATION = uint64(15)
)

// WriteStatus status of write
type WriteStatus byte

const (
	NonStatTy WriteStatus = iota
	CanonStatTy
	SideStatTy
)

// CacheConfig contains the configuration values for the trie caching/pruning
// that's resident in a blockchain.
type CacheConfig struct {
	Disabled       bool          // Whether to disable trie write caching (archive node)
	TrieCleanLimit int           // Memory allowance (MB) to use for caching trie nodes in memory
	TrieDirtyLimit int           // Memory limit (MB) at which to start flushing dirty trie nodes to disk
	TrieTimeLimit  time.Duration // Time limit after which to flush the current in-memory trie to disk
}

type CoinbaseAmountAboutHeight struct {
	CoinbaseAmount *types.TokenBalances
	StakePreBlock  big.Int
}

// MinorBlockChain represents the canonical chain given a database with a genesis
// block. The Blockchain manages chain imports, reverts, chain reorganisations.
//
// Importing blocks in to the block chain happens according to the set of rules
// defined by the two stage Validator. Processing of blocks is done using the
// Processor which processes the included transaction. The validation of the state
// is done in the second part of the Validator. Failing results in aborting of
// the import.
//
// The MinorBlockChain also helps in returning blocks from **any** chain included
// in the database as well as blocks that represents the canonical chain. It's
// important to note that GetBlock can return any block and does not need to be
// included in the canonical one where as GetBlockByNumber always represents the
// canonical chain.
type MinorBlockChain struct {
	ethChainConfig *params.ChainConfig
	clusterConfig  *config.ClusterConfig // Chain & network configuration
	cacheConfig    *CacheConfig          // Cache configuration for pruning

	db ethdb.Database // Low level persistent database to store final content in

	rmLogsFeed    event.Feed
	chainFeed     event.Feed
	chainSideFeed event.Feed
	chainHeadFeed event.Feed
	logsFeed      event.Feed
	subLogsFeed   event.Feed
	scope         event.SubscriptionScope
	genesisBlock  *types.MinorBlock

	mu      sync.RWMutex // global mutex for locking chain operations
	chainmu sync.RWMutex // blockchain insertion lock
	procmu  sync.RWMutex // block processor lock

	checkpoint   int          // checkpoint counts towards the new checkpoint
	currentBlock atomic.Value // Current head of the block chain

	receiptsCache       *lru.Cache[common.Hash, types.Receipts]
	blockCache          *lru.Cache[common.Hash, *types.MinorBlock]
	futureBlocks        *lru.Cache[common.Hash, *types.MinorBlock]
	rootBlockCache      *lru.Cache[common.Hash, *types.RootBlock]
	lastConfirmCache    *lru.Cache[common.Hash, common.Hash]
	coinbaseAmountCache map[uint64]CoinbaseAmountAboutHeight

	quit    chan struct{} // blockchain quit channel
	running int32         // running must be called atomically
	// procInterrupt must be atomically called
	procInterrupt int32          // interrupt signaler for block processing
	wg            sync.WaitGroup // chain processing wait group for shutting down

	engine    consensus.Engine
	processor Processor // block processor interface
	validator Validator // block and state validator interface
	vmConfig  vm.Config

	shouldPreserve func(*types.MinorBlock) bool // Function used to determine whether should preserve the given block.

	branch                   account.Branch
	shardConfig              *config.ShardConfig
	rootTip                  *types.RootBlock
	lastDereferenceRoot      common.Hash
	confirmedHeaderTip       *types.MinorBlock
	initialized              bool
	rewardCalc               *qkcCommon.ConstMinorBlockRewardCalculator
	minRecordMinorBlock      uint64
	heightToMinorBlockHashes map[uint64]map[common.Hash]struct{}
	heightToMBlockHashCount  map[uint64]int
	logInfo                  string
	addMinorBlockAndBroad    func(block *types.MinorBlock) error
	gasLimit                 *big.Int
	xShardGasLimit           *big.Int
	signer                   types.Signer
}

// NewMinorBlockChain returns a fully initialised block chain using information
// available in the database. It initialises the default Ethereum Validator and
// Processor.
func NewMinorBlockChain(
	db ethdb.Database,
	cacheConfig *CacheConfig,
	chainConfig *params.ChainConfig,
	clusterConfig *config.ClusterConfig,
	engine consensus.Engine,
	vmConfig vm.Config,
	shouldPreserve func(block *types.MinorBlock) bool,
	fullShardID uint32,
) (*MinorBlockChain, error) {
	chainConfig = &qkcParams.DefaultConstantinople //TODO default is constantinople
	if clusterConfig == nil || chainConfig == nil {
		return nil, errors.New("can not new minorBlock: config is nil")
	}
	if cacheConfig == nil {
		cacheConfig = &CacheConfig{
			TrieCleanLimit: 128,
			TrieDirtyLimit: 128,
			TrieTimeLimit:  5 * time.Minute,
			Disabled:       clusterConfig.NoPruning,
		}
	}
	bc := &MinorBlockChain{
		ethChainConfig:           chainConfig,
		clusterConfig:            clusterConfig,
		cacheConfig:              cacheConfig,
		db:                       db,
		quit:                     make(chan struct{}),
		shouldPreserve:           shouldPreserve,
		receiptsCache:            lru.NewCache[common.Hash, types.Receipts](receiptsCacheLimit),
		blockCache:               lru.NewCache[common.Hash, *types.MinorBlock](blockCacheLimit),
		futureBlocks:             lru.NewCache[common.Hash, *types.MinorBlock](maxFutureBlocks),
		rootBlockCache:           lru.NewCache[common.Hash, *types.RootBlock](maxRootBlockLimit),
		lastConfirmCache:         lru.NewCache[common.Hash, common.Hash](maxLastConfirmLimit),
		coinbaseAmountCache:      make(map[uint64]CoinbaseAmountAboutHeight),
		engine:                   engine,
		vmConfig:                 vmConfig,
		minRecordMinorBlock:      uint64(0xFFFFFFFFFFFFFFFF),
		heightToMinorBlockHashes: make(map[uint64]map[common.Hash]struct{}),
		heightToMBlockHashCount:  make(map[uint64]int),
		branch:                   account.Branch{Value: fullShardID},
		shardConfig:              clusterConfig.Quarkchain.GetShardConfigByFullShardID(fullShardID),
		rewardCalc:               &qkcCommon.ConstMinorBlockRewardCalculator{},
		logInfo:                  fmt.Sprintf("shard:%x", fullShardID),
	}
	bc.signer = types.MakeSigner(clusterConfig.Quarkchain.NetworkID)
	var err error
	bc.gasLimit, err = bc.clusterConfig.Quarkchain.GasLimit(bc.branch.Value)
	if err != nil {
		return nil, err
	}
	bc.xShardGasLimit = new(big.Int).Set(bc.gasLimit)
	bc.xShardGasLimit = bc.xShardGasLimit.Div(bc.xShardGasLimit, new(big.Int).SetUint64(2))
	bc.SetValidator(NewBlockValidator(clusterConfig.Quarkchain, bc, engine, bc.branch))
	// TODO(execution-issue): bc.SetProcessor(NewStateProcessor(bc.ethChainConfig, bc, engine))

	genesisBlock := bc.GetBlockByNumber(0)
	if qkcCommon.IsNil(genesisBlock) {
		return nil, ErrNoGenesis
	}
	bc.genesisBlock = genesisBlock.(*types.MinorBlock)
	if bc.genesisBlock == nil {
		return nil, ErrNoGenesis
	}
	if err := bc.loadLastState(); err != nil {
		return nil, err
	}
	// TODO(execution-issue): bc.posw = consensus.CreatePoSWCalculator(bc, bc.shardConfig.PoswConfig)
	// TODO(execution-issue): bc.txPool = NewTxPool(DefaultTxPoolConfig, bc)
	// Take ownership of this particular state
	go bc.update()
	return bc, nil
}

func (m *MinorBlockChain) EthChainID() uint32 {
	return m.clusterConfig.Quarkchain.BaseEthChainID + 1 + m.shardConfig.ChainID
}

func (m *MinorBlockChain) SetBroadcastMinorBlockFunc(f func(block *types.MinorBlock) error) {
	m.addMinorBlockAndBroad = f
}

func (m *MinorBlockChain) AddBlock(block types.IBlock) error {
	minorBlock, ok := block.(*types.MinorBlock)
	if !ok {
		return errors.New("block is not minorBlock")
	}
	return m.addMinorBlockAndBroad(minorBlock)
}

func (m *MinorBlockChain) getProcInterrupt() bool {
	return atomic.LoadInt32(&m.procInterrupt) == 1
}

// GetVMConfig returns the block chain VM config.
func (m *MinorBlockChain) GetVMConfig() *vm.Config {
	return &m.vmConfig
}

// loadLastState loads the last known chain state from the database. This method
// assumes that the chain manager mutex is held.
func (m *MinorBlockChain) loadLastState() error {
	// Restore the last known head block
	head := rawdb.ReadHeadBlockHash(m.db)
	if head == (common.Hash{}) {
		// Corrupt or empty database, init from scratch
		log.Warn("Empty database, resetting chain")
		return m.Reset()
	}
	// Make sure the entire head block is available
	currentBlock := m.GetMinorBlock(head)
	if currentBlock == nil {
		// Corrupt or empty database, init from scratch
		log.Warn("Head block missing, resetting chain", "hash", head)
		return m.Reset()
	}

	// TODO(execution-issue): verify the state associated with the head block is
	// available (StateAt(currentBlock.GetMetaData().Root)) and repair otherwise.

	// Everything seems to be fine, set as the head block
	m.currentBlock.Store(currentBlock)

	return nil
}

// SetHead rewinds the local chain to a new head. In the case of Headers, everything
// above the new head will be deleted and the new one set. In the case of blocks
// though, the head may be further rewound if block bodies are missing (non-archive
// nodes after a fast sync).
// already have locked
func (m *MinorBlockChain) SetHead(head uint64) error {
	m.chainmu.Lock()
	defer m.chainmu.Unlock()
	return m.setHead(head)
}

func (m *MinorBlockChain) setHead(head uint64) error {
	log.Warn(m.logInfo+" Rewinding blockchain", "target", head)
	defer log.Warn(m.logInfo+" Rewinding blockchain-end", "target number", head)
	// Rewind the header chain, deleting all block bodies until then
	batch := m.db.NewBatch()
	for block := m.CurrentBlock(); block != nil && block.NumberU64() > head; block = m.CurrentBlock() {
		rawdb.DeleteMinorBlock(batch, block.Hash())
		rawdb.DeleteCanonicalHash(batch, rawdb.ChainTypeMinor, block.NumberU64())
		m.currentBlock.Store(m.GetMinorBlock(block.ParentHash()))
	}
	batch.Write()

	// TODO(execution-issue): if the rewound head's state is missing, reset to genesis.

	// If either blocks reached nil, reset to the genesis state
	if currentBlock := m.CurrentBlock(); currentBlock == nil {
		m.currentBlock.Store(m.genesisBlock)
	}
	rawdb.WriteHeadBlockHash(m.db, m.CurrentBlock().Hash())

	// Clear out any stale content from the caches
	m.receiptsCache.Purge()
	m.blockCache.Purge()
	m.futureBlocks.Purge()
	m.rootBlockCache.Purge()
	m.lastConfirmCache.Purge()

	return m.loadLastState()
}

// GasLimit returns the gas limit of the current HEAD block.
func (m *MinorBlockChain) GasLimit() uint64 {
	return m.currentBlock.Load().(*types.MinorBlock).GasLimit().Uint64()
}

// CurrentBlock retrieves the current head block of the canonical chain. The
// block is retrieved from the blockchain's internal cache.
func (m *MinorBlockChain) CurrentBlock() *types.MinorBlock {
	loaded := m.currentBlock.Load()
	if loaded == nil {
		return nil
	}
	return loaded.(*types.MinorBlock)
}

// CurrentHeader retrieves the current header of the canonical chain.
func (m *MinorBlockChain) CurrentHeader() types.IHeader {
	return m.CurrentBlock().IHeader()
}

// SetProcessor sets the processor required for making state modifications.
func (m *MinorBlockChain) SetProcessor(processor Processor) {
	m.procmu.Lock()
	defer m.procmu.Unlock()
	m.processor = processor
}

// SetValidator sets the validator which is used to validate incoming blocks.
func (m *MinorBlockChain) SetValidator(validator Validator) {
	m.procmu.Lock()
	defer m.procmu.Unlock()
	m.validator = validator
}

// Validator returns the current validator.
func (m *MinorBlockChain) Validator() Validator {
	m.procmu.RLock()
	defer m.procmu.RUnlock()
	return m.validator
}

// Processor returns the current processor.
func (m *MinorBlockChain) Processor() Processor {
	m.procmu.RLock()
	defer m.procmu.RUnlock()
	return m.processor
}

// Config returns the quarkchain config of the chain.
func (m *MinorBlockChain) Config() *config.QuarkChainConfig {
	return m.clusterConfig.Quarkchain
}

func (m *MinorBlockChain) SkipDifficultyCheck() bool {
	return m.Config().SkipMinorDifficultyCheck
}

// GetAdjustedDifficulty returns the header difficulty, optionally adjusted by
// PoSW staking.
//
// TODO(execution-issue): the PoSW adjustment requires coinbase balances from the
// state layer (posw.PoSWDiffAdjust); until then the difficulty is used as-is.
func (m *MinorBlockChain) GetAdjustedDifficulty(header types.IHeader) (*big.Int, uint64, error) {
	return header.GetDifficulty(), 1, nil
}

// Reset purges the entire blockchain, restoring it to its genesis state.
func (m *MinorBlockChain) Reset() error {
	return m.ResetWithGenesisBlock(m.genesisBlock)
}

// ResetWithGenesisBlock purges the entire blockchain, restoring it to the
// specified genesis state.
func (m *MinorBlockChain) ResetWithGenesisBlock(genesis *types.MinorBlock) error {
	// Dump the entire block chain and purge the caches
	if err := m.SetHead(0); err != nil {
		return err
	}

	rawdb.WriteMinorBlock(m.db, genesis)

	m.genesisBlock = genesis
	m.insert(m.genesisBlock)
	m.currentBlock.Store(m.genesisBlock)

	return nil
}

// Export writes the active chain to the given writer.
func (m *MinorBlockChain) Export(w io.Writer) error {
	return m.ExportN(w, uint64(0), m.CurrentBlock().NumberU64())
}

// ExportN writes a subset of the active chain to the given writer.
func (m *MinorBlockChain) ExportN(w io.Writer, first uint64, last uint64) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if first > last {
		return fmt.Errorf("export failed: first (%d) is greater than last (%d)", first, last)
	}
	log.Info("Exporting batch of blocks", "count", last-first+1)

	start, reported := time.Now(), time.Now()
	for nr := first; nr <= last; nr++ {
		block := m.GetBlockByNumber(nr)
		if block == nil {
			return fmt.Errorf("export failed on #%d: not found", nr)
		}
		data, err := serialize.SerializeToBytes(block)
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		if err != nil {
			return err
		}

		if time.Since(reported) >= statsReportLimit {
			log.Info("Exporting blocks", "exported", block.NumberU64()-first, "elapsed", common.PrettyDuration(time.Since(start)))
			reported = time.Now()
		}
	}

	return nil
}

// insert injects a new head block into the current block chain. This method
// assumes that the block is indeed a true head. It will also reset the head
// header and the head fast sync block to this very same block if they are older
// or if they are on a different side chain.
//
// Note, this function assumes that the `mu` mutex is held!
func (m *MinorBlockChain) insert(block *types.MinorBlock) {
	// Add the block to the canonical chain number scheme and mark as the head
	rawdb.WriteCanonicalHash(m.db, rawdb.ChainTypeMinor, block.Hash(), block.NumberU64())
	rawdb.WriteHeadBlockHash(m.db, block.Hash())

	m.currentBlock.Store(block)
}

// Genesis retrieves the chain's genesis block.
func (m *MinorBlockChain) Genesis() *types.MinorBlock {
	return m.genesisBlock
}

// HasBlock checks if a block is fully present in the database or not.
func (m *MinorBlockChain) HasBlock(hash common.Hash) bool {
	return m.IsMinorBlockCommittedByHash(hash)
}

// HasBlockAndState checks if a block and associated state trie is fully present
// in the database or not, caching it if present.
//
// TODO(execution-issue): without the state layer this is equivalent to HasBlock;
// the state-trie availability check (HasState on Meta.Root) comes with the
// execution issue.
func (m *MinorBlockChain) HasBlockAndState(hash common.Hash) bool {
	return m.HasBlock(hash)
}

// GetBlock retrieves a block from the database by hash and number,
// caching it if found.
func (m *MinorBlockChain) GetBlock(hash common.Hash) types.IBlock {
	block := m.GetMinorBlock(hash)
	if block == nil {
		// Return an untyped nil so interface comparisons against nil hold.
		return nil
	}
	return block
}

// GetMinorBlock retrieves a block from the database by hash, caching it if found.
func (m *MinorBlockChain) GetMinorBlock(hash common.Hash) *types.MinorBlock {
	// Short circuit if the block's already in the cache, retrieve otherwise
	if block, ok := m.blockCache.Get(hash); ok {
		return block
	}
	block := rawdb.ReadMinorBlock(m.db, hash)
	if block == nil {
		return nil
	}
	// Cache the found block for next time and return
	m.blockCache.Add(block.Hash(), block)
	return block
}

// GetBlockByNumber retrieves a block from the database by number, caching it
// (associated with its hash) if found.
func (m *MinorBlockChain) GetBlockByNumber(number uint64) types.IBlock {
	hash := rawdb.ReadCanonicalHash(m.db, rawdb.ChainTypeMinor, number)
	if hash == (common.Hash{}) {
		return nil
	}
	return m.GetBlock(hash)
}

// GetHeader retrieves a block header from the database by hash.
func (m *MinorBlockChain) GetHeader(hash common.Hash) types.IHeader {
	block := m.GetMinorBlock(hash)
	if block == nil {
		return nil
	}
	return block.Header()
}

// GetHeaderByNumber retrieves a block header from the canonical chain by number.
func (m *MinorBlockChain) GetHeaderByNumber(number uint64) types.IHeader {
	block := m.GetBlockByNumber(number)
	if qkcCommon.IsNil(block) {
		return nil
	}
	return block.(*types.MinorBlock).Header()
}

func (m *MinorBlockChain) GetHashByHeight(height *uint64) (common.Hash, error) {
	if height != nil {
		hash := rawdb.ReadCanonicalHash(m.db, rawdb.ChainTypeMinor, *height)
		if hash == (common.Hash{}) {
			return hash, fmt.Errorf("shard %v do no have this  height  %v", m.branch.Value, *height)
		}
		return hash, nil
	}
	return m.CurrentBlock().Hash(), nil
}

// GetReceiptsByHash retrieves the receipts for all transactions in a given block.
func (m *MinorBlockChain) GetReceiptsByHash(hash common.Hash) types.Receipts {
	if receipts, ok := m.receiptsCache.Get(hash); ok {
		return receipts
	}
	block := m.GetMinorBlock(hash)
	if block == nil {
		return nil
	}
	receipts := rawdb.ReadReceipts(m.db, hash)
	m.receiptsCache.Add(hash, receipts)
	return receipts
}

func (m *MinorBlockChain) GetLogs(hash common.Hash) [][]*types.Log {
	receipts := m.GetReceiptsByHash(hash)
	logs := make([][]*types.Log, len(receipts))
	for index, receipt := range receipts {
		logs[index] = receipt.Logs
	}
	return logs
}

// Stop stops the blockchain service. If any imports are currently in progress
// it will abort them using the procInterrupt.
func (m *MinorBlockChain) Stop() {
	if !atomic.CompareAndSwapInt32(&m.running, 0, 1) {
		return
	}
	// Unsubscribe all subscriptions registered from blockchain
	m.scope.Close()
	close(m.quit)
	atomic.StoreInt32(&m.procInterrupt, 1)

	m.wg.Wait()

	// TODO(execution-issue): flush the recent state tries (HEAD, HEAD-1,
	// HEAD-127) to disk and dereference the trie GC queue, as goquarkchain did.
	log.Info("Blockchain manager stopped")
}

func (m *MinorBlockChain) procFutureBlocks() {
	blocks := make([]types.IBlock, 0, m.futureBlocks.Len())
	for _, hash := range m.futureBlocks.Keys() {
		if block, exist := m.futureBlocks.Peek(hash); exist {
			blocks = append(blocks, block)
		}
	}
	if len(blocks) > 0 {
		sort.Slice(blocks, func(i, j int) bool { return blocks[i].NumberU64() < blocks[j].NumberU64() })
		// Insert one by one as chain insertion needs contiguous ancestry between blocks
		for i := range blocks {
			m.InsertChain(blocks[i:i+1], false)
		}
	}
}

// Rollback is designed to remove a chain of links from the database that aren't
// certain enough to be valid.
func (m *MinorBlockChain) Rollback(chain []common.Hash) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := len(chain) - 1; i >= 0; i-- {
		hash := chain[i]

		if currentBlock := m.CurrentBlock(); currentBlock.Hash() == hash {
			newBlock := m.GetMinorBlock(currentBlock.ParentHash())
			m.currentBlock.Store(newBlock)
			rawdb.WriteHeadBlockHash(m.db, newBlock.Hash())
		}
	}
}

// WriteBlockWithoutState writes only the block and its metadata to the database,
// but does not write any state. This is used to construct competing side forks
// up to the point where they exceed the canonical total difficulty.
func (m *MinorBlockChain) WriteBlockWithoutState(block types.IBlock) (err error) {
	m.wg.Add(1)
	defer m.wg.Done()

	rawdb.WriteMinorBlock(m.db, block.(*types.MinorBlock))
	m.blockCache.Add(block.Hash(), block.(*types.MinorBlock))

	return nil
}

// addFutureBlock checks if the block is within the max allowed window to get
// accepted for future processing, and returns an error if the block is too far
// ahead and was not added.
func (m *MinorBlockChain) addFutureBlock(block types.IBlock) error {
	max := big.NewInt(time.Now().Unix() + maxTimeFutureBlocks)
	if block.Time() > max.Uint64() {
		return fmt.Errorf("future block timestamp %v > allowed %v", block.Time(), max)
	}
	m.futureBlocks.Add(block.Hash(), block.(*types.MinorBlock))
	return nil
}

// InsertChain attempts to insert the given batch of blocks in to the canonical
// chain or, otherwise, create a fork. If an error is returned it will return
// the index number of the failing block as well an error describing what went
// wrong.
//
// After insertion is done, all accumulated events will be fired.
func (m *MinorBlockChain) InsertChain(chain []types.IBlock, isCheckDB bool) (int, error) {
	// Sanity check that we have something meaningful to import
	if len(chain) == 0 {
		return 0, nil
	}
	// Do a sanity check that the provided chain is actually ordered and linked
	for i := 1; i < len(chain); i++ {
		if chain[i].NumberU64() != chain[i-1].NumberU64()+1 || chain[i].ParentHash() != chain[i-1].Hash() {
			// Chain broke ancestry, log a message (programming error) and skip insertion
			log.Error("Non contiguous block insert", "number", chain[i].NumberU64(), "hash", chain[i].Hash(),
				"parent", chain[i].ParentHash(), "prevnumber", chain[i-1].NumberU64(), "prevhash", chain[i-1].Hash())

			return 0, fmt.Errorf("non contiguous insert: item %d is #%d [%x…], item %d is #%d [%x…] (parent [%x…])", i-1, chain[i-1].NumberU64(),
				chain[i-1].Hash().Bytes()[:4], i, chain[i].NumberU64(), chain[i].Hash().Bytes()[:4], chain[i].ParentHash().Bytes()[:4])
		}
	}
	// Pre-checks passed, start the full block imports
	m.wg.Add(1)
	m.chainmu.Lock()
	n, events, logs, err := m.insertChain(chain, true, isCheckDB)
	m.chainmu.Unlock()
	m.wg.Done()

	m.PostChainEvents(events, logs)
	return n, err
}

// insertChain is the internal implementation of InsertChain, which assumes that
// 1) chains are contiguous, and 2) The chain mutex is held.
//
// The structural import validates each block and persists it; the canonical head
// advances when the block extends the current head.
//
// TODO(execution-issue): goquarkchain ran the state processor here
// (runBlock -> Processor.Process, Validator.ValidateState, WriteBlockWithState
// with trie GC and reorg support). The execution issue plugs into this seam.
func (m *MinorBlockChain) insertChain(chain []types.IBlock, verifySeals bool, isCheckDB bool) (int, []interface{}, []*types.Log, error) {
	// If the chain is terminating, don't even bother starting u
	if atomic.LoadInt32(&m.procInterrupt) == 1 {
		return 0, nil, nil, nil
	}

	events := make([]interface{}, 0, len(chain))

	for i, block := range chain {
		mBlock := block.(*types.MinorBlock)
		// If the chain is terminating, stop processing blocks
		if atomic.LoadInt32(&m.procInterrupt) == 1 {
			log.Debug("Premature abort during blocks processing")
			break
		}
		err := m.Validator().ValidateBlock(mBlock, isCheckDB)
		switch {
		case err == ErrKnownBlock:
			if m.CurrentBlock().NumberU64() >= mBlock.NumberU64() {
				continue
			}
		case err == ErrFutureBlock || (err == consensus.ErrUnknownAncestor && m.futureBlocks.Contains(mBlock.ParentHash())):
			if err := m.addFutureBlock(mBlock); err != nil {
				return i, events, nil, err
			}
			continue
		case err != nil:
			m.reportBlock(mBlock, nil, err)
			return i, events, nil, err
		}

		// TODO(execution-issue): process the block's transactions against the
		// parent state and validate the resulting state here.

		currentBlock := m.CurrentBlock()
		if err := m.putMinorBlock(mBlock, nil); err != nil {
			return i, events, nil, err
		}
		rawdb.WriteReceipts(m.db, mBlock.Hash(), nil)
		m.CommitMinorBlockByHash(mBlock.Hash())
		if mBlock.ParentHash() == currentBlock.Hash() {
			m.insert(mBlock)
			m.putTxIndexFromBlock(mBlock)
			events = append(events, MinorChainHeadEvent{mBlock})
		} else {
			// Side fork: the block is stored but the canonical head is kept.
			// TODO(execution-issue): reorg to the side chain when it grows past
			// the canonical head (requires replaying state).
			events = append(events, MinorChainSideEvent{mBlock})
		}
		m.futureBlocks.Remove(mBlock.Hash())
	}
	return len(chain), events, nil, nil
}

// putTxIndexFromBlock writes the positional metadata for transaction lookups.
func (m *MinorBlockChain) putTxIndexFromBlock(block types.IBlock) {
	rawdb.WriteBlockContentLookupEntriesWithCrossShardHashList(m.db, block, nil)
}

// reportBlock logs a bad block error.
func (m *MinorBlockChain) reportBlock(block types.IBlock, receipts types.Receipts, err error) {
	log.Error(fmt.Sprintf(`
########## BAD BLOCK #########
Chain config: %v

Number: %v
Hash: 0x%x

Error: %v
##############################
`, m.clusterConfig.Quarkchain.NetworkID, block.NumberU64(), block.Hash(), err))
}

// PostChainEvents iterates over the events generated by a chain insertion and
// posts them into the event feeds.
func (m *MinorBlockChain) PostChainEvents(events []interface{}, logs []*types.Log) {
	// post event logs for further processing
	if logs != nil {
		m.logsFeed.Send(logs)
		var logss [][]*types.Log
		logss = append(logss, logs)
		m.subLogsFeed.Send(LoglistEvent{Logs: logss, IsRemoved: false})
	}
	for _, event := range events {
		switch ev := event.(type) {
		case MinorChainEvent:
			m.chainFeed.Send(ev)

		case MinorChainHeadEvent:
			m.chainHeadFeed.Send(ev)

		case MinorChainSideEvent:
			m.chainSideFeed.Send(ev)
		}
	}
}

func (m *MinorBlockChain) update() {
	futureTimer := time.NewTicker(5 * time.Second)
	defer futureTimer.Stop()
	for {
		select {
		case <-futureTimer.C:
			m.procFutureBlocks()
		case <-m.quit:
			return
		}
	}
}

// SubscribeChainHeadEvent registers a subscription of MinorChainHeadEvent.
func (m *MinorBlockChain) SubscribeChainHeadEvent(ch chan<- MinorChainHeadEvent) event.Subscription {
	return m.scope.Track(m.chainHeadFeed.Subscribe(ch))
}

// SubscribeChainEvent registers a subscription of MinorChainEvent.
func (m *MinorBlockChain) SubscribeChainEvent(ch chan<- MinorChainEvent) event.Subscription {
	return m.scope.Track(m.chainFeed.Subscribe(ch))
}

// SubscribeChainSideEvent registers a subscription of MinorChainSideEvent.
func (m *MinorBlockChain) SubscribeChainSideEvent(ch chan<- MinorChainSideEvent) event.Subscription {
	return m.scope.Track(m.chainSideFeed.Subscribe(ch))
}

// SubscribeLogsEvent registers a subscription of []*types.Log.
func (m *MinorBlockChain) SubscribeLogsEvent(ch chan<- []*types.Log) event.Subscription {
	return m.scope.Track(m.logsFeed.Subscribe(ch))
}

// ---- helpers ported from goquarkchain's minorblockchain_addon.go ----

func (m *MinorBlockChain) ReadLastConfirmedMinorBlockHeaderAtRootBlock(hash common.Hash) common.Hash {
	if data, ok := m.lastConfirmCache.Get(hash); ok {
		return data
	}
	data := rawdb.ReadLastConfirmedMinorBlockHeaderAtRootBlock(m.db, hash)
	if data != (common.Hash{}) {
		m.lastConfirmCache.Add(hash, data)
	}

	return data
}

func (m *MinorBlockChain) getLastConfirmedMinorBlockHeaderAtRootBlock(hash common.Hash) *types.MinorBlock {
	rMinorHeaderHash := m.ReadLastConfirmedMinorBlockHeaderAtRootBlock(hash)
	if rMinorHeaderHash == qkcCommon.EmptyHash {
		return nil
	}
	return m.GetMinorBlock(rMinorHeaderHash)
}

func (m *MinorBlockChain) getCoinbaseAmount(height uint64) *types.TokenBalances {
	epoch := height / m.shardConfig.EpochInterval
	cache, ok := m.coinbaseAmountCache[epoch]
	if ok {
		return cache.CoinbaseAmount.Copy()
	}
	m.calcCoinbaseAmountByHeight(epoch)
	cache = m.coinbaseAmountCache[epoch]
	return cache.CoinbaseAmount.Copy()
}

func powerBigInt(data *big.Int, p uint64) *big.Int {
	t := new(big.Int).Set(data)
	ret := new(big.Int).SetUint64(1)
	for i := uint64(0); i < p; i++ {
		ret.Mul(ret, t)
	}
	return ret
}

func (m *MinorBlockChain) calcCoinbaseAmountByHeight(epoch uint64) {
	decayNumerator := powerBigInt(m.clusterConfig.Quarkchain.BlockRewardDecayFactor.Num(), epoch)
	decayDenominator := powerBigInt(new(big.Rat).Set(m.clusterConfig.Quarkchain.BlockRewardDecayFactor).Denom(), epoch)
	coinbaseAmount := qkcCommon.BigIntMulBigRat(m.shardConfig.CoinbaseAmount, m.clusterConfig.Quarkchain.LocalFeeRate)
	coinbaseAmount = new(big.Int).Mul(coinbaseAmount, decayNumerator)
	coinbaseAmount = new(big.Int).Div(coinbaseAmount, decayDenominator)
	data := make(map[uint64]*big.Int)
	data[m.clusterConfig.Quarkchain.GetDefaultChainTokenID()] = coinbaseAmount
	balances := types.NewTokenBalancesWithMap(data)

	value := m.clusterConfig.Quarkchain.GetShardConfigByFullShardID(m.branch.Value).PoswConfig.TotalStakePerBlock
	delayData := new(big.Int).Mul(value, decayNumerator)
	delayData = new(big.Int).Div(delayData, decayDenominator)

	m.coinbaseAmountCache[epoch] = CoinbaseAmountAboutHeight{
		CoinbaseAmount: balances,
		StakePreBlock:  *delayData,
	}
}

func (m *MinorBlockChain) getTotalTxCount(hash common.Hash) *uint32 {
	return rawdb.ReadTotalTx(m.db, hash)
}

func (m *MinorBlockChain) putTotalTxCount(mBlock *types.MinorBlock) error {
	prevCount := uint32(0)
	if mBlock.NumberU64() > 1 {
		dbPreCount := m.getTotalTxCount(mBlock.ParentHash())
		if dbPreCount == nil {
			return errors.New("get totalTx failed")
		}
		prevCount += *dbPreCount
	}
	prevCount += uint32(len(mBlock.Transactions()))
	rawdb.WriteTotalTx(m.db, mBlock.Hash(), prevCount)
	return nil
}

func (m *MinorBlockChain) putConfirmedCrossShardTransactionDepositList(hash common.Hash, xShardReceiveTxList []*types.CrossShardTransactionDeposit) error {
	if !m.clusterConfig.EnableTransactionHistory {
		return nil
	}
	data := &types.CrossShardTransactionDepositList{TXList: xShardReceiveTxList}
	rawdb.WriteConfirmedCrossShardTxList(m.db, hash, data)
	return nil
}

func (m *MinorBlockChain) putXShardDepositHashList(h common.Hash, hList *rawdb.HashList) {
	rawdb.PutXShardDepositHashList(m.db, h, hList)
}

func (m *MinorBlockChain) putMinorBlock(mBlock *types.MinorBlock, xShardReceiveTxList []*types.CrossShardTransactionDeposit) error {
	if _, ok := m.heightToMinorBlockHashes[mBlock.NumberU64()]; !ok {
		m.heightToMinorBlockHashes[mBlock.NumberU64()] = make(map[common.Hash]struct{})
		if m.minRecordMinorBlock > mBlock.NumberU64() {
			m.minRecordMinorBlock = mBlock.NumberU64()
		}
	}
	m.heightToMinorBlockHashes[mBlock.NumberU64()][mBlock.Hash()] = struct{}{}
	if len(m.heightToMinorBlockHashes) > maxCacheCountHeight2Hash {
		minNumber := mBlock.NumberU64() - maxCacheCountHeight2Hash/2
		for number, hashes := range m.heightToMinorBlockHashes {
			if number < minNumber {
				delete(m.heightToMinorBlockHashes, number)
				if len(hashes) > 1 {
					m.heightToMBlockHashCount[number] = len(hashes)
				}
			}
		}
	}
	if !m.HasBlock(mBlock.Hash()) {
		rawdb.WriteMinorBlock(m.db, mBlock)
		m.blockCache.Add(mBlock.Hash(), mBlock)
	}
	if err := m.putTotalTxCount(mBlock); err != nil {
		return err
	}

	if err := m.putConfirmedCrossShardTransactionDepositList(mBlock.Hash(), xShardReceiveTxList); err != nil {
		return err
	}

	hashList := new(rawdb.HashList)
	hashList.HList = make([]common.Hash, 0)
	for _, tx := range xShardReceiveTxList {
		hashList.HList = append(hashList.HList, tx.TxHash)
	}
	m.putXShardDepositHashList(mBlock.Hash(), hashList)
	return nil
}

func (m *MinorBlockChain) isSameRootChain(long types.IBlock, short types.IBlock) bool {
	f := func(hash common.Hash) common.Hash {
		if b := m.GetRootBlockByHash(hash); b == nil {
			return common.Hash{}
		} else {
			return b.ParentHash()
		}
	}
	return isSameChain(f, long, short)
}

func (m *MinorBlockChain) GetParentHashByHash(hash common.Hash) common.Hash {
	if b := m.GetMinorBlock(hash); b == nil {
		return common.Hash{}
	} else {
		return b.ParentHash()
	}
}

func (m *MinorBlockChain) GetBranch() account.Branch {
	return m.branch
}

func (m *MinorBlockChain) IsMinorBlockCommittedByHash(h common.Hash) bool {
	return rawdb.HasCommitMinorBlock(m.db, h)
}

func (m *MinorBlockChain) CommitMinorBlockByHash(h common.Hash) {
	rawdb.WriteCommitMinorBlock(m.db, h)
}

func (m *MinorBlockChain) GetRootBlockByHash(hash common.Hash) *types.RootBlock {
	if data, ok := m.rootBlockCache.Get(hash); ok {
		return data
	}
	data := rawdb.ReadRootBlock(m.db, hash)
	if data != nil {
		m.rootBlockCache.Add(hash, data)
		return data
	}
	return nil
}

func (m *MinorBlockChain) GetRootBlockByHeight(h common.Hash, height uint64) *types.RootBlock {
	rHeader := m.GetRootBlockByHash(h)
	if rHeader == nil || height > rHeader.NumberU64() {
		return nil
	}
	for height != rHeader.NumberU64() {
		if rHeader = m.GetRootBlockByHash(rHeader.ParentHash()); rHeader == nil {
			log.Crit("bug should fix", "GetRootBlockByHeight rootBlock is nil")
		}
	}
	return rHeader
}

func (m *MinorBlockChain) GetGenesisToken() uint64 {
	return m.clusterConfig.Quarkchain.GetDefaultChainTokenID()
}

func (m *MinorBlockChain) GetGenesisRootHeight() uint32 {
	return m.clusterConfig.Quarkchain.GetGenesisRootHeight(m.branch.Value)
}
