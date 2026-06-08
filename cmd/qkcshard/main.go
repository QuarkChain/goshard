// qkcshard is a minimal standalone runner for QuarkChain shardchains
// (slavechains) ported under qkc/. It plays the role of a goquarkchain *slave*
// process: it hosts one or more shards (a FULL_SHARD_ID_LIST), each with its own
// database, genesis and MinorBlockChain, and — with --mine — drives block
// production through the qkc/miner module.
//
// Mining itself lives in qkc/miner (the internal miner loop plus the external
// RPC GetWork/SubmitWork path), exactly as in goquarkchain; this runner only
// wires it up by implementing miner.MinerAPI and forwarding chain-head events.
// The consensus engine is selected per shard from its CONSENSUS_TYPE:
//   - POW_DOUBLESHA256: real proof-of-work sealing (qkc/consensus/doublesha256)
//   - anything else (POW_SIMULATE/NONE): paced production at TARGET_BLOCK_TIME
//     (qkc/consensus/simulate)
//
// The cluster layer (master/slaves, p2p, RPC) stays on the original
// goquarkchain code and is out of scope here; transaction execution plugs in
// via the TODO(execution-issue) seams.
//
// Examples:
//
//	go run ./cmd/qkcshard --mine --blocks 3                        # one shard, in-memory
//	go run ./cmd/qkcshard --fullshardid 2,3 --mine                 # two shards in parallel
//	go run ./cmd/qkcshard --datadir /tmp/slave0 --fullshardid 2,3 --mine
//	go run ./cmd/qkcshard --datadir /tmp/slave0 --fullshardid 2,3  # print status
//	go run ./cmd/qkcshard --config cluster.json --fullshardid 0x10002 --mine
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/consensus"
	"github.com/ethereum/go-ethereum/qkc/consensus/doublesha256"
	"github.com/ethereum/go-ethereum/qkc/consensus/simulate"
	qkccore "github.com/ethereum/go-ethereum/qkc/core"
	qkcrawdb "github.com/ethereum/go-ethereum/qkc/core/rawdb"
	"github.com/ethereum/go-ethereum/qkc/miner"
	"github.com/ethereum/go-ethereum/qkc/types"
)

var (
	datadirFlag      = flag.String("datadir", "", "data directory; each shard uses <datadir>/shard-0xN (empty = in-memory)")
	configFlag       = flag.String("config", "", "cluster config JSON file (empty = built-in default config)")
	fullShardIDsFlag = flag.String("fullshardid", "2", "comma-separated full shard ids (chainID<<16 | shardSize | shardID), e.g. 2,3,0x10002")
	mineFlag         = flag.Bool("mine", false, "produce blocks on all shards")
	blocksFlag       = flag.Int("blocks", 0, "stop after producing this many blocks per shard (0 = run until interrupted)")
	verbosityFlag    = flag.Int("verbosity", 3, "log verbosity (0=crit .. 5=trace)")
)

func main() {
	flag.Parse()
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.FromLegacyLevel(*verbosityFlag), true)))

	if err := run(); err != nil {
		log.Crit("qkcshard failed", "err", err)
	}
}

// shard bundles a running shard: its chain, engine and miner. It implements
// miner.MinerAPI — the contract through which qkc/miner drives production.
type shard struct {
	id     uint32
	db     ethdb.Database
	chain  *qkccore.MinorBlockChain
	engine consensus.Engine
	miner  *miner.Miner
	cfg    *config.ShardConfig
	logger log.Logger
}

// --- miner.MinerAPI ---

func (s *shard) GetDefaultCoinbaseAddress() account.Address {
	return account.CreatEmptyAddress(s.id)
}

func (s *shard) CreateBlockToMine(addr *account.Address) (types.IBlock, *big.Int, uint64, error) {
	parent := s.chain.CurrentBlock()
	createTime := uint64(time.Now().Unix())
	if createTime <= parent.Time() {
		createTime = parent.Time() + 1
	}
	diff, err := s.engine.CalcDifficulty(s.chain, createTime, parent)
	if err != nil {
		return nil, nil, 0, err
	}
	next := parent.CreateBlockToAppend(&createTime, diff, addr, nil, nil, nil, nil, nil, nil)
	// TODO(execution-issue): pull pending txs from the txpool, run the state
	// processor and finalize with the real receipts/state root/coinbase amount.
	next.Finalize(types.Receipts{}, types.EmptyTrieHash, nil, nil,
		types.NewEmptyTokenBalances(), parent.Meta().XShardTxCursorInfo)
	return next, diff, 1, nil
}

func (s *shard) InsertMinedBlock(block types.IBlock) error {
	_, err := s.chain.InsertChain([]types.IBlock{block}, false)
	return err
}

func (s *shard) IsSyncing() bool { return false }
func (s *shard) GetTip() uint64  { return s.chain.CurrentBlock().NumberU64() }

func (s *shard) close() {
	if s.miner != nil {
		s.miner.Stop()
	}
	s.chain.Stop()
	s.db.Close()
}

func run() error {
	// ---- cluster config + shard ids --------------------------------------
	clusterCfg, err := loadClusterConfig(*configFlag)
	if err != nil {
		return err
	}
	ids, err := parseShardIDs(*fullShardIDsFlag)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if clusterCfg.Quarkchain.GetShardConfigByFullShardID(id) == nil {
			return fmt.Errorf("no shard config for full shard id %#x (default config has 3 chains x 2 shards: 0x2 0x3 0x10002 0x10003 0x20002 0x20003)", id)
		}
	}

	// The anchor root block is shared by all shards: in production it is
	// produced by the external root chain (original goquarkchain code); here
	// the configured root genesis is written idempotently into every shard db.
	gspec := qkccore.NewGenesis(clusterCfg.Quarkchain)
	rootBlock := gspec.CreateRootBlock()

	// ---- start every shard ------------------------------------------------
	shards := make([]*shard, 0, len(ids))
	defer func() {
		for _, s := range shards {
			s.close()
		}
	}()
	for _, id := range ids {
		s, err := startShard(clusterCfg, gspec, rootBlock, id, *datadirFlag)
		if err != nil {
			return fmt.Errorf("shard %#x: %w", id, err)
		}
		shards = append(shards, s)

		branch := s.chain.GetBranch()
		head := s.chain.CurrentBlock()
		s.logger.Info("Shardchain started",
			"chain", branch.GetChainID(), "shard", branch.GetShardID(),
			"consensus", s.cfg.ConsensusType,
			"head", head.NumberU64(), "headHash", head.Hash(),
			"db", databaseDesc(*datadirFlag, id),
		)
	}

	if !*mineFlag {
		log.Info("Not mining (pass --mine to produce blocks); exiting", "shards", len(shards))
		return nil
	}

	// ---- mine on all shards until interrupted / target reached -----------
	stop := make(chan struct{})
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-interrupt
		log.Info("Interrupt received, shutting down")
		close(stop)
	}()

	var wg sync.WaitGroup
	for _, s := range shards {
		wg.Add(1)
		go func(s *shard) {
			defer wg.Done()
			driveShard(s, stop)
		}(s)
	}
	wg.Wait()
	return nil
}

// startShard opens the shard database, sets up genesis, the chain object, the
// engine and the miner (not yet started).
func startShard(clusterCfg *config.ClusterConfig, gspec *qkccore.Genesis, rootBlock *types.RootBlock, fullShardID uint32, datadir string) (*shard, error) {
	shardCfg := clusterCfg.Quarkchain.GetShardConfigByFullShardID(fullShardID)
	logger := log.New("fullShardId", fmt.Sprintf("%#x", fullShardID))

	db, err := openDatabase(datadir, fullShardID)
	if err != nil {
		return nil, err
	}

	// Anchor root block + idempotent shard genesis.
	qkcrawdb.WriteRootBlock(db, rootBlock)
	if _, hash, err := qkccore.SetupGenesisMinorBlock(db, gspec, rootBlock, fullShardID); err != nil {
		db.Close()
		return nil, fmt.Errorf("genesis setup: %w", err)
	} else {
		logger.Info("Shard genesis ready", "hash", hash, "prevRootBlock", rootBlock.Hash())
	}

	engine := newEngine(shardCfg)

	chain, err := qkccore.NewMinorBlockChain(db, nil, nil, clusterCfg, engine, vm.Config{}, nil, fullShardID)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("open minor block chain: %w", err)
	}
	s := &shard{id: fullShardID, db: db, chain: chain, engine: engine, cfg: shardCfg, logger: logger}
	if *mineFlag {
		s.miner = miner.New(s, engine)
	}
	return s, nil
}

// newEngine builds the consensus engine for a shard from its CONSENSUS_TYPE.
func newEngine(shardCfg *config.ShardConfig) consensus.Engine {
	diffCalc := &consensus.EthDifficultyCalculator{
		AdjustmentCutoff:  shardCfg.DifficultyAdjustmentCutoffTime,
		AdjustmentFactor:  shardCfg.DifficultyAdjustmentFactor,
		MinimumDifficulty: new(big.Int).SetUint64(shardCfg.Genesis.Difficulty),
	}
	switch shardCfg.ConsensusType {
	case config.PoWDoubleSha256:
		return doublesha256.New(diffCalc, false, nil)
	default: // POW_SIMULATE / NONE
		var blockInterval uint64 = 3
		if shardCfg.ConsensusConfig != nil && shardCfg.ConsensusConfig.TargetBlockTime > 0 {
			blockInterval = uint64(shardCfg.ConsensusConfig.TargetBlockTime)
		}
		return simulate.New(diffCalc, false, nil, blockInterval)
	}
}

// driveShard starts the shard's miner and keeps it producing by forwarding
// chain-head events to HandleNewTip — the same wiring a goquarkchain shard
// uses. It stops after --blocks blocks or when stop is closed.
func driveShard(s *shard, stop <-chan struct{}) {
	headCh := make(chan qkccore.MinorChainHeadEvent, 16)
	sub := s.chain.SubscribeChainHeadEvent(headCh)
	defer sub.Unsubscribe()

	s.miner.SetMining(true)

	for produced := 0; ; {
		select {
		case ev := <-headCh:
			produced++
			s.logger.Info("Mined block", "number", ev.Block.NumberU64(), "hash", ev.Block.Hash(),
				"difficulty", ev.Block.Difficulty(), "nonce", ev.Block.Nonce(), "txs", len(ev.Block.Transactions()))
			if *blocksFlag > 0 && produced >= *blocksFlag {
				s.logger.Info("Block target reached")
				s.miner.SetMining(false)
				return
			}
			s.miner.HandleNewTip()
		case <-stop:
			return
		}
	}
}

func parseShardIDs(s string) ([]uint32, error) {
	parts := strings.Split(s, ",")
	ids := make([]uint32, 0, len(parts))
	seen := make(map[uint32]bool)
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.ParseUint(p, 0, 32) // base 0: accepts decimal and 0x-hex
		if err != nil {
			return nil, fmt.Errorf("invalid full shard id %q: %w", p, err)
		}
		if seen[uint32(v)] {
			return nil, fmt.Errorf("duplicate full shard id %#x", v)
		}
		seen[uint32(v)] = true
		ids = append(ids, uint32(v))
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("no full shard ids given")
	}
	return ids, nil
}

func loadClusterConfig(path string) (*config.ClusterConfig, error) {
	if path == "" {
		return config.NewClusterConfig(), nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cluster config: %w", err)
	}
	cfg := new(config.ClusterConfig)
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse cluster config %s: %w", path, err)
	}
	return cfg, nil
}

// openDatabase opens the per-shard database. Each shard MUST have its own key
// space: the qkc rawdb schema stores per-chain singletons (LastBlock, canonical
// height mappings, ...) under fixed keys.
func openDatabase(datadir string, fullShardID uint32) (ethdb.Database, error) {
	if datadir == "" {
		log.Warn("No --datadir given, using an in-memory database", "fullShardId", fmt.Sprintf("%#x", fullShardID))
		return rawdb.NewMemoryDatabase(), nil
	}
	path := shardDBPath(datadir, fullShardID)
	kv, err := pebble.New(path, 128, 128, "qkcshard", false)
	if err != nil {
		return nil, fmt.Errorf("open pebble database at %s: %w", path, err)
	}
	return rawdb.NewDatabase(kv), nil
}

func shardDBPath(datadir string, fullShardID uint32) string {
	return filepath.Join(datadir, fmt.Sprintf("shard-%#x", fullShardID))
}

func databaseDesc(datadir string, fullShardID uint32) string {
	if datadir == "" {
		return "memory"
	}
	return shardDBPath(datadir, fullShardID)
}
