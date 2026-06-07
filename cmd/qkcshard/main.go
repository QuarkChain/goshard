// qkcshard is a minimal standalone runner for QuarkChain shardchains
// (slavechains). It opens (or creates) one database per shard, commits each
// shard's genesis minor block anchored to the configured root block, starts the
// structural MinorBlockChains and, with --mine, produces blocks on all of them
// in parallel — the same shape as a goquarkchain slave hosting its
// FULL_SHARD_ID_LIST.
//
// Every shard gets its own database (in --datadir mode a per-shard
// subdirectory): the qkc rawdb key schema (LastBlock, canonical mappings, ...)
// is per-chain global state, so shards must not share one key space.
//
// The consensus engine is selected per shard from its CONSENSUS_TYPE:
//   - POW_DOUBLESHA256: real proof-of-work sealing (qkc/consensus/doublesha256)
//   - anything else (POW_SIMULATE/NONE): timed block production at the shard's
//     TARGET_BLOCK_TIME without a seal (FakeEngine)
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
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/consensus"
	"github.com/ethereum/go-ethereum/qkc/consensus/doublesha256"
	qkccore "github.com/ethereum/go-ethereum/qkc/core"
	qkcrawdb "github.com/ethereum/go-ethereum/qkc/core/rawdb"
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

// shardNode bundles everything one running shard needs.
type shardNode struct {
	id     uint32
	db     ethdb.Database
	chain  *qkccore.MinorBlockChain
	engine consensus.Engine
	isPoW  bool
	cfg    *config.ShardConfig
	logger log.Logger
}

func (s *shardNode) close() {
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
	shards := make([]*shardNode, 0, len(ids))
	defer func() {
		for _, s := range shards {
			s.close()
		}
	}()
	for _, id := range ids {
		shard, err := startShard(clusterCfg, gspec, rootBlock, id, *datadirFlag)
		if err != nil {
			return fmt.Errorf("shard %#x: %w", id, err)
		}
		shards = append(shards, shard)

		branch := shard.chain.GetBranch()
		head := shard.chain.CurrentBlock()
		shard.logger.Info("Shardchain started",
			"chain", branch.GetChainID(), "shard", branch.GetShardID(),
			"consensus", shard.cfg.ConsensusType,
			"head", head.NumberU64(), "headHash", head.Hash(),
			"db", databaseDesc(*datadirFlag, id),
		)
	}

	if !*mineFlag {
		log.Info("Not mining (pass --mine to produce blocks); exiting", "shards", len(shards))
		return nil
	}

	// ---- mine on all shards until interrupted -----------------------------
	stop := make(chan struct{})
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-interrupt
		log.Info("Interrupt received, shutting down")
		close(stop)
	}()

	var wg sync.WaitGroup
	errs := make([]error, len(shards))
	for i, s := range shards {
		wg.Add(1)
		go func(i int, s *shardNode) {
			defer wg.Done()
			errs[i] = mine(s, stop)
		}(i, s)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return fmt.Errorf("shard %#x: %w", shards[i].id, err)
		}
	}
	return nil
}

// startShard opens the shard database, sets up genesis and the chain object.
func startShard(clusterCfg *config.ClusterConfig, gspec *qkccore.Genesis, rootBlock *types.RootBlock, fullShardID uint32, datadir string) (*shardNode, error) {
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

	// Consensus engine, selected by the shard's CONSENSUS_TYPE.
	diffCalc := &consensus.EthDifficultyCalculator{
		AdjustmentCutoff:  shardCfg.DifficultyAdjustmentCutoffTime,
		AdjustmentFactor:  shardCfg.DifficultyAdjustmentFactor,
		MinimumDifficulty: new(big.Int).SetUint64(shardCfg.Genesis.Difficulty),
	}
	var (
		engine consensus.Engine
		isPoW  bool
	)
	switch shardCfg.ConsensusType {
	case config.PoWDoubleSha256:
		engine, isPoW = doublesha256.New(diffCalc, false, nil), true
	default: // POW_SIMULATE / NONE: timed production, no seal
		engine, isPoW = consensus.NewFakeEngine(diffCalc), false
	}

	chain, err := qkccore.NewMinorBlockChain(db, nil, nil, clusterCfg, engine, vm.Config{}, nil, fullShardID)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("open minor block chain: %w", err)
	}
	return &shardNode{id: fullShardID, db: db, chain: chain, engine: engine, isPoW: isPoW, cfg: shardCfg, logger: logger}, nil
}

// mine produces blocks on one shard until the --blocks limit is reached or
// stop is closed.
func mine(s *shardNode, stop <-chan struct{}) error {
	targetBlockTime := 3 * time.Second
	if s.cfg.ConsensusConfig != nil && s.cfg.ConsensusConfig.TargetBlockTime > 0 {
		targetBlockTime = time.Duration(s.cfg.ConsensusConfig.TargetBlockTime) * time.Second
	}

	for produced := 0; *blocksFlag == 0 || produced < *blocksFlag; {
		parent := s.chain.CurrentBlock()

		// Simulated consensus paces itself with the target block time.
		if !s.isPoW {
			select {
			case <-time.After(targetBlockTime):
			case <-stop:
				return nil
			}
		}

		createTime := uint64(time.Now().Unix())
		if createTime <= parent.Time() {
			createTime = parent.Time() + 1
		}
		diff, err := s.engine.CalcDifficulty(s.chain, createTime, parent)
		if err != nil {
			return fmt.Errorf("calc difficulty: %w", err)
		}

		next := parent.CreateBlockToAppend(&createTime, diff, nil, nil, nil, nil, nil, nil, nil)
		// TODO(execution-issue): run the state processor over pending txs here
		// and finalize with the real receipts/state root/coinbase amount.
		next.Finalize(types.Receipts{}, types.EmptyTrieHash, nil, nil,
			types.NewEmptyTokenBalances(), parent.Meta().XShardTxCursorInfo)

		sealed := next
		if s.isPoW {
			results := make(chan types.IBlock, 1)
			sealStop := make(chan struct{})
			start := time.Now()
			if err := s.engine.Seal(nil, next, nil, 1, results, sealStop); err != nil {
				return fmt.Errorf("seal: %w", err)
			}
			select {
			case res := <-results:
				sealed = res.(*types.MinorBlock)
				s.logger.Debug("Sealed block", "elapsed", time.Since(start))
			case <-stop:
				close(sealStop)
				return nil
			}
		}

		if _, err := s.chain.InsertChain([]types.IBlock{sealed}, false); err != nil {
			return fmt.Errorf("insert block #%d: %w", sealed.NumberU64(), err)
		}
		head := s.chain.CurrentBlock()
		s.logger.Info("Mined block", "number", head.NumberU64(), "hash", head.Hash(),
			"difficulty", head.Difficulty(), "nonce", head.Nonce(), "txs", len(head.Transactions()))
		produced++

		// Non-blocking stop check between blocks (PoW path has no pacing).
		select {
		case <-stop:
			return nil
		default:
		}
	}
	s.logger.Info("Block target reached")
	return nil
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
