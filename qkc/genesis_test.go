// Copyright 2026-2027, QuarkChain.

package qkc

import (
	"bytes"
	"encoding/json"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/types"
)

const (
	fixtureMainnet = "config/singularity/mainnet.json"
	fixtureDevnet  = "config/singularity/devnet.json"
)

// firstShardID is chain 0's only shard in both singularity networks (shard size
// 1); foreignShardID is chain 1's, used to exercise cross-shard rejections.
const (
	firstShardID   = uint32(0x00000001)
	foreignShardID = uint32(0x00010001)
)

func loadFixture(t *testing.T, path string) *config.ClusterConfig {
	t.Helper()
	cfg, err := config.LoadClusterConfig(path)
	if err != nil {
		t.Fatalf("LoadClusterConfig(%s): %v", path, err)
	}
	return cfg
}

// shardEnv loads a fixture and returns the config, the first shard's config, and
// the root genesis header every shard spec links to.
func shardEnv(t *testing.T, path string) (*config.ClusterConfig, *config.ShardConfig, *types.RootBlockHeader) {
	t.Helper()
	cfg := loadFixture(t, path)
	root, err := CreateRootBlock(cfg.Quarkchain)
	if err != nil {
		t.Fatalf("CreateRootBlock: %v", err)
	}
	return cfg, cfg.Quarkchain.GetShardConfigByFullShardID(firstShardID), root
}

// TestRootBlockFromFixture pins the root genesis hash derived from each real
// QuarkChain network config (the live mainnet and the running devnet) against
// pyquarkchain's own GenesisManager.create_root_block().header.get_hash(). This is
// the tightest compatibility contract. Regeneration: qkc/config/singularity/README.md.
func TestRootBlockFromFixture(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		hash string
		seal string
	}{
		{
			name: "mainnet",
			path: "config/singularity/mainnet.json",
			hash: "0x4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51",
			seal: "0xe7dcdecc09e724ad81e493d70dedcd6d9ea0ee830d7ab2528a5648f2a0cf8178",
		},
		{
			name: "devnet",
			path: "config/singularity/devnet.json",
			hash: "0x5ad443efb7cf5246a3d1bbc1734bd02bf3a5d83bedeccfcfe707d0ebee03780d",
			seal: "0x055a7b410a50c098c52c123983e3596c5914ba227bf9e2c0c93309ff8f650d41",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.LoadClusterConfig(tc.path)
			if err != nil {
				t.Fatalf("load fixture: %v", err)
			}
			header, err := CreateRootBlock(cfg.Quarkchain)
			if err != nil {
				t.Fatalf("CreateRootBlock: %v", err)
			}
			if got := header.Hash(); got != common.HexToHash(tc.hash) {
				t.Errorf("root genesis hash mismatch\n got %s\nwant %s", got.Hex(), tc.hash)
			}
			if got := header.SealHash(); got != common.HexToHash(tc.seal) {
				t.Errorf("root genesis seal hash mismatch\n got %s\nwant %s", got.Hex(), tc.seal)
			}
			// total_difficulty is set equal to difficulty (pyquarkchain genesis.py).
			if header.Difficulty.Cmp(header.TotalDifficulty) != 0 {
				t.Errorf("total_difficulty %d != difficulty %d", header.TotalDifficulty, header.Difficulty)
			}
		})
	}
}

// TestRootBlockSynthetic exercises every ROOT.GENESIS field non-zero, pinned
// against a pyquarkchain RootBlockHeader built with the same fields. See README.md.
func TestRootBlockSynthetic(t *testing.T) {
	qkc := config.NewQuarkChainConfig()
	qkc.Root.Genesis = &config.RootGenesis{
		Version:        7,
		Height:         3,
		HashPrevBlock:  "1111111111111111111111111111111111111111111111111111111111111111",
		HashMerkleRoot: "2222222222222222222222222222222222222222222222222222222222222222",
		Timestamp:      1234567890,
		Difficulty:     big.NewInt(11259375), // 0xabcdef
		Nonce:          99,
	}

	header, err := CreateRootBlock(qkc)
	if err != nil {
		t.Fatalf("CreateRootBlock: %v", err)
	}

	const (
		wantHash = "0x70dc3e4bcfe83d1bb81f95015550c9a741168677fe711128aec2d811423991c2"
		wantSeal = "0xbb6649648cd925271b70a49269257b602366b0a24db14d93038ec04a665ad67e"
	)
	if got := header.Hash(); got != common.HexToHash(wantHash) {
		t.Errorf("synthetic root genesis hash mismatch\n got %s\nwant %s", got.Hex(), wantHash)
	}
	if got := header.SealHash(); got != common.HexToHash(wantSeal) {
		t.Errorf("synthetic root genesis seal hash mismatch\n got %s\nwant %s", got.Hex(), wantSeal)
	}
}

func TestRootBlockRejectsBadHash(t *testing.T) {
	qkc := config.NewQuarkChainConfig()
	qkc.Root.Genesis = &config.RootGenesis{HashPrevBlock: "zznothex"}
	if _, err := CreateRootBlock(qkc); err == nil {
		t.Fatal("expected error for malformed HASH_PREV_BLOCK, got nil")
	}
}

// minorGenesisGolden is pyquarkchain's own create_minor_block() output for chain
// 0's shard of each singularity network.
type minorGenesisGolden struct {
	Name            string    `json:"name"`
	Config          string    `json:"config"`
	FullShardID     uint32    `json:"full_shard_id"`
	RootGenesisHash string    `json:"root_genesis_hash"`
	CoinbaseAddress string    `json:"coinbase_address"`
	CoinbaseTokenID uint64    `json:"coinbase_token_id"`
	CoinbaseAmount  string    `json:"coinbase_amount"`
	GasLimit        uint64    `json:"gas_limit"`
	XShardGasLimit  uint64    `json:"xshard_gas_limit"`
	XShardCursor    [3]uint64 `json:"xshard_cursor"`
	StateRoot       string    `json:"state_root"`
	MetaHash        string    `json:"meta_hash"`
	HeaderHash      string    `json:"header_hash"`
}

func loadMinorGenesisGolden(t *testing.T) []minorGenesisGolden {
	t.Helper()
	blob, err := os.ReadFile("testdata/minor_genesis_golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var file struct {
		Networks []minorGenesisGolden `json:"networks"`
	}
	if err := json.Unmarshal(blob, &file); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if len(file.Networks) == 0 {
		t.Fatal("golden file has no networks")
	}
	return file.Networks
}

// TestMinorGenesisGolden cross-validates the derived genesis against
// pyquarkchain's own GenesisManager.create_minor_block() for both real networks,
// down to the block hash. This is the tightest compatibility contract there is
// for a shard: it covers the QuarkChain account encoding (the state root), the
// meta and header field layout, and the QKC serialization of all three.
// Regeneration: qkc/config/singularity/README.md.
func TestMinorGenesisGolden(t *testing.T) {
	for _, want := range loadMinorGenesisGolden(t) {
		t.Run(want.Name, func(t *testing.T) {
			cfg := loadFixture(t, want.Config)
			root, err := CreateRootBlock(cfg.Quarkchain)
			if err != nil {
				t.Fatalf("CreateRootBlock: %v", err)
			}
			block, err := CreateMinorBlock(cfg.Quarkchain, want.FullShardID, root)
			if err != nil {
				t.Fatalf("CreateMinorBlock: %v", err)
			}
			h, m := block.Header, block.Meta

			if h.PrevRootBlockHash != common.HexToHash(want.RootGenesisHash) {
				t.Errorf("PrevRootBlockHash = %s, want %s", h.PrevRootBlockHash, want.RootGenesisHash)
			}
			if got := h.Coinbase.ToHex(); got != want.CoinbaseAddress {
				t.Errorf("Coinbase = %s, want %s", got, want.CoinbaseAddress)
			}
			wantAmount, ok := new(big.Int).SetString(want.CoinbaseAmount, 10)
			if !ok {
				t.Fatalf("golden coinbase_amount %q is not a number", want.CoinbaseAmount)
			}
			got := h.CoinbaseAmount.GetTokenBalance(want.CoinbaseTokenID)
			if got == nil || got.ToBig().Cmp(wantAmount) != 0 {
				t.Errorf("coinbase amount on token %d = %v, want %s", want.CoinbaseTokenID, got, wantAmount)
			}
			if h.GasLimit.Value.Uint64() != want.GasLimit {
				t.Errorf("GasLimit = %v, want %d", h.GasLimit.Value, want.GasLimit)
			}
			if m.XShardGasLimit.Value.Uint64() != want.XShardGasLimit {
				t.Errorf("XShardGasLimit = %v, want %d", m.XShardGasLimit.Value, want.XShardGasLimit)
			}
			wantCursor := types.XShardTxCursorInfo{
				RootBlockHeight:    want.XShardCursor[0],
				MinorBlockIndex:    want.XShardCursor[1],
				XShardDepositIndex: want.XShardCursor[2],
			}
			if m.XShardTxCursor != wantCursor {
				t.Errorf("XShardTxCursor = %+v, want %+v", m.XShardTxCursor, wantCursor)
			}

			// The whole point: materializing ALLOC and assembling the block must
			// reproduce pyquarkchain's genesis block, byte for byte.
			if got := block.Meta.Root; got != common.HexToHash(want.StateRoot) {
				t.Errorf("genesis state root\n got %s\nwant %s", got.Hex(), want.StateRoot)
			}
			if got := block.Meta.Hash(); got != common.HexToHash(want.MetaHash) {
				t.Errorf("genesis meta hash\n got %s\nwant %s", got.Hex(), want.MetaHash)
			}
			if got := block.Header.MetaHash; got != common.HexToHash(want.MetaHash) {
				t.Errorf("header's meta hash\n got %s\nwant %s", got.Hex(), want.MetaHash)
			}
			if got := block.Hash(); got != common.HexToHash(want.HeaderHash) {
				t.Errorf("genesis block hash\n got %s\nwant %s", got.Hex(), want.HeaderHash)
			}
		})
	}
}

// TestCreateMinorBlock asserts the genesis block built from each real network
// config: the header and meta fields the config feeds, the root linkage, and the
// coinbase reward.
func TestCreateMinorBlock(t *testing.T) {
	for _, path := range []string{fixtureMainnet, fixtureDevnet} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			cfg, shardCfg, root := shardEnv(t, path)

			block, err := CreateMinorBlock(cfg.Quarkchain, firstShardID, root)
			if err != nil {
				t.Fatalf("CreateMinorBlock: %v", err)
			}
			h, m, sg := block.Header, block.Meta, shardCfg.Genesis

			if h.Branch.GetFullShardID() != firstShardID {
				t.Errorf("branch = 0x%08x, want 0x%08x", h.Branch.GetFullShardID(), firstShardID)
			}
			if h.Number != 0 {
				t.Errorf("height = %d, want 0", h.Number)
			}
			if h.Version != sg.Version || h.Time != sg.Timestamp ||
				h.Difficulty.Uint64() != sg.Difficulty || h.GasLimit.Value.Uint64() != sg.GasLimit {
				t.Errorf("header = v%d/t%d/d%d/g%d, want v%d/t%d/d%d/g%d",
					h.Version, h.Time, h.Difficulty, h.GasLimit.Value,
					sg.Version, sg.Timestamp, sg.Difficulty, sg.GasLimit)
			}
			if !bytes.Equal(h.Extra, sg.ExtraData) {
				t.Errorf("extra data = %x, want %x", h.Extra, sg.ExtraData)
			}
			// pyquarkchain passes no nonce, bloom or mixhash to the genesis header.
			if h.Nonce != 0 || h.MixDigest != (common.Hash{}) || h.Bloom != (types.Bloom{}) {
				t.Errorf("nonce/mixhash/bloom = %d/%s/%x, want all zero", h.Nonce, h.MixDigest, h.Bloom)
			}

			// Root linkage, and a cross-shard cursor starting at (root height, 0, 0).
			if h.PrevRootBlockHash != root.Hash() {
				t.Errorf("PrevRootBlockHash = %s, want the root genesis %s", h.PrevRootBlockHash, root.Hash())
			}
			if want := (types.XShardTxCursorInfo{RootBlockHeight: uint64(root.Number)}); m.XShardTxCursor != want {
				t.Errorf("XShardTxCursor = %+v, want %+v", m.XShardTxCursor, want)
			}

			// Meta: the config's merkle root, a materialized state root, and an
			// empty receipt root — zero, not the empty trie hash.
			if m.TxHash != common.HexToHash(sg.HashMerkleRoot) {
				t.Errorf("meta TxHash = %s, want %s", m.TxHash, sg.HashMerkleRoot)
			}
			if m.Root == (common.Hash{}) {
				t.Error("meta state root is zero, want the materialized ALLOC root")
			}
			if m.ReceiptHash != (common.Hash{}) {
				t.Errorf("meta ReceiptHash = %s, want the zero hash", m.ReceiptHash)
			}
			if m.GasUsed.Value.Sign() != 0 || m.CrossShardGasUsed.Value.Sign() != 0 {
				t.Errorf("gas used = %v/%v, want 0/0", m.GasUsed.Value, m.CrossShardGasUsed.Value)
			}
			if m.XShardGasLimit.Value.Uint64() != defaultXShardGasLimit {
				t.Errorf("XShardGasLimit = %v, want %d", m.XShardGasLimit.Value, uint64(defaultXShardGasLimit))
			}
			if h.MetaHash != m.Hash() {
				t.Errorf("header commits to meta %s, meta hashes to %s", h.MetaHash, m.Hash())
			}

			// Coinbase: the shard's empty address, paid COINBASE_AMOUNT scaled by
			// the local fee rate on the genesis token.
			if want := account.CreatEmptyAddress(firstShardID); h.Coinbase != want {
				t.Errorf("coinbase = %s, want %s", h.Coinbase.ToHex(), want.ToHex())
			}
			wantReward := qkcCommon.BigIntMulBigRat(shardCfg.CoinbaseAmount, cfg.Quarkchain.LocalFeeRate)
			got := h.CoinbaseAmount.GetTokenBalance(cfg.Quarkchain.GetDefaultChainTokenID())
			if got == nil || got.ToBig().Cmp(wantReward) != 0 {
				t.Errorf("coinbase amount = %v, want %s", got, wantReward)
			}
			if h.CoinbaseAmount.Len() != 1 {
				t.Errorf("coinbase carries %d tokens, want only the genesis token", h.CoinbaseAmount.Len())
			}

			// The genesis body is empty.
			if len(block.Transactions) != 0 || len(block.TrackingData) != 0 {
				t.Errorf("body = %d txs / %d tracking bytes, want empty", len(block.Transactions), len(block.TrackingData))
			}
		})
	}
}

// TestCommitGenesisState: the flush half of the hash/flush split. The root it
// writes must be the one CreateMinorBlock already sealed into the block's meta —
// deriving the identity and materializing the state are two passes over the same
// allocation, and nothing else checks that they agree.
func TestCommitGenesisState(t *testing.T) {
	for _, path := range []string{fixtureMainnet, fixtureDevnet} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			cfg, _, root := shardEnv(t, path)

			block, err := CreateMinorBlock(cfg.Quarkchain, firstShardID, root)
			if err != nil {
				t.Fatalf("CreateMinorBlock: %v", err)
			}

			db := rawdb.NewMemoryDatabase()
			stateRoot, err := CommitGenesisState(cfg.Quarkchain, firstShardID, db)
			if err != nil {
				t.Fatalf("CommitGenesisState: %v", err)
			}
			if stateRoot != block.Meta.Root {
				t.Errorf("committed state root = %s, want the derived meta root %s", stateRoot, block.Meta.Root)
			}
			if !rawdb.HasLegacyTrieNode(db, stateRoot) {
				t.Errorf("state root %s was not written to the database", stateRoot)
			}
		})
	}
}

// TestCommitGenesisStateRejectsUnstandableShard: both halves of the split go
// through the same validation, so the flush cannot write state for a shard the
// derivation would have refused.
func TestCommitGenesisStateRejectsUnstandableShard(t *testing.T) {
	cfg, shardCfg, _ := shardEnv(t, fixtureMainnet)

	if _, err := CommitGenesisState(cfg.Quarkchain, 0x00990099, rawdb.NewMemoryDatabase()); err == nil ||
		!strings.Contains(err.Error(), "not configured in any chain") {
		t.Errorf("CommitGenesisState(unknown shard) err = %v, want 'not configured in any chain'", err)
	}

	shardCfg.Genesis.RootHeight = 1
	if _, err := CommitGenesisState(cfg.Quarkchain, firstShardID, rawdb.NewMemoryDatabase()); err == nil ||
		!strings.Contains(err.Error(), "GENESIS.ROOT_HEIGHT") {
		t.Errorf("CommitGenesisState(ROOT_HEIGHT 1) err = %v, want a ROOT_HEIGHT rejection", err)
	}
}

// TestCreateMinorBlockDeterministic: the same config derives the same block, and
// any input a reopen must catch moves the block hash — that hash is the identity
// the datadir is checked against.
func TestCreateMinorBlockDeterministic(t *testing.T) {
	cfg, _, root := shardEnv(t, fixtureMainnet)
	derive := func(c *config.ClusterConfig, r *types.RootBlockHeader) common.Hash {
		t.Helper()
		block, err := CreateMinorBlock(c.Quarkchain, firstShardID, r)
		if err != nil {
			t.Fatalf("CreateMinorBlock: %v", err)
		}
		return block.Hash()
	}
	base := derive(cfg, root)
	if again := derive(cfg, root); again != base {
		t.Fatalf("block hash not deterministic: %s then %s", base, again)
	}

	// A different root genesis relinks the shard.
	otherRoot, err := CreateRootBlock(loadFixture(t, fixtureDevnet).Quarkchain)
	if err != nil {
		t.Fatalf("CreateRootBlock(devnet): %v", err)
	}
	if derive(cfg, otherRoot) == base {
		t.Error("block hash unchanged after a root-genesis change")
	}

	// A shard genesis field, and an allocated balance (which moves the state root).
	fresh := loadFixture(t, fixtureMainnet)
	fresh.Quarkchain.GetShardConfigByFullShardID(firstShardID).Genesis.Timestamp++
	if derive(fresh, root) == base {
		t.Error("block hash unchanged after a TIMESTAMP change")
	}

	fresh = loadFixture(t, fixtureMainnet)
	for _, alloc := range fresh.Quarkchain.GetShardConfigByFullShardID(firstShardID).Genesis.Alloc {
		for token := range alloc.Balances {
			alloc.Balances[token] = new(big.Int).Add(alloc.Balances[token], big.NewInt(1))
			break
		}
		break
	}
	if derive(fresh, root) == base {
		t.Error("block hash unchanged after an ALLOC balance change")
	}
}

// TestCreateMinorBlockSnapshotsExtraData: the block copies EXTRA_DATA out of the
// config, so mutating the config afterwards cannot drift a derived block.
func TestCreateMinorBlockSnapshotsExtraData(t *testing.T) {
	cfg, shardCfg, root := shardEnv(t, fixtureMainnet)
	if len(shardCfg.Genesis.ExtraData) == 0 {
		t.Skip("fixture has no EXTRA_DATA")
	}
	block, err := CreateMinorBlock(cfg.Quarkchain, firstShardID, root)
	if err != nil {
		t.Fatalf("CreateMinorBlock: %v", err)
	}
	base := block.Hash()

	shardCfg.Genesis.ExtraData[0]++
	if block.Hash() != base {
		t.Error("derived block drifted with a config mutation")
	}
}

// TestCreateMinorBlockRejectsNonzeroRootHeight: a shard whose genesis is created
// at a later root height would link a root block this skeleton does not have — it
// is refused loudly instead of being stood on the root genesis silently.
func TestCreateMinorBlockRejectsNonzeroRootHeight(t *testing.T) {
	cfg, shardCfg, root := shardEnv(t, fixtureMainnet)
	shardCfg.Genesis.RootHeight = 3
	if _, err := CreateMinorBlock(cfg.Quarkchain, firstShardID, root); err == nil ||
		!strings.Contains(err.Error(), "ROOT_HEIGHT") {
		t.Fatalf("CreateMinorBlock err = %v, want a ROOT_HEIGHT rejection", err)
	}
}

// TestCreateMinorBlockRejectsNonzeroHeight: the genesis block is block 0 by
// definition — geth refuses the same thing in Genesis.Commit.
func TestCreateMinorBlockRejectsNonzeroHeight(t *testing.T) {
	cfg, shardCfg, root := shardEnv(t, fixtureMainnet)
	shardCfg.Genesis.Height = 5
	if _, err := CreateMinorBlock(cfg.Quarkchain, firstShardID, root); err == nil ||
		!strings.Contains(err.Error(), "must be block 0") {
		t.Fatalf("CreateMinorBlock err = %v, want a HEIGHT rejection", err)
	}
}

// TestCreateMinorBlockRejectsForeignAlloc: an ALLOC address that belongs to
// another shard is refused, the check pyquarkchain performs while writing the
// genesis state (quarkchain/genesis.py:57). Inline ALLOC is validated nowhere
// else.
func TestCreateMinorBlockRejectsForeignAlloc(t *testing.T) {
	cfg, shardCfg, root := shardEnv(t, fixtureMainnet)
	foreign := account.CreatEmptyAddress(foreignShardID)
	shardCfg.Genesis.Alloc[foreign] = config.Allocation{Balances: map[string]*big.Int{"QKC": big.NewInt(1)}}

	_, err := CreateMinorBlock(cfg.Quarkchain, firstShardID, root)
	if err == nil || !strings.Contains(err.Error(), "belongs to shard") ||
		!strings.Contains(err.Error(), "0x00010001") {
		t.Fatalf("CreateMinorBlock err = %v, want a foreign-ALLOC rejection naming the owning shard", err)
	}
}

// TestCreateMinorBlockRejectsBadTokenName: an ALLOC balance key outside
// pyquarkchain's [0-9A-Z]{1,12} token domain reaches common.TokenIDEncode, which
// panics ("unknown character 108"). CreateMinorBlock returns errors, so it must
// report the bad name rather than take the process down with it. The same holds
// for GENESIS_TOKEN, which the coinbase amount is denominated in.
func TestCreateMinorBlockRejectsBadTokenName(t *testing.T) {
	cfg, shardCfg, root := shardEnv(t, fixtureMainnet)
	addr := account.CreatEmptyAddress(firstShardID)
	shardCfg.Genesis.Alloc[addr] = config.Allocation{Balances: map[string]*big.Int{"lowercase": big.NewInt(1)}}
	if _, err := CreateMinorBlock(cfg.Quarkchain, firstShardID, root); err == nil ||
		!strings.Contains(err.Error(), "illegal character") {
		t.Fatalf("CreateMinorBlock err = %v, want an illegal-token-name rejection", err)
	}

	cfg, _, root = shardEnv(t, fixtureMainnet)
	cfg.Quarkchain.GenesisToken = "qkc"
	if _, err := CreateMinorBlock(cfg.Quarkchain, firstShardID, root); err == nil ||
		!strings.Contains(err.Error(), "GENESIS_TOKEN") {
		t.Fatalf("CreateMinorBlock err = %v, want a GENESIS_TOKEN rejection", err)
	}
}

func TestCreateMinorBlockRejectsUnknownShard(t *testing.T) {
	cfg, _, root := shardEnv(t, fixtureMainnet)
	if _, err := CreateMinorBlock(cfg.Quarkchain, 0x00990099, root); err == nil ||
		!strings.Contains(err.Error(), "not configured") {
		t.Fatalf("CreateMinorBlock err = %v, want 'not configured'", err)
	}
}

// TestShardChainConfig: the EVM chain id is BASE_ETH_CHAIN_ID + CHAIN_ID + 1
// under Petersburg-only rules.
func TestShardChainConfig(t *testing.T) {
	for _, tc := range []struct {
		name       string
		path       string
		ethChainID uint64
	}{
		{"mainnet", fixtureMainnet, 100001},
		{"devnet", fixtureDevnet, 110001},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, shardCfg, _ := shardEnv(t, tc.path)
			cc, err := ShardChainConfig(cfg.Quarkchain, shardCfg)
			if err != nil {
				t.Fatalf("ShardChainConfig: %v", err)
			}
			if cc.ChainID.Cmp(new(big.Int).SetUint64(tc.ethChainID)) != 0 {
				t.Errorf("ChainID = %v, want %d", cc.ChainID, tc.ethChainID)
			}
			if cc.PetersburgBlock == nil || cc.PetersburgBlock.Sign() != 0 {
				t.Errorf("PetersburgBlock = %v, want 0", cc.PetersburgBlock)
			}
			if cc.IstanbulBlock != nil {
				t.Errorf("IstanbulBlock = %v, want nil (Petersburg-only rules)", cc.IstanbulBlock)
			}
		})
	}
}

// TestShardChainConfigRejectsInconsistentEthChainID: a per-chain ETH_CHAIN_ID
// that disagrees with the forced pyquarkchain derivation is rejected.
func TestShardChainConfigRejectsInconsistentEthChainID(t *testing.T) {
	cfg, shardCfg, _ := shardEnv(t, fixtureMainnet)
	shardCfg.EthChainID = 42
	if _, err := ShardChainConfig(cfg.Quarkchain, shardCfg); err == nil ||
		!strings.Contains(err.Error(), "ETH_CHAIN_ID") {
		t.Fatalf("ShardChainConfig err = %v, want an ETH_CHAIN_ID mismatch", err)
	}
}

// TestShardChainConfigEthChainIDNoOverflow: the derivation is computed in
// uint64 — pyquarkchain's arithmetic is unbounded, so BASE_ETH_CHAIN_ID =
// MaxUint32 derives 4294967296, not a wrapped 0 (which would read as "absent" and
// put a wrong replay-protection chain id into the EVM rule set).
func TestShardChainConfigEthChainIDNoOverflow(t *testing.T) {
	cfg, shardCfg, _ := shardEnv(t, fixtureMainnet)
	cfg.Quarkchain.BaseEthChainID = math.MaxUint32
	// The loader has already derived ETH_CHAIN_ID from the fixture's base; clear
	// it to exercise the derivation on the not-yet-derived path.
	shardCfg.EthChainID = 0

	cc, err := ShardChainConfig(cfg.Quarkchain, shardCfg)
	if err != nil {
		t.Fatalf("ShardChainConfig: %v", err)
	}
	if want := new(big.Int).SetUint64(1 << 32); cc.ChainID.Cmp(want) != 0 {
		t.Errorf("ChainID = %v, want %v", cc.ChainID, want)
	}
}
