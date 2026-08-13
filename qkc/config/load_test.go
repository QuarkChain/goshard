// Copyright 2026-2027, QuarkChain.

package config

import (
	"encoding/json"
	"math"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/qkc/account"
)

const (
	fixtureMainnet = "./singularity/mainnet.json"
	fixtureDevnet  = "./singularity/devnet.json"
	templatePath   = "./testdata/cluster_config_template.json"
)

// network describes the boot-relevant values of one real network config. Both
// singularity networks have 8 chains and 4 slaves S0..S3 each owning 2 shards;
// they differ in network id, consensus, db path, and root difficulty.
type network struct {
	name        string
	path        string
	dbRoot      string
	networkID   uint32
	s0Consensus string // consensus of S0's shards
	chain6      string // consensus of chain 6 (qkchash on mainnet, simulate on devnet)
	rootDiff    uint64
}

var networks = []network{
	{"mainnet", fixtureMainnet, "./qkc-data/mainnet", 1, PoWEthash, PoWQkchash, 10000000000000},
	{"devnet", fixtureDevnet, "./qkc-data/devnet", 255, PoWSimulate, PoWSimulate, 100000},
}

// TestLoadClusterConfigFixture asserts the boot-relevant values parsed from each
// real network config: 8 chains, 4 slaves each owning 2 shards. S0's first id is
// written "0x1" in both configs, so leading-zero-tolerant hex must parse it to 1.
func TestLoadClusterConfigFixture(t *testing.T) {
	for _, nw := range networks {
		t.Run(nw.name, func(t *testing.T) {
			cfg, err := LoadClusterConfig(nw.path)
			if err != nil {
				t.Fatalf("LoadClusterConfig: %v", err)
			}
			if cfg.DbPathRoot != nw.dbRoot {
				t.Errorf("DB_PATH_ROOT = %q, want %q", cfg.DbPathRoot, nw.dbRoot)
			}
			if cfg.Quarkchain.NetworkID != nw.networkID {
				t.Errorf("NETWORK_ID = %d, want %d", cfg.Quarkchain.NetworkID, nw.networkID)
			}
			if len(cfg.SlaveList) != 4 {
				t.Fatalf("SLAVE_LIST len = %d, want 4", len(cfg.SlaveList))
			}

			s0 := cfg.SlaveList[0]
			if s0.ID != "S0" {
				t.Errorf("slave ID = %q, want S0", s0.ID)
			}
			wantIDs := []uint32{0x00000001, 0x00040001}
			if len(s0.FullShardList) != len(wantIDs) {
				t.Fatalf("FULL_SHARD_ID_LIST = %v, want %v", s0.FullShardList, wantIDs)
			}
			for i, id := range wantIDs {
				if s0.FullShardList[i] != id {
					t.Errorf("FULL_SHARD_ID_LIST[%d] = 0x%08x, want 0x%08x", i, s0.FullShardList[i], id)
				}
				// full shard id math: (chain_id<<16) | shard_size | shard_id.
				shard := cfg.Quarkchain.GetShardConfigByFullShardID(id)
				if shard == nil {
					t.Fatalf("shard 0x%08x not configured", id)
				}
				if got := shard.GetFullShardId(); got != id {
					t.Errorf("GetFullShardId = 0x%08x, want 0x%08x", got, id)
				}
				if shard.ConsensusType != nw.s0Consensus {
					t.Errorf("shard 0x%08x consensus = %q, want %q", id, shard.ConsensusType, nw.s0Consensus)
				}
			}
			if c6 := cfg.Quarkchain.GetShardConfigByFullShardID(0x00060001); c6 == nil || c6.ConsensusType != nw.chain6 {
				t.Errorf("shard 0x00060001 consensus = %v, want %q", c6, nw.chain6)
			}

			root := cfg.Quarkchain.Root.Genesis
			if root.Difficulty == nil || root.Difficulty.Uint64() != nw.rootDiff {
				t.Errorf("ROOT.GENESIS.DIFFICULTY = %v, want %d", root.Difficulty, nw.rootDiff)
			}
			if root.Timestamp != 1556639999 {
				t.Errorf("ROOT.GENESIS.TIMESTAMP = %d, want 1556639999", root.Timestamp)
			}
		})
	}
}

func TestResolveSlave(t *testing.T) {
	for _, nw := range networks {
		t.Run(nw.name, func(t *testing.T) {
			cfg, err := LoadClusterConfig(nw.path)
			if err != nil {
				t.Fatalf("LoadClusterConfig: %v", err)
			}

			sc, err := cfg.ResolveSlave("S0")
			if err != nil {
				t.Fatalf("ResolveSlave(S0): %v", err)
			}
			if sc.ID != "S0" || sc.DBPathRoot != nw.dbRoot {
				t.Errorf("SlaveContext = %+v, want ID=S0 DBPathRoot=%q", sc, nw.dbRoot)
			}
			shards := sc.ShardConfigs()
			if len(shards) != 2 {
				t.Fatalf("ShardConfigs len = %d, want 2", len(shards))
			}
			if shards[0].GetFullShardId() != 0x00000001 || shards[1].GetFullShardId() != 0x00040001 {
				t.Errorf("ShardConfigs order wrong: %x, %x", shards[0].GetFullShardId(), shards[1].GetFullShardId())
			}

			_, err = cfg.ResolveSlave("S9")
			if err == nil {
				t.Fatal("ResolveSlave(S9): expected error, got nil")
			}
			if !strings.Contains(err.Error(), `unknown node id "S9"`) || !strings.Contains(err.Error(), "S3") {
				t.Errorf("ResolveSlave(S9) error = %q, want mention of S9 and the configured ids (incl S3)", err)
			}
		})
	}
}

// TestWebSocketPortParsing covers the optional single WEBSOCKET_JSON_RPC_PORT field
// (pyquarkchain's form, adapted in slave_config.go): devnet's S0 omits it (null) and
// S1 sets 38591.
func TestWebSocketPortParsing(t *testing.T) {
	cfg, err := LoadClusterConfig(fixtureDevnet)
	if err != nil {
		t.Fatalf("LoadClusterConfig: %v", err)
	}
	if cfg.SlaveList[0].WSPort != nil {
		t.Errorf("S0 WEBSOCKET_JSON_RPC_PORT = %v, want nil", *cfg.SlaveList[0].WSPort)
	}
	if got := cfg.SlaveList[1].WSPort; got == nil || *got != 38591 {
		t.Errorf("S1 WEBSOCKET_JSON_RPC_PORT = %v, want 38591", got)
	}
}

func TestValidateRejectsUnknownShard(t *testing.T) {
	cfg := mustLoad(t)
	cfg.SlaveList[0].FullShardList = []uint32{0x00990099}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("Validate err = %v, want 'not configured'", err)
	}
}

func TestValidateRejectsDuplicateShard(t *testing.T) {
	cfg := mustLoad(t)
	cfg.SlaveList[0].FullShardList = []uint32{0x00000001, 0x00000001}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("Validate err = %v, want 'duplicate'", err)
	}
}

// TestValidateAllowsCrossSlaveReplica asserts that the same full shard id appearing
// on two different slaves is accepted: pyquarkchain treats this as a valid
// multi-replica deployment (master.py:1122), so the duplicate check is intra-slave
// only. Global shard-coverage/replica validation belongs at the master layer.
func TestValidateAllowsCrossSlaveReplica(t *testing.T) {
	cfg := mustLoad(t)
	// S1 now replicates S0's shards: a valid multi-replica config.
	cfg.SlaveList[1].FullShardList = append([]uint32(nil), cfg.SlaveList[0].FullShardList...)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate rejected a valid multi-replica config: %v", err)
	}
}

// TestDeriveEthChainID: pyquarkchain forces ETH_CHAIN_ID = BASE_ETH_CHAIN_ID +
// CHAIN_ID + 1 on load (config.py:534). Derivation fills every chain and the shard
// copies made before it runs, accepts a consistent explicit value, rejects an
// inconsistent one, and does not wrap the uint64 arithmetic into uint32.
func TestDeriveEthChainID(t *testing.T) {
	content, err := os.ReadFile(fixtureMainnet)
	if err != nil {
		t.Fatal(err)
	}
	// parse without the LoadClusterConfig normalization, so ETH_CHAIN_ID is still
	// absent (the fixtures omit it) and derivation can be exercised in isolation.
	parse := func() *ClusterConfig {
		cfg, err := unmarshalClusterConfig(content)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		return cfg
	}

	cfg := parse()
	if err := cfg.deriveEthChainID(); err != nil {
		t.Fatalf("deriveEthChainID: %v", err)
	}
	for _, chain := range cfg.Quarkchain.Chains {
		if want := cfg.Quarkchain.BaseEthChainID + chain.ChainID + 1; chain.EthChainID != want {
			t.Errorf("chain %d ETH_CHAIN_ID = %d, want %d", chain.ChainID, chain.EthChainID, want)
		}
	}
	// The shard copies (deep-copied from the chain config before derivation) must
	// carry the derived id too, not the pre-derivation zero.
	shard := cfg.Quarkchain.GetShardConfigByFullShardID(0x00000001)
	if want := cfg.Quarkchain.BaseEthChainID + 1; shard == nil || shard.EthChainID != want {
		t.Errorf("shard 0x00000001 ETH_CHAIN_ID = %v, want %d", shard, want)
	}

	// A present-but-inconsistent value is rejected rather than silently overwritten.
	cfg = parse()
	cfg.Quarkchain.Chains[0].EthChainID = 42
	if err := cfg.deriveEthChainID(); err == nil || !strings.Contains(err.Error(), "ETH_CHAIN_ID") {
		t.Fatalf("deriveEthChainID err = %v, want ETH_CHAIN_ID mismatch", err)
	}

	// The derivation is computed in uint64 and must not wrap into uint32: with
	// BASE_ETH_CHAIN_ID = MaxUint32 the true id 4294967296 does not fit a uint32.
	cfg = parse()
	cfg.Quarkchain.BaseEthChainID = math.MaxUint32
	if err := cfg.deriveEthChainID(); err == nil || !strings.Contains(err.Error(), "exceeds uint32") {
		t.Fatalf("deriveEthChainID err = %v, want 'exceeds uint32'", err)
	}
}

func TestValidateRejectsBadRootHash(t *testing.T) {
	cfg := mustLoad(t)
	cfg.Quarkchain.Root.Genesis.HashPrevBlock = "nothex!!"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "HASH_PREV_BLOCK") {
		t.Fatalf("Validate err = %v, want 'HASH_PREV_BLOCK'", err)
	}
}

func TestValidateRejectsGenesisRootHeightMismatch(t *testing.T) {
	cfg := mustLoad(t)
	cfg.Quarkchain.GetShardConfigByFullShardID(0x00000001).Genesis.RootHeight = 5
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "ROOT_HEIGHT") {
		t.Fatalf("Validate err = %v, want 'ROOT_HEIGHT'", err)
	}
}

// TestLoadRejectsChainMaskList parses the larger real template (which still uses
// the legacy CHAIN_MASK_LIST assignment form): it must parse without panicking,
// but be rejected with an explicit legacy-config error.
func TestLoadRejectsChainMaskList(t *testing.T) {
	content, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	// Parse stage tolerates the legacy form, nulls, and extra fields.
	if _, err := unmarshalClusterConfig(content); err != nil {
		t.Fatalf("parse template: %v", err)
	}
	// Validate stage rejects it.
	if _, err := LoadClusterConfig(templatePath); err == nil ||
		!strings.Contains(err.Error(), "legacy config not supported") {
		t.Fatalf("LoadClusterConfig(template) err = %v, want 'legacy config not supported'", err)
	}
}

// TestValidateRejectsBadShardGenesisHash: an owned shard's genesis hash fields are
// validated at load, so a malformed value fails while reporting config rather than
// being deferred (and silently mis-decoded) at shard genesis creation.
func TestValidateRejectsBadShardGenesisHash(t *testing.T) {
	cfg := mustLoad(t)
	cfg.Quarkchain.GetShardConfigByFullShardID(0x00000001).Genesis.HashPrevMinorBlock = "nothex!!"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "HASH_PREV_MINOR_BLOCK") {
		t.Fatalf("Validate err = %v, want 'HASH_PREV_MINOR_BLOCK'", err)
	}
}

// TestShardGenesisRejectsBadExtraData: EXTRA_DATA is decoded to bytes at parse time,
// so it must be strictly validated there. common.Hex2Bytes silently turns "zz" into
// an empty slice; decodeGenesisHex reports it instead.
func TestShardGenesisRejectsBadExtraData(t *testing.T) {
	var g ShardGenesis
	if err := json.Unmarshal([]byte(`{"EXTRA_DATA":"zz"}`), &g); err == nil || !strings.Contains(err.Error(), "EXTRA_DATA") {
		t.Fatalf("Unmarshal err = %v, want 'EXTRA_DATA'", err)
	}
	if err := json.Unmarshal([]byte(`{"EXTRA_DATA":"abcd"}`), &g); err != nil {
		t.Fatalf("Unmarshal valid EXTRA_DATA: %v", err)
	}
}

// TestValidateRejectsEmptyChainMaskList: a present-but-empty CHAIN_MASK_LIST is a
// non-nil zero-length slice; it must be rejected as the unsupported legacy form
// rather than slipping through len()>0 and reporting a slave with zero shards.
func TestValidateRejectsEmptyChainMaskList(t *testing.T) {
	cfg := mustLoad(t)
	cfg.SlaveList[0].ChainMaskListForBackward = []uint32{}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "legacy config not supported") {
		t.Fatalf("Validate err = %v, want 'legacy config not supported'", err)
	}
}

// TestValidateRejectsZeroShardSlave: a slave that resolves to no shards (e.g. an
// empty FULL_SHARD_ID_LIST) is meaningless and must fail loudly.
func TestValidateRejectsZeroShardSlave(t *testing.T) {
	cfg := mustLoad(t)
	cfg.SlaveList[0].FullShardList = nil
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "owns no shards") {
		t.Fatalf("Validate err = %v, want 'owns no shards'", err)
	}
}

// TestValidateRejectsBadTokenName: token names feed common.TokenIDEncode, which
// panics outside pyquarkchain's [0-9A-Z]{1,12} domain, so GENESIS_TOKEN and every
// ALLOC balance key are checked at load. An ALLOC entry such as {"lowercase": 1}
// used to crash the process ("unknown character 108") from inside the
// error-returning genesis path.
func TestValidateRejectsBadTokenName(t *testing.T) {
	for _, tc := range []struct{ name, token, want string }{
		{"lowercase", "lowercase", "illegal character"},
		{"punctuation", "QKC-2", "illegal character"},
		{"empty", "", "empty"},
		{"too long", "ZZZZZZZZZZZZZ", "longer than 12"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := mustLoad(t)
			shard := cfg.Quarkchain.GetShardConfigByFullShardID(0x00000001)
			addr := account.CreatEmptyAddress(0x00000001)
			shard.Genesis.Alloc[addr] = Allocation{Balances: map[string]*big.Int{tc.token: big.NewInt(1)}}
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate err = %v, want %q", err, tc.want)
			}
			if !strings.Contains(err.Error(), "GENESIS.ALLOC") || !strings.Contains(err.Error(), addr.ToHex()) {
				t.Errorf("Validate err = %v, want the offending ALLOC entry named", err)
			}

			cfg = mustLoad(t)
			cfg.Quarkchain.GenesisToken = tc.token
			err = cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "GENESIS_TOKEN") {
				t.Fatalf("Validate err = %v, want a GENESIS_TOKEN rejection (%q)", err, tc.want)
			}
		})
	}
}

// TestSlaveContextOwns: ownership follows FULL_SHARD_ID_LIST, not the shared
// chain config — S1's shards are configured, and still not S0's to host.
func TestSlaveContextOwns(t *testing.T) {
	cfg := mustLoad(t)
	sc, err := cfg.ResolveSlave("S0")
	if err != nil {
		t.Fatalf("ResolveSlave(S0): %v", err)
	}
	for _, tc := range []struct {
		id   uint32
		want bool
	}{
		{0x00000001, true},  // S0's first shard
		{0x00040001, true},  // S0's second shard
		{0x00010001, false}, // configured, but S1's
		{0x00990099, false}, // configured nowhere
	} {
		if got := sc.Owns(tc.id); got != tc.want {
			t.Errorf("S0.Owns(0x%08x) = %v, want %v", tc.id, got, tc.want)
		}
	}
}

func mustLoad(t *testing.T) *ClusterConfig {
	t.Helper()
	cfg, err := LoadClusterConfig(fixtureMainnet)
	if err != nil {
		t.Fatalf("LoadClusterConfig: %v", err)
	}
	return cfg
}
