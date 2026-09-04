// Copyright 2026-2027, QuarkChain.

//go:build interop

package slave

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// =============================================================================
// InteropCluster — reusable real Python Master + Go Slaves test harness
// =============================================================================
//
// Usage:
//
//   cluster := startInteropCluster(t, [][]uint32{{0x00000001}, {0x00010001}})
//   defer cluster.Stop()
//   if !cluster.WaitBootstrap(30 * time.Second) {
//       t.Fatal("bootstrap timed out")
//   }
//   s0 := cluster.Slave(0)
//   // ... assertions on s0.MasterConn(), s0.xshardPool, etc.

// InteropCluster holds a running Python Master + Go Slaves cluster.
type InteropCluster struct {
	slaves     []*SlaveComm
	ports      []int
	p2pPort    int
	configPath string
	shardList  [][]uint32
	masterCmd  *exec.Cmd
	masterOut  *safeBuffer
	cancel     context.CancelFunc
}

// startInteropCluster starts Go slaves, generates a cluster_config.json,
// and launches the real Python master. The caller must call Stop() to
// tear down the master process.
//
// shardLists[i] is the full shard ID list for slave i. Slave IDs are
// auto-generated as "S0", "S1", ...
func startInteropCluster(t *testing.T, shardLists [][]uint32) *InteropCluster {
	t.Helper()

	pyRoot := requirePyquarkchain(t)
	requirePython3(t)

	n := len(shardLists)
	if n == 0 {
		t.Fatal("need at least 1 slave")
	}

	// ── 1. Start Go slaves ──────────────────────────────────────────────
	slaves := make([]*SlaveComm, n)
	ports := make([]int, n)
	for i := range n {
		id := fmt.Sprintf("S%d", i)
		slaves[i], ports[i] = startTestSlave(t, id, shardLists[i])
	}

	// ── 2. Reserve P2P port for SimpleNetwork peer connections ──────────
	p2pPort := freePort(t)

	// ── 3. Generate cluster_config.json ─────────────────────────────────
	configPath := filepath.Join(t.TempDir(), "cluster_config.json")
	writeClusterConfig(t, configPath, ports, p2pPort, shardLists)

	// ── 3. Start Python Master ──────────────────────────────────────────
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine source file location")
	}
	wrapperPy := filepath.Join(filepath.Dir(thisFile), "testdata", "bootstrap_master_wrapper.py")
	ctx, cancel := context.WithCancel(context.Background())

	cmd := exec.CommandContext(ctx, "python3", "-u", wrapperPy, "--cluster_config", configPath)
	cmd.Env = append(os.Environ(),
		"PYQUARKCHAIN="+pyRoot,
		"PYTHONPATH="+pyRoot,
	)
	var out safeBuffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start master: %v", err)
	}

	return &InteropCluster{
		slaves:     slaves,
		ports:      ports,
		p2pPort:    p2pPort,
		configPath: configPath,
		shardList:  shardLists,
		masterCmd:  cmd,
		masterOut:  &out,
		cancel:     cancel,
	}
}

// Stop cancels the Python master process and waits for it to exit.
// Go slaves are stopped by t.Cleanup registered in startTestSlave.
func (c *InteropCluster) Stop() {
	c.cancel()
	c.masterCmd.Wait()
}

// WaitBootstrap polls until all slaves have a master connection and at
// least one xshard connection (if N > 1). Returns false on timeout.
func (c *InteropCluster) WaitBootstrap(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	n := len(c.slaves)

	for time.Now().Before(deadline) {
		allReady := true
		for i := range n {
			if c.slaves[i].MasterConn() == nil {
				allReady = false
				break
			}
			// xshard connections only relevant when there are 2+ slaves
			if n > 1 {
				x := c.slaves[i].xshardPool.OutboundSize() + c.slaves[i].xshardPool.InboundSize()
				if x == 0 {
					allReady = false
					break
				}
			}
		}
		if allReady {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// Slave returns the i-th SlaveComm.
func (c *InteropCluster) Slave(i int) *SlaveComm {
	return c.slaves[i]
}

// MasterOutput returns the combined stdout/stderr of the Python master.
func (c *InteropCluster) MasterOutput() string {
	return c.masterOut.String()
}

// P2PPort returns the port the master's SimpleNetwork server listens on.
func (c *InteropCluster) P2PPort() int {
	return c.p2pPort
}

// ConfigPath returns the path to the generated cluster_config.json.
func (c *InteropCluster) ConfigPath() string {
	return c.configPath
}

// =============================================================================
// Cluster config generation
// =============================================================================

// writeClusterConfig writes a minimal cluster_config.json for N slaves.
//
// Full shard ID encoding: (chain_id << 16) | shard_size | shard_id
func writeClusterConfig(t *testing.T, path string, ports []int, p2pPort int, shardLists [][]uint32) {
	t.Helper()

	config := buildClusterConfig(ports, p2pPort, shardLists)
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatalf("marshal cluster config: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write cluster config: %v", err)
	}
	t.Logf("cluster config written to %s", path)
}

func buildClusterConfig(ports []int, p2pPort int, shardLists [][]uint32) map[string]any {
	n := len(ports)

	formatShardList := func(shards []uint32) []string {
		list := make([]string, len(shards))
		for i, s := range shards {
			list[i] = fmt.Sprintf("0x%08x", s)
		}
		return list
	}

	// Build slave list
	slaveList := make([]any, n)
	for i := range n {
		slaveList[i] = map[string]any{
			"HOST":               "127.0.0.1",
			"PORT":               ports[i],
			"ID":                 fmt.Sprintf("S%d", i),
			"FULL_SHARD_ID_LIST": formatShardList(shardLists[i]),
		}
	}

	// Build chain configs — one chain per slave, each with 1 shard
	chains := make([]any, n)
	for i := range n {
		chains[i] = chainConfig(i)
	}

	return map[string]any{
		"P2P_PORT":                   p2pPort,
		"JSON_RPC_PORT":              0,
		"PRIVATE_JSON_RPC_PORT":      0,
		"ENABLE_TRANSACTION_HISTORY": false,
		"DB_PATH_ROOT":               "",
		"LOG_LEVEL":                  "info",
		"START_SIMULATED_MINING":     false,
		"CLEAN":                      false,
		"GENESIS_DIR":                nil,

		"QUARKCHAIN": map[string]any{
			"CHAIN_SIZE":                             n,
			"BASE_ETH_CHAIN_ID":                      110000,
			"MAX_NEIGHBORS":                          32,
			"NETWORK_ID":                             255,
			"TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD": 10000,
			"BLOCK_EXTRA_DATA_SIZE_LIMIT":            1024,
			"GUARDIAN_PUBLIC_KEY":                    "ab856abd0983a82972021e454fcf66ed5940ed595b0898bcd75cbe2d0a51a00f5358b566df22395a2a8bf6c022c1d51a2c3defe654e91a8d244947783029694d",
			"ROOT_SIGNER_PRIVATE_KEY":                nil,
			"P2P_PROTOCOL_VERSION":                   0,
			"P2P_COMMAND_SIZE_LIMIT":                 134217728,
			"SKIP_ROOT_DIFFICULTY_CHECK":             false,
			"SKIP_MINOR_DIFFICULTY_CHECK":            false,
			"GENESIS_TOKEN":                          "QKC",
			"ROOT":                                   rootConfig(),
			"CHAINS":                                 chains,
			"REWARD_TAX_RATE":                        0.5,
			"BLOCK_REWARD_DECAY_FACTOR":              0.88,
			"ROOT_CHAIN_POSW_CONTRACT_BYTECODE_HASH": "0000000000000000000000000000000000000000000000000000000000000000",
		},

		"MASTER": map[string]any{
			"MASTER_TO_SLAVE_CONNECT_RETRY_DELAY": 1.0,
		},

		"SLAVE_LIST": slaveList,

		"P2P": map[string]any{
			"BOOT_NODES":                       "",
			"PRIV_KEY":                         "",
			"MAX_PEERS":                        25,
			"UPNP":                             false,
			"ALLOW_DIAL_IN_RATIO":              1.0,
			"PREFERRED_NODES":                  "",
			"DISCOVERY_ONLY":                   false,
			"CRAWLING_ROUTING_TABLE_FILE_PATH": nil,
		},

		"MONITORING": map[string]any{
			"NETWORK_NAME":       "",
			"CLUSTER_ID":         "127.0.0.1",
			"KAFKA_REST_ADDRESS": "",
			"MINER_TOPIC":        "qkc_miner",
			"PROPAGATION_TOPIC":  "block_propagation",
			"ERRORS":             "error",
		},
	}
}

func rootConfig() map[string]any {
	return map[string]any{
		"MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF": 22500,
		"CONSENSUS_TYPE":                   "POW_SIMULATE",
		"CONSENSUS_CONFIG": map[string]any{
			"TARGET_BLOCK_TIME": 10,
			"REMOTE_MINE":       false,
		},
		"GENESIS": map[string]any{
			"VERSION":          0,
			"HEIGHT":           0,
			"HASH_PREV_BLOCK":  "0000000000000000000000000000000000000000000000000000000000000000",
			"HASH_MERKLE_ROOT": "0000000000000000000000000000000000000000000000000000000000000000",
			"TIMESTAMP":        1556639999,
			"DIFFICULTY":       100000,
			"NONCE":            0,
		},
		"COINBASE_ADDRESS":                  "000000000000000000000000000000000000000000000000",
		"COINBASE_AMOUNT":                   json.Number("156000000000000000000"),
		"DIFFICULTY_ADJUSTMENT_CUTOFF_TIME": 40,
		"DIFFICULTY_ADJUSTMENT_FACTOR":      1024,
		"EPOCH_INTERVAL":                    525600,
		"POSW_CONFIG": map[string]any{
			"ENABLED":               false,
			"ENABLE_TIMESTAMP":      0,
			"DIFF_DIVIDER":          100,
			"WINDOW_SIZE":           256,
			"TOTAL_STAKE_PER_BLOCK": 0,
		},
	}
}

func chainConfig(chainID int) map[string]any {
	return map[string]any{
		"CHAIN_ID":            chainID,
		"SHARD_SIZE":          1,
		"DEFAULT_CHAIN_TOKEN": "QKC",
		"CONSENSUS_TYPE":      "POW_SIMULATE",
		"CONSENSUS_CONFIG": map[string]any{
			"TARGET_BLOCK_TIME": 10,
			"REMOTE_MINE":       false,
		},
		"GENESIS": map[string]any{
			"ROOT_HEIGHT":           0,
			"VERSION":               0,
			"HEIGHT":                0,
			"HASH_PREV_MINOR_BLOCK": "0000000000000000000000000000000000000000000000000000000000000000",
			"HASH_MERKLE_ROOT":      "0000000000000000000000000000000000000000000000000000000000000000",
			"EXTRA_DATA":            "497420776173207468652062657374206f662074696d6573",
			"TIMESTAMP":             1556639999,
			"DIFFICULTY":            10000,
			"GAS_LIMIT":             12000000,
			"NONCE":                 0,
			"ALLOC":                 map[string]any{},
		},
		"COINBASE_ADDRESS":                  "000000000000000000000000000000000000000000000000",
		"COINBASE_AMOUNT":                   json.Number("6500000000000000000"),
		"DIFFICULTY_ADJUSTMENT_CUTOFF_TIME": 7,
		"DIFFICULTY_ADJUSTMENT_FACTOR":      512,
		"EXTRA_SHARD_BLOCKS_IN_ROOT_BLOCK":  3,
		"POSW_CONFIG": map[string]any{
			"ENABLED":               false,
			"DIFF_DIVIDER":          20,
			"WINDOW_SIZE":           256,
			"TOTAL_STAKE_PER_BLOCK": 0,
		},
		"EPOCH_INTERVAL": 3153600,
	}
}
