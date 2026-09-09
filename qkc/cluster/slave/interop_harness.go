// Copyright 2026-2027, QuarkChain.

//go:build interop

package slave

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/internal/testlog"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// =============================================================================
// Environment guards
// =============================================================================

// requirePyquarkchain returns the pyquarkchain root path.
// Skips the test if PYQUARKCHAIN is not set or the directory doesn't exist.
func requirePyquarkchain(t *testing.T) string {
	t.Helper()
	root := os.Getenv("PYQUARKCHAIN")
	if root == "" {
		t.Skip("PYQUARKCHAIN not set")
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("PYQUARKCHAIN directory not found: %v", err)
	}
	return root
}

// requirePython3 skips the test if python3 is not available.
func requirePython3(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
}

// safeBuffer is a goroutine-safe bytes.Buffer.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// freePort returns an unused TCP port on 127.0.0.1.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// =============================================================================
// interopBackend — communication-only slave backend for Python interop tests
// =============================================================================

// interopBackend serves every business RPC with a trivially-successful response
// and records the lifecycle events the tests assert on (shard creation from a
// PING root tip). It satisfies SlaveConfig.Master, .Peer and .Xshard. Business
// methods are inherited from fakeMasterHandler, stubPeerHandler and
// testXshardHandler; only orchestration-relevant methods are overridden here.
type interopBackend struct {
	*fakeMasterHandler
	fullShardIDList []uint32

	mu sync.Mutex
	// createShardsCalls counts CreateShards invocations (shard creation only,
	// driven by ShardCreator below, not the business backend).
	createShardsCalls int
	// shardsCreated is closed once at least one PING root tip initialized this
	// slave's shards. It is the readiness gate WaitBootstrap polls.
	shardsCreated chan struct{}
}

func newInteropBackend(fullShardIDList []uint32) *interopBackend {
	return &interopBackend{
		fakeMasterHandler: &fakeMasterHandler{},
		fullShardIDList:   append([]uint32(nil), fullShardIDList...),
		shardsCreated:     make(chan struct{}),
	}
}

func (b *interopBackend) createShards(_ *wire.RawBytes) ([]uint32, error) {
	b.mu.Lock()
	b.createShardsCalls++
	b.mu.Unlock()
	select {
	case <-b.shardsCreated:
	default:
		close(b.shardsCreated)
	}
	return append([]uint32(nil), b.fullShardIDList...), nil
}

func (b *interopBackend) CreateShardsCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.createShardsCalls
}

func (b *interopBackend) ShardsCreated() <-chan struct{} {
	return b.shardsCreated
}

// =============================================================================
// Slave startup
// =============================================================================

// startTestSlave starts a SlaveComm with a communication-only backend. fullShards
// are this slave's shards and must equal the config's FULL_SHARD_ID_LIST for the
// master's PONG validation; clusterShards are the cluster-wide shard set.
// peerOver optionally overrides the shared PeerHandler (defaults to stubPeerHandler).
func startTestSlave(t *testing.T, id string, fullShards, clusterShards []uint32, peerOver ...PeerHandler) (*SlaveComm, *interopBackend, int) {
	t.Helper()
	backend := newInteropBackend(fullShards)

	var peerHandler PeerHandler = stubPeerHandler{}
	if len(peerOver) > 0 {
		peerHandler = peerOver[0]
	}

	for attempt := 0; ; attempt++ {
		port := freePort(t)
		cfg := SlaveConfig{
			ID:                     []byte(id),
			FullShardIDList:        append([]uint32(nil), fullShards...),
			ClusterFullShardIDList: append([]uint32(nil), clusterShards...),
			Port:                   port,
			MaxPayloadSize:         0,
			Logger:                 testlog.Logger(t, log.LvlInfo),
			Master:                 backend,
			ShardCreator:           backend.createShards,
			Peer:                   peerHandler,
			Xshard:                 testXshardHandler{},
		}
		srv, err := NewSlaveComm(cfg)
		if err != nil {
			t.Fatalf("new slave server: %v", err)
		}
		if err := srv.Start(); err != nil {
			if attempt < 5 && strings.Contains(err.Error(), "address already in use") {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			t.Fatalf("start slave server: %v", err)
		}
		t.Cleanup(func() { srv.Stop() })
		return srv, backend, port
	}
}

// clusterShardSet returns the union of all slaves' shard sets.
func clusterShardSet(shardLists [][]uint32) []uint32 {
	seen := make(map[uint32]struct{})
	var out []uint32
	for _, list := range shardLists {
		for _, s := range list {
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// =============================================================================
// Python master process driver
// =============================================================================

// scenarioMasterProc runs a testdata/master_harness.py scenario in the
// background, capturing its combined stdout/stderr. Scenarios run to completion
// (exit 0 on success) but some need to be observed mid-flight, so the process
// is started eagerly and its output polled line by line.
type scenarioMasterProc struct {
	t      *testing.T
	cmd    *exec.Cmd
	cancel context.CancelFunc
	out    *safeBuffer
	done   chan struct{} // closed once the process exits
	err    error         // set before done is closed
}

// startScenarioMaster drives a master_harness.py sub-command in the background.
// subcommand is one of "rpc", "peer", "disconnect" (and optionally "bootstrap").
// The caller must Close() it to release the process.
func startScenarioMaster(t *testing.T, subcommand string, args ...string) *scenarioMasterProc {
	t.Helper()
	requirePython3(t)
	pyRoot := requirePyquarkchain(t)

	script := masterScript(t)
	pyArgs := append([]string{"-u", script, subcommand}, args...)

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "python3", pyArgs...)
	cmd.Env = append(os.Environ(), "PYQUARKCHAIN="+pyRoot)

	out := &safeBuffer{}
	cmd.Stdout = out
	cmd.Stderr = out

	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start master_harness: %v", err)
	}

	p := &scenarioMasterProc{t: t, cmd: cmd, cancel: cancel, out: out, done: make(chan struct{})}
	go func() {
		perr := cmd.Wait()
		if perr != nil {
			p.cancel()
		}
		p.err = perr
		close(p.done)
	}()

	t.Cleanup(p.Close)
	return p
}

// output returns all output so far.
func (p *scenarioMasterProc) output() string {
	return p.out.String()
}

// WaitLine polls the output until substr appears or the process exits or
// timeout elapses. Returns true when the line appeared in time.
func (p *scenarioMasterProc) WaitLine(substr string, timeout time.Duration) bool {
	p.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(p.output(), substr) {
			return true
		}
		select {
		case <-p.done:
			return strings.Contains(p.output(), substr)
		case <-time.After(50 * time.Millisecond):
		}
	}
	return false
}

// WaitExit waits for the process to exit within timeout and returns its error.
func (p *scenarioMasterProc) WaitExit(timeout time.Duration) error {
	p.t.Helper()
	select {
	case <-p.done:
	case <-time.After(timeout):
		return context.DeadlineExceeded
	}
	return p.err
}

// Close cancels the process and waits for it to exit. Idempotent.
func (p *scenarioMasterProc) Close() {
	p.cancel()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
	}
}

// masterScript returns the path to testdata/master_harness.py next to this file.
func masterScript(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine source file location")
	}
	return filepath.Join(filepath.Dir(thisFile), "testdata", "master_harness.py")
}

// =============================================================================
// InteropCluster — full Python Master + N Go Slaves bootstrap harness
// =============================================================================
//
// InteropCluster starts N Go slaves, writes a cluster_config.json, and launches
// the real Python Master via master_harness.py bootstrap → master.main(). The
// master performs the full bootstrap by itself: connects to every slave,
// PING/PONGs, instructs the slaves to dial each other (CONNECT_TO_SLAVES →
// xshard), and initializes shards with a root-tip PING. WaitBootstrap reports
// when all of that has happened. InteropCluster implements no protocol and no
// business logic; it only owns processes, ports and config.

type InteropCluster struct {
	slaves     []*SlaveComm
	backends   []*interopBackend
	shardLists [][]uint32
	ports      []int
	p2pPort    int
	configPath string

	masterCmd *exec.Cmd
	masterOut *safeBuffer
	cancel    context.CancelFunc
}

// startInteropCluster starts the Go slaves, generates cluster_config.json and
// launches the full real Python Master. The caller must call Stop() to tear
// down the master process. shardLists[i] is slave i's full shard id list.
func startInteropCluster(t *testing.T, shardLists [][]uint32) *InteropCluster {
	t.Helper()

	requirePyquarkchain(t)
	requirePython3(t)

	n := len(shardLists)
	if n == 0 {
		t.Fatal("need at least 1 slave")
	}

	clusterShards := clusterShardSet(shardLists)

	// 1. Start Go slaves.
	slaves := make([]*SlaveComm, n)
	backends := make([]*interopBackend, n)
	ports := make([]int, n)
	for i := range n {
		id := fmt.Sprintf("S%d", i)
		slaves[i], backends[i], ports[i] = startTestSlave(t, id, shardLists[i], clusterShards)
	}

	// 2. Reserve P2P port.
	p2pPort := freePort(t)

	// 3. Generate cluster_config.json.
	configPath := filepath.Join(t.TempDir(), "cluster_config.json")
	writeClusterConfig(t, configPath, ports, p2pPort, shardLists)

	// 4. Launch the full real Python Master.
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "python3", "-u", masterScript(t), "bootstrap", "--cluster_config", configPath)
	cmd.Env = append(os.Environ(), "PYQUARKCHAIN="+requirePyquarkchain(t))
	var out safeBuffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start master: %v", err)
	}
	t.Cleanup(func() { cancel(); cmd.Wait() })

	return &InteropCluster{
		slaves:     slaves,
		backends:   backends,
		shardLists: shardLists,
		ports:      ports,
		p2pPort:    p2pPort,
		configPath: configPath,
		masterCmd:  cmd,
		masterOut:  &out,
		cancel:     cancel,
	}
}

// Stop cancels the Python master process. Go slaves are stopped by t.Cleanup
// registered in startTestSlave.
func (c *InteropCluster) Stop() {
	c.cancel()
}

// WaitBootstrap returns true when every slave is fully bootstrapped: shards
// initialized by a root-tip PING and, for multi-slave clusters, at least one
// registered xshard connection.
func (c *InteropCluster) WaitBootstrap(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.bootstrapReady() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// bootstrapReady reports whether all slaves reached the fully-initialized state.
func (c *InteropCluster) bootstrapReady() bool {
	n := len(c.slaves)
	for i := range n {
		select {
		case <-c.backends[i].ShardsCreated():
		default:
			return false
		}
		if n > 1 && !hasXshard(c.slaves[i], c.shardLists[i], clusterShardSet(c.shardLists)) {
			return false
		}
	}
	return true
}

// hasXshard reports whether the slave registered an xshard connection to a
// shard it does not itself own (i.e. a real peer slave, not a self-link).
func hasXshard(s *SlaveComm, ownShards, clusterShards []uint32) bool {
	own := make(map[uint32]struct{}, len(ownShards))
	for _, shard := range ownShards {
		own[shard] = struct{}{}
	}
	for _, shard := range clusterShards {
		if _, ok := own[shard]; ok {
			continue
		}
		if len(s.xshardPool.Lookup(shard)) > 0 {
			return true
		}
	}
	return false
}

// Slave returns the i-th SlaveComm.
func (c *InteropCluster) Slave(i int) *SlaveComm { return c.slaves[i] }

// SlaveCount returns the number of Go slaves in the cluster.
func (c *InteropCluster) SlaveCount() int { return len(c.slaves) }

// Backend returns the i-th communication-only backend.
func (c *InteropCluster) Backend(i int) *interopBackend { return c.backends[i] }

// MasterOutput returns the combined stdout/stderr of the Python master.
func (c *InteropCluster) MasterOutput() string { return c.masterOut.String() }

// =============================================================================
// Cluster config generation
// =============================================================================

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

	slaveList := make([]any, n)
	for i := range n {
		slaveList[i] = map[string]any{
			"HOST":               "127.0.0.1",
			"PORT":               ports[i],
			"ID":                 fmt.Sprintf("S%d", i),
			"FULL_SHARD_ID_LIST": formatShardList(shardLists[i]),
		}
	}
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
