// Copyright 2026-2027, QuarkChain.

//go:build interop

package slave

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// =============================================================================
// Interop tests: real Python Master ↔ real Go Slave
//
// These verify wire / opcode / serializer / bootstrap compatibility against the
// real pyquarkchain wire stack, driven by testdata/master_harness.py. All
// business behavior is served by the communication-only interopBackend.
// =============================================================================

// TestInteropBootstrap runs the full Python Master (master.main()) against two
// Go slaves and verifies the complete bootstrap: PING/PONG, root-tip shard
// initialization, and CONNECT_TO_SLAVES establishing xshard connections.
func TestInteropBootstrap(t *testing.T) {
	cluster := startInteropCluster(t, [][]uint32{
		{0x00000001},
		{0x00010001},
	})
	defer cluster.Stop()

	if !cluster.WaitBootstrap(30 * time.Second) {
		t.Fatalf("bootstrap timed out — master did not fully bootstrap all slaves\nmaster output:\n%s", cluster.MasterOutput())
	}

	for i := 0; i < cluster.SlaveCount(); i++ {
		s := cluster.Slave(i)
		if s.master.Load() == nil {
			t.Errorf("slave %d has no established MasterConn", i)
		}
		if n := numXshardConns(s.xshardPool); n == 0 {
			t.Errorf("slave %d registered no xshard connections", i)
		}
		// Bootstrap must not create any cluster peer connections.
		if n := len(s.peers); n != 0 {
			t.Errorf("slave %d has %d unexpected peer connections", i, n)
		}
		t.Logf("slave %d: master=%v createShards=%d xshard=%d",
			i, s.master.Load() != nil, cluster.Backend(i).CreateShardsCalls(), numXshardConns(s.xshardPool))
	}
}

// TestInteropMasterRpcRoundTrip drives a GET_ACCOUNT_DATA round-trip over the
// real wire and confirms the slave answers with error_code 0.
func TestInteropMasterRpcRoundTrip(t *testing.T) {
	_, _, port := startTestSlave(t, "S0", []uint32{0x00000001}, []uint32{0x00000001})

	zeroAddress := "0000000000000000000000000000000000000000"
	p := startScenarioMaster(t, "rpc", "127.0.0.1", strconv.Itoa(port), "0x00000001", zeroAddress)

	if !p.WaitLine("RPC_OK error_code=0", 15*time.Second) {
		t.Fatalf("rpc round-trip not confirmed\n%s", p.output())
	}
	if err := p.WaitExit(15 * time.Second); err != nil {
		t.Fatalf("scenario master failed: %v\n%s", err, p.output())
	}
}

// TestInteropPeerCreateOrDestroy drives two CREATE_CLUSTER_PEER_CONNECTION and a
// single DESTROY over the real wire, confirming both peers appear, and that
// destroying peer 1 leaves peer 2 untouched.
func TestInteropPeerCreateOrDestroy(t *testing.T) {
	slave, _, port := startTestSlave(t, "S0", []uint32{0x00000001}, []uint32{0x00000001})

	// Pre-activate the local branch so created virtual peers get a PeerConn.
	if err := slave.createShards(nil); err != nil {
		t.Fatalf("create shards: %v", err)
	}

	const peer1, peer2 = 1, 2
	p := startScenarioMaster(t, "peer", "127.0.0.1", strconv.Itoa(port),
		"0x00000001", strconv.Itoa(peer1), strconv.Itoa(peer2), "--hold", "3")

	if !p.WaitLine("PEER_CREATED 1", 15*time.Second) {
		t.Fatalf("peer 1 create not confirmed\n%s", p.output())
	}
	if !waitForPeer(t, slave, peer1, true, 10*time.Second) {
		t.Fatalf("peer 1 not present in slave registry\n%s", p.output())
	}

	if !p.WaitLine("PEER_CREATED 2", 15*time.Second) {
		t.Fatalf("peer 2 create not confirmed\n%s", p.output())
	}
	if !waitForPeer(t, slave, peer2, true, 10*time.Second) {
		t.Fatalf("peer 2 not present in slave registry\n%s", p.output())
	}

	if !p.WaitLine("PEER_DESTROYED", 15*time.Second) {
		t.Fatalf("peer destroy not confirmed\n%s", p.output())
	}
	if !waitForPeer(t, slave, peer1, false, 10*time.Second) {
		t.Fatalf("peer 1 not removed from slave registry")
	}
	if !waitForPeer(t, slave, peer2, true, 10*time.Second) {
		t.Fatalf("peer 2 was removed with peer 1: destroy(peer1) must not affect peer2")
	}

	if err := p.WaitExit(15 * time.Second); err != nil {
		t.Fatalf("scenario master failed: %v\n%s", err, p.output())
	}
}

// routingRecorder is the shared PeerHandler for TestInteropPeerMessageRouting. It
// signals over gotNewTxList when a NEW_TRANSACTION_LIST command is delivered.
// PeerHandler is shared across all of a slave's PeerConns and its methods carry no
// route identity, so routing-correctness for (cluster_peer_id, branch) is asserted
// by combining this delivery signal with the peer (cluster_peer_id, branch) registry.
type routingRecorder struct {
	mu           sync.Mutex
	calls        int
	gotNewTxList chan struct{}
}

func newRoutingRecorder() *routingRecorder {
	return &routingRecorder{gotNewTxList: make(chan struct{})}
}

func (r *routingRecorder) NewTransactionList(*wire.NewTransactionListCommand) error {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	select {
	case <-r.gotNewTxList:
	default:
		close(r.gotNewTxList)
	}
	return nil
}

func (r *routingRecorder) NewMinorBlockHeaderList(*wire.NewMinorBlockHeaderListCommand) error {
	return nil
}
func (r *routingRecorder) NewBlockMinor(*wire.NewBlockMinorCommand) error { return nil }

func (r *routingRecorder) GetMinorBlockHeaderList(*wire.GetMinorBlockHeaderListRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
	return &wire.GetMinorBlockHeaderListResponse{}, nil
}

func (r *routingRecorder) GetMinorBlockList(*wire.GetMinorBlockListRequest) (*wire.GetMinorBlockListResponse, error) {
	return &wire.GetMinorBlockListResponse{}, nil
}

func (r *routingRecorder) GetMinorBlockHeaderListWithSkip(*wire.GetMinorBlockHeaderListWithSkipRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
	return &wire.GetMinorBlockHeaderListResponse{}, nil
}

// TestInteropPeerMessageRouting drives a P2P NEW_TRANSACTION_LIST command from the
// Python master through the real frame path:
//
//	Python Master (ClusterMetadata{branch, cluster_peer_id})
//	  → Go MasterConn.routeFrame
//	  → PeerConn.HandleFrame
//	  → PeerHandler.NewTransactionList
//
// It asserts the unique target peer (cluster_peer_id, branch) is registered and
// that the command actually reaches the PeerHandler.
func TestInteropPeerMessageRouting(t *testing.T) {
	recorder := newRoutingRecorder()
	slave, _, port := startTestSlave(t, "S0", []uint32{0x00000001}, []uint32{0x00000001}, recorder)

	// Pre-activate the local branch so the created virtual peer gets a PeerConn.
	if err := slave.createShards(nil); err != nil {
		t.Fatalf("create shards: %v", err)
	}

	const clusterPeerID = 1
	const branch = 0x00000001
	p := startScenarioMaster(t, "peermsg", "127.0.0.1", strconv.Itoa(port),
		"0x00000001", strconv.Itoa(clusterPeerID), fmt.Sprintf("%d", branch))

	// The peer must be created and registered on the exact (cluster_peer_id, branch).
	if !waitForPeer(t, slave, clusterPeerID, true, 10*time.Second) {
		t.Fatalf("peer %d not created\n%s", clusterPeerID, p.output())
	}
	slave.peersMu.RLock()
	_, hasBranch := slave.peers[clusterPeerID][branch]
	slave.peersMu.RUnlock()
	if !hasBranch {
		t.Fatalf("peer %d has no PeerConn for branch 0x%x\n%s", clusterPeerID, branch, p.output())
	}

	// Python now sends a NEW_TRANSACTION_LIST addressed to (cluster_peer_id, branch).
	// It must be routed through the real PeerConn to the recording PeerHandler.
	select {
	case <-recorder.gotNewTxList:
		t.Log("NEW_TRANSACTION_LIST reached PeerHandler via routeFrame→PeerConn")
	case <-time.After(15 * time.Second):
		t.Fatalf("peer command did not reach PeerHandler\n%s", p.output())
	}

	if err := p.WaitExit(15 * time.Second); err != nil {
		t.Fatalf("scenario master failed: %v\n%s", err, p.output())
	}
}

// TestInteropMasterDisconnectShutdown confirms that losing the master
// connection (per pyquarkchain semantics) shuts the slave down.
func TestInteropMasterDisconnectShutdown(t *testing.T) {
	slave, _, port := startTestSlave(t, "S0", []uint32{0x00000001}, []uint32{0x00000001})

	p := startScenarioMaster(t, "disconnect", "127.0.0.1", strconv.Itoa(port), "0x00000001")

	if !p.WaitLine("CONNECTED", 15*time.Second) {
		t.Fatalf("master did not connect to slave\n%s", p.output())
	}
	if !p.WaitLine("DISCONNECT_SENT", 15*time.Second) {
		t.Fatalf("master did not close the connection\n%s", p.output())
	}

	select {
	case <-slave.WaitStopped():
		t.Log("slave shut down after master disconnect")
	case <-time.After(15 * time.Second):
		t.Fatalf("slave did not shut down after master disconnect\n%s", p.output())
	}

	if err := p.WaitExit(15 * time.Second); err != nil {
		t.Fatalf("scenario master failed: %v\n%s", err, p.output())
	}
}

// waitForPeer polls slave.peers until the given clusterPeerID exists (true) or
// no longer exists (false). Returns false on timeout.
func waitForPeer(t *testing.T, srv *SlaveComm, clusterPeerID uint64, expectExists bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		srv.peersMu.RLock()
		_, exists := srv.peers[clusterPeerID]
		srv.peersMu.RUnlock()
		if exists == expectExists {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// TestInteropAddMinorBlockHeaderToMaster exercises the only outbound protocol
// direction: Go Slave → Python Master. The slave serializes a real
// AddMinorBlockHeaderRequest, the Python harness decodes it with the real
// request serializer, replies with a real AddMinorBlockHeaderResponse, and the
// slave decodes it. This verifies Go wire serialization, Python wire
// deserialization, and the RPC request/response framing in the slave→master
// direction (py: SlaveServer.send_minor_block_header_to_master).
func TestInteropAddMinorBlockHeaderToMaster(t *testing.T) {
	slave, _, port := startTestSlave(t, "S0", []uint32{0x00000001}, []uint32{0x00000001})

	// The Python harness connects, blocks on PING→PONG, then prints READY: at
	// that point the slave's MasterConn is established and active.
	p := startScenarioMaster(t, "addminorblock", "127.0.0.1", strconv.Itoa(port), "0x00000001")
	if !p.WaitLine("READY", 15*time.Second) {
		t.Fatalf("python master did not complete handshake\n%s", p.output())
	}

	// Go encodes the request and awaits the Python-decoded response.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := slave.SendMinorBlockHeaderToMaster(ctx, newInteropMinorBlockHeaderRequest())
	if err != nil {
		t.Fatalf("SendMinorBlockHeaderToMaster: %v\n%s", err, p.output())
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("AddMinorBlockHeader error_code=%d, want 0\n%s", resp.ErrorCode, p.output())
	}

	if !p.WaitLine("ADD_MINOR_BLOCK_OK error_code=0", 15*time.Second) {
		t.Fatalf("add minor block round-trip not confirmed\n%s", p.output())
	}
	if err := p.WaitExit(15 * time.Second); err != nil {
		t.Fatalf("scenario master failed: %v\n%s", err, p.output())
	}
}

// newInteropMinorBlockHeaderRequest builds a minimal-but-legal
// AddMinorBlockHeaderRequest through the formal wire types. The field layout of
// MinorBlockHeader mirrors pyquarkchain core.MinorBlockHeader (core.py) so that
// the Python harness's real deserializer can consume the encoded bytes.
func newInteropMinorBlockHeaderRequest() *wire.AddMinorBlockHeaderRequest {
	return &wire.AddMinorBlockHeaderRequest{
		MinorBlockHeader:  newInteropMinorBlockHeader(),
		TxCount:           0,
		XShardTxCount:     0,
		CoinbaseAmountMap: qkcCommon.NewEmptyTokenBalances(),
		ShardStats:        wire.ShardStats{Branch: 0x00000001},
	}
}

func newInteropMinorBlockHeader() *types.MinorBlockHeader {
	return &types.MinorBlockHeader{
		Version:           0,
		Branch:            account.Branch{Value: 0x00000001},
		Number:            0,
		Coinbase:          account.Address{},
		CoinbaseAmount:    qkcCommon.NewEmptyTokenBalances(),
		ParentHash:        common.Hash{},
		PrevRootBlockHash: common.Hash{},
		GasLimit:          &serialize.Uint256{},
		MetaHash:          common.Hash{},
		Time:              0,
		Difficulty:        new(big.Int),
		Nonce:             0,
		Bloom:             types.Bloom{},
		Extra:             nil,
		MixDigest:         common.Hash{},
	}
}
