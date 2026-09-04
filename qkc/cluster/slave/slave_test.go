// Copyright 2026-2027, QuarkChain.

package slave

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// ── helpers ──────────────────────────────────────────────────────────────────

var (
	testSlaveID     = []byte("S0")
	testSlaveShards = []uint32{0x00010001, 0x00020001}
)

// commTestHandler is the test composition layer: it serves the business RPCs
// with the established fakeMasterHandler test double. The master's
// communication-topology commands (CONNECT_TO_SLAVES,
// CREATE/DESTROY_CLUSTER_PEER_CONNECTION) are served directly by the SlaveComm
// under test, not forwarded through this handler.
type commTestHandler struct {
	*fakeMasterHandler
	// reportedBranches is what the next CreateShards reports as created,
	// standing in for the branches the business runtime reports back.
	reportedBranches atomic.Pointer[[]uint32]
}

// reportCreatedBranches arms the branches the next CreateShards reports.
func (h *commTestHandler) reportCreatedBranches(branches ...uint32) {
	h.reportedBranches.Store(&branches)
}

// CreateShards delegates to the embedded double for counting/error injection
// and returns the armed branch report.
func (h *commTestHandler) CreateShards(rootTip *wire.RawBytes) ([]uint32, error) {
	if _, err := h.fakeMasterHandler.CreateShards(rootTip); err != nil {
		return nil, err
	}
	if b := h.reportedBranches.Load(); b != nil {
		return *b, nil
	}
	return nil, nil
}

// testRootTip returns an opaque RootTip payload. The communication layer never
// decodes it — the business handler owns the RootTip semantics — so tests only
// need a non-nil payload to trigger the PING orchestration.
func testRootTip() *wire.RawBytes {
	rb := wire.RawBytes{0x01, 0x02}
	return &rb
}

// startTestSlaveComm starts a SlaveComm on a free loopback port with both
// local shards already created (as if an earlier PING had reported them) and
// returns it plus its dial address.
func startTestSlaveComm(t *testing.T) (*SlaveComm, string) {
	t.Helper()
	return startTestSlaveCommWithBranches(t, testSlaveShards)
}

// startTestSlaveCommWithBranches starts a SlaveComm whose already-created
// branch set is preCreated, standing in for the branches an earlier
// CreateShards had reported. Passing nil starts with nothing created.
func startTestSlaveCommWithBranches(t *testing.T, preCreated []uint32) (*SlaveComm, string) {
	t.Helper()
	// Reserve a port, release it, then let Start bind it. Back-to-back runs
	// (go test -count) can briefly reuse a port whose accepted connection is
	// still closing asynchronously (runMasterConn is deliberately not joined
	// by Stop), so retry with a fresh reservation on EADDRINUSE.
	for attempt := 0; ; attempt++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		addr := ln.Addr().String()
		port := ln.Addr().(*net.TCPAddr).Port
		ln.Close()

		handler := &commTestHandler{fakeMasterHandler: &fakeMasterHandler{}}
		comm, err := NewSlaveComm(SlaveConfig{
			ID:                     append([]byte(nil), testSlaveID...),
			FullShardIDList:        append([]uint32(nil), testSlaveShards...),
			ClusterFullShardIDList: append([]uint32(nil), testSlaveShards...),
			Port:                   port,
			Logger:                 log.New(),
			Master:                 handler,
			Peer:                   stubPeerHandler{},
			Xshard:                 testXshardHandler{},
		})
		if err != nil {
			t.Fatalf("new slave comm: %v", err)
		}
		if err := comm.Start(); err != nil {
			if attempt < 5 && strings.Contains(err.Error(), "address already in use") {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			t.Fatalf("start slave comm: %v", err)
		}
		comm.peersMu.Lock()
		for _, branch := range preCreated {
			comm.localBranches[branch] = struct{}{}
		}
		comm.peersMu.Unlock()
		t.Cleanup(comm.Stop)
		return comm, addr
	}
}

// dialComm dials the comm's listener and returns the raw connection.
func dialComm(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial comm: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// sendFrame writes one frame to conn.
func sendFrame(t *testing.T, conn net.Conn, f *wire.Frame) {
	t.Helper()
	if err := wire.WriteFrame(conn, f); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// readFrame reads one frame from conn with a bounded wait.
func readFrame(t *testing.T, conn net.Conn) *wire.Frame {
	t.Helper()
	// Reading is bounded by the test's overall timeout via SetReadDeadline.
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	f, err := wire.ReadFrame(bufio.NewReader(conn), 0)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return f
}

// waitFor polls until cond returns true. Fails the test on timeout.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// sendCreatePeer sends CREATE_CLUSTER_PEER_CONNECTION and returns the
// response's error code.
func sendCreatePeer(t *testing.T, conn net.Conn, rpcID uint64, clusterPeerID uint64) uint32 {
	t.Helper()
	payload, err := serialize.SerializeToBytes(&wire.CreateClusterPeerConnectionRequest{ClusterPeerID: clusterPeerID})
	if err != nil {
		t.Fatalf("serialize create request: %v", err)
	}
	sendFrame(t, conn, &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpCreateClusterPeerConnectionRequest),
		RPCID:   rpcID,
		Payload: payload,
	})

	resp := readFrame(t, conn)
	if resp.Opcode != byte(wire.ClusterOpCreateClusterPeerConnectionResponse) || resp.RPCID != rpcID {
		t.Fatalf("unexpected create response: opcode=0x%x rpc_id=%d", resp.Opcode, resp.RPCID)
	}
	var out wire.CreateClusterPeerConnectionResponse
	if err := serialize.DeserializeFromBytes(resp.Payload, &out); err != nil {
		t.Fatalf("deserialize create response: %v", err)
	}
	return out.ErrorCode
}

// sendDestroyPeer sends DESTROY_CLUSTER_PEER_CONNECTION (fire-and-forget).
func sendDestroyPeer(t *testing.T, conn net.Conn, clusterPeerID uint64) {
	t.Helper()
	payload, err := serialize.SerializeToBytes(&wire.DestroyClusterPeerConnectionCommand{ClusterPeerID: clusterPeerID})
	if err != nil {
		t.Fatalf("serialize destroy command: %v", err)
	}
	sendFrame(t, conn, &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpDestroyClusterPeerConnectionCommand),
		RPCID:   0,
		Payload: payload,
	})
}

// sendPingRootTip sends a master PING carrying rootTip with the given rpcID
// (RPC ids must strictly increase per connection) and waits for the PONG,
// which is written only after the CreateShards orchestration has completed.
func sendPingRootTip(t *testing.T, conn net.Conn, rpcID uint64, rootTip *wire.RawBytes) {
	t.Helper()
	payload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              append([]byte(nil), testSlaveID...),
		FullShardIDList: append([]uint32(nil), testSlaveShards...),
		RootTip:         rootTip,
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}
	sendFrame(t, conn, &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   rpcID,
		Payload: payload,
	})
	resp := readFrame(t, conn)
	if resp.Opcode != byte(wire.ClusterOpPong) || resp.RPCID != rpcID {
		t.Fatalf("unexpected ping response: opcode=0x%x rpc_id=%d", resp.Opcode, resp.RPCID)
	}
}

// sendPingRootTipNoResponse sends a master PING carrying rootTip without
// reading a response. Used when the orchestration is expected to fail and the
// connection to close before any PONG is written.
func sendPingRootTipNoResponse(t *testing.T, conn net.Conn, rpcID uint64, rootTip *wire.RawBytes) {
	t.Helper()
	payload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              append([]byte(nil), testSlaveID...),
		FullShardIDList: append([]uint32(nil), testSlaveShards...),
		RootTip:         rootTip,
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}
	sendFrame(t, conn, &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   rpcID,
		Payload: payload,
	})
}

// numXshardConns returns the number of connections tracked by the pool. It is
// a white-box test helper: the pool intentionally exposes no connection
// counter (Python has no such public API).
func numXshardConns(p *XshardPool) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.connections)
}

// establishXshardInbound dials the comm's listener as a second connection and
// completes the xshard handshake as the remote outbound client, so the comm
// indexes it in its pool.
func establishXshardInbound(t *testing.T, comm *SlaveComm, addr string) {
	t.Helper()
	conn := dialComm(t, addr)
	client, err := newXshardConn(conn, 0, []byte("S1"), []uint32{0x00010001}, append([]byte(nil), testSlaveID...), append([]uint32(nil), testSlaveShards...), testXshardHandler{}, log.New())
	if err != nil {
		t.Fatalf("new client xshard conn: %v", err)
	}
	client.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, shards, err := client.sendPing(ctx)
	if err != nil {
		t.Fatalf("xshard handshake: %v", err)
	}
	if !bytes.Equal(id, testSlaveID) || !slicesEqualUint32(shards, testSlaveShards) {
		t.Fatalf("unexpected pong identity: id=%q shards=%v", id, shards)
	}
	waitFor(t, "xshard inbound indexing", func() bool {
		return comm.xshardPool.hasSlaveID([]byte("S1"))
	})
}

func slicesEqualUint32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// White-box introspection helpers into SlaveComm's peer registry (this is a
// same-package test; the production surface is LookupPeer/peerCount).

func (s *SlaveComm) lookupPeer(clusterPeerID uint64, branch uint32) *PeerConn {
	return s.LookupPeer(clusterPeerID, branch)
}

func (s *SlaveComm) peerCountFor(clusterPeerID uint64) int {
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	return len(s.peers[clusterPeerID])
}

func (s *SlaveComm) peerCount() int {
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	return len(s.peers)
}

func (s *SlaveComm) localBranchCount() int {
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	return len(s.localBranches)
}

// ── tests ────────────────────────────────────────────────────────────────────

// TestSlaveComm_FirstInboundIsMasterAndRestAreXshard verifies the accept-loop
// dispatch: the first inbound connection becomes the MasterConn, and a second
// inbound connection goes through the xshard handshake into the pool
// (py: __handle_new_connection).
func TestSlaveComm_FirstInboundIsMasterAndRestAreXshard(t *testing.T) {
	comm, addr := startTestSlaveComm(t)

	masterConn := dialComm(t, addr)
	_ = masterConn

	// A second connection is not claimed as master; it must complete the
	// xshard handshake and land in the pool.
	establishXshardInbound(t, comm, addr)
}

// TestSlaveComm_CreateAndDestroyPeerConn verifies CREATE creates a PeerConn on
// every local branch, a duplicate CREATE is a no-op, and DESTROY removes and
// closes them (py: slave.py:329-370, 321-327).
func TestSlaveComm_CreateAndDestroyPeerConn(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	masterConn := dialComm(t, addr)

	const cid = 7
	if code := sendCreatePeer(t, masterConn, 1, cid); code != 0 {
		t.Fatalf("create returned error_code=%d", code)
	}
	for _, branch := range testSlaveShards {
		if pc := comm.lookupPeer(cid, branch); pc == nil {
			t.Fatalf("no PeerConn for branch 0x%x after CREATE", branch)
		}
	}
	if got := comm.peerCountFor(cid); got != len(testSlaveShards) {
		t.Fatalf("registered %d PeerConns, want %d", got, len(testSlaveShards))
	}

	// Duplicate CREATE: existing branches are skipped, response stays success.
	if code := sendCreatePeer(t, masterConn, 2, cid); code != 0 {
		t.Fatalf("duplicate create returned error_code=%d", code)
	}
	if got := comm.peerCountFor(cid); got != len(testSlaveShards) {
		t.Fatalf("duplicate create changed registry: %d PeerConns", got)
	}

	// DESTROY removes and closes every PeerConn of the id.
	sendDestroyPeer(t, masterConn, cid)
	waitFor(t, "peer destruction", func() bool {
		return comm.peerCountFor(cid) == 0
	})
	if pc := comm.lookupPeer(cid, testSlaveShards[0]); pc != nil && !pc.IsClosed() {
		t.Fatal("destroyed PeerConn is not closed")
	}
}

// TestSlaveComm_PingDelegatesCreateShardsAndBackfills verifies the PING
// orchestration chain: MasterConn → SlaveComm.CreateShardsAndPeerConnections →
// business MasterHandler.CreateShards, followed by an idempotent peer-registry
// convergence (every known cluster peer × every local branch), and that
// multiple peers coexist without cross-talk.
func TestSlaveComm_PingDelegatesCreateShardsAndBackfills(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	masterConn := dialComm(t, addr)
	handler := comm.cfg.Master.(*commTestHandler)

	// PING with a RootTip before any peer exists: business CreateShards runs,
	// the registry stays empty.
	sendPingRootTip(t, masterConn, 1, testRootTip())
	if got := handler.createShardsCalls.Load(); got != 1 {
		t.Fatalf("business CreateShards calls: got %d, want 1", got)
	}
	if got := comm.peerCount(); got != 0 {
		t.Fatalf("registry holds %d peers before any CREATE", got)
	}

	// CREATE two peers: each gets one PeerConn per local branch.
	const cidA, cidB = 13, 14
	for i, cid := range []uint64{cidA, cidB} {
		if code := sendCreatePeer(t, masterConn, uint64(i+2), cid); code != 0 {
			t.Fatalf("create %d returned error_code=%d", cid, code)
		}
	}
	if got := comm.peerCount(); got != 2 {
		t.Fatalf("registered %d peers, want 2", got)
	}
	for _, cid := range []uint64{cidA, cidB} {
		if got := comm.peerCountFor(cid); got != len(testSlaveShards) {
			t.Fatalf("cluster_peer_id %d has %d PeerConns, want %d", cid, got, len(testSlaveShards))
		}
	}

	// A second PING converges the registry idempotently: no new conns, no
	// duplicate logs, and the business handler is invoked again.
	sendPingRootTip(t, masterConn, 4, testRootTip())
	if got := handler.createShardsCalls.Load(); got != 2 {
		t.Fatalf("business CreateShards calls: got %d, want 2", got)
	}
	if got := comm.peerCount(); got != 2 {
		t.Fatalf("second PING changed peer count to %d", got)
	}
	for _, cid := range []uint64{cidA, cidB} {
		if got := comm.peerCountFor(cid); got != len(testSlaveShards) {
			t.Fatalf("second PING changed cluster_peer_id %d to %d PeerConns", cid, got)
		}
	}
}

// TestSlaveComm_CreateShardsErrorClosesComm verifies that a business
// CreateShards failure propagates out of CreateShardsAndPeerConnections as a
// connection-level error: the PING handler fails, the master connection
// closes, and the shutdown cascade runs.
func TestSlaveComm_CreateShardsErrorClosesComm(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	handler := comm.cfg.Master.(*commTestHandler)
	handler.errCreateShards = errors.New("boom")

	masterConn := dialComm(t, addr)

	sendPingRootTipNoResponse(t, masterConn, 1, testRootTip())
	select {
	case <-comm.WaitStopped():
	case <-time.After(10 * time.Second):
		t.Fatal("CreateShards error did not trigger shutdown")
	}
}

// TestSlaveComm_PingAfterDestroyKeepsPeerGone verifies that a later PING
// (backfill) does not resurrect a destroyed cluster peer id: it has been
// removed from the known-peer set, so convergence has nothing to create.
func TestSlaveComm_PingAfterDestroyKeepsPeerGone(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	masterConn := dialComm(t, addr)
	handler := comm.cfg.Master.(*commTestHandler)

	const cid = 15
	if code := sendCreatePeer(t, masterConn, 1, cid); code != 0 {
		t.Fatalf("create returned error_code=%d", code)
	}
	pc := comm.lookupPeer(cid, testSlaveShards[0])

	sendDestroyPeer(t, masterConn, cid)
	waitFor(t, "peer destruction", func() bool {
		return comm.peerCountFor(cid) == 0
	})
	if !pc.IsClosed() {
		t.Fatal("PeerConn closed by DESTROY was not closed")
	}

	// The business runtime reports a fresh branch: there is no known peer
	// left, so nothing is resurrected.
	handler.reportCreatedBranches(testSlaveShards...)
	sendPingRootTip(t, masterConn, 2, testRootTip())
	if got := handler.createShardsCalls.Load(); got != 1 {
		t.Fatalf("business CreateShards calls: got %d, want 1", got)
	}
	if got := comm.peerCount(); got != 0 {
		t.Fatalf("backfill after DESTROY resurrected %d peer entries", got)
	}
}

// TestSlaveComm_CreatePeerOnlyOnCreatedBranches verifies the shard/peer model:
// a cluster peer announced while nothing is created gets no PeerConn (a branch
// present in FullShardIDList but not created must not be connected), and each
// branch a later CreateShards reports backfills exactly that branch for the
// already-known peer (py: Shard.create_peer_shard_connections per new shard).
func TestSlaveComm_CreatePeerOnlyOnCreatedBranches(t *testing.T) {
	comm, addr := startTestSlaveCommWithBranches(t, nil)
	handler := comm.cfg.Master.(*commTestHandler)
	masterConn := dialComm(t, addr)

	// CREATE before any branch exists: the peer is registered, nothing is
	// connected even though both branches are in FullShardIDList.
	const cid = 21
	if code := sendCreatePeer(t, masterConn, 1, cid); code != 0 {
		t.Fatalf("create returned error_code=%d", code)
	}
	if got := comm.peerCountFor(cid); got != 0 {
		t.Fatalf("cluster_peer_id %d has %d PeerConns before any branch exists, want 0", cid, got)
	}

	// The business runtime reports b1 only: b2 stays unconnected.
	handler.reportCreatedBranches(testSlaveShards[0])
	sendPingRootTip(t, masterConn, 2, testRootTip())
	if pc := comm.lookupPeer(cid, testSlaveShards[0]); pc == nil {
		t.Fatal("no PeerConn on created branch b1")
	}
	if pc := comm.lookupPeer(cid, testSlaveShards[1]); pc != nil {
		t.Fatal("PeerConn created on branch b2 before the branch existed")
	}
	if got := comm.peerCountFor(cid); got != 1 {
		t.Fatalf("cluster_peer_id %d has %d PeerConns, want 1", cid, got)
	}

	// The root chain advances: the business runtime creates b2 and reports it,
	// which backfills the pre-existing peer.
	handler.reportCreatedBranches(testSlaveShards[1])
	sendPingRootTip(t, masterConn, 3, testRootTip())
	if pc := comm.lookupPeer(cid, testSlaveShards[1]); pc == nil {
		t.Fatal("pre-existing peer was not backfilled after branch b2 was created")
	}
	if got := comm.peerCountFor(cid); got != len(testSlaveShards) {
		t.Fatalf("cluster_peer_id %d has %d PeerConns after backfill, want %d", cid, got, len(testSlaveShards))
	}

	// A second peer announced afterwards lands on both created branches.
	const cid2 = 22
	if code := sendCreatePeer(t, masterConn, 4, cid2); code != 0 {
		t.Fatalf("create returned error_code=%d", code)
	}
	if got := comm.peerCountFor(cid2); got != len(testSlaveShards) {
		t.Fatalf("cluster_peer_id %d has %d PeerConns, want %d", cid2, got, len(testSlaveShards))
	}
	if got := comm.peerCount(); got != 2 {
		t.Fatalf("registry holds %d peers, want 2", got)
	}
}

// TestSlaveComm_PingWithoutCreatedBranchesLeavesTopology covers the "root
// height below every genesis height" case: CreateShards reports no branch, so
// neither the created-branch set nor the peer registry changes.
func TestSlaveComm_PingWithoutCreatedBranchesLeavesTopology(t *testing.T) {
	comm, addr := startTestSlaveCommWithBranches(t, nil)
	handler := comm.cfg.Master.(*commTestHandler)
	masterConn := dialComm(t, addr)

	const cid = 31
	if code := sendCreatePeer(t, masterConn, 1, cid); code != 0 {
		t.Fatalf("create returned error_code=%d", code)
	}

	handler.reportCreatedBranches()
	sendPingRootTip(t, masterConn, 2, testRootTip())

	if got := handler.createShardsCalls.Load(); got != 1 {
		t.Fatalf("business CreateShards calls: got %d, want 1", got)
	}
	if got := comm.localBranchCount(); got != 0 {
		t.Fatalf("created-branch set holds %d branches, want 0", got)
	}
	if got := comm.peerCountFor(cid); got != 0 {
		t.Fatalf("cluster_peer_id %d has %d PeerConns, want 0", cid, got)
	}
}

// TestSlaveComm_PingCreatesEveryReportedBranch covers the "root height past
// every genesis height" case: one CreateShards reporting several branches
// equips the announced peer on all of them, and repeating the same report
// changes nothing.
func TestSlaveComm_PingCreatesEveryReportedBranch(t *testing.T) {
	comm, addr := startTestSlaveCommWithBranches(t, nil)
	handler := comm.cfg.Master.(*commTestHandler)
	masterConn := dialComm(t, addr)

	const cid = 32
	if code := sendCreatePeer(t, masterConn, 1, cid); code != 0 {
		t.Fatalf("create returned error_code=%d", code)
	}

	handler.reportCreatedBranches(testSlaveShards...)
	sendPingRootTip(t, masterConn, 2, testRootTip())

	if got := comm.localBranchCount(); got != len(testSlaveShards) {
		t.Fatalf("created-branch set holds %d branches, want %d", got, len(testSlaveShards))
	}
	for _, branch := range testSlaveShards {
		if pc := comm.lookupPeer(cid, branch); pc == nil {
			t.Fatalf("no PeerConn for branch 0x%x after it was created", branch)
		}
	}
	first := comm.lookupPeer(cid, testSlaveShards[0])

	// A repeated report is a no-op: the branch is already created, so the
	// existing PeerConn is kept rather than rebuilt.
	sendPingRootTip(t, masterConn, 3, testRootTip())
	if got := comm.peerCountFor(cid); got != len(testSlaveShards) {
		t.Fatalf("repeated report changed cluster_peer_id %d to %d PeerConns", cid, got)
	}
	if comm.lookupPeer(cid, testSlaveShards[0]) != first {
		t.Fatal("repeated report replaced an existing PeerConn")
	}
}

// TestSlaveComm_RepeatDestroyIsNoop verifies a repeated DESTROY for the same
// cluster peer id leaves the registry consistent and does not double-close.
func TestSlaveComm_RepeatDestroyIsNoop(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	masterConn := dialComm(t, addr)

	const cid = 16
	if code := sendCreatePeer(t, masterConn, 1, cid); code != 0 {
		t.Fatalf("create returned error_code=%d", code)
	}
	pc := comm.lookupPeer(cid, testSlaveShards[0])

	sendDestroyPeer(t, masterConn, cid)
	sendDestroyPeer(t, masterConn, cid)
	waitFor(t, "peer destruction", func() bool {
		return comm.peerCount() == 0
	})
	if !pc.IsClosed() {
		t.Fatal("PeerConn is not closed after DESTROY")
	}
}

// TestSlaveComm_DestroyUnknownPeerIsNoop verifies DESTROY for a cluster peer
// id that was never created is a no-op and leaves the comm usable.
func TestSlaveComm_DestroyUnknownPeerIsNoop(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	masterConn := dialComm(t, addr)

	sendDestroyPeer(t, masterConn, 99)
	if got := comm.peerCount(); got != 0 {
		t.Fatalf("registry holds %d entries after destroying an unknown id", got)
	}

	const cid = 17
	if code := sendCreatePeer(t, masterConn, 1, cid); code != 0 {
		t.Fatalf("create returned error_code=%d", code)
	}
	if got := comm.peerCountFor(cid); got != len(testSlaveShards) {
		t.Fatalf("registered %d PeerConns, want %d", got, len(testSlaveShards))
	}
}

// TestSlaveComm_ConnectToSlaves verifies the comm dials the advertised slaves
// into its xshard pool, skips itself, and reports per-entry failures.
func TestSlaveComm_ConnectToSlaves(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	masterConn := dialComm(t, addr)

	rs := startRemoteSlave(t, []byte("S1"), []uint32{0x00010001})
	defer rs.close()

	req := &wire.ConnectToSlavesRequest{
		SlaveInfoList: []wire.SlaveInfo{
			// Self: must be skipped without dialing.
			{ID: append([]byte(nil), testSlaveID...), Host: []byte("127.0.0.1"), Port: 1, FullShardIDList: append([]uint32(nil), testSlaveShards...)},
			rs.slaveInfo([]byte("S1"), []uint32{0x00010001}),
		},
	}
	payload, err := serialize.SerializeToBytes(req)
	if err != nil {
		t.Fatalf("serialize connect request: %v", err)
	}
	sendFrame(t, masterConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpConnectToSlavesRequest),
		RPCID:   1,
		Payload: payload,
	})

	resp := readFrame(t, masterConn)
	if resp.Opcode != byte(wire.ClusterOpConnectToSlavesResponse) {
		t.Fatalf("unexpected response opcode 0x%x", resp.Opcode)
	}
	var out wire.ConnectToSlavesResponse
	if err := serialize.DeserializeFromBytes(resp.Payload, &out); err != nil {
		t.Fatalf("deserialize connect response: %v", err)
	}
	if len(out.ResultList) != 2 {
		t.Fatalf("result list has %d entries, want 2", len(out.ResultList))
	}
	if len(out.ResultList[0]) != 0 {
		t.Fatalf("self entry reported failure: %q", out.ResultList[0])
	}
	if len(out.ResultList[1]) != 0 {
		t.Fatalf("remote entry reported failure: %q", out.ResultList[1])
	}
	waitFor(t, "xshard pool registration of S1", func() bool {
		return comm.xshardPool.hasSlaveID([]byte("S1"))
	})
}

// TestSlaveComm_MasterCloseCascade verifies the Python close cascade: losing
// the master closes all PeerConns, the xshard pool, and the listener.
func TestSlaveComm_MasterCloseCascade(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	masterConn := dialComm(t, addr)

	establishXshardInbound(t, comm, addr)

	const cid = 9
	if code := sendCreatePeer(t, masterConn, 1, cid); code != 0 {
		t.Fatalf("create returned error_code=%d", code)
	}

	// The remote master side drops the TCP connection.
	masterConn.Close()

	// The master-loss shutdown cascade: runMasterConn notices the close and
	// calls Stop. WaitStopped resolves once Stop has issued every close, and
	// since those closes clear the peer registry and pool synchronously, the
	// drained state below is deterministic.
	select {
	case <-comm.WaitStopped():
	case <-time.After(10 * time.Second):
		t.Fatal("master close did not trigger shutdown")
	}

	if pc := comm.lookupPeer(cid, testSlaveShards[0]); pc != nil {
		t.Fatal("PeerConn survived the master close cascade")
	}
	if got := comm.peerCount(); got != 0 {
		t.Fatalf("peer registry still holds %d entries", got)
	}
	if n := numXshardConns(comm.xshardPool); n != 0 {
		t.Fatalf("xshard pool still holds %d connections", n)
	}
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		c.Close()
		t.Fatal("listener still accepts connections after shutdown")
	}
}

// TestSlaveComm_StopIdempotent verifies Stop is safe to call repeatedly.
func TestSlaveComm_StopIdempotent(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	dialComm(t, addr)

	done := make(chan struct{})
	go func() {
		comm.Stop()
		comm.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("repeated Stop did not return (deadlock?)")
	}
}

// TestSlaveComm_ConcurrentShutdown verifies a master-initiated stop and an
// explicit Stop racing each other terminate cleanly without deadlock.
func TestSlaveComm_ConcurrentShutdown(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	masterConn := dialComm(t, addr)

	// Close the master TCP side while Stop runs concurrently.
	go masterConn.Close()
	done := make(chan struct{})
	go func() {
		comm.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent shutdown deadlocked")
	}
	// Stop's body runs synchronously, so shutdown is fully initiated once it
	// returns; both racing paths converge on the same once-only sequence.
	select {
	case <-comm.WaitStopped():
	default:
		t.Fatal("WaitStopped not closed after Stop returned")
	}
}

// TestSlaveComm_LookupPeerUnknown verifies that LookupPeer returns nil for an
// unknown cluster peer id; peer lookup for a created peer is covered by
// TestSlaveComm_CreateAndDestroyPeerConn.
func TestSlaveComm_LookupPeerUnknown(t *testing.T) {
	comm, _ := startTestSlaveComm(t)

	if pc := comm.LookupPeer(11, testSlaveShards[0]); pc != nil {
		t.Fatal("LookupPeer returned a peer for an unknown id")
	}
}

// TestSlaveComm_SendToMasterNotReady verifies the nil gate: a master-dependent
// send before the master connection is established returns ErrNotActive
// without panicking (py: the equivalent call crashes with AttributeError;
// ErrNotActive is the diagnostic equivalent).
func TestSlaveComm_SendToMasterNotReady(t *testing.T) {
	comm, _ := startTestSlaveComm(t)

	if _, err := comm.SendMinorBlockHeaderToMaster(context.Background(), &wire.AddMinorBlockHeaderRequest{}); !errors.Is(err, conn.ErrNotActive) {
		t.Fatalf("SendMinorBlockHeaderToMaster before establishment: err=%v, want ErrNotActive", err)
	}
	if _, err := comm.SendMinorBlockHeaderListToMaster(context.Background(), &wire.AddMinorBlockHeaderListRequest{}); !errors.Is(err, conn.ErrNotActive) {
		t.Fatalf("SendMinorBlockHeaderListToMaster before establishment: err=%v, want ErrNotActive", err)
	}
}

// TestSlaveComm_SendToMasterEstablishedButClosed verifies that once the master
// connection has been published and then closed, a send reaches the delegate
// (loading a non-nil pointer) and surfaces the delegate's closed error rather
// than the nil-gate ErrNotActive.
func TestSlaveComm_SendToMasterEstablishedButClosed(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	masterConn := dialComm(t, addr)
	defer masterConn.Close()

	// Establishing the master publishes the pointer.
	sendPingRootTip(t, masterConn, 1, testRootTip())
	if comm.master.Load() == nil {
		t.Fatal("master not published after establishment")
	}

	// Closing the socket makes the slave-side MasterConn reach Closed.
	masterConn.Close()
	waitFor(t, "master connection closed", func() bool {
		mc := comm.master.Load()
		return mc != nil && mc.IsClosed()
	})

	if _, err := comm.SendMinorBlockHeaderToMaster(context.Background(), &wire.AddMinorBlockHeaderRequest{}); !errors.Is(err, conn.ErrConnectionClosed) {
		t.Fatalf("send with closed delegate: err=%v, want ErrConnectionClosed", err)
	}
}

// TestSlaveComm_MasterAtomicPublication verifies that many goroutines loading
// the published master pointer all observe the same fully-initialized
// MasterConn. The publication word is atomic: a Store by runMasterConn and the
// concurrent Loads below are race-free under go test -race.
func TestSlaveComm_MasterAtomicPublication(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	masterConn := dialComm(t, addr)
	defer masterConn.Close()

	sendPingRootTip(t, masterConn, 1, testRootTip())
	want := comm.master.Load()
	if want == nil {
		t.Fatal("master not published after establishment")
	}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := comm.master.Load(); got != want {
				t.Errorf("master.Load() = %v, want %v", got, want)
			}
		}()
	}
	wg.Wait()
}

// TestSlaveComm_DrainPeersOnStop verifies Stop drains every registered PeerConn
// (py: MasterConnection.close → close all peer forwarding connections): after a
// peer connection was created, Stop leaves the registry empty.
func TestSlaveComm_DrainPeersOnStop(t *testing.T) {
	comm, addr := startTestSlaveComm(t)
	masterConn := dialComm(t, addr)
	defer masterConn.Close()

	// Establish the master and create a peer so Stop has something to drain.
	sendPingRootTip(t, masterConn, 1, testRootTip())
	if code := sendCreatePeer(t, masterConn, 2, 1); code != 0 {
		t.Fatalf("create returned error_code=%d", code)
	}

	comm.Stop()

	if n := comm.peerCount(); n != 0 {
		t.Fatalf("peerCount after Stop = %d, want 0", n)
	}
}
