// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// fakePeerRuntime is a PeerRuntime test double owning a peer registry built via
// NewPeerConn. masterConn is late-bound after NewMasterConn returns.
type fakePeerRuntime struct {
	mu         sync.Mutex
	peers      map[uint64]map[uint32]*PeerConn
	masterConn *MasterConn
	handler    PeerHandler
	branches   []uint32 // default expansion set for the interface CreatePeerConns
}

func newFakePeerRuntime(mc *MasterConn, handler PeerHandler, branches []uint32) *fakePeerRuntime {
	return &fakePeerRuntime{
		peers:      make(map[uint64]map[uint32]*PeerConn),
		masterConn: mc,
		handler:    handler,
		branches:   branches,
	}
}

func (f *fakePeerRuntime) CreatePeerConns(clusterPeerID uint64) {
	f.createPeerConns(clusterPeerID, f.branches)
}

// createPeerConns is a test helper (not an interface method): creates PeerConns
// for explicit branches. Empty branches registers nothing (Python: empty
// self.shards.values() -> CREATE is a no-op).
func (f *fakePeerRuntime) createPeerConns(clusterPeerID uint64, branches []uint32) {
	if clusterPeerID == ReservedClusterPeerID {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	bm, ok := f.peers[clusterPeerID]
	if !ok {
		bm = make(map[uint32]*PeerConn)
	}
	for _, branch := range branches {
		if _, exists := bm[branch]; exists {
			continue
		}
		pc := NewPeerConn(clusterPeerID, branch, f.masterConn, f.handler, f.masterConn.Logger())
		pc.Start()
		bm[branch] = pc
	}
	if len(bm) > 0 {
		f.peers[clusterPeerID] = bm
	}
}

func (f *fakePeerRuntime) DestroyPeerConns(clusterPeerID uint64) {
	f.mu.Lock()
	bm, ok := f.peers[clusterPeerID]
	if ok {
		delete(f.peers, clusterPeerID)
	}
	f.mu.Unlock()
	if !ok {
		return
	}
	for _, pc := range bm {
		pc.Close()
	}
}

func (f *fakePeerRuntime) LookupPeer(clusterPeerID uint64, branch uint32) *PeerConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	bm, ok := f.peers[clusterPeerID]
	if !ok {
		return nil
	}
	return bm[branch]
}

func (f *fakePeerRuntime) CloseAllPeers() {
	f.mu.Lock()
	var all []*PeerConn
	for _, bm := range f.peers {
		for _, pc := range bm {
			all = append(all, pc)
		}
	}
	f.peers = make(map[uint64]map[uint32]*PeerConn)
	f.mu.Unlock()
	for _, pc := range all {
		pc.Close()
	}
}

func (f *fakePeerRuntime) peerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.peers)
}

// registerPeer inserts an already-constructed PeerConn into the fake registry.
func (f *fakePeerRuntime) registerPeer(pc *PeerConn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	bm, ok := f.peers[pc.ClusterPeerID()]
	if !ok {
		bm = make(map[uint32]*PeerConn)
		f.peers[pc.ClusterPeerID()] = bm
	}
	bm[pc.Branch()] = pc
}

// newMasterConn creates a MasterConn over a local TCP pair with a fake
// PeerRuntime injected (reachable via client.peerRuntime.(*fakePeerRuntime)).
func newMasterConn(t *testing.T) (client *MasterConn, serverConn net.Conn, cleanup func()) {
	t.Helper()
	return newMasterConnWithBranches(t, []uint32{0x00010001, 0x00020001})
}

// newMasterConnWithBranches is newMasterConn with an explicit default shard set
// for the fake runtime; empty branches models a runtime with no shards yet.
func newMasterConnWithBranches(t *testing.T, branches []uint32) (client *MasterConn, serverConn net.Conn, cleanup func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var srvConn net.Conn
	var acceptErr error
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		srvConn, acceptErr = ln.Accept()
		ln.Close()
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	<-accepted
	if acceptErr != nil {
		t.Fatalf("accept: %v", acceptErr)
	}

	logger := log.New()
	fake := newFakePeerRuntime(nil, nil, branches)
	client, err = NewMasterConn(MasterConnConfig{
		Conn:                 clientConn,
		LocalID:              []byte("go-slave"),
		LocalFullShardIDList: []uint32{0x00010001, 0x00020001},
		Handler:              &fakeMasterHandler{},
		PeerRuntime:          fake,
		Logger:               logger,
	})
	if err != nil {
		t.Fatalf("new master conn: %v", err)
	}
	fake.masterConn = client // late-bind: PeerConns constructed by the fake need the transport
	client.Start()
	serverConn = srvConn

	cleanup = func() {
		client.Close()
		if serverConn != nil {
			serverConn.Close()
		}
	}
	return
}

// waitForCondition polls cond until it returns true or the timeout elapses.
func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// readMasterFrame reads a 12-byte metadata frame from the fake master side.
func readMasterFrame(t *testing.T, conn net.Conn) *wire.Frame {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	frame, err := wire.ReadFrame(conn, 0)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return frame
}

// writeMasterFrame writes a 12-byte metadata frame from the fake master side.
func writeMasterFrame(t *testing.T, conn net.Conn, frame *wire.Frame) {
	t.Helper()
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set write deadline: %v", err)
	}
	if err := wire.WriteFrame(conn, frame); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// TestMasterConn_RouteToMaster verifies that frames with cluster_peer_id == 0
// are handled by MasterConn itself (PING -> PONG).
func TestMasterConn_RouteToMaster(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}

	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	})

	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG opcode 0x%x, got 0x%x", wire.ClusterOpPong, resp.Opcode)
	}
	if resp.RPCID != 1 {
		t.Fatalf("expected rpc_id 1, got %d", resp.RPCID)
	}
	if resp.Meta.ClusterPeerID != 0 {
		t.Fatalf("expected cluster_peer_id 0 for master-local response, got %d", resp.Meta.ClusterPeerID)
	}

	var pong wire.PongResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &pong); err != nil {
		t.Fatalf("deserialize pong: %v", err)
	}
	if string(pong.ID) != "go-slave" {
		t.Fatalf("pong id mismatch: got %q", pong.ID)
	}

	_ = client
}

// TestMasterConn_RouteToPeerConn verifies that frames with cluster_peer_id != 0
// are forwarded to the matching virtual PeerConn. Since all PeerConn handlers
// are unimplemented stubs, the PeerConn closes after the handler returns
// ErrHandlerNotImplemented; MasterConn must survive.
func TestMasterConn_RouteToPeerConn(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 7
	const branch uint32 = 0x00010001

	fake := client.peerRuntime.(*fakePeerRuntime)
	fake.createPeerConns(clusterPeerID, []uint32{branch})
	pc := fake.peers[clusterPeerID][branch]

	reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockListRequest{
		MinorBlockHashList: [][wire.HashLength]byte{},
	})
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}

	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: branch, ClusterPeerID: clusterPeerID},
		Opcode:  byte(wire.CommandOpGetMinorBlockListRequest),
		RPCID:   3,
		Payload: reqPayload,
	})

	// The handler returns ErrHandlerNotImplemented, which triggers
	// shutdown on the PeerConn.
	select {
	case <-pc.WaitUntilClosed():
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("PeerConn did not close after handler error")
	}

	// MasterConn must still be alive for master-local traffic.
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	})

	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG after PeerConn handler error, got opcode 0x%x", resp.Opcode)
	}
	if resp.RPCID != 1 {
		t.Fatalf("expected rpc_id 1, got %d", resp.RPCID)
	}
}

// TestMasterConn_UnknownPeerDropped verifies that frames for an unregistered
// cluster_peer_id are dropped and do not close MasterConn.
func TestMasterConn_UnknownPeerDropped(t *testing.T) {
	_, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockListRequest{
		MinorBlockHashList: [][wire.HashLength]byte{},
	})
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}

	// Unknown peer frame should be consumed and dropped.
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001, ClusterPeerID: 999},
		Opcode:  byte(wire.CommandOpGetMinorBlockListRequest),
		RPCID:   1,
		Payload: reqPayload,
	})

	// The fake master should see no response for the dropped frame, but a
	// subsequent master-local PING must still work.
	if err := serverConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := wire.ReadFrame(serverConn, 0); err == nil {
		t.Fatal("expected no response for unknown peer frame")
	}

	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   2,
		Payload: pingPayload,
	})

	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG after unknown peer drop, got opcode 0x%x", resp.Opcode)
	}
	if resp.RPCID != 2 {
		t.Fatalf("expected rpc_id 2 in pong, got %d", resp.RPCID)
	}
}

// TestMasterConn_CreateWithEmptyShardSet verifies that "no shards yet" lives
// inside the runtime (empty shard set), not in MasterConn: CREATE returns
// error_code=0 with no PeerConns, and peer frames are dropped
// (Python: slave.py:329-370, NULL_CONNECTION at slave.py:131-146).
func TestMasterConn_CreateWithEmptyShardSet(t *testing.T) {
	client, serverConn, cleanup := newMasterConnWithBranches(t, nil)
	defer cleanup()

	const clusterPeerID uint64 = 31

	createPayload, err := serialize.SerializeToBytes(&wire.CreateClusterPeerConnectionRequest{ClusterPeerID: clusterPeerID})
	if err != nil {
		t.Fatalf("serialize create request: %v", err)
	}
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpCreateClusterPeerConnectionRequest),
		RPCID:   1,
		Payload: createPayload,
	})

	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.ClusterOpCreateClusterPeerConnectionResponse) {
		t.Fatalf("expected create response opcode 0x%x, got 0x%x", wire.ClusterOpCreateClusterPeerConnectionResponse, resp.Opcode)
	}
	var createResp wire.CreateClusterPeerConnectionResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &createResp); err != nil {
		t.Fatalf("deserialize create response: %v", err)
	}
	if createResp.ErrorCode != 0 {
		t.Fatalf("expected error_code 0, got %d", createResp.ErrorCode)
	}

	// Empty shard set in the runtime: no PeerConns were created.
	fake := client.peerRuntime.(*fakePeerRuntime)
	if len(fake.peers) != 0 {
		t.Fatalf("expected no peer conns with empty shard set, got %d", len(fake.peers))
	}

	// A peer frame must be dropped (no response) and must not close MasterConn.
	reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockListRequest{
		MinorBlockHashList: [][wire.HashLength]byte{},
	})
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001, ClusterPeerID: clusterPeerID},
		Opcode:  byte(wire.CommandOpGetMinorBlockListRequest),
		RPCID:   1,
		Payload: reqPayload,
	})
	if err := serverConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := wire.ReadFrame(serverConn, 0); err == nil {
		t.Fatal("expected no response for dropped peer frame")
	}

	// MasterConn must still be alive. (RPC id 2: CREATE already used id 1 on
	// this connection, and the master-local sequence is monotonic.)
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   2,
		Payload: pingPayload,
	})
	pingResp := readMasterFrame(t, serverConn)
	if pingResp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG after empty-shard CREATE, got opcode 0x%x", pingResp.Opcode)
	}
}

// TestPeerConn_RPCIDIsolation verifies that two PeerConns sharing a MasterConn
// can use the same RPC ID without collision. MasterConn's RPC ID validation
// only applies to cluster_peer_id=0 traffic; peer traffic is forwarded before
// validation. Each PeerConn has its own BaseConn and thus its own independent
// RPC ID sequence.
//
// Since all PeerConn handlers are unimplemented, both PeerConns close after
// the handler returns ErrHandlerNotImplemented; MasterConn must survive.
func TestPeerConn_RPCIDIsolation(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	fake := client.peerRuntime.(*fakePeerRuntime)
	fake.createPeerConns(7, []uint32{0x00010001})
	fake.createPeerConns(9, []uint32{0x00020001})

	pc7 := fake.peers[7][0x00010001]
	pc9 := fake.peers[9][0x00020001]

	reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockListRequest{
		MinorBlockHashList: [][wire.HashLength]byte{},
	})
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}

	// Both peers use rpc_id=5. Each PeerConn has its own RPC ID counter, so
	// the same value must be accepted by both.
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001, ClusterPeerID: 7},
		Opcode:  byte(wire.CommandOpGetMinorBlockListRequest),
		RPCID:   5,
		Payload: reqPayload,
	})
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00020001, ClusterPeerID: 9},
		Opcode:  byte(wire.CommandOpGetMinorBlockListRequest),
		RPCID:   5,
		Payload: reqPayload,
	})

	// Both PeerConns must close due to the handler returning
	// ErrHandlerNotImplemented (not due to RPC ID validation failure).
	select {
	case <-pc7.WaitUntilClosed():
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("peer 7 did not close after handler error")
	}
	select {
	case <-pc9.WaitUntilClosed():
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("peer 9 did not close after handler error")
	}

	// MasterConn must still be alive.
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	})

	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG after PeerConn handler errors, got opcode 0x%x", resp.Opcode)
	}
	if resp.RPCID != 1 {
		t.Fatalf("expected rpc_id 1, got %d", resp.RPCID)
	}
}

// TestMasterConn_CreateDestroyPeerConnection verifies that the master commands
// create and destroy virtual peer connections through the MasterConn registry.
func TestMasterConn_CreateDestroyPeerConnection(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 21

	createPayload, err := serialize.SerializeToBytes(&wire.CreateClusterPeerConnectionRequest{ClusterPeerID: clusterPeerID})
	if err != nil {
		t.Fatalf("serialize create request: %v", err)
	}

	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpCreateClusterPeerConnectionRequest),
		RPCID:   1,
		Payload: createPayload,
	})

	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.ClusterOpCreateClusterPeerConnectionResponse) {
		t.Fatalf("expected create response opcode 0x%x, got 0x%x", wire.ClusterOpCreateClusterPeerConnectionResponse, resp.Opcode)
	}
	var createResp wire.CreateClusterPeerConnectionResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &createResp); err != nil {
		t.Fatalf("deserialize create response: %v", err)
	}
	if createResp.ErrorCode != 0 {
		t.Fatalf("expected error_code 0, got %d", createResp.ErrorCode)
	}

	// Capture PeerConn pointers before destroy; the expansion scope is decided
	// by the runtime (fake), not by MasterConn.
	fake := client.peerRuntime.(*fakePeerRuntime)
	branchMap := fake.peers[clusterPeerID]
	if len(branchMap) != len(fake.branches) {
		t.Fatalf("expected %d peer conns, got %d", len(fake.branches), len(branchMap))
	}
	peerConns := make([]*PeerConn, 0, len(fake.branches))
	for _, branch := range fake.branches {
		pc := branchMap[branch]
		if pc == nil {
			t.Fatalf("missing peer conn for branch 0x%x", branch)
		}
		if pc.IsClosed() {
			t.Fatalf("peer conn for branch 0x%x is already closed", branch)
		}
		peerConns = append(peerConns, pc)
	}

	// Destroy the peer connections.
	destroyPayload, err := serialize.SerializeToBytes(&wire.DestroyClusterPeerConnectionCommand{ClusterPeerID: clusterPeerID})
	if err != nil {
		t.Fatalf("serialize destroy command: %v", err)
	}
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpDestroyClusterPeerConnectionCommand),
		RPCID:   0, // non-RPC
		Payload: destroyPayload,
	})

	// Wait for peer connections to be closed (async since handler runs in goroutine).
	waitForCondition(t, 2*time.Second, func() bool {
		for _, pc := range peerConns {
			if !pc.IsClosed() {
				return false
			}
		}
		return true
	})

	// MasterConn must still be alive for a follow-up PING.
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   2,
		Payload: pingPayload,
	})

	pingResp := readMasterFrame(t, serverConn)
	if pingResp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG after destroy, got opcode 0x%x", pingResp.Opcode)
	}
}

// TestMasterConn_CloseClosesPeerConns verifies that closing MasterConn closes
// all associated PeerConns and clears the peer registry.
func TestMasterConn_CloseClosesPeerConns(t *testing.T) {
	client, _, cleanup := newMasterConn(t)
	defer cleanup()

	fake := client.peerRuntime.(*fakePeerRuntime)
	fake.createPeerConns(7, []uint32{0x00010001, 0x00020001})
	fake.createPeerConns(9, []uint32{0x00010001})

	// Keep references before Close clears the map.
	var peerConns []*PeerConn
	for _, branchMap := range fake.peers {
		for _, pc := range branchMap {
			peerConns = append(peerConns, pc)
		}
	}
	if len(peerConns) != 3 {
		t.Fatalf("expected 3 peer conns, got %d", len(peerConns))
	}

	client.Close()

	for _, pc := range peerConns {
		if !pc.IsClosed() {
			t.Fatalf("peer conn %d/%d was not closed by MasterConn.Close", pc.ClusterPeerID(), pc.Branch())
		}
	}

	if got := fake.peerCount(); got != 0 {
		t.Fatalf("peer registry not cleared: got %d cluster_peer_id entries", got)
	}
}

// TestPeerConn_OutboundRPCThroughMasterConn verifies that a PeerConn can issue
// an outbound RPC and the request is written to the underlying MasterConn with
// the correct cluster_peer_id metadata.
func TestPeerConn_OutboundRPCThroughMasterConn(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 31
	const branch uint32 = 0x00010001

	fake := client.peerRuntime.(*fakePeerRuntime)
	fake.createPeerConns(clusterPeerID, []uint32{branch})
	pc := fake.peers[clusterPeerID][branch]

	req := &wire.GetMinorBlockListRequest{MinorBlockHashList: [][wire.HashLength]byte{}}
	reqPayload, err := serialize.SerializeToBytes(req)
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}

	// Echo the request back as a response from the fake master.
	go func() {
		frame := readMasterFrame(t, serverConn)
		if frame.Meta.ClusterPeerID != clusterPeerID {
			t.Errorf("outbound request cluster_peer_id mismatch: got %d, want %d", frame.Meta.ClusterPeerID, clusterPeerID)
		}
		if frame.Meta.Branch != branch {
			t.Errorf("outbound request branch mismatch: got 0x%x, want 0x%x", frame.Meta.Branch, branch)
		}
		writeMasterFrame(t, serverConn, &wire.Frame{
			Meta:    frame.Meta,
			Opcode:  frame.Opcode + 1,
			RPCID:   frame.RPCID,
			Payload: frame.Payload,
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	respAny, err := pc.SendRPCMeta(ctx, byte(wire.CommandOpGetMinorBlockListRequest), reqPayload, wire.ClusterMetadata{})
	if err != nil {
		t.Fatalf("peer conn SendRPCMeta: %v", err)
	}
	resp, ok := respAny.(*wire.GetMinorBlockListResponse)
	if !ok {
		t.Fatalf("expected *wire.GetMinorBlockListResponse, got %T", respAny)
	}
	_ = resp
}

// ── Additional tests ─────────────────────────────────────────────────────────

// TestMasterConn_DuplicateCreatePeerConn verifies that a duplicate create
// request for the same cluster_peer_id and branch does not replace the existing
// PeerConn. This matches Python's behavior of logging an error and skipping.
// Python: slave.py#L335-L341
func TestMasterConn_DuplicateCreatePeerConn(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 41
	const branch uint32 = 0x00010001

	// First create.
	fake := client.peerRuntime.(*fakePeerRuntime)
	fake.createPeerConns(clusterPeerID, []uint32{branch})
	original := fake.peers[clusterPeerID][branch]
	if original == nil {
		t.Fatal("expected peer conn after first create")
	}

	// Duplicate create — should not replace the existing PeerConn.
	fake.createPeerConns(clusterPeerID, []uint32{branch})
	afterDup := fake.peers[clusterPeerID][branch]
	if afterDup != original {
		t.Fatal("duplicate create replaced the existing PeerConn")
	}

	// The branch map should still have exactly one entry.
	if len(fake.peers[clusterPeerID]) != 1 {
		t.Fatalf("expected 1 branch entry, got %d", len(fake.peers[clusterPeerID]))
	}

	// MasterConn must still be alive.
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	})
	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG, got opcode 0x%x", resp.Opcode)
	}
}

// TestMasterConn_NonRPCCommandRouted verifies that fire-and-forget (non-RPC)
// commands are routed to the correct PeerConn.
// Python: shard.py OP_NONRPC_MAP (L275-L279)
func TestMasterConn_NonRPCCommandRouted(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 42
	const branch uint32 = 0x00010001

	fake := client.peerRuntime.(*fakePeerRuntime)
	fake.createPeerConns(clusterPeerID, []uint32{branch})

	cmdPayload, err := serialize.SerializeToBytes(&wire.NewMinorBlockHeaderListCommand{
		RootBlockHeader:      nil,
		MinorBlockHeaderList: nil,
	})
	if err != nil {
		t.Fatalf("serialize command: %v", err)
	}

	// Send a non-RPC command (rpc_id must be 0).
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: branch, ClusterPeerID: clusterPeerID},
		Opcode:  byte(wire.CommandOpNewMinorBlockHeaderList),
		RPCID:   0,
		Payload: cmdPayload,
	})

	// Non-RPC commands produce no response. Verify no frame is sent back.
	if err := serverConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := wire.ReadFrame(serverConn, 0); err == nil {
		t.Fatal("expected no response for non-RPC command")
	}

	// MasterConn must still be alive — a follow-up PING should work.
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	})
	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG after non-RPC command, got opcode 0x%x", resp.Opcode)
	}
}

// TestPeerConn_CloseStopsReadLoop verifies that closing a PeerConn causes its
// read loop to exit (no goroutine leak). After Close(), the closed channel
// should be signaled.
func TestPeerConn_CloseStopsReadLoop(t *testing.T) {
	client, _, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 43
	const branch uint32 = 0x00010001

	fake := client.peerRuntime.(*fakePeerRuntime)
	fake.createPeerConns(clusterPeerID, []uint32{branch})
	pc := fake.peers[clusterPeerID][branch]

	// Verify the PeerConn becomes active and its read loop is running.
	select {
	case <-pc.WaitUntilActive():
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("PeerConn did not become active after Start")
	}

	// Close the PeerConn.
	pc.Close()

	// The closed channel should be signaled promptly.
	select {
	case <-pc.WaitUntilClosed():
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("PeerConn read loop did not exit after Close")
	}

	// HandleFrame after close should return an error.
	if err := pc.HandleFrame(&wire.Frame{}); err == nil {
		t.Fatal("expected error from HandleFrame after Close")
	}
}

// TestMasterConn_DestroyNonexistentPeer verifies that destroying a non-existent
// cluster_peer_id is a no-op and does not affect MasterConn.
// Python: slave.py#L321-L327 (pop with default None)
func TestMasterConn_DestroyNonexistentPeer(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	// Destroy a cluster_peer_id that was never created.
	fake := client.peerRuntime.(*fakePeerRuntime)
	fake.DestroyPeerConns(9999)

	// MasterConn must still be alive.
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	})
	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG after destroying nonexistent peer, got opcode 0x%x", resp.Opcode)
	}

	// Destroy the same ID again — should still be idempotent.
	fake.DestroyPeerConns(9999)

	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   2,
		Payload: pingPayload,
	})
	resp = readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG after second destroy, got opcode 0x%x", resp.Opcode)
	}
}

// ── New coverage: response path, concurrency, backpressure ──────────────────

// newTestResponderPeer builds a PeerConn whose GetMinorBlockListRequest handler
// returns a real (empty) response, so the dispatch -> virtualTransport ->
// MasterConn return path can be exercised without unstubbing the production
// handlers.
func newTestResponderPeer(clusterPeerID uint64, branch uint32, masterConn *MasterConn, logger log.Logger) *PeerConn {
	vt := newVirtualTransport(clusterPeerID, branch, masterConn)
	pc := &PeerConn{clusterPeerID: clusterPeerID, branch: branch, vt: vt}
	pc.BaseConn = conn.NewBaseConn(conn.Config{
		Transport: vt,
		Serializers: map[byte]*conn.OpSerializer{
			byte(wire.CommandOpGetMinorBlockListRequest): conn.OpSerializerFor[wire.GetMinorBlockListRequest, wire.GetMinorBlockListResponse](byte(wire.CommandOpGetMinorBlockListResponse)),
		},
		Handlers: map[byte]conn.TypedHandler{
			byte(wire.CommandOpGetMinorBlockListRequest): func(any) (any, error) {
				return &wire.GetMinorBlockListResponse{}, nil
			},
		},
		Logger: logger,
	})
	return pc
}

// TestPeerConn_OutboundCommand verifies that a PeerConn fire-and-forget command
// is written to the underlying MasterConn with rpc_id == 0 and the peer's
// branch + cluster_peer_id metadata.
func TestPeerConn_OutboundCommand(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 71
	const branch uint32 = 0x00010001
	fake := client.peerRuntime.(*fakePeerRuntime)
	fake.createPeerConns(clusterPeerID, []uint32{branch})
	pc := fake.peers[clusterPeerID][branch]

	cmdPayload, err := serialize.SerializeToBytes(&wire.NewTransactionListCommand{})
	if err != nil {
		t.Fatalf("serialize command: %v", err)
	}

	// Fire-and-forget command (no response expected).
	if err := pc.SendCommandMeta(byte(wire.CommandOpNewTransactionList), cmdPayload, wire.ClusterMetadata{}); err != nil {
		t.Fatalf("send command: %v", err)
	}

	req := readMasterFrame(t, serverConn)
	if req.Opcode != byte(wire.CommandOpNewTransactionList) {
		t.Fatalf("expected command opcode 0x%x, got 0x%x", wire.CommandOpNewTransactionList, req.Opcode)
	}
	if req.RPCID != 0 {
		t.Fatalf("expected rpc_id 0 for command, got %d", req.RPCID)
	}
	if req.Meta.ClusterPeerID != clusterPeerID {
		t.Fatalf("expected cluster_peer_id %d, got %d", clusterPeerID, req.Meta.ClusterPeerID)
	}
	if req.Meta.Branch != branch {
		t.Fatalf("expected branch 0x%x, got 0x%x", branch, req.Meta.Branch)
	}
}

// TestPeerConn_InboundRPCResponseViaMaster verifies the full inbound round trip:
// a request routed to a PeerConn is dispatched, serialized, and the response
// travels back out through the MasterConn with the rpc_id preserved and the
// branch + cluster_peer_id metadata stamped.
func TestPeerConn_InboundRPCResponseViaMaster(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 81
	const branch uint32 = 0x00010001
	const rpcID uint64 = 42

	fake := client.peerRuntime.(*fakePeerRuntime)
	pc := newTestResponderPeer(clusterPeerID, branch, client, log.New())
	fake.registerPeer(pc)
	pc.Start()

	reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockListRequest{
		MinorBlockHashList: [][wire.HashLength]byte{},
	})
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}

	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: branch, ClusterPeerID: clusterPeerID},
		Opcode:  byte(wire.CommandOpGetMinorBlockListRequest),
		RPCID:   rpcID,
		Payload: reqPayload,
	})

	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.CommandOpGetMinorBlockListResponse) {
		t.Fatalf("expected response opcode 0x%x, got 0x%x", wire.CommandOpGetMinorBlockListResponse, resp.Opcode)
	}
	if resp.RPCID != rpcID {
		t.Fatalf("expected rpc_id preserved (%d), got %d", rpcID, resp.RPCID)
	}
	if resp.Meta.ClusterPeerID != clusterPeerID {
		t.Fatalf("expected cluster_peer_id %d in response meta, got %d", clusterPeerID, resp.Meta.ClusterPeerID)
	}
	if resp.Meta.Branch != branch {
		t.Fatalf("expected branch 0x%x in response meta, got 0x%x", branch, resp.Meta.Branch)
	}
	var out wire.GetMinorBlockListResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &out); err != nil {
		t.Fatalf("deserialize response payload: %v", err)
	}
}

// TestPeerConn_ConcurrentWrites verifies that many PeerConns writing outbound
// RPCs concurrently through the single MasterConn TCP do not corrupt frame
// boundaries. The fake master echoes every request back; a corrupt frame would
// fail to parse or leave a sender's RPC hanging.
func TestPeerConn_ConcurrentWrites(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const branch uint32 = 0x00010001
	const numPeers = 8
	const reqPerPeer = 16

	fake := client.peerRuntime.(*fakePeerRuntime)
	peers := make([]*PeerConn, numPeers)
	for i := 0; i < numPeers; i++ {
		cid := uint64(100 + i)
		fake.createPeerConns(cid, []uint32{branch})
		peers[i] = fake.peers[cid][branch]
	}

	reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockListRequest{
		MinorBlockHashList: [][wire.HashLength]byte{},
	})
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}

	total := numPeers * reqPerPeer
	propErr := make(chan error, 1)
	go func() {
		for i := 0; i < total; i++ {
			if err := serverConn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
				propErr <- err
				return
			}
			req, err := wire.ReadFrame(serverConn, 0)
			if err != nil {
				propErr <- fmt.Errorf("read outbound frame %d: %w", i, err)
				return
			}
			if err := serverConn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				propErr <- err
				return
			}
			// Echo back so the sender's pending RPC completes. A corrupt frame
			// boundary from unserialized concurrent writes would fail here.
			if err := wire.WriteFrame(serverConn, &wire.Frame{
				Meta:    req.Meta,
				Opcode:  req.Opcode + 1,
				RPCID:   req.RPCID,
				Payload: req.Payload,
			}); err != nil {
				propErr <- fmt.Errorf("echo frame %d: %w", i, err)
				return
			}
		}
		propErr <- nil
	}()

	var wg sync.WaitGroup
	for i := 0; i < numPeers; i++ {
		for j := 0; j < reqPerPeer; j++ {
			wg.Add(1)
			go func(p *PeerConn) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := p.SendRPCMeta(ctx, byte(wire.CommandOpGetMinorBlockListRequest), reqPayload, wire.ClusterMetadata{}); err != nil {
					t.Errorf("send rpc: %v", err)
				}
			}(peers[i])
		}
	}
	wg.Wait()

	if err := <-propErr; err != nil {
		t.Fatalf("master-side echo: %v", err)
	}
}

// TestMasterConn_ReaderNotBlockedBySlowPeer verifies the MasterConn reader
// goroutine is never stalled while delivering frames to a PeerConn whose
// consumer has stopped reading (a slow/stalled consumer). The inbound queue is
// unbounded, so delivery must be non-blocking even well beyond the old 64-slot
// bound.
func TestMasterConn_ReaderNotBlockedBySlowPeer(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 91
	const branch uint32 = 0x00010001

	// Register a peer but deliberately never Start() it: its reader loop is not
	// consuming the inbound queue, simulating a stalled consumer.
	fake := client.peerRuntime.(*fakePeerRuntime)
	pc := newTestResponderPeer(clusterPeerID, branch, client, log.New())
	fake.registerPeer(pc)

	reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockListRequest{
		MinorBlockHashList: [][wire.HashLength]byte{},
	})
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}

	// Burst far exceeding the previous 64-slot bound.
	for i := 0; i < 300; i++ {
		writeMasterFrame(t, serverConn, &wire.Frame{
			Meta:    wire.ClusterMetadata{Branch: branch, ClusterPeerID: clusterPeerID},
			Opcode:  byte(wire.CommandOpGetMinorBlockListRequest),
			RPCID:   uint64(i + 1),
			Payload: reqPayload,
		})
	}

	// The still-readable MasterConn must answer a follow-up master-local PING
	// promptly, proving the reader goroutine was not stalled by the burst.
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	})

	if err := serverConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	resp, err := wire.ReadFrame(serverConn, 0)
	if err != nil {
		t.Fatalf("PING not answered after peer burst: %v", err)
	}
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG, got opcode 0x%x", resp.Opcode)
	}
	_ = pc
}
