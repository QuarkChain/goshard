// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// newMasterConnWithDispatcher creates a MasterConn over a local TCP pair and
// wires a Dispatcher. The caller gets the raw server-side net.Conn so it can
// act as the fake master, plus the client MasterConn and cleanup.
func newMasterConnWithDispatcher(t *testing.T) (client *MasterConn, serverConn net.Conn, cleanup func()) {
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
	client = NewMasterConnFromConn(clientConn, 0, []byte("go-slave"), []uint32{0x00010001, 0x00020001}, logger)
	dispatcher := NewDispatcher(logger)
	client.SetDispatcher(dispatcher)
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

// TestDispatcher_RouteToMasterConn verifies that frames with cluster_peer_id == 0
// are handled by MasterConn itself (PING -> PONG).
func TestDispatcher_RouteToMasterConn(t *testing.T) {
	client, serverConn, cleanup := newMasterConnWithDispatcher(t)
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

// TestDispatcher_RouteToPeerConn verifies that frames with cluster_peer_id != 0
// are forwarded to the matching virtual PeerConn and the stub response is sent
// back through MasterConn.
func TestDispatcher_RouteToPeerConn(t *testing.T) {
	client, serverConn, cleanup := newMasterConnWithDispatcher(t)
	defer cleanup()

	const clusterPeerID uint64 = 7
	const branch uint32 = 0x00010001

	client.dispatcher.CreatePeerConns(clusterPeerID, []uint32{branch}, client, log.New())

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

	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.CommandOpGetMinorBlockListResponse) {
		t.Fatalf("expected response opcode 0x%x, got 0x%x", wire.CommandOpGetMinorBlockListResponse, resp.Opcode)
	}
	if resp.RPCID != 3 {
		t.Fatalf("expected rpc_id 3, got %d", resp.RPCID)
	}
	if resp.Meta.Branch != branch || resp.Meta.ClusterPeerID != clusterPeerID {
		t.Fatalf("metadata mismatch: got %+v, want branch=%d cluster_peer_id=%d", resp.Meta, branch, clusterPeerID)
	}

	var listResp wire.GetMinorBlockListResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &listResp); err != nil {
		t.Fatalf("deserialize response: %v", err)
	}
}

// TestDispatcher_UnknownPeerDropped verifies that frames for an unregistered
// cluster_peer_id are dropped and do not close MasterConn.
func TestDispatcher_UnknownPeerDropped(t *testing.T) {
	_, serverConn, cleanup := newMasterConnWithDispatcher(t)
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

// TestPeerConn_RPCIDIsolation verifies that two PeerConns sharing a MasterConn
// can use the same RPC ID without collision; responses are routed back to the
// correct peer via metadata.
func TestPeerConn_RPCIDIsolation(t *testing.T) {
	client, serverConn, cleanup := newMasterConnWithDispatcher(t)
	defer cleanup()

	client.dispatcher.CreatePeerConns(7, []uint32{0x00010001}, client, log.New())
	client.dispatcher.CreatePeerConns(9, []uint32{0x00020001}, client, log.New())

	reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockListRequest{
		MinorBlockHashList: [][wire.HashLength]byte{},
	})
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}

	// Both peers use rpc_id=5.
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

	resp1 := readMasterFrame(t, serverConn)
	resp2 := readMasterFrame(t, serverConn)

	if resp1.RPCID != 5 || resp2.RPCID != 5 {
		t.Fatalf("expected both responses to have rpc_id 5, got %d and %d", resp1.RPCID, resp2.RPCID)
	}

	// Each response must belong to a distinct peer/branch pair.
	peers := map[uint64]uint32{
		resp1.Meta.ClusterPeerID: resp1.Meta.Branch,
		resp2.Meta.ClusterPeerID: resp2.Meta.Branch,
	}
	if len(peers) != 2 {
		t.Fatalf("responses were not routed to distinct peers: %+v", peers)
	}
	if peers[7] != 0x00010001 {
		t.Fatalf("peer 7 response routed to wrong branch: got 0x%x", peers[7])
	}
	if peers[9] != 0x00020001 {
		t.Fatalf("peer 9 response routed to wrong branch: got 0x%x", peers[9])
	}

	for _, resp := range []*wire.Frame{resp1, resp2} {
		if resp.Opcode != byte(wire.CommandOpGetMinorBlockListResponse) {
			t.Fatalf("expected GetMinorBlockListResponse, got opcode 0x%x", resp.Opcode)
		}
		var listResp wire.GetMinorBlockListResponse
		if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &listResp); err != nil {
			t.Fatalf("deserialize response: %v", err)
		}
	}
}

// TestPeerConn_PeerHandlerRPCRoundTrip sends a CommandOp RPC through a virtual
// PeerConn and verifies the stub response deserializes correctly.
func TestPeerConn_PeerHandlerRPCRoundTrip(t *testing.T) {
	client, serverConn, cleanup := newMasterConnWithDispatcher(t)
	defer cleanup()

	const clusterPeerID uint64 = 11
	const branch uint32 = 0x00010001

	client.dispatcher.CreatePeerConns(clusterPeerID, []uint32{branch}, client, log.New())

	reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockHeaderListRequest{
		Branch:    branch,
		BlockHash: [wire.HashLength]byte{},
		Limit:     10,
		Direction: 0,
	})
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}

	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: branch, ClusterPeerID: clusterPeerID},
		Opcode:  byte(wire.CommandOpGetMinorBlockHeaderListRequest),
		RPCID:   1,
		Payload: reqPayload,
	})

	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.CommandOpGetMinorBlockHeaderListResponse) {
		t.Fatalf("expected response opcode 0x%x, got 0x%x", wire.CommandOpGetMinorBlockHeaderListResponse, resp.Opcode)
	}
	if resp.RPCID != 1 {
		t.Fatalf("expected rpc_id 1, got %d", resp.RPCID)
	}

	var headerResp wire.GetMinorBlockHeaderListResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &headerResp); err != nil {
		t.Fatalf("deserialize response: %v", err)
	}
}

// TestMasterConn_CreateDestroyPeerConnection verifies that the master commands
// create and destroy virtual peer connections through the dispatcher.
func TestMasterConn_CreateDestroyPeerConnection(t *testing.T) {
	client, serverConn, cleanup := newMasterConnWithDispatcher(t)
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

	// The dispatcher should now hold one PeerConn per local shard.
	branchMap := client.dispatcher.peers[clusterPeerID]
	if len(branchMap) != len(client.localFullShardIDList) {
		t.Fatalf("expected %d peer conns, got %d", len(client.localFullShardIDList), len(branchMap))
	}
	for _, branch := range client.localFullShardIDList {
		if branchMap[branch] == nil {
			t.Fatalf("missing peer conn for branch 0x%x", branch)
		}
		if branchMap[branch].IsClosed() {
			t.Fatalf("peer conn for branch 0x%x is already closed", branch)
		}
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

	// Give the close goroutines a moment to finish.
	time.Sleep(50 * time.Millisecond)

	for _, branch := range client.localFullShardIDList {
		if pc := client.dispatcher.peers[clusterPeerID][branch]; pc != nil && !pc.IsClosed() {
			t.Fatalf("peer conn for branch 0x%x was not closed after destroy", branch)
		}
	}

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
// all associated PeerConns and clears the dispatcher registry.
func TestMasterConn_CloseClosesPeerConns(t *testing.T) {
	client, _, cleanup := newMasterConnWithDispatcher(t)
	defer cleanup()

	client.dispatcher.CreatePeerConns(7, []uint32{0x00010001, 0x00020001}, client, log.New())
	client.dispatcher.CreatePeerConns(9, []uint32{0x00010001}, client, log.New())

	// Keep references before Close clears the map.
	var peerConns []*PeerConn
	for _, branchMap := range client.dispatcher.peers {
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

	if len(client.dispatcher.peers) != 0 {
		t.Fatalf("dispatcher registry not cleared: got %d cluster_peer_id entries", len(client.dispatcher.peers))
	}
}

// TestPeerConn_OutboundRPCThroughMasterConn verifies that a PeerConn can issue
// an outbound RPC and the request is written to the underlying MasterConn with
// the correct cluster_peer_id metadata.
func TestPeerConn_OutboundRPCThroughMasterConn(t *testing.T) {
	client, serverConn, cleanup := newMasterConnWithDispatcher(t)
	defer cleanup()

	const clusterPeerID uint64 = 31
	const branch uint32 = 0x00010001

	client.dispatcher.CreatePeerConns(clusterPeerID, []uint32{branch}, client, log.New())
	pc := client.dispatcher.peers[clusterPeerID][branch]

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

	resp, err := pc.SendRPCMeta(ctx, byte(wire.CommandOpGetMinorBlockListRequest), reqPayload, wire.ClusterMetadata{})
	if err != nil {
		t.Fatalf("peer conn SendRPCMeta: %v", err)
	}
	if resp.Opcode != byte(wire.CommandOpGetMinorBlockListResponse) {
		t.Fatalf("expected response opcode 0x%x, got 0x%x", wire.CommandOpGetMinorBlockListResponse, resp.Opcode)
	}
}

// ── Additional tests ─────────────────────────────────────────────────────────

// TestDispatcher_DuplicateCreatePeerConn verifies that a duplicate create
// request for the same cluster_peer_id and branch does not replace the existing
// PeerConn. This matches Python's behavior of logging an error and skipping.
// Python: slave.py#L335-L341
func TestDispatcher_DuplicateCreatePeerConn(t *testing.T) {
	client, serverConn, cleanup := newMasterConnWithDispatcher(t)
	defer cleanup()

	const clusterPeerID uint64 = 41
	const branch uint32 = 0x00010001

	// First create.
	client.dispatcher.CreatePeerConns(clusterPeerID, []uint32{branch}, client, log.New())
	original := client.dispatcher.peers[clusterPeerID][branch]
	if original == nil {
		t.Fatal("expected peer conn after first create")
	}

	// Duplicate create — should not replace the existing PeerConn.
	client.dispatcher.CreatePeerConns(clusterPeerID, []uint32{branch}, client, log.New())
	afterDup := client.dispatcher.peers[clusterPeerID][branch]
	if afterDup != original {
		t.Fatal("duplicate create replaced the existing PeerConn")
	}

	// The branch map should still have exactly one entry.
	if len(client.dispatcher.peers[clusterPeerID]) != 1 {
		t.Fatalf("expected 1 branch entry, got %d", len(client.dispatcher.peers[clusterPeerID]))
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

// TestDispatcher_NonRPCCommandRouted verifies that fire-and-forget (non-RPC)
// commands are routed through the Dispatcher to the correct PeerConn.
// Python: shard.py OP_NONRPC_MAP (L275-L279)
func TestDispatcher_NonRPCCommandRouted(t *testing.T) {
	client, serverConn, cleanup := newMasterConnWithDispatcher(t)
	defer cleanup()

	const clusterPeerID uint64 = 42
	const branch uint32 = 0x00010001

	client.dispatcher.CreatePeerConns(clusterPeerID, []uint32{branch}, client, log.New())

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
// read loop to exit (no goroutine leak). After Close(), the read loop's
// deferred Close should be a no-op and the closed channel should be signaled.
func TestPeerConn_CloseStopsReadLoop(t *testing.T) {
	client, _, cleanup := newMasterConnWithDispatcher(t)
	defer cleanup()

	const clusterPeerID uint64 = 43
	const branch uint32 = 0x00010001

	client.dispatcher.CreatePeerConns(clusterPeerID, []uint32{branch}, client, log.New())
	pc := client.dispatcher.peers[clusterPeerID][branch]

	// Verify the PeerConn is active and its read loop is running.
	if !pc.IsActive() {
		t.Fatal("expected PeerConn to be active after Start")
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

// TestDispatcher_DestroyNonexistentPeer verifies that destroying a non-existent
// cluster_peer_id is a no-op and does not affect MasterConn.
// Python: slave.py#L321-L327 (pop with default None)
func TestDispatcher_DestroyNonexistentPeer(t *testing.T) {
	client, serverConn, cleanup := newMasterConnWithDispatcher(t)
	defer cleanup()

	// Destroy a cluster_peer_id that was never created.
	client.dispatcher.DestroyPeerConns(9999)

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
	client.dispatcher.DestroyPeerConns(9999)

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
