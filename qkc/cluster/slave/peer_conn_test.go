// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"errors"
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

// fakeSlaveService is a test double for the future SlaveService: it embeds
// fakeMasterHandler for the business RPC stubs, implements the cluster-peer
// CREATE/DESTROY business with a peer registry built via NewPeerConn, and
// implements SlaveConnHandler.LookupPeer (shadowing the embedded no-peers
// stub). masterConn is late-bound after NewMasterConn returns.
type fakeSlaveService struct {
	*fakeMasterHandler
	mu         sync.Mutex
	peers      map[uint64]map[uint32]*PeerConn
	masterConn *MasterConn
	handler    PeerHandler
	branches   []uint32 // local shard set for CREATE
}

// stubPeerHandler stands in for the not-yet-migrated business layer: every
// method returns ErrHandlerNotImplemented, so routed frames still exercise
// the handler-error path (PeerConn closes, MasterConn survives).
type stubPeerHandler struct{}

func (stubPeerHandler) NewMinorBlockHeaderList(*wire.NewMinorBlockHeaderListCommand) error {
	return conn.ErrHandlerNotImplemented
}

func (stubPeerHandler) NewTransactionList(*wire.NewTransactionListCommand) error {
	return conn.ErrHandlerNotImplemented
}

func (stubPeerHandler) NewBlockMinor(*wire.NewBlockMinorCommand) error {
	return conn.ErrHandlerNotImplemented
}

func (stubPeerHandler) GetMinorBlockHeaderList(*wire.GetMinorBlockHeaderListRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
	return nil, conn.ErrHandlerNotImplemented
}

func (stubPeerHandler) GetMinorBlockList(*wire.GetMinorBlockListRequest) (*wire.GetMinorBlockListResponse, error) {
	return nil, conn.ErrHandlerNotImplemented
}

func (stubPeerHandler) GetMinorBlockHeaderListWithSkip(*wire.GetMinorBlockHeaderListWithSkipRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
	return nil, conn.ErrHandlerNotImplemented
}

// peerHandlerCall is one dispatched request captured by recordingPeerHandler.
type peerHandlerCall struct {
	opcode byte
	req    any
}

// recordingPeerHandler implements PeerHandler by capturing every dispatched
// request and answering RPCs with a decodable response. Unlike stubPeerHandler
// (which always fails and therefore closes the PeerConn) it keeps the PeerConn
// alive, so a test can assert that a routed frame really reached the peer's
// handler instead of inferring delivery from the connection closing — an
// inference that also holds when the frame was silently dropped.
type recordingPeerHandler struct {
	calls chan peerHandlerCall
}

func newRecordingPeerHandler() *recordingPeerHandler {
	return &recordingPeerHandler{calls: make(chan peerHandlerCall, 64)}
}

// record hands the call to the test. The channel is buffered and drained by
// the assertions, so dispatch goroutines do not pile up behind an unread call.
func (h *recordingPeerHandler) record(op wire.CommandOp, req any) {
	h.calls <- peerHandlerCall{opcode: byte(op), req: req}
}

// next waits for the next dispatched request.
func (h *recordingPeerHandler) next(t *testing.T, timeout time.Duration) peerHandlerCall {
	t.Helper()
	select {
	case c := <-h.calls:
		return c
	case <-time.After(timeout):
		t.Fatal("timed out waiting for the peer handler to be invoked")
		return peerHandlerCall{}
	}
}

func (h *recordingPeerHandler) NewMinorBlockHeaderList(req *wire.NewMinorBlockHeaderListCommand) error {
	h.record(wire.CommandOpNewMinorBlockHeaderList, req)
	return nil
}

func (h *recordingPeerHandler) NewTransactionList(req *wire.NewTransactionListCommand) error {
	h.record(wire.CommandOpNewTransactionList, req)
	return nil
}

func (h *recordingPeerHandler) NewBlockMinor(req *wire.NewBlockMinorCommand) error {
	h.record(wire.CommandOpNewBlockMinor, req)
	return nil
}

func (h *recordingPeerHandler) GetMinorBlockHeaderList(req *wire.GetMinorBlockHeaderListRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
	h.record(wire.CommandOpGetMinorBlockHeaderListRequest, req)
	return &wire.GetMinorBlockHeaderListResponse{RootTip: &wire.RawBytes{}, ShardTip: &wire.RawBytes{}}, nil
}

func (h *recordingPeerHandler) GetMinorBlockList(req *wire.GetMinorBlockListRequest) (*wire.GetMinorBlockListResponse, error) {
	h.record(wire.CommandOpGetMinorBlockListRequest, req)
	return &wire.GetMinorBlockListResponse{}, nil
}

func (h *recordingPeerHandler) GetMinorBlockHeaderListWithSkip(req *wire.GetMinorBlockHeaderListWithSkipRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
	h.record(wire.CommandOpGetMinorBlockHeaderListWithSkipRequest, req)
	return &wire.GetMinorBlockHeaderListResponse{RootTip: &wire.RawBytes{}, ShardTip: &wire.RawBytes{}}, nil
}

func newFakeSlaveService(mc *MasterConn, handler PeerHandler, branches []uint32) *fakeSlaveService {
	if handler == nil {
		handler = stubPeerHandler{}
	}
	return &fakeSlaveService{
		fakeMasterHandler: &fakeMasterHandler{},
		peers:             make(map[uint64]map[uint32]*PeerConn),
		masterConn:        mc,
		handler:           handler,
		branches:          branches,
	}
}

// CreateClusterPeerConnection implements MasterHandler: creates PeerConns on
// every configured branch and registers them (py: slave.py:329-370).
func (f *fakeSlaveService) CreateClusterPeerConnection(req *wire.CreateClusterPeerConnectionRequest) (*wire.CreateClusterPeerConnectionResponse, error) {
	f.createPeerConns(req.ClusterPeerID, f.branches)
	return &wire.CreateClusterPeerConnectionResponse{ErrorCode: 0}, nil
}

// DestroyClusterPeerConnection implements MasterHandler: removes and closes
// every PeerConn of the given cluster_peer_id (py: slave.py:321-327).
func (f *fakeSlaveService) DestroyClusterPeerConnection(req *wire.DestroyClusterPeerConnectionCommand) error {
	f.DestroyPeerConns(req.ClusterPeerID)
	return nil
}

// createPeerConns is a test helper (not an interface method): creates PeerConns
// for explicit branches. Empty branches registers nothing (Python: empty
// self.shards.values() -> CREATE is a no-op).
func (f *fakeSlaveService) createPeerConns(clusterPeerID uint64, branches []uint32) {
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
		pc, err := NewPeerConn(clusterPeerID, branch, f.masterConn, f.handler, f.masterConn.Logger())
		if err != nil {
			panic(err) // unreachable in tests: masterConn is late-bound and handler non-nil
		}
		pc.Start()
		bm[branch] = pc
	}
	if len(bm) > 0 {
		f.peers[clusterPeerID] = bm
	}
}

func (f *fakeSlaveService) DestroyPeerConns(clusterPeerID uint64) {
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

// LookupPeer implements the SlaveConnHandler lookup used by MasterConn's
// router: (cluster_peer_id, branch) -> PeerConn, nil when there is no match.
func (f *fakeSlaveService) LookupPeer(clusterPeerID uint64, branch uint32) *PeerConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	bm, ok := f.peers[clusterPeerID]
	if !ok {
		return nil
	}
	return bm[branch]
}

// closeAll closes every registered PeerConn (test cleanup helper; the
// production counterpart is SlaveService shutdown, not MasterConn close).
func (f *fakeSlaveService) closeAll() {
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

func (f *fakeSlaveService) peerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.peers)
}

// registerPeer inserts an already-constructed PeerConn into the fake registry.
func (f *fakeSlaveService) registerPeer(pc *PeerConn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	bm, ok := f.peers[pc.clusterPeerID]
	if !ok {
		bm = make(map[uint32]*PeerConn)
		f.peers[pc.clusterPeerID] = bm
	}
	bm[pc.branch] = pc
}

// newMasterConn creates a MasterConn over a local TCP pair with a fake
// SlaveService injected as both SlaveConnHandler and Handler (reachable via
// client.slaveConnHandler.(*fakeSlaveService)).
func newMasterConn(t *testing.T) (client *MasterConn, serverConn net.Conn, cleanup func()) {
	t.Helper()
	return newMasterConnWithBranches(t, []uint32{0x00010001, 0x00020001})
}

// newMasterConnWithBranches is newMasterConn with an explicit local shard set
// for the fake service and the PONG list; empty branches models a runtime with
// no shards yet. The global configured set is the default shard pair.
func newMasterConnWithBranches(t *testing.T, branches []uint32) (client *MasterConn, serverConn net.Conn, cleanup func()) {
	t.Helper()
	return newMasterConnWithShardSets(t, []uint32{0x00010001, 0x00020001}, branches)
}

// newMasterConnWithShardSets is newMasterConn with an explicit global
// configured shard set and local shard assignment (both are required by
// MasterConnConfig; Python: global quark_chain_config.get_full_shard_ids() vs
// local slave_config.FULL_SHARD_ID_LIST).
//
// The two sets are what make the routing boundary testable: a branch outside
// the global set is fatal for MasterConn, while a globally valid branch that
// this slave does not own is only dropped.
func newMasterConnWithShardSets(t *testing.T, global []uint32, local []uint32) (client *MasterConn, serverConn net.Conn, cleanup func()) {
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
	fake := newFakeSlaveService(nil, nil, local)
	client, err = NewMasterConn(MasterConnConfig{
		Conn:                 clientConn,
		LocalID:              []byte("go-slave"),
		LocalFullShardIDList: local,
		ClusterShardIDs:      global,
		SlaveConnHandler:     fake,
		Handler:              fake,
		Logger:               logger,
	})
	if err != nil {
		t.Fatalf("new master conn: %v", err)
	}
	fake.masterConn = client // late-bind: PeerConns constructed by the fake need the transport
	client.Start()
	serverConn = srvConn

	cleanup = func() {
		fake.closeAll()
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

// expectNoFrame asserts that no frame arrives within timeout. It is the probe
// for "the frame was consumed without producing a response": a dropped frame
// (NULL_CONNECTION) and a fire-and-forget command look identical from the
// master side.
func expectNoFrame(t *testing.T, conn net.Conn, timeout time.Duration) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if f, err := wire.ReadFrame(conn, 0); err == nil {
		t.Fatalf("expected no frame, got opcode 0x%x", f.Opcode)
	}
}

// pingMaster sends a master-local PING and requires the PONG back, proving
// MasterConn is still readable after the peer traffic under test. rpcID must
// exceed every master-local rpc_id already sent on this connection: BaseConn
// enforces a strictly increasing inbound sequence per connection.
func pingMaster(t *testing.T, conn net.Conn, rpcID uint64) *wire.Frame {
	t.Helper()
	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}
	writeMasterFrame(t, conn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   rpcID,
		Payload: pingPayload,
	})
	resp := readMasterFrame(t, conn)
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG, got opcode 0x%x", resp.Opcode)
	}
	if resp.RPCID != rpcID {
		t.Fatalf("expected rpc_id %d in PONG, got %d", rpcID, resp.RPCID)
	}
	return resp
}

// newRecordingPeerConn builds a PeerConn whose handler records every dispatched
// request and registers it with the harness' fake service, so MasterConn routes
// frames addressed to (clusterPeerID, branch) to it. Start() is left to the
// caller: leaving it unstarted models a peer whose reader is not consuming.
func newRecordingPeerConn(t *testing.T, masterConn *MasterConn, clusterPeerID uint64, branch uint32) (*PeerConn, *recordingPeerHandler) {
	t.Helper()
	handler := newRecordingPeerHandler()
	pc, err := NewPeerConn(clusterPeerID, branch, masterConn, handler, masterConn.Logger())
	if err != nil {
		t.Fatalf("new peer conn: %v", err)
	}
	masterConn.slaveConnHandler.(*fakeSlaveService).registerPeer(pc)
	return pc, handler
}

// TestMasterConn_RouteToPeerConn verifies the full routing round trip for a
// frame with cluster_peer_id != 0: MasterConn hands it to the PeerConn
// registered for (cluster_peer_id, branch), the peer's handler runs, and the
// response travels back out through MasterConn stamped with the peer's routing
// metadata. MasterConn itself must survive the exchange.
func TestMasterConn_RouteToPeerConn(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 7
	const branch uint32 = 0x00010001
	const rpcID uint64 = 3

	pc, handler := newRecordingPeerConn(t, client, clusterPeerID, branch)
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

	// The request must reach the peer's handler, decoded.
	call := handler.next(t, 2*time.Second)
	if call.opcode != byte(wire.CommandOpGetMinorBlockListRequest) {
		t.Fatalf("handler invoked for opcode 0x%x, want 0x%x", call.opcode, wire.CommandOpGetMinorBlockListRequest)
	}
	if _, ok := call.req.(*wire.GetMinorBlockListRequest); !ok {
		t.Fatalf("handler received %T, want *wire.GetMinorBlockListRequest", call.req)
	}

	resp := readMasterFrame(t, serverConn)
	if resp.Opcode != byte(wire.CommandOpGetMinorBlockListResponse) {
		t.Fatalf("expected response opcode 0x%x, got 0x%x", wire.CommandOpGetMinorBlockListResponse, resp.Opcode)
	}
	if resp.RPCID != rpcID {
		t.Fatalf("expected rpc_id preserved (%d), got %d", rpcID, resp.RPCID)
	}
	if resp.Meta.ClusterPeerID != clusterPeerID || resp.Meta.Branch != branch {
		t.Fatalf("response meta mismatch: got %+v, want cid=%d branch=0x%x", resp.Meta, clusterPeerID, branch)
	}

	// MasterConn must still be alive for master-local traffic.
	pingMaster(t, serverConn, 1)

	// The PeerConn outlived the exchange: a routing hit is not a close event.
	if pc.IsClosed() {
		t.Fatal("PeerConn closed by a successful routed exchange")
	}
}

// TestMasterConn_UnroutablePeerFrameDropped verifies the NULL_CONNECTION half
// of the routing table: a peer frame that resolves to no PeerConn is consumed
// and dropped — no response, and MasterConn survives. Two distinct misses are
// covered, because they are separate branches in Python and in routeFrame:
//
//   - unknown cluster_peer_id (py: slave.py:136-146)
//   - known cluster_peer_id but a branch this slave does not own (py: slave.py:131-134)
func TestMasterConn_UnroutablePeerFrameDropped(t *testing.T) {
	cases := []struct {
		name          string
		clusterPeerID uint64
		branch        uint32
		register      func(*fakeSlaveService)
	}{
		{
			name:          "unknown cluster peer id",
			clusterPeerID: 999,
			branch:        0x00010001,
		},
		{
			name:          "known peer id on an unowned branch",
			clusterPeerID: 7,
			branch:        0x00020001, // branch 0x00010001 only
			register:      func(f *fakeSlaveService) { f.createPeerConns(7, []uint32{0x00010001}) },
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client, serverConn, cleanup := newMasterConn(t)
			defer cleanup()

			if c.register != nil {
				c.register(client.slaveConnHandler.(*fakeSlaveService))
			}

			reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockListRequest{
				MinorBlockHashList: [][wire.HashLength]byte{},
			})
			if err != nil {
				t.Fatalf("serialize request: %v", err)
			}

			writeMasterFrame(t, serverConn, &wire.Frame{
				Meta:    wire.ClusterMetadata{Branch: c.branch, ClusterPeerID: c.clusterPeerID},
				Opcode:  byte(wire.CommandOpGetMinorBlockListRequest),
				RPCID:   1,
				Payload: reqPayload,
			})

			expectNoFrame(t, serverConn, 200*time.Millisecond)

			// A subsequent master-local PING must still work.
			pingMaster(t, serverConn, 2)
		})
	}
}

// TestMasterConn_InvalidBranchClosesConnection verifies that a peer frame
// whose branch is outside the configured shard set is fatal for the whole
// MasterConn (py: slave.py:123-129 close_with_error("incorrect forwarding
// branch")).
func TestMasterConn_InvalidBranchClosesConnection(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockListRequest{
		MinorBlockHashList: [][wire.HashLength]byte{},
	})
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}

	// Branch 0x00030001 is not in the configured set {0x00010001, 0x00020001}.
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00030001, ClusterPeerID: 55},
		Opcode:  byte(wire.CommandOpGetMinorBlockListRequest),
		RPCID:   1,
		Payload: reqPayload,
	})

	select {
	case <-client.WaitUntilClosed():
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("MasterConn did not close on incorrect forwarding branch")
	}
}

// TestMasterConn_PeerFrameForForeignShardDropped verifies the two-level
// forwarding semantics: a branch inside the global cluster config but NOT
// owned by this slave is NOT fatal — the frame is dropped and MasterConn
// survives. The close at py: slave.py:123-129 only triggers for branches
// outside the global config (quark_chain_config.get_full_shard_ids()); a
// valid branch missing from the local shard registry is Python's
// NULL_CONNECTION (slave.py:131-134).
func TestMasterConn_PeerFrameForForeignShardDropped(t *testing.T) {
	client, serverConn, cleanup := newMasterConnWithShardSets(t,
		[]uint32{0x00010001, 0x00020001, 0x00030001}, // global cluster config
		[]uint32{0x00010001, 0x00020001},             // this slave's assignment
	)
	defer cleanup()

	reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockListRequest{
		MinorBlockHashList: [][wire.HashLength]byte{},
	})
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}

	// Branch 0x00030001 belongs to another slave: drop, do not close.
	writeMasterFrame(t, serverConn, &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00030001, ClusterPeerID: 77},
		Opcode:  byte(wire.CommandOpGetMinorBlockListRequest),
		RPCID:   1,
		Payload: reqPayload,
	})

	// No response for the dropped frame...
	expectNoFrame(t, serverConn, 200*time.Millisecond)

	// ...and MasterConn must still be alive.
	pingMaster(t, serverConn, 2)

	if client.IsClosed() {
		t.Fatal("MasterConn closed by foreign-shard frame; only branches outside the global config are fatal")
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
	fake := client.slaveConnHandler.(*fakeSlaveService)
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
	expectNoFrame(t, serverConn, 200*time.Millisecond)

	// MasterConn must still be alive. (RPC id 2: CREATE already used id 1 on
	// this connection, and the master-local sequence is monotonic.)
	pingMaster(t, serverConn, 2)
}

// TestPeerConn_RPCIDIsolation verifies that two PeerConns sharing a MasterConn
// can use the same RPC ID without collision. MasterConn's monotonic inbound
// rpc_id check is per connection and only applies to cluster_peer_id=0
// traffic; peer traffic is forwarded before validation, so each PeerConn keeps
// its own sequence and both must be served.
func TestPeerConn_RPCIDIsolation(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	pc7, handler7 := newRecordingPeerConn(t, client, 7, 0x00010001)
	pc9, handler9 := newRecordingPeerConn(t, client, 9, 0x00020001)
	pc7.Start()
	pc9.Start()

	reqPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockListRequest{
		MinorBlockHashList: [][wire.HashLength]byte{},
	})
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}

	// Both peers use rpc_id=5. MasterConn's monotonic inbound rpc_id check is
	// per connection, so the same value must be accepted by both peers: if it
	// were applied to forwarded peer traffic, the second frame would fail the
	// sequence check and take MasterConn down.
	for _, c := range []struct {
		clusterPeerID uint64
		branch        uint32
	}{
		{7, 0x00010001},
		{9, 0x00020001},
	} {
		writeMasterFrame(t, serverConn, &wire.Frame{
			Meta:    wire.ClusterMetadata{Branch: c.branch, ClusterPeerID: c.clusterPeerID},
			Opcode:  byte(wire.CommandOpGetMinorBlockListRequest),
			RPCID:   5,
			Payload: reqPayload,
		})
	}

	// Each handler must have been invoked with its own peer's request: a
	// crossed or dropped delivery would leave one of them empty.
	for _, h := range []*recordingPeerHandler{handler7, handler9} {
		call := h.next(t, 2*time.Second)
		if call.opcode != byte(wire.CommandOpGetMinorBlockListRequest) {
			t.Fatalf("handler invoked for opcode 0x%x, want 0x%x", call.opcode, wire.CommandOpGetMinorBlockListRequest)
		}
	}
	if pc7.IsClosed() || pc9.IsClosed() {
		t.Fatal("PeerConn closed by a duplicate rpc_id across peers; the namespaces must be independent")
	}

	// Both responses come back over the shared transport, each still carrying
	// its own peer's routing metadata and the colliding rpc_id.
	for i := 0; i < 2; i++ {
		resp := readMasterFrame(t, serverConn)
		if resp.Opcode != byte(wire.CommandOpGetMinorBlockListResponse) {
			t.Fatalf("response %d: expected opcode 0x%x, got 0x%x", i+1, wire.CommandOpGetMinorBlockListResponse, resp.Opcode)
		}
		if resp.RPCID != 5 {
			t.Fatalf("response %d: rpc_id: got %d, want 5", i+1, resp.RPCID)
		}
		if resp.Meta.ClusterPeerID != 7 && resp.Meta.ClusterPeerID != 9 {
			t.Fatalf("response %d: unexpected cluster_peer_id %d", i+1, resp.Meta.ClusterPeerID)
		}
	}

	// MasterConn must still be alive.
	pingMaster(t, serverConn, 1)
}

// TestMasterConn_CreateDestroyPeerConnection verifies that the CREATE/DESTROY
// master commands are dispatched to the service layer (the fake), which
// creates and destroys the virtual peer connections.
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
	fake := client.slaveConnHandler.(*fakeSlaveService)
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
	pingMaster(t, serverConn, 2)
}

// TestMasterConn_CloseDoesNotClosePeerConns verifies that closing MasterConn
// is a connection event only: PeerConns are owned by the SlaveService (here
// the fake's registry), not by MasterConn, and survive the MasterConn close.
// Peer cleanup belongs to the service shutdown path (fake.closeAll).
func TestMasterConn_CloseDoesNotClosePeerConns(t *testing.T) {
	client, _, cleanup := newMasterConn(t)
	defer cleanup()

	fake := client.slaveConnHandler.(*fakeSlaveService)
	fake.createPeerConns(7, []uint32{0x00010001, 0x00020001})
	fake.createPeerConns(9, []uint32{0x00010001})

	// Keep references before closeAll clears the map.
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

	// MasterConn close does not cascade to PeerConns.
	for _, pc := range peerConns {
		if pc.IsClosed() {
			t.Fatalf("peer conn %d/%d closed by MasterConn.Close; peer lifecycle is owned by the service", pc.clusterPeerID, pc.branch)
		}
	}
	if got := fake.peerCount(); got != 2 {
		t.Fatalf("peer registry mutated by MasterConn.Close: got %d cluster_peer_id entries", got)
	}

	// The service-side shutdown path (fake.closeAll) closes them all.
	fake.closeAll()
	for _, pc := range peerConns {
		if !pc.IsClosed() {
			t.Fatalf("peer conn %d/%d was not closed by service closeAll", pc.clusterPeerID, pc.branch)
		}
	}
}

// ── PeerConn lifecycle ──────────────────────────────────────────────────────

// TestMasterConn_DuplicateCreatePeerConn verifies that a duplicate CREATE for
// the same cluster_peer_id is idempotent: the PeerConns created by the first
// request survive, so a peer that is already exchanging traffic is not torn
// down and rebuilt underneath it. Both requests are driven over the wire so
// the whole MasterConn dispatch path is exercised, not just the fake registry.
// Python: slave.py:335-341 (logs an error and skips the duplicate).
func TestMasterConn_DuplicateCreatePeerConn(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 41
	const branch uint32 = 0x00010001

	createPayload, err := serialize.SerializeToBytes(&wire.CreateClusterPeerConnectionRequest{ClusterPeerID: clusterPeerID})
	if err != nil {
		t.Fatalf("serialize create request: %v", err)
	}

	fake := client.slaveConnHandler.(*fakeSlaveService)
	var original *PeerConn
	for i, rpcID := range []uint64{1, 2} {
		writeMasterFrame(t, serverConn, &wire.Frame{
			Meta:    wire.ClusterMetadata{Branch: 0x00010001},
			Opcode:  byte(wire.ClusterOpCreateClusterPeerConnectionRequest),
			RPCID:   rpcID,
			Payload: createPayload,
		})

		resp := readMasterFrame(t, serverConn)
		if resp.Opcode != byte(wire.ClusterOpCreateClusterPeerConnectionResponse) {
			t.Fatalf("create #%d: expected response opcode 0x%x, got 0x%x",
				i+1, wire.ClusterOpCreateClusterPeerConnectionResponse, resp.Opcode)
		}
		if resp.RPCID != rpcID {
			t.Fatalf("create #%d: rpc_id echo: got %d, want %d", i+1, resp.RPCID, rpcID)
		}
		var createResp wire.CreateClusterPeerConnectionResponse
		if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &createResp); err != nil {
			t.Fatalf("create #%d: deserialize response: %v", i+1, err)
		}
		if createResp.ErrorCode != 0 {
			t.Fatalf("create #%d: expected error_code 0, got %d", i+1, createResp.ErrorCode)
		}

		// Look the peer up through the interface MasterConn itself uses.
		pc := fake.LookupPeer(clusterPeerID, branch)
		if pc == nil {
			t.Fatalf("create #%d: no peer conn for cluster_peer_id %d", i+1, clusterPeerID)
		}
		if pc.IsClosed() {
			t.Fatalf("create #%d: peer conn is closed", i+1)
		}
		if i == 0 {
			original = pc
		} else if pc != original {
			t.Fatal("duplicate CREATE replaced the existing PeerConn")
		}
	}

	// The duplicate must not have expanded the registry either.
	if got := fake.peerCount(); got != 1 {
		t.Fatalf("expected 1 cluster_peer_id entry, got %d", got)
	}

	pingMaster(t, serverConn, 3)
}

// TestMasterConn_NonRPCCommandRouted verifies that a fire-and-forget (non-RPC)
// command is routed to the correct PeerConn, dispatched to its handler, and
// produces no response frame — while MasterConn stays alive.
//
// The delivery assertion matters: "no response on the wire" is also what a
// dropped frame looks like, so the test confirms the handler actually ran.
// Python: shard.py OP_NONRPC_MAP (L275-L279)
func TestMasterConn_NonRPCCommandRouted(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 42
	const branch uint32 = 0x00010001

	pc, handler := newRecordingPeerConn(t, client, clusterPeerID, branch)
	pc.Start()

	cmd := &wire.NewMinorBlockHeaderListCommand{
		RootBlockHeader:      &wire.RawBytes{},
		MinorBlockHeaderList: []*wire.RawBytes{{0x01}},
	}
	cmdPayload, err := serialize.SerializeToBytes(cmd)
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

	// The command reached the peer's handler, decoded.
	call := handler.next(t, 2*time.Second)
	if call.opcode != byte(wire.CommandOpNewMinorBlockHeaderList) {
		t.Fatalf("handler invoked for opcode 0x%x, want 0x%x", call.opcode, wire.CommandOpNewMinorBlockHeaderList)
	}
	got, ok := call.req.(*wire.NewMinorBlockHeaderListCommand)
	if !ok {
		t.Fatalf("handler received %T, want *wire.NewMinorBlockHeaderListCommand", call.req)
	}
	if len(got.MinorBlockHeaderList) != len(cmd.MinorBlockHeaderList) {
		t.Fatalf("handler received %d headers, want %d", len(got.MinorBlockHeaderList), len(cmd.MinorBlockHeaderList))
	}

	// Non-RPC commands produce no response.
	expectNoFrame(t, serverConn, 200*time.Millisecond)

	// MasterConn must still be alive — a follow-up PING should work.
	pingMaster(t, serverConn, 1)

	if pc.IsClosed() {
		t.Fatal("PeerConn closed by a routed non-RPC command")
	}
}

// TestPeerConn_CloseStopsReadLoop verifies that closing a PeerConn causes its
// read loop to exit (no goroutine leak). After Close(), the closed channel
// should be signaled and HandleFrame must reject further frames.
func TestPeerConn_CloseStopsReadLoop(t *testing.T) {
	client, _, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 43
	const branch uint32 = 0x00010001

	pc, _ := newRecordingPeerConn(t, client, clusterPeerID, branch)
	pc.Start()

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

// ── Concurrency, backpressure, handler dispatch ───────────────────────────

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

	fake := client.slaveConnHandler.(*fakeSlaveService)
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
	pc, _ := newRecordingPeerConn(t, client, clusterPeerID, branch)

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
	pingMaster(t, serverConn, 1)

	// Nothing was dispatched: the stalled peer's queue absorbed the burst.
	if pc.IsClosed() {
		t.Fatal("PeerConn closed while absorbing the burst; delivery must be non-blocking and non-fatal")
	}
}

// ── Typed outbound wrapper tests ────────────────────────────────────────
//
// These verify the connection-layer API surface (opcode, rpc_id, payload
// round-trip, and stamped Meta) only. They intentionally do not exercise
// business logic such as when to broadcast or what to construct from shard
// state.

// TestPeerConn_SendNewBlock verifies the typed SendNewBlock wrapper writes a
// NEW_BLOCK_MINOR fire-and-forget command with rpc_id 0 and the peer's
// branch + cluster_peer_id metadata. Python: PeerShardConnection.send_new_block.
func TestPeerConn_SendNewBlock(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 91
	const branch uint32 = 0x00010001
	fake := client.slaveConnHandler.(*fakeSlaveService)
	fake.createPeerConns(clusterPeerID, []uint32{branch})
	pc := fake.peers[clusterPeerID][branch]

	if err := pc.SendNewBlock(&wire.NewBlockMinorCommand{Block: &wire.RawBytes{}}); err != nil {
		t.Fatalf("SendNewBlock: %v", err)
	}

	req := readMasterFrame(t, serverConn)
	if req.Opcode != byte(wire.CommandOpNewBlockMinor) {
		t.Fatalf("expected opcode 0x%x, got 0x%x", wire.CommandOpNewBlockMinor, req.Opcode)
	}
	if req.RPCID != 0 {
		t.Fatalf("expected rpc_id 0 for command, got %d", req.RPCID)
	}
	if req.Meta.ClusterPeerID != clusterPeerID || req.Meta.Branch != branch {
		t.Fatalf("meta mismatch: got %+v, want cid=%d branch=0x%x", req.Meta, clusterPeerID, branch)
	}
	var out wire.NewBlockMinorCommand
	if err := serialize.Deserialize(serialize.NewByteBuffer(req.Payload), &out); err != nil {
		t.Fatalf("deserialize payload: %v", err)
	}
}

// TestPeerConn_SendNewMinorBlockHeaderList verifies the typed
// SendNewMinorBlockHeaderList wrapper writes a NEW_MINOR_BLOCK_HEADER_LIST
// fire-and-forget command with rpc_id 0 and the peer's branch +
// cluster_peer_id metadata.
// Python: PeerShardConnection.broadcast_new_tip (outbound primitive only).
func TestPeerConn_SendNewMinorBlockHeaderList(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 92
	const branch uint32 = 0x00010001
	fake := client.slaveConnHandler.(*fakeSlaveService)
	fake.createPeerConns(clusterPeerID, []uint32{branch})
	pc := fake.peers[clusterPeerID][branch]

	cmd := &wire.NewMinorBlockHeaderListCommand{
		RootBlockHeader:      &wire.RawBytes{},
		MinorBlockHeaderList: []*wire.RawBytes{{}},
	}
	if err := pc.SendNewMinorBlockHeaderList(cmd); err != nil {
		t.Fatalf("SendNewMinorBlockHeaderList: %v", err)
	}

	req := readMasterFrame(t, serverConn)
	if req.Opcode != byte(wire.CommandOpNewMinorBlockHeaderList) {
		t.Fatalf("expected opcode 0x%x, got 0x%x", wire.CommandOpNewMinorBlockHeaderList, req.Opcode)
	}
	if req.RPCID != 0 {
		t.Fatalf("expected rpc_id 0 for command, got %d", req.RPCID)
	}
	if req.Meta.ClusterPeerID != clusterPeerID || req.Meta.Branch != branch {
		t.Fatalf("meta mismatch: got %+v, want cid=%d branch=0x%x", req.Meta, clusterPeerID, branch)
	}
	var out wire.NewMinorBlockHeaderListCommand
	if err := serialize.Deserialize(serialize.NewByteBuffer(req.Payload), &out); err != nil {
		t.Fatalf("deserialize payload: %v", err)
	}
}

// TestPeerConn_SendTransactionList verifies the typed SendTransactionList
// wrapper writes a NEW_TRANSACTION_LIST fire-and-forget command with rpc_id 0
// and the peer's branch + cluster_peer_id metadata.
// Python: PeerShardConnection.broadcast_tx_list.
func TestPeerConn_SendTransactionList(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 93
	const branch uint32 = 0x00010001
	fake := client.slaveConnHandler.(*fakeSlaveService)
	fake.createPeerConns(clusterPeerID, []uint32{branch})
	pc := fake.peers[clusterPeerID][branch]

	cmd := &wire.NewTransactionListCommand{TransactionList: []*wire.RawBytes{{}}}
	if err := pc.SendTransactionList(cmd); err != nil {
		t.Fatalf("SendTransactionList: %v", err)
	}

	req := readMasterFrame(t, serverConn)
	if req.Opcode != byte(wire.CommandOpNewTransactionList) {
		t.Fatalf("expected opcode 0x%x, got 0x%x", wire.CommandOpNewTransactionList, req.Opcode)
	}
	if req.RPCID != 0 {
		t.Fatalf("expected rpc_id 0 for command, got %d", req.RPCID)
	}
	if req.Meta.ClusterPeerID != clusterPeerID || req.Meta.Branch != branch {
		t.Fatalf("meta mismatch: got %+v, want cid=%d branch=0x%x", req.Meta, clusterPeerID, branch)
	}
	var out wire.NewTransactionListCommand
	if err := serialize.Deserialize(serialize.NewByteBuffer(req.Payload), &out); err != nil {
		t.Fatalf("deserialize payload: %v", err)
	}
}

// TestPeerConn_GetMinorBlockList verifies the typed GetMinorBlockList wrapper
// issues a GET_MINOR_BLOCK_LIST_REQUEST RPC and parses the echoed response,
// with the peer's branch + cluster_peer_id metadata stamped on the wire.
// Python: PeerShardConnection.write_rpc_request(GET_MINOR_BLOCK_LIST_REQUEST).
func TestPeerConn_GetMinorBlockList(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 101
	const branch uint32 = 0x00010001
	fake := client.slaveConnHandler.(*fakeSlaveService)
	fake.createPeerConns(clusterPeerID, []uint32{branch})
	pc := fake.peers[clusterPeerID][branch]

	req := &wire.GetMinorBlockListRequest{MinorBlockHashList: [][wire.HashLength]byte{}}

	go func() {
		frame := readMasterFrame(t, serverConn)
		if frame.Meta.ClusterPeerID != clusterPeerID || frame.Meta.Branch != branch {
			t.Errorf("outbound request meta mismatch: got cid=%d branch=0x%x, want cid=%d branch=0x%x",
				frame.Meta.ClusterPeerID, frame.Meta.Branch, clusterPeerID, branch)
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

	resp, err := pc.GetMinorBlockList(ctx, req)
	if err != nil {
		t.Fatalf("GetMinorBlockList: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected non-nil GetMinorBlockListResponse")
	}
}

// TestPeerConn_GetMinorBlockHeaderList verifies the typed GetMinorBlockHeaderList
// wrapper issues a GET_MINOR_BLOCK_HEADER_LIST_REQUEST RPC and parses the
// response, with the peer's branch + cluster_peer_id metadata stamped on the
// wire. Python: SyncTask.__download_block_headers (shard.py:441-451).
func TestPeerConn_GetMinorBlockHeaderList(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 102
	const branch uint32 = 0x00010001
	fake := client.slaveConnHandler.(*fakeSlaveService)
	fake.createPeerConns(clusterPeerID, []uint32{branch})
	pc := fake.peers[clusterPeerID][branch]

	req := &wire.GetMinorBlockHeaderListRequest{
		BlockHash: [wire.HashLength]byte{},
		Branch:    branch,
		Limit:     1,
		Direction: wire.DirectionGenesis,
	}

	go func() {
		frame := readMasterFrame(t, serverConn)
		if frame.Meta.ClusterPeerID != clusterPeerID || frame.Meta.Branch != branch {
			t.Errorf("outbound request meta mismatch: got cid=%d branch=0x%x, want cid=%d branch=0x%x",
				frame.Meta.ClusterPeerID, frame.Meta.Branch, clusterPeerID, branch)
		}
		if frame.Opcode != byte(wire.CommandOpGetMinorBlockHeaderListRequest) {
			t.Errorf("unexpected request opcode 0x%x", frame.Opcode)
		}
		respPayload, err := serialize.SerializeToBytes(&wire.GetMinorBlockHeaderListResponse{
			RootTip:  &wire.RawBytes{},
			ShardTip: &wire.RawBytes{},
		})
		if err != nil {
			t.Errorf("serialize response: %v", err)
			return
		}
		writeMasterFrame(t, serverConn, &wire.Frame{
			Meta:    frame.Meta,
			Opcode:  byte(wire.CommandOpGetMinorBlockHeaderListResponse),
			RPCID:   frame.RPCID,
			Payload: respPayload,
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := pc.GetMinorBlockHeaderList(ctx, req)
	if err != nil {
		t.Fatalf("GetMinorBlockHeaderList: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected non-nil GetMinorBlockHeaderListResponse")
	}
}

// TestPeerConn_ConstructionValidation verifies the NewPeerConn invariants.
//
// The reserved cluster_peer_id check is the notable one: a PeerConn represents
// peer traffic, so the master-local id 0 (py: RESERVED_CLUSTER_PEER_ID, only
// used for master↔slave traffic) can never become a PeerConn identity. Go
// rejects it at the creation entry rather than at write time (Python defers to
// get_metadata_to_write because its CREATE handler accepts cid=0).
func TestPeerConn_ConstructionValidation(t *testing.T) {
	client, _, cleanup := newMasterConn(t)
	defer cleanup()

	cases := []struct {
		name          string
		clusterPeerID uint64
		masterConn    *MasterConn
		handler       PeerHandler
	}{
		{"nil master connection", 1, nil, stubPeerHandler{}},
		{"nil peer handler", 1, client, nil},
		{"reserved cluster peer id", 0, client, stubPeerHandler{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := NewPeerConn(c.clusterPeerID, 0x00010001, c.masterConn, c.handler, log.New()); err == nil {
				t.Fatal("expected NewPeerConn to reject the invalid configuration")
			}
		})
	}
}

// TestPeerConn_InboundHandlerDispatch walks every opcode PeerConn registers:
// the frame must reach the matching PeerHandler method with the decoded
// request, and only RPC opcodes may produce a response.
//
// This is the PeerConn half of the routing contract — routeFrame decides which
// PeerConn gets the frame, this test decides what the PeerConn then does with
// it. Without it, four of the six handlers are entirely unexercised.
// Python: PeerShardConnection OP_SERIALIZER_MAP / OP_NONRPC_MAP / OP_RPC_MAP.
func TestPeerConn_InboundHandlerDispatch(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 61
	const branch uint32 = 0x00010001

	pc, handler := newRecordingPeerConn(t, client, clusterPeerID, branch)
	pc.Start()

	cases := []struct {
		name    string
		op      wire.CommandOp
		request any
		wantReq any // expected dynamic type handed to the handler
		respOp  wire.CommandOp
	}{
		{
			name:    "NewMinorBlockHeaderList",
			op:      wire.CommandOpNewMinorBlockHeaderList,
			request: &wire.NewMinorBlockHeaderListCommand{RootBlockHeader: &wire.RawBytes{}, MinorBlockHeaderList: []*wire.RawBytes{{0x01}}},
			wantReq: &wire.NewMinorBlockHeaderListCommand{},
		},
		{
			name:    "NewTransactionList",
			op:      wire.CommandOpNewTransactionList,
			request: &wire.NewTransactionListCommand{TransactionList: []*wire.RawBytes{{0x02}}},
			wantReq: &wire.NewTransactionListCommand{},
		},
		{
			name:    "NewBlockMinor",
			op:      wire.CommandOpNewBlockMinor,
			request: &wire.NewBlockMinorCommand{Block: &wire.RawBytes{}},
			wantReq: &wire.NewBlockMinorCommand{},
		},
		{
			name:    "GetMinorBlockList",
			op:      wire.CommandOpGetMinorBlockListRequest,
			request: &wire.GetMinorBlockListRequest{MinorBlockHashList: [][wire.HashLength]byte{}},
			wantReq: &wire.GetMinorBlockListRequest{},
			respOp:  wire.CommandOpGetMinorBlockListResponse,
		},
		{
			name:    "GetMinorBlockHeaderList",
			op:      wire.CommandOpGetMinorBlockHeaderListRequest,
			request: &wire.GetMinorBlockHeaderListRequest{Branch: branch, Limit: 1, Direction: wire.DirectionGenesis},
			wantReq: &wire.GetMinorBlockHeaderListRequest{},
			respOp:  wire.CommandOpGetMinorBlockHeaderListResponse,
		},
		{
			name:    "GetMinorBlockHeaderListWithSkip",
			op:      wire.CommandOpGetMinorBlockHeaderListWithSkipRequest,
			request: &wire.GetMinorBlockHeaderListWithSkipRequest{Branch: branch, Limit: 1, Direction: wire.DirectionGenesis},
			wantReq: &wire.GetMinorBlockHeaderListWithSkipRequest{},
			respOp:  wire.CommandOpGetMinorBlockHeaderListWithSkipResponse,
		},
	}

	// rpcID is shared by the RPC cases: a single PeerConn enforces a strictly
	// increasing inbound sequence, so every RPC case must get its own value.
	rpcID := uint64(0)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			payload, err := serialize.SerializeToBytes(c.request)
			if err != nil {
				t.Fatalf("serialize request: %v", err)
			}

			// Fire-and-forget commands carry rpc_id 0; RPC requests get the
			// next value of the per-connection sequence.
			isRPC := c.respOp != 0
			if isRPC {
				rpcID++
			}
			writeMasterFrame(t, serverConn, &wire.Frame{
				Meta:    wire.ClusterMetadata{Branch: branch, ClusterPeerID: clusterPeerID},
				Opcode:  byte(c.op),
				RPCID:   rpcID,
				Payload: payload,
			})

			call := handler.next(t, 2*time.Second)
			if call.opcode != byte(c.op) {
				t.Fatalf("handler invoked for opcode 0x%x, want 0x%x", call.opcode, c.op)
			}
			gotType, wantType := fmt.Sprintf("%T", call.req), fmt.Sprintf("%T", c.wantReq)
			if gotType != wantType {
				t.Fatalf("handler received %s, want %s", gotType, wantType)
			}

			if !isRPC {
				expectNoFrame(t, serverConn, 200*time.Millisecond)
				return
			}

			resp := readMasterFrame(t, serverConn)
			if resp.Opcode != byte(c.respOp) {
				t.Fatalf("response opcode: got 0x%x, want 0x%x", resp.Opcode, c.respOp)
			}
			if resp.RPCID != rpcID {
				t.Fatalf("rpc_id echo: got %d, want %d", resp.RPCID, rpcID)
			}
			if resp.Meta.ClusterPeerID != clusterPeerID || resp.Meta.Branch != branch {
				t.Fatalf("response meta mismatch: got %+v, want cid=%d branch=0x%x", resp.Meta, clusterPeerID, branch)
			}
		})
	}

	if pc.IsClosed() {
		t.Fatal("PeerConn closed while dispatching valid requests")
	}
}

// TestPeerConn_CloseDoesNotCloseMasterConn is the mirror of
// TestMasterConn_CloseDoesNotClosePeerConns: several PeerConns share one
// MasterConn as their transport, so closing a virtual peer endpoint must not
// take the physical connection down with it.
func TestPeerConn_CloseDoesNotCloseMasterConn(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	pc, _ := newRecordingPeerConn(t, client, 51, 0x00010001)
	pc.Start()

	select {
	case <-pc.WaitUntilClosed():
		t.Fatal("PeerConn closed before Close was called")
	default:
	}

	pc.Close()

	select {
	case <-pc.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("PeerConn did not close")
	}

	// The shared MasterConn is owned by the slave server, not by PeerConn.
	if client.IsClosed() {
		t.Fatal("closing a PeerConn closed the shared MasterConn")
	}
	pingMaster(t, serverConn, 1)
}

// TestPeerConn_HandleFrameConcurrentWithClose hammers HandleFrame from several
// goroutines while the PeerConn is closed underneath them. HandleFrame is the
// injection entry driven by MasterConn's reader goroutine, so it must stay
// race-free and non-blocking against Close: every call either enqueues the
// frame or reports ErrConnectionClosed, and none may panic or hang.
//
// The peer is deliberately left unstarted so the queue is never drained — the
// worst case for a producer racing with Close.
func TestPeerConn_HandleFrameConcurrentWithClose(t *testing.T) {
	client, serverConn, cleanup := newMasterConn(t)
	defer cleanup()

	const clusterPeerID uint64 = 52
	const branch uint32 = 0x00010001

	pc, _ := newRecordingPeerConn(t, client, clusterPeerID, branch)

	cmdPayload, err := serialize.SerializeToBytes(&wire.NewTransactionListCommand{TransactionList: []*wire.RawBytes{{}}})
	if err != nil {
		t.Fatalf("serialize command: %v", err)
	}

	const senders = 8
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := pc.HandleFrame(&wire.Frame{
					Meta:    wire.ClusterMetadata{Branch: branch, ClusterPeerID: clusterPeerID},
					Opcode:  byte(wire.CommandOpNewTransactionList),
					RPCID:   0,
					Payload: cmdPayload,
				})
				// After Close the only acceptable outcome is a clean rejection.
				if err != nil && !errors.Is(err, conn.ErrConnectionClosed) {
					t.Errorf("HandleFrame: unexpected error %v", err)
					return
				}
			}
		}()
	}

	// Let the senders build up a backlog before and after the close.
	time.Sleep(20 * time.Millisecond)
	pc.Close()
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()

	// The peer's own close must not have disturbed the shared transport.
	if client.IsClosed() {
		t.Fatal("closing a PeerConn closed the shared MasterConn")
	}
	pingMaster(t, serverConn, 1)
}
