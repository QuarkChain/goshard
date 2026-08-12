// Copyright 2026-2027, QuarkChain.

package slave

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// waitForCondition polls f until it returns true or the timeout expires.
func waitForCondition(t *testing.T, timeout time.Duration, f func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if f() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("condition not met within %v", timeout)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// newMasterTestConnPair creates a pair of MasterConns connected over a local TCP
// socket. The caller is responsible for calling cleanup.
func newMasterTestConnPair(t *testing.T) (client, server *MasterConn, cleanup func()) {
	t.Helper()
	return newMasterTestConnPairWithIdentity(
		t,
		[]byte("go-slave-client"), []uint32{0x00010001},
		[]byte("go-slave-server"), []uint32{0x00010001, 0x00020001},
	)
}

func newMasterTestConnPairWithIdentity(
	t *testing.T,
	clientID []byte, clientShards []uint32,
	serverID []byte, serverShards []uint32,
) (client, server *MasterConn, cleanup func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var serverConn net.Conn
	var acceptErr error
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		serverConn, acceptErr = ln.Accept()
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
	client = NewMasterConnFromConn(clientConn, 0, clientID, clientShards, logger)
	server = NewMasterConnFromConn(serverConn, 0, serverID, serverShards, logger)
	cleanup = func() {
		client.Close()
		server.Close()
	}
	return
}

// masterFakeTransport is a test-only FrameTransport that injects frames into
// the readerLoop and captures outbound writes. It implements
// interruptibleTransport so BaseConn can unblock pending reads/writes during
// shutdown without relying on a real net.Conn.
type masterFakeTransport struct {
	frames    chan *wire.Frame
	writes    chan *wire.Frame
	closed    chan struct{}
	closeOnce sync.Once
}

func newMasterFakeTransport() *masterFakeTransport {
	return &masterFakeTransport{
		frames: make(chan *wire.Frame, 16),
		writes: make(chan *wire.Frame, 16),
		closed: make(chan struct{}),
	}
}

func (t *masterFakeTransport) ReadFrame() (*wire.Frame, error) {
	select {
	case f := <-t.frames:
		return f, nil
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *masterFakeTransport) WriteFrame(f *wire.Frame) error {
	select {
	case t.writes <- f:
		return nil
	case <-t.closed:
		return net.ErrClosed
	}
}

func (t *masterFakeTransport) interrupt() error {
	return t.Close()
}

func (t *masterFakeTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func (t *masterFakeTransport) RemoteAddr() string {
	return "fake-master"
}

// newMasterConnWithFakeTransport creates a MasterConn backed by a fake
// transport. Frames injected via tr.frames are processed through the full
// readerLoop → dispatch path; responses are captured via tr.writes.
func newMasterConnWithFakeTransport(
	t *testing.T,
	localID []byte,
	localFullShardIDList []uint32,
) (*MasterConn, *masterFakeTransport) {
	t.Helper()
	tr := newMasterFakeTransport()
	mc := &MasterConn{
		BaseConn:             conn.NewBaseConn(tr, log.New()),
		localID:              append([]byte(nil), localID...),
		localFullShardIDList: append([]uint32(nil), localFullShardIDList...),
		lastMasterRPCID:      -1,
	}
	mc.BaseConn.SetValidateRPCID(mc.validateMasterRPCID)
	mc.registerOpSerializers()
	mc.registerHandlers()
	return mc, tr
}

// TestMasterConn_CommunicationHandlersRegistered verifies that communication
// handlers (PING and fire-and-forget) are registered and respond correctly.
func TestMasterConn_CommunicationHandlersRegistered(t *testing.T) {
	client, server, cleanup := newMasterTestConnPair(t)
	defer cleanup()

	server.Start()
	client.Start()

	// PING must return PONG.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	resp, err := client.SendRPCMeta(ctx, byte(wire.ClusterOpPing), pingPayload, wire.ClusterMetadata{})
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected PONG, got 0x%x", resp.Opcode)
	}
}

// TestMasterConn_BusinessHandlerReturnsNotImplemented verifies that business
// handlers return ErrHandlerNotImplemented and close the connection.
func TestMasterConn_BusinessHandlerReturnsNotImplemented(t *testing.T) {
	server, tr := newMasterConnWithFakeTransport(t, []byte("server"), []uint32{0x00010001})
	defer server.Close()

	server.Start()

	payload, _ := serialize.SerializeToBytes(&wire.GetEcoInfoListRequest{})
	tr.frames <- &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpGetEcoInfoListRequest),
		RPCID:   1,
		Payload: payload,
	}

	select {
	case <-server.WaitUntilClosed():
		// Connection closed as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("server did not close after business handler returned ErrHandlerNotImplemented")
	}
}

// TestMasterConn_UnknownOpcodeClosesConnection verifies that an opcode without
// any handler causes the connection to close.
func TestMasterConn_UnknownOpcodeClosesConnection(t *testing.T) {
	server, tr := newMasterConnWithFakeTransport(t, []byte("server"), []uint32{0x00010001})
	defer server.Close()

	server.Start()

	payload, _ := serialize.SerializeToBytes(&wire.GetEcoInfoListRequest{})

	// 0xEE is not registered as a handler or serializer.
	tr.frames <- &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  0xEE,
		RPCID:   0,
		Payload: payload,
	}

	select {
	case <-server.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not close after unknown opcode")
	}
}

// TestMasterConn_Ping verifies the master→slave PING handshake.
func TestMasterConn_Ping(t *testing.T) {
	clientID := []byte("go-slave-client")
	clientShards := []uint32{0x00010001}
	serverID := []byte("go-slave-server")
	serverShards := []uint32{0x00010001, 0x00020001}

	client, server, cleanup := newMasterTestConnPairWithIdentity(t, clientID, clientShards, serverID, serverShards)
	defer cleanup()

	server.Start()
	client.Start()

	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
		RootTip:         nil,
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.SendRPCMeta(ctx, byte(wire.ClusterOpPing), pingPayload, wire.ClusterMetadata{})
	if err != nil {
		t.Fatalf("send ping: %v", err)
	}
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected opcode 0x%x, got 0x%x", wire.ClusterOpPong, resp.Opcode)
	}

	var pong wire.PongResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &pong); err != nil {
		t.Fatalf("deserialize pong: %v", err)
	}
	if string(pong.ID) != string(serverID) {
		t.Fatalf("pong id mismatch: got %s, expected %s", pong.ID, serverID)
	}
	if len(pong.FullShardIDList) != len(serverShards) {
		t.Fatalf("pong shard list mismatch: got %v", pong.FullShardIDList)
	}
}

// TestMasterConn_NonRPCDispatch verifies that the fire-and-forget
// DESTROY_CLUSTER_PEER_CONNECTION_COMMAND is accepted with rpc_id == 0 and does
// not produce a response or close the connection.
func TestMasterConn_NonRPCDispatch(t *testing.T) {
	server, tr := newMasterConnWithFakeTransport(t, []byte("server"), []uint32{0x00010001})
	defer server.Close()

	server.Start()

	// Fire-and-forget: rpc_id == 0, no response expected.
	payload, _ := serialize.SerializeToBytes(&wire.DestroyClusterPeerConnectionCommand{ClusterPeerID: 42})
	tr.frames <- &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpDestroyClusterPeerConnectionCommand),
		RPCID:   0,
		Payload: payload,
	}

	// A subsequent RPC must still work.
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	tr.frames <- &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	}

	select {
	case resp := <-tr.writes:
		if resp.Opcode != byte(wire.ClusterOpPong) {
			t.Fatalf("expected pong, got opcode 0x%x", resp.Opcode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive pong after non-rpc command")
	}
}

// TestMasterConn_NonRPCWithNonZeroRPCID verifies that a non-RPC command with a
// non-zero rpc_id causes the server to close the connection.
func TestMasterConn_NonRPCWithNonZeroRPCID(t *testing.T) {
	server, tr := newMasterConnWithFakeTransport(t, []byte("server"), []uint32{0x00010001})
	defer server.Close()

	server.Start()

	payload, _ := serialize.SerializeToBytes(&wire.DestroyClusterPeerConnectionCommand{ClusterPeerID: 42})
	tr.frames <- &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpDestroyClusterPeerConnectionCommand),
		RPCID:   1, // non-RPC must have rpc_id == 0
		Payload: payload,
	}

	select {
	case <-server.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not close after non-rpc with non-zero rpc_id")
	}
}

// TestMasterConn_Forwarder verifies that frames with cluster_peer_id != 0 are
// routed through the forwarder hook and are not dispatched locally.
func TestMasterConn_Forwarder(t *testing.T) {
	server, tr := newMasterConnWithFakeTransport(t, []byte("server"), []uint32{0x00010001})
	defer server.Close()

	var forwardedMu sync.Mutex
	var forwarded []*wire.Frame
	server.BaseConn.SetForwarder(func(frame *wire.Frame) bool {
		if frame.Meta.ClusterPeerID == 0 {
			return false
		}
		forwardedMu.Lock()
		forwarded = append(forwarded, frame)
		forwardedMu.Unlock()
		return true
	})

	server.Start()

	// Peer-originated frame: cluster_peer_id != 0.
	payload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("peer"),
		FullShardIDList: []uint32{0x00010001},
	})
	tr.frames <- &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001, ClusterPeerID: 123},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   7,
		Payload: payload,
	}

	waitForCondition(t, 2*time.Second, func() bool {
		forwardedMu.Lock()
		count := len(forwarded)
		forwardedMu.Unlock()
		return count == 1
	})

	// Connection should still be open; a subsequent master RPC works.
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{ID: []byte("m"), FullShardIDList: []uint32{1}})
	tr.frames <- &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	}

	select {
	case resp := <-tr.writes:
		if resp.Opcode != byte(wire.ClusterOpPong) {
			t.Fatalf("expected pong, got 0x%x", resp.Opcode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ping after forwarded frame failed")
	}
}

// TestMasterConn_RPCIDMonotonic verifies that duplicate RPC IDs cause the
// server to close the connection.
func TestMasterConn_RPCIDMonotonic(t *testing.T) {
	server, tr := newMasterConnWithFakeTransport(t, []byte("server"), []uint32{0x00010001})
	defer server.Close()

	server.Start()

	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})

	tr.frames <- &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	}
	tr.frames <- &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1, // duplicate
		Payload: pingPayload,
	}

	select {
	case <-server.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not close after duplicate rpc_id")
	}
}

// TestMasterConn_RPCIDDecreasing verifies that a decreasing RPC ID causes the
// server to close the connection.
func TestMasterConn_RPCIDDecreasing(t *testing.T) {
	server, tr := newMasterConnWithFakeTransport(t, []byte("server"), []uint32{0x00010001})
	defer server.Close()

	server.Start()

	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})

	tr.frames <- &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   2,
		Payload: pingPayload,
	}
	tr.frames <- &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1, // decreasing
		Payload: pingPayload,
	}

	select {
	case <-server.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not close after decreasing rpc_id")
	}
}

// TestMasterConn_CloseWakesPendingRPC verifies that Close wakes all pending
// outbound RPCs with qkcconn.ErrConnectionClosed.
func TestMasterConn_CloseWakesPendingRPC(t *testing.T) {
	client, _, cleanup := newMasterTestConnPair(t)
	defer cleanup()

	// Server intentionally left unstarted so it never replies.
	client.Start()

	var wg sync.WaitGroup
	wg.Add(1)
	errChan := make(chan error, 1)
	go func() {
		wg.Done()
		_, err := client.SendRPCMeta(context.Background(), byte(wire.ClusterOpPing), []byte("ping"), wire.ClusterMetadata{})
		errChan <- err
	}()

	wg.Wait()
	client.Close()

	select {
	case err := <-errChan:
		if err != conn.ErrConnectionClosed {
			t.Fatalf("expected qkcconn.ErrConnectionClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending RPC was not woken by Close")
	}
}

// TestMasterConn_OutboundRPCMeta verifies that outbound RPCs from the slave
// encode ClusterMetadata correctly on the wire.
func TestMasterConn_OutboundRPCMeta(t *testing.T) {
	client, server, cleanup := newMasterTestConnPair(t)
	defer cleanup()

	// Register a custom handler that returns a valid response so the stub
	// handler (which closes the connection) is not invoked.
	server.RegisterTypedHandlers(map[byte]conn.TypedHandler{
		byte(wire.ClusterOpGetEcoInfoListRequest): func(req any) (any, error) {
			_ = req.(*wire.GetEcoInfoListRequest)
			return &wire.GetEcoInfoListResponse{ErrorCode: 0}, nil
		},
	})

	server.Start()
	client.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	meta := wire.ClusterMetadata{Branch: 0x00010001, ClusterPeerID: 99}
	payload, _ := serialize.SerializeToBytes(&wire.GetEcoInfoListRequest{})
	resp, err := client.SendRPCMeta(ctx, byte(wire.ClusterOpGetEcoInfoListRequest), payload, meta)
	if err != nil {
		t.Fatalf("send rpc: %v", err)
	}
	if resp.Opcode != byte(wire.ClusterOpGetEcoInfoListResponse) {
		t.Fatalf("unexpected response opcode 0x%x", resp.Opcode)
	}
	if resp.Meta.Branch != meta.Branch || resp.Meta.ClusterPeerID != meta.ClusterPeerID {
		t.Fatalf("response metadata mismatch: got %+v, want %+v", resp.Meta, meta)
	}
}

// TestMasterConn_SendAddMinorBlockHeader verifies the typed outbound helper.
func TestMasterConn_SendAddMinorBlockHeader(t *testing.T) {
	client, server, cleanup := newMasterTestConnPair(t)
	defer cleanup()

	server.RegisterTypedHandlers(map[byte]conn.TypedHandler{
		byte(wire.ClusterOpAddMinorBlockHeaderRequest): func(req any) (any, error) {
			r := req.(*wire.AddMinorBlockHeaderRequest)
			if r.TxCount != 5 {
				t.Fatalf("unexpected tx_count: %d", r.TxCount)
			}
			return &wire.AddMinorBlockHeaderResponse{ErrorCode: 0}, nil
		},
	})

	server.Start()
	client.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &wire.AddMinorBlockHeaderRequest{
		MinorBlockHeader:  &wire.RawBytes{},
		TxCount:           5,
		XShardTxCount:     0,
		CoinbaseAmountMap: &wire.RawBytes{},
		ShardStats:        wire.ShardStats{Branch: 0x00010001},
	}
	resp, err := client.SendAddMinorBlockHeader(ctx, req)
	if err != nil {
		t.Fatalf("SendAddMinorBlockHeader: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("unexpected error_code: %d", resp.ErrorCode)
	}
}

// TestClusterMetadata_Marshal verifies that ClusterMetadata is encoded as
// 4-byte branch followed by 8-byte cluster_peer_id (12 bytes total).
func TestClusterMetadata_Marshal(t *testing.T) {
	meta := wire.ClusterMetadata{Branch: 0x01020304, ClusterPeerID: 0x1122334455667788}
	b := wire.MarshalClusterMetadata(meta)
	if len(b) != 12 {
		t.Fatalf("metadata length: got %d, want 12", len(b))
	}
	if binary.BigEndian.Uint32(b[0:4]) != meta.Branch {
		t.Fatalf("branch mismatch")
	}
	if binary.BigEndian.Uint64(b[4:12]) != meta.ClusterPeerID {
		t.Fatalf("cluster_peer_id mismatch")
	}
}

// TestMasterConn_EmptyPayloadDeserialization verifies that request types with
// empty bodies deserialize correctly.
func TestMasterConn_EmptyPayloadDeserialization(t *testing.T) {
	server, tr := newMasterConnWithFakeTransport(t, []byte("server"), []uint32{0x00010001})
	defer server.Close()

	// Register a custom handler so the stub (which closes the connection) is not invoked.
	server.RegisterTypedHandlers(map[byte]conn.TypedHandler{
		byte(wire.ClusterOpGetEcoInfoListRequest): func(req any) (any, error) {
			_ = req.(*wire.GetEcoInfoListRequest)
			return &wire.GetEcoInfoListResponse{ErrorCode: 0}, nil
		},
	})

	server.Start()

	tr.frames <- &wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpGetEcoInfoListRequest),
		RPCID:   1,
		Payload: []byte{},
	}

	select {
	case resp := <-tr.writes:
		if resp.Opcode != byte(wire.ClusterOpGetEcoInfoListResponse) {
			t.Fatalf("expected GetEcoInfoListResponse, got 0x%x", resp.Opcode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive response for empty payload request")
	}
}

// TestMasterConn_MetadataPreserved verifies that request metadata is echoed
// back in the response.
func TestMasterConn_MetadataPreserved(t *testing.T) {
	client, server, cleanup := newMasterTestConnPair(t)
	defer cleanup()

	server.Start()
	client.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	meta := wire.ClusterMetadata{Branch: 0xDEADBEEF, ClusterPeerID: 0xCAFEBABECAFEBABE}
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{1},
	})
	resp, err := client.SendRPCMeta(ctx, byte(wire.ClusterOpPing), pingPayload, meta)
	if err != nil {
		t.Fatalf("send rpc: %v", err)
	}
	if resp.Meta != meta {
		t.Fatalf("metadata not preserved: got %+v, want %+v", resp.Meta, meta)
	}
}

// TestMasterConn_FrameWireLayout verifies the full ClusterMetadata frame layout
// written by MasterConn matches the Python protocol.
func TestMasterConn_FrameWireLayout(t *testing.T) {
	var buf bytes.Buffer
	frame := &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x01020304, ClusterPeerID: 0x1122334455667788},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   0xAABBCCDDEEFF0011,
		Payload: []byte{0xAA, 0xBB},
	}
	if err := wire.WriteFrame(&buf, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	wireBytes := buf.Bytes()
	if len(wireBytes) != 4+12+1+8+2 {
		t.Fatalf("frame length: got %d, want %d", len(wireBytes), 4+12+1+8+2)
	}

	if got := binary.BigEndian.Uint32(wireBytes[0:4]); got != 2 {
		t.Fatalf("payload_len: got %d, want 2", got)
	}
	if got := binary.BigEndian.Uint32(wireBytes[4:8]); got != frame.Meta.Branch {
		t.Fatalf("branch mismatch: got 0x%x", got)
	}
	if got := binary.BigEndian.Uint64(wireBytes[8:16]); got != frame.Meta.ClusterPeerID {
		t.Fatalf("cluster_peer_id mismatch: got 0x%x", got)
	}
	if wireBytes[16] != frame.Opcode {
		t.Fatalf("opcode mismatch: got 0x%x", wireBytes[16])
	}
	if got := binary.BigEndian.Uint64(wireBytes[17:25]); got != frame.RPCID {
		t.Fatalf("rpc_id mismatch: got 0x%x", got)
	}
	if !bytes.Equal(wireBytes[25:], frame.Payload) {
		t.Fatalf("payload mismatch: got %x", wireBytes[25:])
	}
}

// TestMasterConn_ForwardFrameConcurrentRace verifies that concurrent SendRPC
// and ForwardFrame do not cause a data race on FrameTransport.WriteFrame
// (which uses a non-thread-safe bufio.Writer in the real transport). After the
// fix, ForwardFrame routes through the owner goroutine → writer mailbox →
// writerLoop, so all writes to bufio.Writer are serialized.
func TestMasterConn_ForwardFrameConcurrentRace(t *testing.T) {
	client, server, cleanup := newMasterTestConnPair(t)
	defer cleanup()
	server.Start()
	client.Start()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
				ID:              []byte("master"),
				FullShardIDList: []uint32{0x00010001},
			})
			client.SendRPCMeta(ctx, byte(wire.ClusterOpPing), pingPayload, wire.ClusterMetadata{})
			cancel()
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			server.ForwardFrame(&wire.Frame{
				Meta:    wire.ClusterMetadata{Branch: 0x00010001, ClusterPeerID: 99},
				Opcode:  byte(wire.ClusterOpPong),
				RPCID:   uint64(1000),
				Payload: []byte{0x01},
			})
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestMasterConn_ForwardFramePreservesRPCIDAndMeta verifies that ForwardFrame
// preserves the frame's RPCID, metadata, opcode, and payload through the
// full owner event → writer mailbox → writerLoop path.
func TestMasterConn_ForwardFramePreservesRPCIDAndMeta(t *testing.T) {
	server, tr := newMasterConnWithFakeTransport(t, []byte("server"), []uint32{0x00010001})
	defer server.Close()
	server.Start()

	frame := &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0xDEAD, ClusterPeerID: 0xBEEF},
		Opcode:  byte(wire.ClusterOpPong),
		RPCID:   0x1122334455667788,
		Payload: []byte{0xAA, 0xBB, 0xCC},
	}
	if err := server.ForwardFrame(frame); err != nil {
		t.Fatalf("ForwardFrame failed: %v", err)
	}

	select {
	case written := <-tr.writes:
		if written.Meta != frame.Meta {
			t.Fatalf("Meta not preserved: got %+v, want %+v", written.Meta, frame.Meta)
		}
		if written.RPCID != frame.RPCID {
			t.Fatalf("RPCID not preserved: got %d, want %d", written.RPCID, frame.RPCID)
		}
		if written.Opcode != frame.Opcode {
			t.Fatalf("Opcode not preserved: got 0x%x, want 0x%x", written.Opcode, frame.Opcode)
		}
		if !bytes.Equal(written.Payload, frame.Payload) {
			t.Fatalf("Payload not preserved: got %x, want %x", written.Payload, frame.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ForwardFrame frame was not written")
	}
}

// TestMasterConn_ForwardFrameAfterClose verifies that SubmitFrame (and
// therefore ForwardFrame) returns ErrConnectionClosed after the connection
// is closed and the writerLoop has stopped.
func TestMasterConn_ForwardFrameAfterClose(t *testing.T) {
	server, _ := newMasterConnWithFakeTransport(t, []byte("server"), []uint32{0x00010001})
	server.Start()
	if err := server.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	err := server.ForwardFrame(&wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpPong),
		RPCID:   1,
		Payload: []byte{0x01},
	})
	if err != conn.ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
}
