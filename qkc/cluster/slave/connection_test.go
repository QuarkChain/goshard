// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

type fakeFrameTransport struct {
	frames     chan *wire.Frame
	writes     chan *wire.Frame
	closed     chan struct{}
	closeOnce  sync.Once
	closeMu    sync.Mutex
	closeCount int
}

func newFakeFrameTransport() *fakeFrameTransport {
	return &fakeFrameTransport{
		frames: make(chan *wire.Frame, 8),
		writes: make(chan *wire.Frame, 8),
		closed: make(chan struct{}),
	}
}

func (t *fakeFrameTransport) readFrame() (*wire.Frame, error) {
	select {
	case frame := <-t.frames:
		return frame, nil
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *fakeFrameTransport) writeFrame(frame *wire.Frame) error {
	select {
	case t.writes <- frame:
		return nil
	case <-t.closed:
		return errors.New("fake transport closed")
	}
}

func (t *fakeFrameTransport) close() error {
	t.closeOnce.Do(func() {
		t.closeMu.Lock()
		t.closeCount++
		t.closeMu.Unlock()
		close(t.closed)
	})
	return nil
}

func (t *fakeFrameTransport) RemoteAddr() string {
	return "fake"
}

func (t *fakeFrameTransport) closes() int {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	return t.closeCount
}

// writeRawFrame writes a raw frame directly to the underlying TCP connection,
// bypassing the connection's frame writer. Used to craft malformed/invalid frames
// for protocol-validation tests.
func writeRawFrame(t *testing.T, conn net.Conn, frame *wire.Frame) {
	t.Helper()
	if err := wire.WriteFrameNoMeta(conn, frame); err != nil {
		t.Fatalf("write raw frame: %v", err)
	}
}

func TestBaseConn_ConcurrentSendRPCMetaAndClose(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newBaseConn(tr, log.New())
	conn.Start()

	const senders = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(senders)
	for i := 0; i < senders; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
			if err != nil && err != ErrConnectionClosed && !errors.Is(err, io.EOF) {
				t.Errorf("unexpected SendRPC error: %v", err)
			}
		}()
	}

	close(start)
	conn.Close()
	wg.Wait()

	conn.pendingMu.Lock()
	pending := len(conn.pending)
	conn.pendingMu.Unlock()
	if pending != 0 {
		t.Fatalf("pending RPCs remain after Close: %d", pending)
	}
	if conn.State() != ConnectionStateClosed {
		t.Fatalf("expected closed state, got %v", conn.State())
	}
}

func TestBaseConn_LateResponseAfterTimeoutKeepsConnectionActive(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newBaseConn(tr, log.New())
	conn.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), nil)
	if err == nil {
		t.Fatal("expected RPC timeout")
	}

	select {
	case <-tr.writes:
	case <-time.After(time.Second):
		t.Fatal("fake transport did not receive request")
	}
	tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: 1}

	select {
	case <-time.After(20 * time.Millisecond):
		if !conn.IsActive() {
			t.Fatal("late response closed the connection")
		}
	case <-conn.WaitUntilClosed():
		t.Fatal("late response closed the connection")
	}
	defer conn.Close()

	conn.pendingMu.Lock()
	pending := len(conn.pending)
	conn.pendingMu.Unlock()
	if pending != 0 {
		t.Fatalf("timed-out RPC remains pending: %d", pending)
	}
}

func TestBaseConn_PendingRPCRemovedAfterResponse(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newBaseConn(tr, log.New())
	conn.Start()
	defer conn.Close()

	result := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		result <- err
	}()

	select {
	case request := <-tr.writes:
		tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: request.RPCID}
	case <-time.After(time.Second):
		t.Fatal("fake transport did not receive request")
	}
	if err := <-result; err != nil {
		t.Fatalf("SendRPC failed: %v", err)
	}

	conn.pendingMu.Lock()
	pending := len(conn.pending)
	conn.pendingMu.Unlock()
	if pending != 0 {
		t.Fatalf("pending RPC remains after response: %d", pending)
	}
}

func TestBaseConn_DoubleClose(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newBaseConn(tr, log.New())

	if err := conn.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
	if got := tr.closes(); got != 1 {
		t.Fatalf("transport closed %d times, want 1", got)
	}
}

// newTestConnPair creates a pair of XshardConns connected over a local TCP
// socket. The caller is responsible for calling cleanup.
func newTestConnPair(t *testing.T) (client, server *XshardConn, cleanup func()) {
	t.Helper()
	return newTestConnPairWithIdentity(t, []byte("client-slave"), []uint32{0x00010001}, []byte("server-slave"), []uint32{0x00030004})
}

func newTestConnPairWithIdentity(t *testing.T, clientID []byte, clientShards []uint32, serverID []byte, serverShards []uint32) (client, server *XshardConn, cleanup func()) {
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
	client = NewXshardConnFromConn(clientConn, 0, clientID, clientShards, logger) // 0 = no limit (matches Python)
	server = NewXshardConnFromConn(serverConn, 0, serverID, serverShards, logger)
	cleanup = func() {
		client.Close()
		server.Close()
	}
	return
}

// TestDispatch_UnsupportedOpcodeClosesConnection verifies that receiving a
// frame for an opcode with no registered handler causes the connection to close.
func TestDispatch_UnsupportedOpcodeClosesConnection(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()

	server.Start()
	client.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.SendRPC(ctx, byte(wire.ClusterOpAddRootBlockRequest), []byte("payload"))
	if err == nil {
		t.Fatal("expected error due to connection close, got nil")
	}
}

// TestBaseConn_CloseWakesPendingRPC verifies that Close wakes all pending RPCs.
func TestBaseConn_CloseWakesPendingRPC(t *testing.T) {
	client, _, cleanup := newTestConnPair(t)
	defer cleanup()

	// Server intentionally left unstarted so it never replies.
	client.Start()

	var wg sync.WaitGroup
	wg.Add(1)
	errChan := make(chan error, 1)
	go func() {
		wg.Done() // Signal that goroutine is ready
		_, err := client.SendRPC(context.Background(), byte(wire.ClusterOpPing), []byte("ping"))
		errChan <- err
	}()

	wg.Wait() // Wait for goroutine to start (reliable synchronization)
	client.Close()

	select {
	case err := <-errChan:
		if err != ErrConnectionClosed {
			t.Fatalf("expected ErrConnectionClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending RPC was not woken by Close")
	}
}

// TestBaseConn_RPCIDMonotonic verifies RPC ID monotonic validation.
// Sending a duplicate RPC ID causes the server to close the connection.
func TestBaseConn_RPCIDMonotonic(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()

	server.Start()
	client.Start()

	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client"),
		FullShardIDList: []uint32{0x00010001},
	})

	// Manually send two PING frames with the same RPC ID (=1).
	writeRawFrame(t, client.conn, &wire.Frame{
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	})
	writeRawFrame(t, client.conn, &wire.Frame{
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1, // duplicate rpc_id: should trigger close
		Payload: pingPayload,
	})

	select {
	case <-server.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not close connection after duplicate rpc_id")
	}

	if !server.IsClosed() {
		t.Fatal("server should be closed")
	}
}

// TestBaseConn_RPCIDDecreasing verifies that a decreasing RPC ID closes the
// connection.
func TestBaseConn_RPCIDDecreasing(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()

	server.Start()
	client.Start()

	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client"),
		FullShardIDList: []uint32{0x00010001},
	})

	// Send rpc_id=2 then rpc_id=1 (decreasing).
	writeRawFrame(t, client.conn, &wire.Frame{
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   2,
		Payload: pingPayload,
	})
	writeRawFrame(t, client.conn, &wire.Frame{
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1, // decreasing rpc_id: should trigger close
		Payload: pingPayload,
	})

	select {
	case <-server.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not close connection after decreasing rpc_id")
	}
}

// TestBaseConn_SequentialRPCs verifies that multiple sequential RPCs work
// correctly.
func TestBaseConn_SequentialRPCs(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()

	server.Start()
	client.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client"),
		FullShardIDList: []uint32{0x00010001},
	})

	for i := 0; i < 5; i++ {
		_, err := client.SendRPC(ctx, byte(wire.ClusterOpPing), pingPayload)
		if err != nil {
			t.Fatalf("rpc %d failed: %v", i+1, err)
		}
	}

	if !server.WaitUntilPingReceived() {
		t.Fatal("server did not receive ping")
	}
}

// TestDispatch_TrailingBytesClosesConnection verifies that a frame payload with
// trailing bytes after a valid message causes the connection to close. The
// deserializer must consume exactly the payload length — no more, no less.
func TestDispatch_TrailingBytesClosesConnection(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()

	server.Start()
	client.Start()

	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client"),
		FullShardIDList: []uint32{0x00010001},
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}
	malformedPayload := append(pingPayload, 0xFF)

	writeRawFrame(t, client.conn, &wire.Frame{
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: malformedPayload,
	})

	select {
	case <-server.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not close connection after trailing-bytes payload")
	}
}

// TestDispatch_ExactPayloadProcessesNormally verifies that a well-formed
// payload with no trailing bytes is processed and the connection stays open.
func TestDispatch_ExactPayloadProcessesNormally(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()

	server.Start()
	client.Start()

	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client"),
		FullShardIDList: []uint32{0x00010001},
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.SendRPC(ctx, byte(wire.ClusterOpPing), pingPayload)
	if err != nil {
		t.Fatalf("send ping rpc: %v", err)
	}
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected opcode 0x%x, got 0x%x", wire.ClusterOpPong, resp.Opcode)
	}
	if server.IsClosed() {
		t.Fatal("server should remain open after well-formed exchange")
	}
}
