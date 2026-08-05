// Copyright 2026-2027, QuarkChain.

package conn

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

// ── fake transport ────────────────────────────────────────────────────────────

type fakeFrameTransport struct {
	frames            chan *wire.Frame
	writes            chan *wire.Frame
	closed            chan struct{}
	closeOnce         sync.Once
	closeMu           sync.Mutex
	closeCount        int
	writeMu           sync.Mutex
	writing           bool
	closeWhileWriting bool
	writeStarted      chan struct{}
	writeOnce         sync.Once
	releaseWrite      chan struct{}
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
		if t.releaseWrite != nil {
			t.writeMu.Lock()
			t.writing = true
			t.writeMu.Unlock()
			t.writeOnce.Do(func() { close(t.writeStarted) })
			<-t.releaseWrite
			t.writeMu.Lock()
			t.writing = false
			t.writeMu.Unlock()
		}
		return nil
	case <-t.closed:
		return errors.New("fake transport closed")
	}
}

func (t *fakeFrameTransport) close() error {
	t.closeOnce.Do(func() {
		t.writeMu.Lock()
		t.closeWhileWriting = t.writing
		t.writeMu.Unlock()
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

// ── TCP test pair helper ──────────────────────────────────────────────────────

// writeRawFrame writes a raw frame directly to the underlying TCP connection,
// bypassing the connection's frame writer. Used to craft malformed/invalid frames
// for protocol-validation tests.
func writeRawFrame(t *testing.T, conn net.Conn, frame *wire.Frame) {
	t.Helper()
	if err := wire.WriteFrameNoMeta(conn, frame); err != nil {
		t.Fatalf("write raw frame: %v", err)
	}
}

// newTestBaseConnPair creates a pair of BaseConns connected over a local TCP
// socket, with PING/PONG serializer and a minimal PING handler registered on the
// server side. The caller is responsible for calling cleanup.
func newTestBaseConnPair(t *testing.T) (client, server *BaseConn, cleanup func()) {
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
	readFrame := func(r io.Reader) (*wire.Frame, error) {
		return wire.ReadFrameNoMeta(r, 0)
	}
	client = NewBaseConnFromConn(clientConn, readFrame, wire.WriteFrameNoMeta, logger)
	server = NewBaseConnFromConn(serverConn, readFrame, wire.WriteFrameNoMeta, logger)

	// Register PING serializer and a minimal handler on the server so it can
	// deserialize PING requests and produce PONG responses.
	server.RegisterOpSerializers(map[byte]*OpSerializer{
		byte(wire.ClusterOpPing): OpSerializerFor[wire.PingRequest, wire.PongResponse](),
	})
	server.RegisterTypedHandlers(map[byte]TypedHandler{
		byte(wire.ClusterOpPing): func(req any) (any, error) {
			return &wire.PongResponse{}, nil
		},
	})

	cleanup = func() {
		client.Close()
		server.Close()
	}
	return
}

// ── baseConn unit tests (fake transport) ──────────────────────────────────────

func TestBaseConn_CloseWaitsForOutboundWrite(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writeStarted = make(chan struct{})
	tr.releaseWrite = make(chan struct{})
	conn := NewBaseConn(tr, log.New())
	conn.Start()

	result := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		result <- err
	}()

	select {
	case <-tr.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("fake transport did not start writing")
	}

	closeDone := make(chan struct{})
	go func() {
		conn.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("Close returned while write was blocked")
	case <-time.After(20 * time.Millisecond):
	}

	close(tr.releaseWrite)
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after write completed")
	}
	if tr.closeWhileWriting {
		t.Fatal("transport close ran concurrently with write")
	}
	if err := <-result; err != ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
}

func TestBaseConn_ConcurrentSendRPCMetaAndClose(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())
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

func TestBaseConn_LateResponseAfterTimeoutClosesConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())
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
	case <-conn.WaitUntilClosed():
	case <-time.After(time.Second):
		t.Fatal("late response did not close the connection")
	}

	conn.pendingMu.Lock()
	pending := len(conn.pending)
	conn.pendingMu.Unlock()
	if pending != 0 {
		t.Fatalf("timed-out RPC remains pending: %d", pending)
	}
}

func TestBaseConn_PendingRPCRemovedAfterResponse(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())
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
	conn := NewBaseConn(tr, log.New())

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

// ── baseConn integration tests (TCP pair) ─────────────────────────────────────

// TestBaseConn_CloseWakesPendingRPC verifies that Close wakes all pending RPCs.
func TestBaseConn_CloseWakesPendingRPC(t *testing.T) {
	client, _, cleanup := newTestBaseConnPair(t)
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
	client, server, cleanup := newTestBaseConnPair(t)
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
	client, server, cleanup := newTestBaseConnPair(t)
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
	client, server, cleanup := newTestBaseConnPair(t)
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
}

// TestDispatch_UnsupportedOpcodeClosesConnection verifies that receiving a
// frame for an opcode with no registered handler causes the connection to close.
func TestDispatch_UnsupportedOpcodeClosesConnection(t *testing.T) {
	client, server, cleanup := newTestBaseConnPair(t)
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

// TestDispatch_TrailingBytesClosesConnection verifies that a frame payload with
// trailing bytes after a valid message causes the connection to close. The
// deserializer must consume exactly the payload length — no more, no less.
func TestDispatch_TrailingBytesClosesConnection(t *testing.T) {
	client, server, cleanup := newTestBaseConnPair(t)
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
	client, server, cleanup := newTestBaseConnPair(t)
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
