// Copyright 2026-2027, QuarkChain.

package conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
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
	writeErr          error
	closeErr          error
}

type interruptibleFakeFrameTransport struct {
	*fakeFrameTransport
	interruptOnce sync.Once
}

func (t *interruptibleFakeFrameTransport) interrupt() error {
	t.interruptOnce.Do(func() {
		close(t.releaseWrite)
	})
	return t.Close()
}

func newFakeFrameTransport() *fakeFrameTransport {
	return &fakeFrameTransport{
		frames: make(chan *wire.Frame, 8),
		writes: make(chan *wire.Frame, 8),
		closed: make(chan struct{}),
	}
}

func (t *fakeFrameTransport) ReadFrame() (*wire.Frame, error) {
	select {
	case frame := <-t.frames:
		return frame, nil
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *fakeFrameTransport) WriteFrame(frame *wire.Frame) error {
	if t.writeErr != nil {
		return t.writeErr
	}
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

func (t *fakeFrameTransport) Close() error {
	t.closeOnce.Do(func() {
		t.writeMu.Lock()
		t.closeWhileWriting = t.writing
		t.writeMu.Unlock()
		t.closeMu.Lock()
		t.closeCount++
		t.closeMu.Unlock()
		close(t.closed)
	})
	return t.closeErr
}

func (t *fakeFrameTransport) RemoteAddr() string {
	return "fake"
}

func (t *fakeFrameTransport) closes() int {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	return t.closeCount
}

// staticReaderTransport feeds wire.ReadFrame from a fixed byte stream so tests
// can drive the frame-level EOF semantics (clean EOF vs truncated frame)
// through the full readerLoop -> handleReadFailed path.
type staticReaderTransport struct {
	reader    io.Reader
	closed    chan struct{}
	closeOnce sync.Once
}

func newStaticReaderTransport(r io.Reader) *staticReaderTransport {
	return &staticReaderTransport{reader: r, closed: make(chan struct{})}
}

func (t *staticReaderTransport) ReadFrame() (*wire.Frame, error) {
	return wire.ReadFrame(t.reader, 0)
}

func (t *staticReaderTransport) WriteFrame(*wire.Frame) error { return nil }

func (t *staticReaderTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func (t *staticReaderTransport) RemoteAddr() string { return "static" }

// validPongPayload returns a serialized empty PongResponse for tests that need
// to feed valid response frames through the fake transport.
func validPongPayload(t *testing.T) []byte {
	t.Helper()
	payload, err := serialize.SerializeToBytes(&wire.PongResponse{})
	if err != nil {
		t.Fatalf("serialize pong: %v", err)
	}
	return payload
}

// registerPingSerializer registers a PING/PONG serializer on conn so that
// BaseConn can deserialize inbound PONG responses.
func registerPingSerializer(t *testing.T, conn *BaseConn) {
	t.Helper()
	conn.RegisterOpSerializers(map[byte]*OpSerializer{
		byte(wire.ClusterOpPing): OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong)),
	})
}

// ── TCP test pair helper ──────────────────────────────────────────────────────

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

	// Register PING serializer on both sides so that the client can deserialize
	// inbound PONG responses (BaseConn validates response payloads before
	// rpc_id matching) and the server can deserialize PING requests.
	pingSer := OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong))
	client.RegisterOpSerializers(map[byte]*OpSerializer{
		byte(wire.ClusterOpPing): pingSer,
	})
	server.RegisterOpSerializers(map[byte]*OpSerializer{
		byte(wire.ClusterOpPing): pingSer,
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

// ── BaseConn unit tests (fake transport) ─────────────────────────────────────

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

func TestBaseConn_CloseInterruptsBlockedWriter(t *testing.T) {
	base := newFakeFrameTransport()
	base.writeStarted = make(chan struct{})
	base.releaseWrite = make(chan struct{})
	tr := &interruptibleFakeFrameTransport{fakeFrameTransport: base}
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
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt blocked writer")
	}
	if err := <-result; err != ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
}

func TestBaseConn_CleanCloseDoesNotPublishError(t *testing.T) {
	conn := NewBaseConn(newFakeFrameTransport(), log.New())
	conn.Start()
	if err := conn.Close(); err != nil {
		t.Fatalf("close connection: %v", err)
	}
	select {
	case err := <-conn.Error():
		t.Fatalf("clean close published error: %v", err)
	default:
	}
}

func TestBaseConn_CloseReturnsTransportError(t *testing.T) {
	closeErr := errors.New("close failed")
	tr := newFakeFrameTransport()
	tr.closeErr = closeErr
	conn := NewBaseConn(tr, log.New())
	if err := conn.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("expected transport close error, got %v", err)
	}
}

// TestBaseConn_CanceledRPCNotWritten verifies that a blocked SendRPC whose
// context expires while waiting for writeMu does not write a frame. In the
// pure-mutex model there is no writer queue; the blocked SendRPC checks
// ctx.Err() after acquiring writeMu and returns without allocating an rpcID.
func TestBaseConn_CanceledRPCNotWritten(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writeStarted = make(chan struct{})
	tr.releaseWrite = make(chan struct{})
	conn := NewBaseConn(tr, log.New())
	conn.Start()

	// RPC with background context — will hold writeMu and block in WriteFrame.
	firstResult := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		firstResult <- err
	}()
	select {
	case <-tr.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("first write did not block")
	}
	select {
	case <-tr.writes:
	case <-time.After(time.Second):
		t.Fatal("first write was not recorded")
	}

	// Second RPC with a short timeout — blocks on writeMu.Lock() because
	// the first RPC still holds it. The context expires while waiting.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	secondResult := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), nil)
		secondResult <- err
	}()

	// Wait for the second RPC's context to expire.
	time.Sleep(30 * time.Millisecond)

	// Release the slow write — the blocked SendRPC acquires writeMu,
	// checks ctx.Err(), and returns timeout without allocating an rpcID
	// or writing a frame.
	close(tr.releaseWrite)

	select {
	case err := <-secondResult:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline exceeded, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked RPC did not return after writer unblocked")
	}

	// Verify no extra frame was written.
	select {
	case frame := <-tr.writes:
		t.Fatalf("canceled RPC was written: %#v", frame)
	case <-time.After(20 * time.Millisecond):
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close connection: %v", err)
	}
	if err := <-firstResult; err != ErrConnectionClosed {
		t.Fatalf("expected first RPC to be closed, got %v", err)
	}
}

func TestConcurrentCloseAndSendRPC(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writes = make(chan *wire.Frame, 64)
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
			if err != nil && err != ErrConnectionClosed && err != ErrNotActive {
				t.Errorf("unexpected SendRPC error: %v", err)
			}
		}()
	}

	close(start)
	closeDone := make(chan struct{})
	go func() {
		conn.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked with concurrent SendRPC")
	}
	wg.Wait()

	pending := conn.pendingLen()
	if pending != 0 {
		t.Fatalf("pending RPCs remain after Close: %d", pending)
	}
	if conn.State() != ConnectionStateClosed {
		t.Fatalf("expected closed state, got %v", conn.State())
	}
}

// TestBaseConn_SubmitWhileShutdown verifies that concurrent SubmitFrame and
// SendRPC during Close neither deadlock, race, nor panic. Close acquires mu
// to mark the connection Closed (so submitters see a non-Active state and
// return), then takes the writeMu barrier to drain in-flight writes before
// closing the transport. Submitters blocked on writeMu are released once the
// barrier completes, and any blocked writer is interrupted via the transport.
func TestBaseConn_SubmitWhileShutdown(t *testing.T) {
	base := newFakeFrameTransport()
	base.writes = make(chan *wire.Frame, 4096)
	base.writeStarted = make(chan struct{})
	base.releaseWrite = make(chan struct{})
	tr := &interruptibleFakeFrameTransport{fakeFrameTransport: base}
	conn := NewBaseConn(tr, log.New())
	conn.Start()

	const submitters = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(submitters)
	for i := 0; i < submitters; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 1000; j++ {
				if i%2 == 0 {
					if err := conn.SubmitFrame(&wire.Frame{
						Meta:    wire.ClusterMetadata{Branch: 1, ClusterPeerID: 99},
						Opcode:  byte(wire.ClusterOpPong),
						RPCID:   uint64(j),
						Payload: []byte{0x01},
					}); err != nil && err != ErrConnectionClosed {
						t.Errorf("unexpected SubmitFrame error: %v", err)
					}
				} else {
					ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
					_, _ = conn.SendRPC(ctx, byte(wire.ClusterOpPing), nil)
					cancel()
				}
			}
		}()
	}

	close(start)
	closeDone := make(chan struct{})
	go func() {
		conn.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked with concurrent SubmitFrame/SendRPC")
	}
	wg.Wait()

	if conn.State() != ConnectionStateClosed {
		t.Fatalf("expected closed state, got %v", conn.State())
	}
}

// TestBaseConn_LateHandlerCompletedAfterClose verifies that a handler
// goroutine that finishes after Close drops its response without panicking
// and without leaking goroutines.
func TestBaseConn_LateHandlerCompletedAfterClose(t *testing.T) {
	base := newFakeFrameTransport()
	base.releaseWrite = make(chan struct{})
	tr := &interruptibleFakeFrameTransport{fakeFrameTransport: base}
	conn := NewBaseConn(tr, log.New())

	const op = byte(wire.ClusterOpPing)
	conn.RegisterOpSerializers(map[byte]*OpSerializer{
		op: OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong)),
	})
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	conn.RegisterTypedHandlers(map[byte]TypedHandler{
		op: func(req any) (any, error) {
			close(handlerStarted)
			<-releaseHandler
			return &wire.PongResponse{}, nil
		},
	})

	before := runtime.NumGoroutine()
	conn.Start()
	<-conn.WaitUntilActive()

	// Feed a request frame: readerLoop calls dispatch goroutine, which
	// parks inside the handler.
	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{FullShardIDList: []uint32{1}})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}
	base.frames <- &wire.Frame{Meta: wire.ClusterMetadata{}, Opcode: op, RPCID: 1, Payload: pingPayload}
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	// Trigger shutdown while the handler goroutine is still in flight.
	closeDone := make(chan struct{})
	go func() {
		conn.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked with in-flight handler")
	}

	if conn.State() != ConnectionStateClosed {
		t.Fatalf("expected closed state, got %v", conn.State())
	}

	// Release the handler: dispatch checks state (Closed) and drops the
	// response without writing. No panic, goroutine exits cleanly.
	close(releaseHandler)
	waitForGoroutines(t, before)
}

// waitForGoroutines polls until the goroutine count drops back to (or below)
// the baseline, failing the test if it never does.
func waitForGoroutines(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: %d goroutines, baseline %d", runtime.NumGoroutine(), baseline)
}

func TestLateResponseAfterTimeoutDoesNotCloseConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())
	registerPingSerializer(t, conn)
	conn.Start()
	defer conn.Close()

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
	tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: 1, Payload: validPongPayload(t)}

	select {
	case <-time.After(50 * time.Millisecond):
		if conn.IsClosed() {
			t.Fatal("late response closed the connection")
		}
	case <-conn.WaitUntilClosed():
		t.Fatal("late response closed the connection")
	}

	pending := conn.pendingLen()
	if pending != 0 {
		t.Fatalf("timed-out RPC remains pending: %d", pending)
	}
}

func TestBaseConn_CancelPreservesContextError(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())
	conn.Start()
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestBaseConn_SendRPCWithAlreadyCancelledContext(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())
	conn.Start()
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling SendRPC
	_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), nil)
	if err == nil {
		t.Fatal("expected error for already-cancelled context, got nil")
	}
}

func TestBaseConn_WriteFailurePublishesError(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writeErr = errors.New("write failed")
	conn := NewBaseConn(tr, log.New())
	conn.Start()

	_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
	if err != ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
	select {
	case cerr := <-conn.Error():
		if cerr == nil {
			t.Fatal("expected non-nil error on Error() channel after write failure")
		}
	case <-time.After(time.Second):
		t.Fatal("Error() channel did not receive write failure")
	}
	<-conn.WaitUntilClosed()
}

func TestBaseConn_LateResponseIsSilentlyIgnored(t *testing.T) {
	// Matches Python: timed-out RPC ids stay in rpc_future_map forever;
	// late responses are silently dropped regardless of delay.

	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())
	registerPingSerializer(t, conn)
	conn.Start()
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), nil)
		result <- err
	}()
	var request *wire.Frame
	select {
	case request = <-tr.writes:
	case <-time.After(time.Second):
		t.Fatal("fake transport did not receive request")
	}
	cancel()
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatal("SendRPC did not return after cancellation")
	}

	// Send a late response — connection should NOT close.
	tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: request.RPCID, Payload: validPongPayload(t)}
	select {
	case <-conn.WaitUntilClosed():
		t.Fatal("late response closed the connection — should have been silently ignored")
	case <-time.After(50 * time.Millisecond):
		// Expected: connection stays open.
	}
}

func TestBaseConn_HandlerPanicShutsDownConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())
	conn.RegisterOpSerializers(map[byte]*OpSerializer{
		byte(wire.ClusterOpPing): OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong)),
	})
	conn.RegisterTypedHandlers(map[byte]TypedHandler{
		byte(wire.ClusterOpPing): func(req any) (any, error) {
			panic("handler panic test")
		},
	})
	conn.Start()

	payload, _ := serialize.SerializeToBytes(&wire.PingRequest{FullShardIDList: []uint32{1}})
	tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: payload}
	select {
	case <-conn.WaitUntilClosed():
		if !conn.IsClosed() {
			t.Fatal("expected connection to close after handler panic")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection did not close after handler panic")
	}
}

func TestBaseConn_UnknownRPCIDResponseShutsDownConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())
	registerPingSerializer(t, conn)
	conn.Start()
	defer conn.Close()

	// Send a response with an rpc_id that was never allocated — neither
	// in pending nor in timedOut. This should close the connection.
	tr.frames <- &wire.Frame{
		Opcode:  byte(wire.ClusterOpPong),
		RPCID:   999,
		Payload: validPongPayload(t),
	}
	select {
	case <-conn.WaitUntilClosed():
		if !conn.IsClosed() {
			t.Fatal("expected connection to close after unknown rpc_id response")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection did not close after unknown rpc_id response")
	}
}

func TestBaseConn_ResponseCancelRaceCleansPending(t *testing.T) {
	const iterations = 100
	pongPayload := validPongPayload(t)
	for i := 0; i < iterations; i++ {
		tr := newFakeFrameTransport()
		conn := NewBaseConn(tr, log.New())
		registerPingSerializer(t, conn)
		conn.Start()

		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), nil)
			result <- err
		}()

		var request *wire.Frame
		select {
		case request = <-tr.writes:
		case <-time.After(time.Second):
			t.Fatal("fake transport did not receive request")
		}
		start := make(chan struct{})
		go func() {
			<-start
			cancel()
		}()
		go func() {
			<-start
			tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: request.RPCID, Payload: pongPayload}
		}()
		close(start)

		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("SendRPC did not complete")
		}
		if pending := conn.pendingLen(); pending != 0 {
			t.Fatalf("iteration %d: pending RPCs remain: %d", i, pending)
		}
		conn.Close()
	}
}

func TestBaseConn_ReadFailureWakesPendingRPC(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())
	conn.Start()

	result := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		result <- err
	}()
	select {
	case <-tr.writes:
	case <-time.After(time.Second):
		t.Fatal("fake transport did not receive request")
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("close fake transport: %v", err)
	}
	select {
	case err := <-result:
		if err != ErrConnectionClosed {
			t.Fatalf("expected ErrConnectionClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read failure did not wake pending RPC")
	}
	<-conn.WaitUntilClosed()
	if pending := conn.pendingLen(); pending != 0 {
		t.Fatalf("pending RPCs remain after read failure: %d", pending)
	}
}

// TestBaseConn_CleanEOFDoesNotPublishError verifies that a peer closing the
// connection before the start of any frame (clean EOF, i.e. wire.ReadFrame
// returning io.EOF) is a graceful close: the connection shuts down but the
// Error channel receives nothing, matching Python's close() on EOF.
func TestBaseConn_CleanEOFDoesNotPublishError(t *testing.T) {
	tr := newStaticReaderTransport(bytes.NewReader(nil))
	conn := NewBaseConn(tr, log.New())
	conn.Start()

	<-conn.WaitUntilClosed()
	if !conn.IsClosed() {
		t.Fatal("connection should be closed after clean EOF")
	}
	select {
	case err := <-conn.Error():
		t.Fatalf("clean EOF published error: %v", err)
	default:
	}
}

// TestBaseConn_TruncatedFramePublishesError verifies that a truncated frame
// (length header consumed, then EOF before the frame body) publishes an error
// on the Error channel. wire.ReadFrame normalizes the zero-byte EOF on the
// metadata read to io.ErrUnexpectedEOF, matching Python's "read unexpected
// EOF" -> close_with_error().
func TestBaseConn_TruncatedFramePublishesError(t *testing.T) {
	// payload_len = 10 but the stream ends right after the length header.
	tr := newStaticReaderTransport(bytes.NewReader([]byte{0x00, 0x00, 0x00, 0x0a}))
	conn := NewBaseConn(tr, log.New())
	conn.Start()

	select {
	case err := <-conn.Error():
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("want io.ErrUnexpectedEOF, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("truncated frame did not publish an error")
	}
	<-conn.WaitUntilClosed()
}

func TestBaseConn_WriteFailureWakesPendingRPC(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writeErr = errors.New("write failed")
	conn := NewBaseConn(tr, log.New())
	conn.Start()

	_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
	if err != ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
	<-conn.WaitUntilClosed()
	if pending := conn.pendingLen(); pending != 0 {
		t.Fatalf("pending RPCs remain after write failure: %d", pending)
	}
}

func TestSendRPC_ConcurrentSendsPreserveRPCIDOrder(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writes = make(chan *wire.Frame, 64)
	conn := NewBaseConn(tr, log.New())
	conn.Start()

	const senders = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(senders)
	for i := 0; i < senders; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, _ = conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		}()
	}

	close(start)
	frames := make([]*wire.Frame, 0, senders)
	for len(frames) < senders {
		select {
		case frame := <-tr.writes:
			frames = append(frames, frame)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for concurrent RPC writes")
		}
	}
	for i, frame := range frames {
		if frame.RPCID != uint64(i+1) {
			t.Fatalf("rpc id at position %d: got %d, want %d", i, frame.RPCID, i+1)
		}
	}
	if conn.IsClosed() {
		t.Fatal("connection closed during concurrent sends")
	}

	conn.Close()
	wg.Wait()
	if conn.IsClosed() == false {
		t.Fatal("connection should be closed after test cleanup")
	}
}

func TestBaseConn_PendingRPCRemovedAfterResponse(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())
	registerPingSerializer(t, conn)
	conn.Start()
	defer conn.Close()

	result := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		result <- err
	}()

	select {
	case request := <-tr.writes:
		tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: request.RPCID, Payload: validPongPayload(t)}
	case <-time.After(time.Second):
		t.Fatal("fake transport did not receive request")
	}
	if err := <-result; err != nil {
		t.Fatalf("SendRPC failed: %v", err)
	}

	pending := conn.pendingLen()
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

func TestBaseConn_StartOnClosedConnectionIsNoOp(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())
	conn.Close()

	// Start on an already-closed connection must be a no-op: no state
	// transition, no readerLoop launch.
	conn.Start()

	if conn.State() != ConnectionStateClosed {
		t.Fatal("expected closed state after Start on closed connection")
	}
}

// TestBaseConn_CloseDoesNotWaitForReaderThatNeverStarted is a deterministic
// regression test for the Start()/Close() lifecycle deadlock. If Start() is
// never called (or returns early because the connection is already closed),
// readerLoop is never launched, and Close() must not block forever waiting on
// readerDone.
func TestBaseConn_CloseDoesNotWaitForReaderThatNeverStarted(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())

	// Close a connection whose Start() was never called: readerStarted is
	// false and readerLoop was never launched.
	closeDone := make(chan struct{})
	go func() {
		conn.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() deadlocked waiting on readerDone for a readerLoop that never started")
	}
}

// TestBaseConn_StartCloseConcurrentStress repeatedly exercises the Start()/Close()
// race, covering the interleaving where Start() is descheduled between its
// mu.Unlock() and readerStarted.Store() while Close() completes shutdown.
func TestBaseConn_StartCloseConcurrentStress(t *testing.T) {
	for i := 0; i < 500; i++ {
		tr := newFakeFrameTransport()
		conn := NewBaseConn(tr, log.New())

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			conn.Start()
		}()
		go func() {
			defer wg.Done()
			<-start
			conn.Close()
		}()
		close(start)

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: Close() deadlocked with concurrent Start()", i)
		}
	}
}

// TestBaseConn_ResponseOpcodeMismatchClosesConnection verifies that a response
// whose opcode does not match the request's expected response opcode is treated
// as a protocol error: the connection closes and the pending RPC is completed
// with an error rather than being mis-delivered as a successful response.
func TestBaseConn_ResponseOpcodeMismatchClosesConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(tr, log.New())

	// Register PING and ADD_XSHARD_TX_LIST serializers so the wrong response
	// opcode is a *known* response opcode that deserializes cleanly but does
	// not match PING's expected PONG.
	conn.RegisterOpSerializers(map[byte]*OpSerializer{
		byte(wire.ClusterOpPing):                   OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong)),
		byte(wire.ClusterOpAddXshardTxListRequest): OpSerializerFor[wire.AddXshardTxListRequest, wire.AddXshardTxListResponse](byte(wire.ClusterOpAddXshardTxListResponse)),
	})
	conn.Start()
	defer conn.Close()

	result := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		result <- err
	}()

	var request *wire.Frame
	select {
	case request = <-tr.writes:
	case <-time.After(time.Second):
		t.Fatal("fake transport did not receive request")
	}

	wrongPayload, err := serialize.SerializeToBytes(&wire.AddXshardTxListResponse{})
	if err != nil {
		t.Fatalf("serialize AddXshardTxListResponse: %v", err)
	}
	tr.frames <- &wire.Frame{
		Opcode:  byte(wire.ClusterOpAddXshardTxListResponse),
		RPCID:   request.RPCID,
		Payload: wrongPayload,
	}

	select {
	case <-conn.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("connection did not close after response opcode mismatch")
	}

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("pending RPC completed successfully on response opcode mismatch")
		}
	case <-time.After(time.Second):
		t.Fatal("pending RPC was not completed after response opcode mismatch")
	}

	if pending := conn.pendingLen(); pending != 0 {
		t.Fatalf("pending RPC remains after mismatch shutdown: %d", pending)
	}
}

// ── BaseConn integration tests (TCP pair) ────────────────────────────────────

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
	tr := newFakeFrameTransport()
	server := NewBaseConn(tr, log.New())
	registerPingSerializer(t, server)
	server.RegisterTypedHandlers(map[byte]TypedHandler{
		byte(wire.ClusterOpPing): func(req any) (any, error) {
			return &wire.PongResponse{}, nil
		},
	})
	defer server.Close()

	server.Start()

	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client"),
		FullShardIDList: []uint32{0x00010001},
	})

	// Inject two PING frames with the same RPC ID (=1).
	tr.frames <- &wire.Frame{
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	}
	tr.frames <- &wire.Frame{
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1, // duplicate rpc_id: should trigger close
		Payload: pingPayload,
	}

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
	tr := newFakeFrameTransport()
	server := NewBaseConn(tr, log.New())
	registerPingSerializer(t, server)
	server.RegisterTypedHandlers(map[byte]TypedHandler{
		byte(wire.ClusterOpPing): func(req any) (any, error) {
			return &wire.PongResponse{}, nil
		},
	})
	defer server.Close()

	server.Start()

	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client"),
		FullShardIDList: []uint32{0x00010001},
	})

	// Send rpc_id=2 then rpc_id=1 (decreasing).
	tr.frames <- &wire.Frame{
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   2,
		Payload: pingPayload,
	}
	tr.frames <- &wire.Frame{
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1, // decreasing rpc_id: should trigger close
		Payload: pingPayload,
	}

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
	tr := newFakeFrameTransport()
	server := NewBaseConn(tr, log.New())
	registerPingSerializer(t, server)
	server.RegisterTypedHandlers(map[byte]TypedHandler{
		byte(wire.ClusterOpPing): func(req any) (any, error) {
			return &wire.PongResponse{}, nil
		},
	})
	defer server.Close()

	server.Start()

	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client"),
		FullShardIDList: []uint32{0x00010001},
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}
	malformedPayload := append(pingPayload, 0xFF)

	tr.frames <- &wire.Frame{
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: malformedPayload,
	}

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

// TestDispatch_MalformedResponsePayloadClosesConnection verifies that a PONG
// response with a malformed payload (trailing bytes) causes the receiver to
// close the connection.
func TestDispatch_MalformedResponsePayloadClosesConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	client := NewBaseConn(tr, log.New())
	registerPingSerializer(t, client)
	defer client.Close()

	client.Start()

	// Append a trailing byte to a valid PONG payload so deserialization fails.
	// BaseConn deserializes response payloads before rpc_id matching, so the
	// malformed payload closes the connection.
	pongPayload, err := serialize.SerializeToBytes(&wire.PongResponse{
		ID:              []byte("server"),
		FullShardIDList: []uint32{0x00010001},
	})
	if err != nil {
		t.Fatalf("serialize pong: %v", err)
	}
	malformedPong := append(pongPayload, 0xFF)

	tr.frames <- &wire.Frame{
		Opcode:  byte(wire.ClusterOpPong),
		RPCID:   1,
		Payload: malformedPong,
	}

	select {
	case <-client.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("client did not close connection after malformed PONG payload")
	}
}

// TestDispatch_UnknownResponseOpcodeClosesConnection verifies that a frame
// with an opcode that is neither a registered request handler nor a registered
// response opcode causes the receiver to close the connection.
func TestDispatch_UnknownResponseOpcodeClosesConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	client := NewBaseConn(tr, log.New())
	registerPingSerializer(t, client)
	defer client.Close()

	client.Start()

	// 0xFF is not a registered ClusterOp on either side: no handler and no
	// response serializer. The receiver must close the connection.
	tr.frames <- &wire.Frame{
		Opcode:  0xFF,
		RPCID:   1,
		Payload: []byte{0x00},
	}

	select {
	case <-client.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("client did not close connection after unknown response opcode")
	}
}

// TestDispatch_ValidResponseBehaviorUnchanged verifies that a valid PONG
// response is delivered to the caller. SendRPC returns *wire.Frame; BaseConn
// validates the payload internally but does not return the deserialized object,
// so the caller deserializes the payload itself.
func TestDispatch_ValidResponseBehaviorUnchanged(t *testing.T) {
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

	// The caller receives the raw frame and deserializes the payload itself.
	var pong wire.PongResponse
	if err := serialize.DeserializeFromBytes(resp.Payload, &pong); err != nil {
		t.Fatalf("deserialize pong: %v", err)
	}

	if client.IsClosed() {
		t.Fatal("client should remain open after valid response")
	}
	if server.IsClosed() {
		t.Fatal("server should remain open after valid response")
	}
}
