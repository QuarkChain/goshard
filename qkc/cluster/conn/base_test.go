// Copyright 2026-2027, QuarkChain.

package conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// -- fake transport -----------------------------------------------------------

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

// interruptibleFakeFrameTransport models a real net.Conn: Close releases a
// write parked inside WriteFrame, matching the contract that shutdown is
// interrupt-first (transport.Close before the writeMu barrier).
type interruptibleFakeFrameTransport struct {
	*fakeFrameTransport
	interruptOnce sync.Once
}

// Close records the close first, so closeWhileWriting deterministically
// observes the still-parked write, then releases the writer.
func (t *interruptibleFakeFrameTransport) Close() error {
	err := t.fakeFrameTransport.Close()
	t.interruptOnce.Do(func() {
		if t.releaseWrite != nil {
			close(t.releaseWrite)
		}
	})
	return err
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
// can drive the frame-level EOF semantics (clean EOF vs truncated frame).
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

// -- test helpers ------------------------------------------------------------

var pingSer = OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong))

var pingSerializers = map[byte]*OpSerializer{
	byte(wire.ClusterOpPing): pingSer,
}

// pongHandler returns a minimal PONG handler for test server connections.
func pongHandler() TypedHandler {
	return func(req any) (any, error) {
		return &wire.PongResponse{}, nil
	}
}

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

// newConn creates a minimal BaseConn with no handlers or serializers.
func newConn(tr FrameTransport) *BaseConn {
	return NewBaseConn(Config{Transport: tr, Logger: log.New()})
}

// newPingConn creates a BaseConn with only PING/PONG serializer (client-side:
// can send PING RPCs and receive PONG responses, but does not handle PING).
func newPingConn(tr FrameTransport) *BaseConn {
	return NewBaseConn(Config{
		Transport:   tr,
		Serializers: pingSerializers,
		Logger:      log.New(),
	})
}

// newPingServerConn creates a BaseConn with PING serializer + handler.
func newPingServerConn(tr FrameTransport) *BaseConn {
	return NewBaseConn(Config{
		Transport:   tr,
		Serializers: pingSerializers,
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): pongHandler(),
		},
		Logger: log.New(),
	})
}

// newTestBaseConnPair creates a pair of BaseConns connected over a local TCP
// socket, with PING/PONG serializer on both sides and a minimal PING handler on
// the server side. The caller is responsible for calling cleanup.
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

	readFrame := func(r io.Reader) (*wire.Frame, error) {
		return wire.ReadFrameNoMeta(r, 0)
	}

	client = NewBaseConn(Config{
		Transport:   NewTCPTransport(clientConn, readFrame, wire.WriteFrameNoMeta),
		Serializers: pingSerializers,
		Logger:      log.New(),
	})
	server = NewBaseConn(Config{
		Transport:   NewTCPTransport(serverConn, readFrame, wire.WriteFrameNoMeta),
		Serializers: pingSerializers,
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): pongHandler(),
		},
		Logger: log.New(),
	})

	cleanup = func() {
		client.Close()
		server.Close()
	}
	return
}

// -- Config validation tests -------------------------------------------------

func TestConfig_NilTransportPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil Transport")
		}
		msg, _ := r.(string)
		if !strings.Contains(msg, "Transport") {
			t.Fatalf("expected Transport in panic message, got %q", msg)
		}
	}()
	NewBaseConn(Config{})
}

func TestConfig_NilValuePanics(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"nil serializer", Config{Transport: newFakeFrameTransport(), Serializers: map[byte]*OpSerializer{0x01: nil}}},
		{"nil handler", Config{Transport: newFakeFrameTransport(), Handlers: map[byte]TypedHandler{0x01: nil}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic for " + tt.name)
				}
			}()
			NewBaseConn(tt.cfg)
		})
	}
}

// -- BaseConn unit tests (fake transport) -------------------------------------

// TestBaseConn_CloseWaitsForOutboundWrite verifies the writeMu barrier: Close
// must not return until an in-flight transport write has left WriteFrame. Uses
// the plain fake so the write is released only by the test (barrier, not
// interrupt, unblocks it).
func TestBaseConn_CloseWaitsForOutboundWrite(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writeStarted = make(chan struct{})
	tr.releaseWrite = make(chan struct{})
	conn := newConn(tr)
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
	// Shutdown is interrupt-first (transport.Close before the writeMu barrier);
	// a barrier-first shutdown would hang on a real net.Conn with a full
	// send buffer.
	if !tr.closeWhileWriting {
		t.Fatal("expected interrupt-first shutdown: transport.Close must run while the write is still in flight")
	}
	if err := <-result; err != ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
}

// TestBaseConn_CloseInterruptsBlockedWriter: with an interruptible transport
// (real net.Conn semantics), Close releases a write parked inside WriteFrame
// and returns without the test releasing it manually. Complements
// TestBaseConn_CloseWaitsForOutboundWrite (plain fake, barrier path).
func TestBaseConn_CloseInterruptsBlockedWriter(t *testing.T) {
	base := newFakeFrameTransport()
	base.writeStarted = make(chan struct{})
	base.releaseWrite = make(chan struct{})
	tr := &interruptibleFakeFrameTransport{fakeFrameTransport: base}
	conn := newConn(tr)
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
	conn := newConn(newFakeFrameTransport())
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
	conn := newConn(tr)
	if err := conn.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("expected transport close error, got %v", err)
	}
}

func TestBaseConn_CanceledRPCNotWritten(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writeStarted = make(chan struct{})
	tr.releaseWrite = make(chan struct{})
	conn := newConn(tr)
	conn.Start()

	// First RPC parks in WriteFrame holding writeMu.
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

	// Second RPC blocks on writeMu; its context expires while waiting.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	secondResult := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), nil)
		secondResult <- err
	}()

	time.Sleep(30 * time.Millisecond)

	// Releasing the slow write lets the blocked RPC acquire writeMu, see the
	// expired context, and return without allocating an rpcID or writing a frame.
	close(tr.releaseWrite)

	select {
	case err := <-secondResult:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline exceeded, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked RPC did not return after writer unblocked")
	}

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

// TestBaseConn_WriteWhileShutdown verifies that concurrent writeFrame and
// SendRPC during Close neither deadlock, race, nor panic. Close is
// interrupt-first: the writer parked in WriteFrame is released by
// transport.Close, never by the writeMu barrier.
func TestBaseConn_WriteWhileShutdown(t *testing.T) {
	base := newFakeFrameTransport()
	base.writes = make(chan *wire.Frame, 4096)
	base.writeStarted = make(chan struct{})
	base.releaseWrite = make(chan struct{})
	tr := &interruptibleFakeFrameTransport{fakeFrameTransport: base}
	conn := newConn(tr)
	conn.Start()

	const writers = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 1000; j++ {
				if i%2 == 0 {
					// writeFrame is the package-private raw write path.
					if err := conn.writeFrame(&wire.Frame{
						Meta:    wire.ClusterMetadata{Branch: 1, ClusterPeerID: 99},
						Opcode:  byte(wire.ClusterOpPong),
						RPCID:   uint64(j),
						Payload: []byte{0x01},
					}); err != nil && err != ErrConnectionClosed {
						t.Errorf("unexpected writeFrame error: %v", err)
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
		t.Fatal("Close deadlocked with concurrent writeFrame/SendRPC")
	}
	wg.Wait()

	if conn.State() != ConnectionStateClosed {
		t.Fatalf("expected closed state, got %v", conn.State())
	}
}

// TestBaseConn_LateHandlerCompletedAfterClose verifies that a handler
// finishing after Close drops its response without panicking or leaking.
func TestBaseConn_LateHandlerCompletedAfterClose(t *testing.T) {
	base := newFakeFrameTransport()
	base.releaseWrite = make(chan struct{})
	tr := &interruptibleFakeFrameTransport{fakeFrameTransport: base}

	const op = byte(wire.ClusterOpPing)
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	conn := NewBaseConn(Config{
		Transport: tr,
		Serializers: map[byte]*OpSerializer{
			op: OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong)),
		},
		Handlers: map[byte]TypedHandler{
			op: func(req any) (any, error) {
				close(handlerStarted)
				<-releaseHandler
				return &wire.PongResponse{}, nil
			},
		},
		Logger: log.New(),
	})

	before := runtime.NumGoroutine()
	conn.Start()
	<-conn.WaitUntilActive()

	// Feed a request frame; the dispatch goroutine parks inside the handler.
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

	// Release the handler: dispatch sees Closed and drops the response.
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

// TestLateResponseAfterTimeoutDoesNotCloseConnection pins the Python
// timed-out semantics: a response arriving after its RPC timed out is dropped,
// not treated as an unknown rpc_id (which would close the connection).
func TestLateResponseAfterTimeoutDoesNotCloseConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newPingConn(tr)
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

func TestBaseConn_CancelErrorContract(t *testing.T) {
	tests := []struct {
		name      string
		newCtx    func() (context.Context, context.CancelFunc)
		wantError func(error) bool
	}{
		{
			name: "deadline exceeded",
			newCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Millisecond)
			},
			wantError: func(err error) bool { return errors.Is(err, context.DeadlineExceeded) },
		},
		{
			name: "already cancelled",
			newCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			wantError: func(err error) bool { return err != nil },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newFakeFrameTransport()
			conn := newConn(tr)
			conn.Start()
			defer conn.Close()

			ctx, cancel := tt.newCtx()
			defer cancel()
			_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), nil)
			if !tt.wantError(err) {
				t.Fatalf("SendRPC error = %v", err)
			}
		})
	}
}

func TestBaseConn_WriteFailurePublishesError(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writeErr = errors.New("write failed")
	conn := newConn(tr)
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

func TestBaseConn_HandlerPanicShutsDownConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(Config{
		Transport: tr,
		Serializers: map[byte]*OpSerializer{
			byte(wire.ClusterOpPing): OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong)),
		},
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): func(req any) (any, error) {
				panic("handler panic test")
			},
		},
		Logger: log.New(),
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

// TestBaseConn_UnknownRPCIDResponseShutsDownConnection covers the third branch
// of handleResponse: a response with an rpc_id never allocated (not in pending,
// not in timedOut) closes the connection, mirroring Python.
func TestBaseConn_UnknownRPCIDResponseShutsDownConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newPingConn(tr)
	conn.Start()
	defer conn.Close()

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
		conn := newPingConn(tr)
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
	conn := newConn(tr)
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

// TestBaseConn_CleanEOFDoesNotPublishError: a clean EOF (peer closes before
// any frame) is a graceful close — no error published, matching Python's
// close() on EOF.
func TestBaseConn_CleanEOFDoesNotPublishError(t *testing.T) {
	tr := newStaticReaderTransport(bytes.NewReader(nil))
	conn := newConn(tr)
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

// TestBaseConn_TruncatedFramePublishesError: EOF mid-frame publishes
// io.ErrUnexpectedEOF, matching Python's close_with_error() on unexpected EOF.
func TestBaseConn_TruncatedFramePublishesError(t *testing.T) {
	// payload_len = 10 but the stream ends right after the length header.
	tr := newStaticReaderTransport(bytes.NewReader([]byte{0x00, 0x00, 0x00, 0x0a}))
	conn := newConn(tr)
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

// TestSendRPC_ConcurrentSendsPreserveRPCIDOrder: concurrent SendRPC calls must
// produce strictly increasing rpc ids — the server-side monotonic check
// depends on it.
func TestSendRPC_ConcurrentSendsPreserveRPCIDOrder(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writes = make(chan *wire.Frame, 64)
	conn := newConn(tr)
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
	for i := 1; i < len(frames); i++ {
		if frames[i].RPCID <= frames[i-1].RPCID {
			t.Fatalf("rpc ids not strictly increasing: %d then %d", frames[i-1].RPCID, frames[i].RPCID)
		}
	}
	if conn.IsClosed() {
		t.Fatal("connection closed during concurrent sends")
	}

	conn.Close()
	wg.Wait()
	if !conn.IsClosed() {
		t.Fatal("connection should be closed after test cleanup")
	}
}

func TestBaseConn_DoubleClose(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newConn(tr)

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

// TestBaseConn_StartCloseLifecycle covers the Start()/Close() lifecycle: Start
// on a closed connection is a no-op, and Close on a connection whose
// readerLoop never started must not block forever on readerDone (deterministic
// regression for the Start/Close deadlock).
func TestBaseConn_StartCloseLifecycle(t *testing.T) {
	t.Run("start on closed is no-op", func(t *testing.T) {
		conn := newConn(newFakeFrameTransport())
		conn.Close()
		conn.Start()
		if conn.State() != ConnectionStateClosed {
			t.Fatal("expected closed state after Start on closed connection")
		}
	})

	t.Run("close without start does not block", func(t *testing.T) {
		conn := newConn(newFakeFrameTransport())
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
	})
}

// TestBaseConn_StartCloseConcurrentStress exercises the Start()/Close() race,
// including the interleaving where Start() commits Active but is descheduled
// before launching readerLoop while Close() completes shutdown in between.
func TestBaseConn_StartCloseConcurrentStress(t *testing.T) {
	for i := 0; i < 100; i++ {
		tr := newFakeFrameTransport()
		conn := newConn(tr)

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

// TestBaseConn_ResponseOpcodeMismatchDeliversResponse: Python matches
// responses by rpc_id only and does not validate the response opcode
// (AbstractConnection.handle_metadata_and_raw_data); a wrong-opcode response
// must still be delivered.
func TestBaseConn_ResponseOpcodeMismatchDeliversResponse(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(Config{
		Transport: tr,
		Serializers: map[byte]*OpSerializer{
			byte(wire.ClusterOpPing):                   OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong)),
			byte(wire.ClusterOpAddXshardTxListRequest): OpSerializerFor[wire.AddXshardTxListRequest, wire.AddXshardTxListResponse](byte(wire.ClusterOpAddXshardTxListResponse)),
		},
		Logger: log.New(),
	})
	conn.Start()
	defer conn.Close()

	result := make(chan rpcResult, 1)
	go func() {
		resp, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		result <- rpcResult{resp: resp, err: err}
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
	case res := <-result:
		if res.err != nil {
			t.Fatalf("SendRPC failed: %v", res.err)
		}
		if res.resp == nil {
			t.Fatal("SendRPC returned nil response")
		}
	case <-time.After(time.Second):
		t.Fatal("SendRPC did not complete after response delivery")
	}

	if conn.IsClosed() {
		t.Fatal("connection should not close on response opcode mismatch")
	}

	if pending := conn.pendingLen(); pending != 0 {
		t.Fatalf("pending RPC remains after response delivery: %d", pending)
	}
}

// -- SendCommand tests -------------------------------------------------------

func TestBaseConn_SendCommandWritesFireAndForgetResponse(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newPingServerConn(tr)
	conn.Start()
	defer conn.Close()

	if err := conn.SendCommand(byte(wire.ClusterOpPing), nil); err != nil {
		t.Fatalf("SendCommand failed: %v", err)
	}

	select {
	case f := <-tr.writes:
		if f.RPCID != 0 {
			t.Fatalf("expected rpc_id=0, got %d", f.RPCID)
		}
		if f.Opcode != byte(wire.ClusterOpPing) {
			t.Fatalf("expected opcode 0x%x, got 0x%x", byte(wire.ClusterOpPing), f.Opcode)
		}
	case <-time.After(time.Second):
		t.Fatal("SendCommand did not write a frame")
	}

	// A fire-and-forget (rpc_id=0) request runs the handler without a response.
	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{FullShardIDList: []uint32{1}})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}
	tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 0, Payload: pingPayload}

	select {
	case f := <-tr.writes:
		t.Fatalf("expected no response for fire-and-forget command, got opcode 0x%x rpc_id=%d", f.Opcode, f.RPCID)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBaseConn_SendCommandOnClosedConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newConn(tr)
	conn.Close()

	err := conn.SendCommand(byte(wire.ClusterOpPing), nil)
	if err != ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
}

// -- BaseConn integration tests (TCP pair) -----------------------------------

// TestBaseConn_RPCIDValidation verifies that inbound RPC ids must be strictly
// increasing: duplicate or decreasing ids close the connection.
func TestBaseConn_RPCIDValidation(t *testing.T) {
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client"),
		FullShardIDList: []uint32{0x00010001},
	})
	tests := []struct {
		name  string
		frame []*wire.Frame
	}{
		{
			"duplicate rpc_id",
			[]*wire.Frame{
				{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: pingPayload},
				{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: pingPayload},
			},
		},
		{
			"decreasing rpc_id",
			[]*wire.Frame{
				{Opcode: byte(wire.ClusterOpPing), RPCID: 2, Payload: pingPayload},
				{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: pingPayload},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newFakeFrameTransport()
			server := newPingServerConn(tr)
			defer server.Close()
			server.Start()

			for _, f := range tt.frame {
				tr.frames <- f
			}
			select {
			case <-server.WaitUntilClosed():
			case <-time.After(2 * time.Second):
				t.Fatal("server did not close connection after invalid rpc_id sequence")
			}
			if !server.IsClosed() {
				t.Fatal("server should be closed")
			}
		})
	}
}

// TestDispatch_UnsupportedOpcodeClosesConnection: an opcode with no registered
// handler closes the connection.
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

// TestDispatch_TrailingBytesClosesConnection: payload trailing bytes close the
// connection — the deserializer must consume exactly the payload length.
func TestDispatch_TrailingBytesClosesConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	server := newPingServerConn(tr)
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

// TestDispatch_ExactPayloadProcessesNormally: a well-formed payload is
// processed and the connection stays open (positive control for the above).
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
	_, ok := resp.(*wire.PongResponse)
	if !ok {
		t.Fatalf("expected *PongResponse, got %T", resp)
	}
	if server.IsClosed() {
		t.Fatal("server should remain open after well-formed exchange")
	}
}

// TestDispatch_MalformedResponsePayloadClosesConnection: a malformed response
// payload (trailing bytes) closes the connection.
func TestDispatch_MalformedResponsePayloadClosesConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	client := newPingConn(tr)
	defer client.Close()

	client.Start()

	// Responses are deserialized before rpc_id matching, so a malformed
	// payload closes the connection.
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

// TestDispatch_UnknownResponseOpcodeClosesConnection: an opcode that is neither
// a request handler nor a response opcode closes the connection.
func TestDispatch_UnknownResponseOpcodeClosesConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	client := newPingConn(tr)
	defer client.Close()

	client.Start()

	// 0xFF is not a registered ClusterOp: no handler, no response serializer.
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

// TestDispatch_ValidResponseBehaviorUnchanged: a valid PONG response is
// delivered to the caller as the already-deserialized object.
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

	pong, ok := resp.(*wire.PongResponse)
	if !ok {
		t.Fatalf("expected *PongResponse, got %T", resp)
	}
	if pong == nil {
		t.Fatal("nil PongResponse")
	}

	if client.IsClosed() {
		t.Fatal("client should remain open after valid response")
	}
	if server.IsClosed() {
		t.Fatal("server should remain open after valid response")
	}
}

// -- Forwarder tests ---------------------------------------------------------

// TestForwarder_RoutesFrame verifies the Config.Forwarder hook: returning true
// consumes the frame (no dispatch), returning false dispatches normally.
func TestForwarder_RoutesFrame(t *testing.T) {
	tests := []struct {
		name      string
		consume   bool
		wantReply bool
	}{
		{"consume", true, false},
		{"pass", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newFakeFrameTransport()
			forwarded := make(chan *wire.Frame, 1)
			conn := NewBaseConn(Config{
				Transport:   tr,
				Serializers: pingSerializers,
				Handlers: map[byte]TypedHandler{
					byte(wire.ClusterOpPing): pongHandler(),
				},
				Forwarder: func(f *wire.Frame) bool {
					forwarded <- f
					return tt.consume
				},
				Logger: log.New(),
			})
			conn.Start()
			defer conn.Close()

			pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{FullShardIDList: []uint32{1}})
			if err != nil {
				t.Fatalf("serialize ping: %v", err)
			}
			tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: pingPayload}

			select {
			case f := <-forwarded:
				if f.RPCID != 1 {
					t.Fatalf("expected rpc_id=1, got %d", f.RPCID)
				}
			case <-time.After(time.Second):
				t.Fatal("forwarder did not receive frame")
			}

			if tt.wantReply {
				select {
				case f := <-tr.writes:
					if f.RPCID != 1 || f.Opcode != byte(wire.ClusterOpPong) {
						t.Fatalf("unexpected reply: opcode 0x%x rpc_id=%d", f.Opcode, f.RPCID)
					}
				case <-time.After(time.Second):
					t.Fatal("handler did not write response")
				}
			} else {
				select {
				case f := <-tr.writes:
					t.Fatalf("expected no response when forwarder consumed frame, got opcode 0x%x", f.Opcode)
				case <-time.After(50 * time.Millisecond):
				}
			}
		})
	}
}

// -- Non-RPC tests ------------------------------------------------------------

// TestNonRPC_DispatchesHandler: a non-RPC opcode dispatches to the handler
// without writing a response.
func TestNonRPC_DispatchesHandler(t *testing.T) {
	tr := newFakeFrameTransport()

	done := make(chan struct{})
	conn := NewBaseConn(Config{
		Transport:   tr,
		Serializers: pingSerializers,
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): func(req any) (any, error) {
				close(done)
				return &wire.PongResponse{}, nil
			},
		},
		NonRPCOps: map[byte]struct{}{
			byte(wire.ClusterOpPing): {},
		},
		Logger: log.New(),
	})
	conn.Start()
	defer conn.Close()

	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{FullShardIDList: []uint32{1}})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}
	nonRPCFrame := &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 0, Payload: pingPayload}

	select {
	case tr.frames <- nonRPCFrame:
	case <-time.After(time.Second):
		t.Fatal("transport write blocked")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler was not invoked for non-RPC opcode")
	}

	select {
	case f := <-tr.writes:
		t.Fatalf("expected no response for non-RPC, got opcode 0x%x rpc_id=%d", f.Opcode, f.RPCID)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestNonRPC_NonZeroRPCIDShutsDown: a non-RPC opcode with non-zero rpc_id
// shuts down the connection.
func TestNonRPC_NonZeroRPCIDShutsDown(t *testing.T) {
	tr := newFakeFrameTransport()

	conn := NewBaseConn(Config{
		Transport:   tr,
		Serializers: pingSerializers,
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): pongHandler(),
		},
		NonRPCOps: map[byte]struct{}{
			byte(wire.ClusterOpPing): {},
		},
		Logger: log.New(),
	})
	conn.Start()

	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{FullShardIDList: []uint32{1}})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}
	nonRPCFrame := &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 99, Payload: pingPayload}

	select {
	case tr.frames <- nonRPCFrame:
	case <-time.After(time.Second):
		t.Fatal("transport write blocked")
	}

	select {
	case <-conn.WaitUntilClosed():
	case <-time.After(time.Second):
		t.Fatal("connection did not close after non-RPC with non-zero rpc_id")
	}
}

// -- Response deserialization tests -------------------------------------------

// TestResponse_MalformedClearsPending: a malformed response payload closes the
// connection and wakes the pending caller with an error.
func TestResponse_MalformedClearsPending(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newPingConn(tr)
	conn.Start()

	errCh := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		errCh <- err
	}()

	var request *wire.Frame
	select {
	case request = <-tr.writes:
	case <-time.After(time.Second):
		t.Fatal("transport did not receive request")
	}

	// Deliver a response with a malformed payload (1 byte).
	tr.frames <- &wire.Frame{
		Opcode:  byte(wire.ClusterOpPong),
		RPCID:   request.RPCID,
		Payload: []byte{0x01},
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error for malformed response, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("SendRPC did not return after malformed response")
	}

	select {
	case <-conn.WaitUntilClosed():
	case <-time.After(time.Second):
		t.Fatal("connection did not close after malformed response")
	}
}

// TestBaseConn_ConcurrentStateReaders exercises State/IsActive/IsClosed under
// concurrent reads while Close writes state (race detector).
func TestBaseConn_ConcurrentStateReaders(t *testing.T) {
	conn := newConn(newFakeFrameTransport())
	conn.Start()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = conn.State()
					_ = conn.IsActive()
					_ = conn.IsClosed()
				}
			}
		}()
	}

	conn.Close()
	close(stop)
	wg.Wait()

	if !conn.IsClosed() {
		t.Fatal("expected connection to be closed")
	}
}
