// Copyright 2026-2027, QuarkChain.

package conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
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
// write parked inside WriteFrame (shutdown is interrupt-first).
type interruptibleFakeFrameTransport struct {
	*fakeFrameTransport
	interruptOnce sync.Once
}

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

// staticReaderTransport feeds wire.ReadFrame from a fixed byte stream (clean
// EOF vs truncated frame semantics).
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

func pongHandler() TypedHandler {
	return func(req any) (any, error) {
		return &wire.PongResponse{}, nil
	}
}

func validPongPayload(t *testing.T) []byte {
	t.Helper()
	payload, err := serialize.SerializeToBytes(&wire.PongResponse{})
	if err != nil {
		t.Fatalf("serialize pong: %v", err)
	}
	return payload
}

func validPingPayload(t *testing.T) []byte {
	t.Helper()
	payload, err := serialize.SerializeToBytes(&wire.PingRequest{FullShardIDList: []uint32{1}})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}
	return payload
}

func newConn(tr FrameTransport) *BaseConn {
	return NewBaseConn(Config{Transport: tr, Logger: log.New()})
}

func newPingConn(tr FrameTransport) *BaseConn {
	return NewBaseConn(Config{
		Transport:   tr,
		Serializers: pingSerializers,
		Logger:      log.New(),
	})
}

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

// newTestBaseConnPair creates a pair of BaseConns over a local TCP socket with
// PING/PONG on both sides and a PING handler on the server side.
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

// waitForGoroutines polls until the goroutine count drops back to the baseline.
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

// -- Config validation tests --------------------------------------------------

// TestConfig_ValidationPanics covers config validation panics.
func TestConfig_ValidationPanics(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"nil transport", Config{}, "Transport"},
		{"nil serializer", Config{Transport: newFakeFrameTransport(), Serializers: map[byte]*OpSerializer{0x01: nil}}, "serializer"},
		{"nil handler", Config{Transport: newFakeFrameTransport(), Handlers: map[byte]TypedHandler{0x01: nil}}, "handler"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected panic for " + tt.name)
				}
				msg, _ := r.(string)
				if !strings.Contains(msg, tt.want) {
					t.Fatalf("panic %q does not contain %q", msg, tt.want)
				}
			}()
			NewBaseConn(tt.cfg)
		})
	}
}

// TestConfig_ResponseOpcodeConflictPanics: a response opcode colliding with a
// request opcode would overwrite the serializer map entry and misroute frames.
func TestConfig_ResponseOpcodeConflictPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for response/request opcode conflict")
		}
	}()
	NewBaseConn(Config{
		Transport: newFakeFrameTransport(),
		Serializers: map[byte]*OpSerializer{
			0x01: {ResponseOpCode: 0x02},
			0x02: {ResponseOpCode: 0x03},
		},
		Logger: log.New(),
	})
}

// -- BaseConn unit tests (fake transport) --------------------------------------

// TestBaseConn_CloseWithInFlightWrite: shutdown is interrupt-first — the writer
// parked in WriteFrame is released by transport.Close, never by the writeMu
// barrier (a barrier-first shutdown would hang on a real net.Conn with a full
// send buffer).
func TestBaseConn_CloseWithInFlightWrite(t *testing.T) {
	t.Run("barrier: Close waits for in-flight write", func(t *testing.T) {
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
		if !tr.closeWhileWriting {
			t.Fatal("expected interrupt-first shutdown: transport.Close must run while the write is still in flight")
		}
		if err := <-result; err != ErrConnectionClosed {
			t.Fatalf("expected ErrConnectionClosed, got %v", err)
		}
	})

	t.Run("interrupt: Close releases blocked writer", func(t *testing.T) {
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
	})
}

// TestBaseConn_CleanShutdown: graceful shutdown (local Close, clean peer EOF)
// publishes no error; transport close errors are returned by Close.
func TestBaseConn_CleanShutdown(t *testing.T) {
	t.Run("local Close publishes no error", func(t *testing.T) {
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
	})

	t.Run("clean peer EOF publishes no error", func(t *testing.T) {
		conn := newConn(newStaticReaderTransport(bytes.NewReader(nil)))
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
	})

	t.Run("Close returns transport error", func(t *testing.T) {
		closeErr := errors.New("close failed")
		tr := newFakeFrameTransport()
		tr.closeErr = closeErr
		conn := newConn(tr)
		if err := conn.Close(); !errors.Is(err, closeErr) {
			t.Fatalf("expected transport close error, got %v", err)
		}
	})
}

// TestBaseConn_CanceledRPCNotSent: an RPC whose context expires while waiting
// for writeMu must not allocate an rpc id, register pending, or write a frame
// once it acquires the lock — the peer must never execute an abandoned call.
func TestBaseConn_CanceledRPCNotSent(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writeStarted = make(chan struct{})
	tr.releaseWrite = make(chan struct{})
	conn := newPingConn(tr)
	conn.Start()

	// First RPC parks inside WriteFrame holding writeMu.
	first := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		first <- err
	}()
	select {
	case <-tr.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("first write did not block")
	}
	select {
	case <-tr.writes: // rpc_id=1 frame recorded; writer still parked
	case <-time.After(time.Second):
		t.Fatal("first write was not recorded")
	}

	// Second RPC's context expires while it waits for writeMu.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), nil)
		second <- err
	}()

	time.Sleep(30 * time.Millisecond)
	close(tr.releaseWrite) // unblock the writer; second RPC acquires writeMu

	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline exceeded, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled RPC did not return after writer unblocked")
	}

	// No frame was written for the canceled RPC.
	select {
	case f := <-tr.writes:
		t.Fatalf("canceled RPC wrote a frame (rpc_id=%d)", f.RPCID)
	default:
	}
	// Only the first RPC remains pending; no id was consumed.
	if n := conn.pendingLen(); n != 1 {
		t.Fatalf("pending = %d, want 1", n)
	}

	// The next RPC gets rpc_id 2, proving the canceled call allocated nothing.
	third := make(chan rpcResult, 1)
	go func() {
		resp, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		third <- rpcResult{resp: resp, err: err}
	}()
	select {
	case f := <-tr.writes:
		if f.RPCID != 2 {
			t.Fatalf("next rpc id = %d, want 2", f.RPCID)
		}
	case <-time.After(time.Second):
		t.Fatal("next RPC did not write")
	}

	// Complete both RPCs and close.
	tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: 1, Payload: validPongPayload(t)}
	tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: 2, Payload: validPongPayload(t)}
	if err := <-first; err != nil {
		t.Fatalf("first RPC failed: %v", err)
	}
	if res := <-third; res.err != nil {
		t.Fatalf("third RPC failed: %v", res.err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close connection: %v", err)
	}
}

// TestBaseConn_CancelErrorContract: SendRPC with an expired or canceled
// context fails immediately without writing.
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
			conn := newConn(newFakeFrameTransport())
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
			byte(wire.ClusterOpPing): pingSer,
		},
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): func(req any) (any, error) {
				panic("handler panic test")
			},
		},
		Logger: log.New(),
	})
	conn.Start()

	tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: validPingPayload(t)}
	select {
	case <-conn.WaitUntilClosed():
		if !conn.IsClosed() {
			t.Fatal("expected connection to close after handler panic")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection did not close after handler panic")
	}
}

// TestBaseConn_ResponseCancelRaceCleansPending: response and cancel racing for
// the same rpc id must complete the RPC exactly once and leave no pending
// entry (race detector coverage).
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
// produce strictly increasing rpc ids — the peer's monotonic check depends on
// it.
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
}

// TestBaseConn_LateHandlerCompletedAfterClose: a handler finishing after Close
// drops its response without panicking or leaking.
func TestBaseConn_LateHandlerCompletedAfterClose(t *testing.T) {
	base := newFakeFrameTransport()
	base.releaseWrite = make(chan struct{})
	tr := &interruptibleFakeFrameTransport{fakeFrameTransport: base}

	const op = byte(wire.ClusterOpPing)
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	conn := NewBaseConn(Config{
		Transport:   tr,
		Serializers: pingSerializers,
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

	tr.frames <- &wire.Frame{Opcode: op, RPCID: 1, Payload: validPingPayload(t)}
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

	close(releaseHandler)
	waitForGoroutines(t, before)
}

// TestBaseConn_StartCloseLifecycle: Start on closed is a no-op, Close without
// Start must not block on readerDone, double Close closes the transport once.
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

	t.Run("double close closes transport once", func(t *testing.T) {
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
	})
}

// TestBaseConn_ResponseOpcodeMismatchDeliversResponse: Python matches
// responses by rpc_id only and never validates the response opcode.
func TestBaseConn_ResponseOpcodeMismatchDeliversResponse(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(Config{
		Transport: tr,
		Serializers: map[byte]*OpSerializer{
			byte(wire.ClusterOpPing):                   pingSer,
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

// -- SendCommand tests --------------------------------------------------------

// TestBaseConn_SendCommand: fire-and-forget commands carry rpc_id=0, run the
// handler without a response, and fail on a closed connection.
func TestBaseConn_SendCommand(t *testing.T) {
	t.Run("writes fire-and-forget request with no response", func(t *testing.T) {
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

		tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 0, Payload: validPingPayload(t)}

		select {
		case f := <-tr.writes:
			t.Fatalf("expected no response for fire-and-forget command, got opcode 0x%x rpc_id=%d", f.Opcode, f.RPCID)
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("returns error on closed connection", func(t *testing.T) {
		conn := newConn(newFakeFrameTransport())
		conn.Close()

		err := conn.SendCommand(byte(wire.ClusterOpPing), nil)
		if err != ErrConnectionClosed {
			t.Fatalf("expected ErrConnectionClosed, got %v", err)
		}
	})
}

// -- Inbound dispatch tests ----------------------------------------------------

// TestDispatch_InvalidFramesCloseConnection: malformed or unrecognized inbound
// frames close the connection, mirroring Python's close-with-error.
func TestDispatch_InvalidFramesCloseConnection(t *testing.T) {
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client"),
		FullShardIDList: []uint32{0x00010001},
	})

	tests := []struct {
		name   string
		server bool // true: server conn (handles PING); false: client conn (receives PONG)
		frames []*wire.Frame
	}{
		{
			name:   "unsupported request opcode",
			server: true,
			frames: []*wire.Frame{{Opcode: byte(wire.ClusterOpAddRootBlockRequest), RPCID: 1, Payload: pingPayload}},
		},
		{
			name:   "trailing bytes in request payload",
			server: true,
			frames: []*wire.Frame{{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: append(append([]byte{}, pingPayload...), 0xFF)}},
		},
		{
			name:   "zero rpc_id on rpc request",
			server: true,
			frames: []*wire.Frame{{Opcode: byte(wire.ClusterOpPing), RPCID: 0, Payload: pingPayload}},
		},
		{
			name:   "duplicate rpc_id",
			server: true,
			frames: []*wire.Frame{
				{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: pingPayload},
				{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: pingPayload},
			},
		},
		{
			name:   "decreasing rpc_id",
			server: true,
			frames: []*wire.Frame{
				{Opcode: byte(wire.ClusterOpPing), RPCID: 2, Payload: pingPayload},
				{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: pingPayload},
			},
		},
		{
			name:   "unknown response opcode",
			server: false,
			frames: []*wire.Frame{{Opcode: 0xFF, RPCID: 1, Payload: []byte{0x00}}},
		},
		{
			name:   "unknown response rpc_id",
			server: false,
			frames: []*wire.Frame{{Opcode: byte(wire.ClusterOpPong), RPCID: 999, Payload: validPongPayload(t)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newFakeFrameTransport()
			var conn *BaseConn
			if tt.server {
				conn = newPingServerConn(tr)
			} else {
				conn = newPingConn(tr)
			}
			conn.Start()
			defer conn.Close()

			for _, f := range tt.frames {
				select {
				case tr.frames <- f:
				case <-time.After(time.Second):
					t.Fatal("transport write blocked feeding frame")
				}
			}

			select {
			case <-conn.WaitUntilClosed():
				if !conn.IsClosed() {
					t.Fatal("expected connection to close")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("connection did not close after invalid frame")
			}
		})
	}
}

// TestPeerRPCID_Uint64Monotonic: inbound rpc ids are compared as unsigned.
// Python peers start at 1 (write_rpc_request pre-increments; 0 is reserved
// for fire-and-forget), so the zero-value sentinel rejects rpc_id=0. High-half
// ids must remain monotonic — an int64 comparison would wrap them negative
// and reject a legitimate Python peer after 2^63 requests.
func TestPeerRPCID_Uint64Monotonic(t *testing.T) {
	conn := newConn(newFakeFrameTransport())

	if conn.defaultValidateRPCID(0) {
		t.Fatal("rpc_id=0 accepted; 0 is reserved for non-RPC commands")
	}
	if !conn.defaultValidateRPCID(1) {
		t.Fatal("first rpc_id=1 rejected")
	}
	if conn.defaultValidateRPCID(1) {
		t.Fatal("duplicate rpc_id=1 accepted")
	}
	for _, id := range []uint64{math.MaxInt64, 1 << 63, math.MaxUint64} {
		if !conn.defaultValidateRPCID(id) {
			t.Fatalf("rpc_id=%d rejected", id)
		}
	}
	if conn.defaultValidateRPCID(math.MaxUint64) {
		t.Fatal("duplicate MaxUint64 accepted")
	}
}

// -- BaseConn integration tests (TCP pair) ------------------------------------

// TestDispatch_ValidExchange: a well-formed PING/PONG exchange over TCP
// delivers the deserialized *PongResponse and leaves both sides open.
func TestDispatch_ValidExchange(t *testing.T) {
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
	if !ok || pong == nil {
		t.Fatalf("expected *PongResponse, got %T", resp)
	}

	if client.IsClosed() || server.IsClosed() {
		t.Fatal("connection closed after valid exchange")
	}
}

// -- Forwarder tests -----------------------------------------------------------

// TestForwarder_RoutesFrame: ForwardConsumed skips dispatch, ForwardPass
// dispatches normally.
func TestForwarder_RoutesFrame(t *testing.T) {
	tests := []struct {
		name      string
		result    ForwardResult
		wantReply bool
	}{
		{"consumed", ForwardConsumed, false},
		{"pass", ForwardPass, true},
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
				Forwarder: func(f *wire.Frame) ForwardResult {
					forwarded <- f
					return tt.result
				},
				Logger: log.New(),
			})
			conn.Start()
			defer conn.Close()

			tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: validPingPayload(t)}

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

// TestForwarder_CloseRequest: ForwardClose shuts the connection down on the
// reader goroutine without dispatching the frame; the reader loop must exit so
// a subsequent Close returns promptly (regression: a forwarder calling Close()
// synchronously deadlocks on readerDone).
func TestForwarder_CloseRequest(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(Config{
		Transport:   tr,
		Serializers: pingSerializers,
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): pongHandler(),
		},
		Forwarder: func(f *wire.Frame) ForwardResult {
			return ForwardClose
		},
		Logger: log.New(),
	})
	conn.Start()

	tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: validPingPayload(t)}

	select {
	case err := <-conn.Error():
		if err == nil || !strings.Contains(err.Error(), "forwarder requested close") {
			t.Fatalf("expected forwarder-close error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ForwardClose did not shut down the connection")
	}
	if !conn.IsClosed() {
		t.Fatal("connection should be closed after ForwardClose")
	}

	// The frame must not have dispatched: no pong response.
	select {
	case f := <-tr.writes:
		t.Fatalf("expected no dispatch after ForwardClose, got opcode 0x%x", f.Opcode)
	default:
	}

	closeDone := make(chan struct{})
	go func() {
		conn.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() deadlocked waiting for readerLoop exit after ForwardClose")
	}
	if got := tr.closes(); got != 1 {
		t.Fatalf("transport closed %d times, want 1", got)
	}
}

// -- Non-RPC tests --------------------------------------------------------------

// TestNonRPC: a non-RPC opcode dispatches to the handler without a response;
// a non-RPC opcode with non-zero rpc_id is a protocol violation.
func TestNonRPC(t *testing.T) {
	t.Run("dispatches handler without response", func(t *testing.T) {
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

		tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 0, Payload: validPingPayload(t)}

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
	})

	t.Run("non-zero rpc_id shuts down connection", func(t *testing.T) {
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

		tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 99, Payload: validPingPayload(t)}

		select {
		case <-conn.WaitUntilClosed():
		case <-time.After(time.Second):
			t.Fatal("connection did not close after non-RPC with non-zero rpc_id")
		}
	})
}
