// Copyright 2026-2027, QuarkChain.

package conn

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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
	writePanic        bool
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
	if t.writePanic {
		panic("fake transport write panic")
	}
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

// receive waits for one value from ch and fails the test instead of hanging
// when it never arrives.
func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatalf("%s never arrived", what)
		var zero T
		return zero
	}
}

// feedFrame delivers a frame into the fake transport's inbound queue.
func feedFrame(t *testing.T, tr *fakeFrameTransport, f *wire.Frame) {
	t.Helper()
	select {
	case tr.frames <- f:
	case <-time.After(time.Second):
		t.Fatal("feeding frame blocked: transport queue full")
	}
}

// awaitClosed fails the test if the connection is not closed shortly.
func awaitClosed(t *testing.T, conn *BaseConn, what string) {
	t.Helper()
	select {
	case <-conn.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatalf("connection did not close after %s", what)
	}
	if !conn.IsClosed() {
		t.Fatalf("connection state is not Closed after %s", what)
	}
}

// -- Config validation tests --------------------------------------------------

// serializerMissingCallback returns a full ping serializer with one callback
// removed, for config validation tests.
func serializerMissingCallback(missing string) *OpSerializer {
	ser := OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong))
	switch missing {
	case "NewRequest":
		ser.NewRequest = nil
	case "NewResponse":
		ser.NewResponse = nil
	case "Deserialize":
		ser.Deserialize = nil
	case "Serialize":
		ser.Serialize = nil
	default:
		panic("unknown callback: " + missing)
	}
	return ser
}

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
		{"missing NewRequest callback", Config{Transport: newFakeFrameTransport(), Serializers: map[byte]*OpSerializer{0x01: serializerMissingCallback("NewRequest")}}, "missing callback"},
		{"missing NewResponse callback", Config{Transport: newFakeFrameTransport(), Serializers: map[byte]*OpSerializer{0x01: serializerMissingCallback("NewResponse")}}, "missing callback"},
		{"missing Deserialize callback", Config{Transport: newFakeFrameTransport(), Serializers: map[byte]*OpSerializer{0x01: serializerMissingCallback("Deserialize")}}, "missing callback"},
		{"missing Serialize callback", Config{Transport: newFakeFrameTransport(), Serializers: map[byte]*OpSerializer{0x01: serializerMissingCallback("Serialize")}}, "missing callback"},
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

// TestConfig_ResponseOpcodePanics: response opcodes must be unique and disjoint
// from every request opcode (RPC or non-RPC), otherwise serializers[respOp]
// would be overwritten by map-iteration order and inbound frames decoded
// through a random serializer.
func TestConfig_ResponseOpcodePanics(t *testing.T) {
	tests := []struct {
		name        string
		serializers map[byte]*OpSerializer
		nonRPC      map[byte]struct{}
	}{
		{
			name: "response opcode conflicts with rpc request opcode",
			serializers: map[byte]*OpSerializer{
				0x01: OpSerializerFor[wire.PingRequest, wire.PongResponse](0x02),
				0x02: OpSerializerFor[wire.PingRequest, wire.PongResponse](0x03),
			},
		},
		{
			name: "duplicate response opcode",
			serializers: map[byte]*OpSerializer{
				0x81: OpSerializerFor[wire.PingRequest, wire.PongResponse](0x90),
				0x82: OpSerializerFor[wire.PingRequest, wire.PongResponse](0x90),
			},
		},
		{
			name: "response opcode conflicts with non-rpc request opcode",
			serializers: map[byte]*OpSerializer{
				0x81: OpSerializerFor[wire.PingRequest, wire.PongResponse](0x90),
				0x90: OpSerializerFor[wire.PingRequest, wire.PongResponse](0x90),
			},
			nonRPC: map[byte]struct{}{0x90: {}},
		},
		{
			name:        "self-referencing rpc response opcode",
			serializers: map[byte]*OpSerializer{0x81: OpSerializerFor[wire.PingRequest, wire.PongResponse](0x81)},
		},
		{
			name:        "zero response opcode for rpc",
			serializers: map[byte]*OpSerializer{0x81: OpSerializerFor[wire.PingRequest, wire.PongResponse](0)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic for " + tt.name)
				}
			}()
			NewBaseConn(Config{
				Transport:   newFakeFrameTransport(),
				Serializers: tt.serializers,
				NonRPCOps:   tt.nonRPC,
				Logger:      log.New(),
			})
		})
	}
}

// TestConfig_NonRPCDummyResponseOpcode: fire-and-forget commands (Python's
// op_non_rpc_map, e.g. NEW_BLOCK_MINOR) configure a dummy self-referencing or
// zero ResponseOpCode. Declared in NonRPCOps, the dummy value must be ignored:
// no panic, no response-opcode registration, no opcode 0x00 pollution.
func TestConfig_NonRPCDummyResponseOpcode(t *testing.T) {
	const op = byte(0x42)
	conn := NewBaseConn(Config{
		Transport: newFakeFrameTransport(),
		Serializers: map[byte]*OpSerializer{
			op: OpSerializerFor[wire.PingRequest, wire.PongResponse](op), // self-referencing dummy, as in slave configs
		},
		NonRPCOps: map[byte]struct{}{op: {}},
		Handlers:  map[byte]TypedHandler{op: pongHandler()},
		Logger:    log.New(),
	})
	if _, ok := conn.serializers[op]; !ok {
		t.Fatal("non-RPC request opcode not registered")
	}
	if len(conn.serializers) != 1 {
		t.Fatalf("serializers has %d entries, want 1 (dummy response opcode must not be registered)", len(conn.serializers))
	}
}

// -- BaseConn unit tests (fake transport) --------------------------------------

// TestBaseConn_CloseWithInFlightWrite verifies that shutdown does not wait for
// an in-flight WriteFrame. Shutdown completes pending RPCs and closes the
// transport (interrupt-first) while a blocked write may still be in progress.
func TestBaseConn_CloseWithInFlightWrite(t *testing.T) {
	t.Run("close does not wait for in-flight write", func(t *testing.T) {
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
		case <-time.After(time.Second):
			t.Fatal("Close blocked waiting for in-flight write")
		}
		if !tr.closeWhileWriting {
			t.Fatal("expected interrupt-first shutdown: transport.Close must run while the write is still in flight")
		}

		close(tr.releaseWrite)
		select {
		case err := <-result:
			if err != ErrConnectionClosed {
				t.Fatalf("expected ErrConnectionClosed, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("RPC did not complete after write released")
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

func TestBaseConn_CleanShutdown(t *testing.T) {
	t.Run("local Close", func(t *testing.T) {
		conn := newConn(newFakeFrameTransport())
		conn.Start()
		conn.Close()
	})

	t.Run("clean peer EOF", func(t *testing.T) {
		conn := newConn(newStaticReaderTransport(bytes.NewReader(nil)))
		conn.Start()

		<-conn.WaitUntilClosed()
		if !conn.IsClosed() {
			t.Fatal("connection should be closed after clean EOF")
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
	conn.Close()
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

// TestBaseConn_TimeoutThenLateResponse: a caller-side timeout removes the
// pending entry, marks the rpc_id as timed out (connection stays open), and a
// late peer response for that id is dropped silently — it must neither wake a
// finished call nor corrupt/shutdown the connection.
func TestBaseConn_TimeoutThenLateResponse(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newPingConn(tr)
	conn.Start()
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), nil)
		result <- err
	}()
	request := receive(t, tr.writes, "ping request")

	err := receive(t, result, "timeout result")
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "rpc timeout") {
		t.Fatalf("expected wrapped rpc timeout, got %v", err)
	}
	if conn.IsClosed() {
		t.Fatal("caller-side timeout must not shut the connection down")
	}
	if n := conn.pendingLen(); n != 0 {
		t.Fatalf("pending = %d after timeout, want 0", n)
	}
	if len(conn.timedOut) != 1 {
		t.Fatalf("timedOut has %d entries, want 1", len(conn.timedOut))
	}

	// Late response for the timed-out id.
	feedFrame(t, tr, &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: request.RPCID, Payload: validPongPayload(t)})

	// Barrier: frames are consumed FIFO by the single readerLoop, so completing
	// this fresh RPC proves the late PONG was fully processed beforehand.
	next := make(chan rpcResult, 1)
	go func() {
		resp, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		next <- rpcResult{resp: resp, err: err}
	}()
	request2 := receive(t, tr.writes, "second ping request")
	feedFrame(t, tr, &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: request2.RPCID, Payload: validPongPayload(t)})

	res := receive(t, next, "second rpc result")
	if res.err != nil {
		t.Fatalf("rpc after late response failed: %v", res.err)
	}
	if len(conn.timedOut) != 0 {
		t.Fatalf("timedOut has %d entries after late response was consumed, want 0 (silent drop must clear the marker)", len(conn.timedOut))
	}
	if conn.IsClosed() {
		t.Fatal("late response must not shut the connection down")
	}
}

func TestBaseConn_WriteFailureClosesConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writeErr = errors.New("write failed")
	conn := newConn(tr)
	conn.Start()

	_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
	if err != ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
	<-conn.WaitUntilClosed()
	if !conn.IsClosed() {
		t.Fatal("connection should be closed after write failure")
	}
}

// TestBaseConn_WriteFailureReleasesWriteMuAndShutsDown: the transport error
// path (not just the panic path) must surface the raw error to the sender,
// shut the connection down (transport closed exactly once), and leave writeMu
// free: follow-up sends observe the Closed state instead of blocking forever.
func TestBaseConn_WriteFailureReleasesWriteMuAndShutsDown(t *testing.T) {
	writeBoom := errors.New("fake write boom")
	tr := newFakeFrameTransport()
	tr.writeErr = writeBoom
	conn := newConn(tr)
	conn.Start()

	errCh := make(chan error, 1)
	go func() { errCh <- conn.SendCommand(byte(wire.ClusterOpPing), nil) }()
	if err := receive(t, errCh, "SendCommand result"); !errors.Is(err, writeBoom) {
		t.Fatalf("SendCommand error = %v, want the raw transport error", err)
	}

	awaitClosed(t, conn, "transport write failure")
	if got := tr.closes(); got != 1 {
		t.Fatalf("transport closed %d times, want 1", got)
	}

	// Follow-up send must return promptly with ErrConnectionClosed.
	errCh2 := make(chan error, 1)
	go func() { errCh2 <- conn.SendCommand(byte(wire.ClusterOpPing), nil) }()
	if err := receive(t, errCh2, "post-failure SendCommand"); err != ErrConnectionClosed {
		t.Fatalf("SendCommand after write failure = %v, want ErrConnectionClosed", err)
	}
}

// TestBaseConn_WriteFramePanicReleasesWriteMu: a panic from the transport's
// WriteFrame is caught by writeFrameLocked (SendRPCMeta/writeFrame release
// writeMu via the returned error, not a defer). If writeMu were leaked, senders
// issued after the panic would block forever at writeMu.Lock() instead of
// observing the Closed state.
func TestBaseConn_WriteFramePanicReleasesWriteMu(t *testing.T) {
	tr := newFakeFrameTransport()
	tr.writePanic = true
	conn := newConn(tr)
	conn.Start()

	// The panic surfaces as an error from the write path, which triggers
	// shutdown rather than leaving writeMu held.
	result := make(chan error, 1)
	go func() {
		_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		result <- err
	}()
	select {
	case err := <-result:
		if err != ErrConnectionClosed {
			t.Fatalf("SendRPC after WriteFrame panic: expected ErrConnectionClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SendRPC blocked on writeMu after WriteFrame panic")
	}
	// A later sender (blocking on writeMu) must observe the Closed state, not
	// wait forever: proves writeMu was released.
	if err := conn.SendCommand(byte(wire.ClusterOpPing), nil); err != ErrConnectionClosed {
		t.Fatalf("SendCommand after WriteFrame panic: expected ErrConnectionClosed, got %v", err)
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

// TestBaseConn_ResponseCancelCloseRaceCompletesOnce: response, cancel, and
// close racing for the same rpc id must complete the RPC exactly once with
// no pending leak and no deadlock (the third arbitration corner not covered
// by TestBaseConn_ResponseCancelRaceCleansPending).
func TestBaseConn_ResponseCancelCloseRaceCompletesOnce(t *testing.T) {
	const iterations = 200
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
		go func() { <-start; cancel() }()
		go func() {
			<-start
			tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: request.RPCID, Payload: pongPayload}
		}()
		go func() { <-start; conn.Close() }()
		close(start)

		select {
		case err := <-result:
			// Any of the three outcomes is valid; completion is what matters.
			if err != nil && err != ErrConnectionClosed &&
				!errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "rpc timeout") {
				t.Fatalf("iteration %d: unexpected error: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: SendRPC never completed (deadlock)", i)
		}
		if pending := conn.pendingLen(); pending != 0 {
			t.Fatalf("iteration %d: pending RPCs remain: %d", i, pending)
		}
		cancel()
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

// TestBaseConn_CloseWakesEveryPendingRPC: N concurrently parked callers are
// all woken exactly once with ErrConnectionClosed; pending drains to zero.
func TestBaseConn_CloseWakesEveryPendingRPC(t *testing.T) {
	before := runtime.NumGoroutine()
	tr := newFakeFrameTransport()
	tr.writes = make(chan *wire.Frame, 64)
	conn := newPingConn(tr)
	conn.Start()

	const n = 16
	results := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
			results <- err
		}()
	}
	for i := 0; i < n; i++ {
		receive(t, tr.writes, "pending request") // every caller reached its wait point
	}

	conn.Close()
	for i := 0; i < n; i++ {
		if err := receive(t, results, "woken rpc"); err != ErrConnectionClosed {
			t.Fatalf("woken rpc returned %v, want ErrConnectionClosed", err)
		}
	}
	wg.Wait()
	if n := conn.pendingLen(); n != 0 {
		t.Fatalf("pending = %d after shutdown, want 0", n)
	}
	waitForGoroutines(t, before)
}

// TestBaseConn_TruncatedFrameClosesConnection: EOF mid-frame shuts the
// connection down (Python close_with_error on unexpected EOF; the cause is
// only logged).
func TestBaseConn_TruncatedFrameClosesConnection(t *testing.T) {
	// payload_len = 10 but the stream ends right after the length header.
	tr := newStaticReaderTransport(bytes.NewReader([]byte{0x00, 0x00, 0x00, 0x0a}))
	conn := newConn(tr)
	conn.Start()

	<-conn.WaitUntilClosed()
	if !conn.IsClosed() {
		t.Fatal("connection should be closed after truncated frame")
	}
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
// Start must not block, double Close closes the transport once.
func TestBaseConn_StartCloseLifecycle(t *testing.T) {
	t.Run("start on closed is no-op", func(t *testing.T) {
		conn := newConn(newFakeFrameTransport())
		conn.Close()
		conn.Start()
		if !conn.IsClosed() {
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
			t.Fatal("Close() blocked/deadlocked for a connection that was never started")
		}
	})

	t.Run("concurrent start and close", func(t *testing.T) {
		conn := newConn(newFakeFrameTransport())
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			conn.Start()
		}()
		go func() {
			defer wg.Done()
			conn.Close()
		}()
		wg.Wait()
		if !conn.IsClosed() {
			t.Fatal("connection should be closed after concurrent Start/Close")
		}
	})

	t.Run("double close closes transport once", func(t *testing.T) {
		tr := newFakeFrameTransport()
		conn := newConn(tr)
		conn.Close()
		conn.Close()
		if got := tr.closes(); got != 1 {
			t.Fatalf("transport closed %d times, want 1", got)
		}
	})
}

// TestBaseConn_WaitUntilActiveLifecycle pins the channel contract: it fires
// once startup completes, stays silent before Start, and still unblocks when
// Close wins the race against Start (shutdown closes activeChan while
// connecting).
func TestBaseConn_WaitUntilActiveLifecycle(t *testing.T) {
	t.Run("Start transitions Connecting to Active", func(t *testing.T) {
		conn := newConn(newFakeFrameTransport())
		if conn.State() != ConnectionStateConnecting {
			t.Fatalf("initial state = %v, want Connecting", conn.State())
		}
		conn.Start()
		select {
		case <-conn.WaitUntilActive():
		case <-time.After(time.Second):
			t.Fatal("WaitUntilActive did not fire after Start")
		}
		if !conn.IsActive() || conn.IsClosed() {
			t.Fatalf("state after Start: active=%v closed=%v", conn.IsActive(), conn.IsClosed())
		}
		conn.Close()
	})

	t.Run("does not fire before Start", func(t *testing.T) {
		conn := newConn(newFakeFrameTransport())
		select {
		case <-conn.WaitUntilActive():
			t.Fatal("active fired before Start")
		default:
		}
	})

	t.Run("unblocks when closed while connecting", func(t *testing.T) {
		conn := newConn(newFakeFrameTransport())
		conn.Close()
		select {
		case <-conn.WaitUntilActive():
		case <-time.After(2 * time.Second):
			t.Fatal("WaitUntilActive did not unblock after early Close")
		}
		if !conn.IsClosed() || conn.IsActive() {
			t.Fatal("expected Closed state after early close")
		}
	})
}

// TestBaseConn_StartIsIdempotent: repeat Start must be a no-op — state stays
// Active, dispatch keeps working exactly once per frame, and shutdown still
// closes the transport exactly once.
func TestBaseConn_StartIsIdempotent(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newPingServerConn(tr)

	before := runtime.NumGoroutine()
	conn.Start()
	conn.Start()

	feedFrame(t, tr, &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: validPingPayload(t)})
	reply := receive(t, tr.writes, "pong reply")
	if reply.RPCID != 1 || reply.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("unexpected reply opcode=0x%x rpc_id=%d", reply.Opcode, reply.RPCID)
	}
	if conn.State() != ConnectionStateActive {
		t.Fatalf("state after second Start = %v, want Active", conn.State())
	}

	conn.Close()
	if got := tr.closes(); got != 1 {
		t.Fatalf("transport closed %d times, want 1", got)
	}
	waitForGoroutines(t, before)
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

// -- Dispatch fatal paths ------------------------------------------------------
//
// One processing error must close the whole connection. The four reviewer-flag
// gaps are covered here: handler error, response serialization failure,
// response deserialization failure, and transport write failure
// (TestBaseConn_WriteFailureReleasesWriteMuAndShutsDown).

// TestBaseConn_TrailingBytesShutDownConnection: serialize.DeserializeFromBytes
// requires full payload consumption, so trailing bytes are a protocol
// violation in both directions — deserialize failure is fatal.
func TestBaseConn_TrailingBytesShutDownConnection(t *testing.T) {
	t.Run("request payload trailing bytes", func(t *testing.T) {
		tr := newFakeFrameTransport()
		conn := newPingServerConn(tr)
		conn.Start()

		good := validPingPayload(t)
		bad := append(good[:len(good):len(good)], 0xFF)
		feedFrame(t, tr, &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: bad})
		awaitClosed(t, conn, "request with trailing bytes")
	})

	t.Run("response payload trailing bytes", func(t *testing.T) {
		tr := newFakeFrameTransport()
		conn := newPingConn(tr)
		conn.Start()

		result := make(chan rpcResult, 1)
		go func() {
			resp, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
			result <- rpcResult{resp: resp, err: err}
		}()
		request := receive(t, tr.writes, "ping request")

		good := validPongPayload(t)
		bad := append(good[:len(good):len(good)], 0xFF)
		feedFrame(t, tr, &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: request.RPCID, Payload: bad})

		awaitClosed(t, conn, "response with trailing bytes")
		res := receive(t, result, "aborted rpc result")
		if res.err != ErrConnectionClosed {
			t.Fatalf("pending rpc error = %v, want ErrConnectionClosed", res.err)
		}
		if n := conn.pendingLen(); n != 0 {
			t.Fatalf("pending = %d after malformed response, want 0", n)
		}
	})
}

// TestBaseConn_HandlerWithoutSerializerShutsDown: Config.validate refuses to
// build a handler-without-serializer, so the handleRequest guard is
// defense-in-depth; internal tests reach it by injecting a handler directly.
func TestBaseConn_HandlerWithoutSerializerShutsDown(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newPingServerConn(tr)
	op := byte(wire.ClusterOpAddRootBlockRequest)
	conn.typedHandlers[op] = func(any) (any, error) { return &wire.PongResponse{}, nil }
	conn.Start()

	feedFrame(t, tr, &wire.Frame{Opcode: op, RPCID: 1, Payload: []byte{0x01}})
	awaitClosed(t, conn, "request whose handler lacks a serializer")
}

// TestBaseConn_HandlerErrorShutsDownConnection: a handler returning an error
// is fatal; no partial response may be written.
func TestBaseConn_HandlerErrorShutsDownConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	handlerErr := errors.New("handler boom")
	conn := NewBaseConn(Config{
		Transport:   tr,
		Serializers: pingSerializers,
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): func(any) (any, error) { return nil, handlerErr },
		},
		Logger: log.New(),
	})
	conn.Start()

	feedFrame(t, tr, &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 5, Payload: validPingPayload(t)})
	awaitClosed(t, conn, "handler error")

	select {
	case f := <-tr.writes:
		t.Fatalf("unexpected write after handler error: opcode 0x%x rpc_id=%d", f.Opcode, f.RPCID)
	default:
	}
}

// TestBaseConn_ResponseSerializationFailureShutsDown: the handler succeeds but
// serializing its response fails -> connection shutdown, nothing written.
func TestBaseConn_ResponseSerializationFailureShutsDown(t *testing.T) {
	tr := newFakeFrameTransport()
	broken := *pingSer // same codec, response Serialize always fails
	broken.Serialize = func(any) ([]byte, error) { return nil, errors.New("serialize boom") }
	conn := NewBaseConn(Config{
		Transport: tr,
		Serializers: map[byte]*OpSerializer{
			byte(wire.ClusterOpPing): &broken,
		},
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): pongHandler(),
		},
		Logger: log.New(),
	})
	conn.Start()

	feedFrame(t, tr, &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 7, Payload: validPingPayload(t)})
	awaitClosed(t, conn, "response serialization failure")

	select {
	case f := <-tr.writes:
		t.Fatalf("unexpected write after serialization failure: opcode 0x%x rpc_id=%d", f.Opcode, f.RPCID)
	default:
	}
}

// TestBaseConn_MalformedResponseClosesPendingRPC: an inbound response payload
// that cannot be deserialized shuts the connection down and ends the waiting
// caller with ErrConnectionClosed.
func TestBaseConn_MalformedResponseClosesPendingRPC(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := newPingConn(tr)
	conn.Start()

	result := make(chan rpcResult, 1)
	go func() {
		resp, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		result <- rpcResult{resp: resp, err: err}
	}()
	request := receive(t, tr.writes, "ping request")

	// Structurally broken payload for the pong codec.
	feedFrame(t, tr, &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: request.RPCID, Payload: []byte{0x01, 0x02, 0x03}})

	awaitClosed(t, conn, "malformed response payload")
	res := receive(t, result, "aborted rpc result")
	if res.err != ErrConnectionClosed {
		t.Fatalf("pending rpc error = %v, want ErrConnectionClosed", res.err)
	}
	if n := conn.pendingLen(); n != 0 {
		t.Fatalf("pending = %d after malformed response, want 0", n)
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

// -- Forwarder tests -----------------------------------------------------------

// TestForwarder_RoutesFrame: returning true (consumed) skips dispatch,
// returning false (pass) dispatches normally.
func TestForwarder_RoutesFrame(t *testing.T) {
	tests := []struct {
		name      string
		consumed  bool
		wantReply bool
	}{
		{"consumed", true, false},
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
					return tt.consumed
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

// TestForwarder_CloseRequest: a forwarder calling c.Close() directly on the
// reader goroutine shuts the connection down without dispatching the frame and
// does not deadlock (regression: a forwarder calling Close() must be safe now
// that Close is non-blocking).
func TestForwarder_CloseRequest(t *testing.T) {
	tr := newFakeFrameTransport()
	var conn *BaseConn
	conn = NewBaseConn(Config{
		Transport:   tr,
		Serializers: pingSerializers,
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): pongHandler(),
		},
		Forwarder: func(f *wire.Frame) bool {
			// Close is non-blocking and safe to call from the reader
			// goroutine; return true so the closing frame is not dispatched.
			conn.Close()
			return true
		},
		Logger: log.New(),
	})
	conn.Start()

	tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: validPingPayload(t)}

	select {
	case <-conn.WaitUntilClosed():
	case <-time.After(time.Second):
		t.Fatal("forwarder Close() did not shut down the connection")
	}
	if !conn.IsClosed() {
		t.Fatal("connection should be closed after forwarder Close()")
	}

	// The frame must not have dispatched: no pong response.
	select {
	case f := <-tr.writes:
		t.Fatalf("expected no dispatch after forwarder Close(), got opcode 0x%x", f.Opcode)
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
		t.Fatal("Close() deadlocked waiting for readerLoop exit after forwarder Close()")
	}
	if got := tr.closes(); got != 1 {
		t.Fatalf("transport closed %d times, want 1", got)
	}
}

// TestForwarder_PanicIsolatesConnection: a panic inside the forwarder must
// not crash the process; it shuts the connection down with a descriptive
// error instead (reader-path panic isolation).
func TestForwarder_PanicIsolatesConnection(t *testing.T) {
	tr := newFakeFrameTransport()
	conn := NewBaseConn(Config{
		Transport:   tr,
		Serializers: pingSerializers,
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): pongHandler(),
		},
		Forwarder: func(f *wire.Frame) bool {
			panic("forwarder boom")
		},
		Logger: log.New(),
	})
	conn.Start()
	defer conn.Close()

	tr.frames <- &wire.Frame{Opcode: byte(wire.ClusterOpPing), RPCID: 1, Payload: validPingPayload(t)}

	select {
	case <-conn.WaitUntilClosed():
	case <-time.After(time.Second):
		t.Fatal("WaitUntilClosed did not fire after forwarder panic")
	}
	if !conn.IsClosed() {
		t.Fatal("connection should be closed after forwarder panic")
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

// TestBaseConn_NonRPCFrameDoesNotMatchPendingResponse: a registered non-RPC
// command is routed to its handler (never through the response matcher), and
// an outstanding outbound RPC stays matchable afterwards.
func TestBaseConn_NonRPCFrameDoesNotMatchPendingResponse(t *testing.T) {
	const cmdOp = byte(0x42) // self-referencing dummy response opcode, as in slave configs
	tr := newFakeFrameTransport()
	handlerCalled := make(chan struct{})
	conn := NewBaseConn(Config{
		Transport: tr,
		Serializers: map[byte]*OpSerializer{
			byte(wire.ClusterOpPing): pingSer,
			cmdOp:                    OpSerializerFor[wire.PingRequest, wire.PongResponse](cmdOp),
		},
		Handlers: map[byte]TypedHandler{
			cmdOp: func(req any) (any, error) {
				close(handlerCalled)
				return &wire.PongResponse{}, nil
			},
		},
		NonRPCOps: map[byte]struct{}{cmdOp: {}},
		Logger:    log.New(),
	})
	conn.Start()
	defer conn.Close()

	pending := make(chan rpcResult, 1)
	go func() {
		resp, err := conn.SendRPC(context.Background(), byte(wire.ClusterOpPing), nil)
		pending <- rpcResult{resp: resp, err: err}
	}()
	request := receive(t, tr.writes, "pending request")

	feedFrame(t, tr, &wire.Frame{Opcode: cmdOp, RPCID: 0, Payload: validPingPayload(t)})
	select {
	case <-handlerCalled:
	case <-time.After(time.Second):
		t.Fatal("non-RPC handler was not invoked")
	}

	// The original RPC completes normally afterwards: the non-RPC dispatch
	// never touched the pending map or the response path.
	feedFrame(t, tr, &wire.Frame{Opcode: byte(wire.ClusterOpPong), RPCID: request.RPCID, Payload: validPongPayload(t)})
	res := receive(t, pending, "pending rpc result")
	if res.err != nil {
		t.Fatalf("pending rpc failed after non-RPC dispatch: %v", res.err)
	}

	// Fire-and-forget commands get no response frame.
	select {
	case f := <-tr.writes:
		t.Fatalf("unexpected write for non-RPC cmd: opcode 0x%x rpc_id=%d", f.Opcode, f.RPCID)
	case <-time.After(50 * time.Millisecond):
	}
	if conn.IsClosed() {
		t.Fatal("non-RPC dispatch must keep the connection open")
	}
}

// -- Full lifecycle simulation (real TCP + Python golden bytes) ----------------
//
// The byte streams below were generated by the real pyquarkchain
// implementation (quarkchain.cluster.rpc Ping/Pong + ClusterMetadata) and
// verify byte-level compatibility of the whole stack over a real TCP socket:
// frame codec, metadata, opcode mapping, payload serialization, rpc_id
// echo, concurrent bidirectional RPC, timeout/late-response, and close.

// Python: Ping(b"S7", [0x00010001, 0x00020001], None) as PING (0x81),
// rpc_id=1, default ClusterMetadata (branch=0, cluster_peer_id=0).
const pythonGoldenPingFrame = "0000001300000000000000000000000081000000000000000100000002533700000002000100010002000100"

// Python: Pong(b"M1", [0x00010001]) as PONG (0x82), rpc_id=1, default metadata.
const pythonGoldenPongFrame = "0000000e000000000000000000000000820000000000000001000000024d310000000100010001"

// Python: same PING with rpc_id=5 and ClusterMetadata(branch=0x00010001,
// cluster_peer_id=7); the expected PONG echoes that metadata and rpc_id.
const (
	pythonGoldenPingFrameMeta = "0000001300010001000000000000000781000000000000000500000002533700000002000100010002000100"
	pythonGoldenPongFrameMeta = "0000000e000100010000000000000007820000000000000005000000024d310000000100010001"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex fixture: %v", err)
	}
	return b
}

// newMetaConnPair creates a BaseConn over a real TCP socket using the
// 12-byte ClusterMetadata codec (master↔slave style), mirroring Python's
// MasterConnection/SlaveConnection construction.
func newMetaConnPair(t *testing.T, cfg Config) (*BaseConn, net.Conn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	peer, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}

	cfg.Transport = NewTCPTransport(peer,
		func(r io.Reader) (*wire.Frame, error) { return wire.ReadFrame(r, 0) },
		wire.WriteFrame)
	cfg.Logger = log.New()
	return NewBaseConn(cfg), raw
}

// readFull reads exactly len(buf) bytes with a deadline.
func readFull(t *testing.T, c net.Conn, buf []byte) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read %d bytes: %v", len(buf), err)
	}
}

// TestSimulation_PythonGoldenBytes: a raw peer (standing in for the Python
// master) writes Python-generated PING bytes; the Go connection must
// deserialize them, dispatch the handler, and reply with exactly the bytes
// Python would have produced for the PONG.
func TestSimulation_PythonGoldenBytes(t *testing.T) {
	var gotPing []*wire.PingRequest
	server, raw := newMetaConnPair(t, Config{
		Serializers: pingSerializers,
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): func(req any) (any, error) {
				ping := req.(*wire.PingRequest)
				gotPing = append(gotPing, ping)
				return &wire.PongResponse{ID: []byte("M1"), FullShardIDList: []uint32{0x00010001}}, nil
			},
		},
	})
	server.Start()
	defer server.Close()

	// Default metadata, rpc_id=1.
	if _, err := raw.Write(mustHex(t, pythonGoldenPingFrame)); err != nil {
		t.Fatalf("write golden ping: %v", err)
	}
	want := mustHex(t, pythonGoldenPongFrame)
	got := make([]byte, len(want))
	readFull(t, raw, got)
	if !bytes.Equal(got, want) {
		t.Fatalf("pong bytes mismatch\n got: %x\nwant: %x", got, want)
	}

	// Routed metadata (branch + cluster_peer_id), rpc_id=5: the response must
	// echo the request metadata verbatim (Python __write_rpc_response).
	if _, err := raw.Write(mustHex(t, pythonGoldenPingFrameMeta)); err != nil {
		t.Fatalf("write golden ping (meta): %v", err)
	}
	want = mustHex(t, pythonGoldenPongFrameMeta)
	got = make([]byte, len(want))
	readFull(t, raw, got)
	if !bytes.Equal(got, want) {
		t.Fatalf("pong bytes (meta) mismatch\n got: %x\nwant: %x", got, want)
	}

	if len(gotPing) != 2 {
		t.Fatalf("handler invoked %d times, want 2", len(gotPing))
	}
	if string(gotPing[0].ID) != "S7" || len(gotPing[0].FullShardIDList) != 2 ||
		gotPing[0].FullShardIDList[0] != 0x00010001 || gotPing[0].FullShardIDList[1] != 0x00020001 {
		t.Fatalf("deserialized ping mismatch: %+v", gotPing[0])
	}
	if gotPing[0].RootTip != nil {
		t.Fatalf("expected nil Optional root_tip, got %v", gotPing[0].RootTip)
	}
	if server.IsClosed() {
		t.Fatal("connection must stay open after golden exchange")
	}
}

// TestSimulation_FullLifecycle walks startup → bidirectional concurrent RPC →
// timeout with late response → clean close, mirroring the Python slave
// runtime interaction over one TCP connection.
func TestSimulation_FullLifecycle(t *testing.T) {
	var slow atomic.Bool // gates the server ping handler to force timeouts

	server, raw := newMetaConnPair(t, Config{
		Serializers: pingSerializers,
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): func(req any) (any, error) {
				if slow.Load() {
					time.Sleep(300 * time.Millisecond)
				}
				return &wire.PongResponse{ID: []byte("M1"), FullShardIDList: []uint32{0x00010001}}, nil
			},
		},
	})
	client := NewBaseConn(Config{
		Transport: NewTCPTransport(raw,
			func(r io.Reader) (*wire.Frame, error) { return wire.ReadFrame(r, 0) },
			wire.WriteFrame),
		Serializers: map[byte]*OpSerializer{
			byte(wire.ClusterOpPing): pingSer,
		},
		Handlers: map[byte]TypedHandler{
			byte(wire.ClusterOpPing): pongHandler(),
		},
		Logger: log.New(),
	})

	baseline := runtime.NumGoroutine()
	server.Start()
	client.Start()

	// -- Startup: both sides become active.
	select {
	case <-server.WaitUntilActive():
	case <-time.After(time.Second):
		t.Fatal("server never became active")
	}
	select {
	case <-client.WaitUntilActive():
	case <-time.After(time.Second):
		t.Fatal("client never became active")
	}

	pingPayload := mustSerialize(t, &wire.PingRequest{
		ID:              []byte("C1"),
		FullShardIDList: []uint32{0x00010001},
	})

	// -- Concurrent RPCs in both directions over the same connection.
	const callers = 8
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			_, err := client.SendRPC(context.Background(), byte(wire.ClusterOpPing), pingPayload)
			results <- err
		}()
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := server.SendRPC(ctx, byte(wire.ClusterOpPing), pingPayload)
			results <- err
		}()
	}
	for i := 0; i < 2*callers; i++ {
		if err := <-results; err != nil {
			t.Fatalf("bidirectional rpc failed: %v", err)
		}
	}

	// -- Timeout with a late response: the connection must survive both.
	slow.Store(true)
	slowCtx, slowCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer slowCancel()
	_, err := client.SendRPC(slowCtx, byte(wire.ClusterOpPing), pingPayload)
	if err == nil || !strings.Contains(err.Error(), "rpc timeout") {
		t.Fatalf("expected rpc timeout, got %v", err)
	}
	if client.IsClosed() || server.IsClosed() {
		t.Fatal("connection closed by rpc timeout")
	}
	// The late PONG for the timed-out call is dropped; the next RPC still works.
	freshCtx, freshCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer freshCancel()
	resp, err := client.SendRPC(freshCtx, byte(wire.ClusterOpPing), pingPayload)
	if err != nil {
		t.Fatalf("rpc after timeout failed: %v", err)
	}
	if _, ok := resp.(*wire.PongResponse); !ok {
		t.Fatalf("expected *PongResponse, got %T", resp)
	}
	if client.IsClosed() {
		t.Fatal("connection closed after late response")
	}

	// -- Close: pending RPCs are aborted; the peer observes clean EOF.
	inflight := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// The slow handler keeps this RPC in flight at Close time.
		_, err := client.SendRPC(ctx, byte(wire.ClusterOpPing), pingPayload)
		inflight <- err
	}()
	time.Sleep(50 * time.Millisecond) // let the request reach the server
	client.Close()

	select {
	case err := <-inflight:
		if err != ErrConnectionClosed {
			t.Fatalf("pending rpc at close: expected ErrConnectionClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending rpc not aborted by Close")
	}
	select {
	case <-server.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe client close")
	}
	if server.pendingLen() != 0 {
		t.Fatalf("server pending rpcs remain: %d", server.pendingLen())
	}
	waitForGoroutines(t, baseline)
}

// mustSerialize is a test helper for payload serialization.
func mustSerialize(t *testing.T, v any) []byte {
	t.Helper()
	b, err := serialize.SerializeToBytes(v)
	if err != nil {
		t.Fatalf("serialize %T: %v", v, err)
	}
	return b
}

// -- TCP transport write deadline ----------------------------------------------
//
// geth bounded-write model: every WriteFrame arms a per-frame write deadline
// so a peer that stops reading cannot block the writer (and writeMu) forever.

// newPipeTransport returns a TCP transport over net.Pipe, which honors
// deadlines and is unbuffered: a write with no reader blocks until deadline.
func newPipeTransport(t *testing.T, c net.Conn) FrameTransport {
	t.Helper()
	return NewTCPTransport(c,
		func(r io.Reader) (*wire.Frame, error) { return wire.ReadFrame(r, 0) },
		wire.WriteFrame)
}

// TestTCPTransport_WriteFrameStalledPeer: with nobody reading the peer end,
// WriteFrame must return a deadline error within a bounded time instead of
// blocking indefinitely.
func TestTCPTransport_WriteFrameStalledPeer(t *testing.T) {
	orig := frameWriteTimeout
	frameWriteTimeout = 50 * time.Millisecond
	defer func() { frameWriteTimeout = orig }()

	local, peer := net.Pipe()
	defer local.Close()
	defer peer.Close()
	tr := newPipeTransport(t, local)

	start := time.Now()
	err := tr.WriteFrame(&wire.Frame{Opcode: 1, RPCID: 1, Payload: []byte("ping")})
	if err == nil {
		t.Fatal("expected deadline error, got nil")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("expected deadline-exceeded error, got %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("write unblocked too late: %v", elapsed)
	}
}

// TestTCPTransport_WriteFrameClearsDeadline: a successful WriteFrame clears
// the write deadline, so it must not stay armed on the conn and break later
// raw writes once it expires.
func TestTCPTransport_WriteFrameClearsDeadline(t *testing.T) {
	orig := frameWriteTimeout
	frameWriteTimeout = 50 * time.Millisecond
	defer func() { frameWriteTimeout = orig }()

	local, peer := net.Pipe()
	defer local.Close()
	defer peer.Close()
	go io.Copy(io.Discard, peer) // drain so writes complete
	tr := newPipeTransport(t, local)

	if err := tr.WriteFrame(&wire.Frame{Opcode: 1, RPCID: 1, Payload: []byte("ping")}); err != nil {
		t.Fatalf("write frame: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // the armed deadline would be past due now

	// If the deadline were still armed, this raw write would fail with a
	// timeout; with it cleared it must succeed.
	if _, err := local.Write([]byte("raw")); err != nil {
		t.Fatalf("raw write after deadline expiry (deadline not cleared?): %v", err)
	}
}
