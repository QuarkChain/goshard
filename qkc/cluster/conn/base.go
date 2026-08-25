// Copyright 2026-2027, QuarkChain.

// Package conn provides the generic RPC engine used by cluster connection
// implementations.
package conn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// TypedHandler processes a deserialized request and returns a deserialized
// response. The framework handles payload serialization/deserialization.
type TypedHandler func(req any) (resp any, err error)

// OpSerializer describes how to deserialize a request and serialize a response
// for a specific opcode.
type OpSerializer struct {
	NewRequest     func() any
	NewResponse    func() any
	Deserialize    func([]byte, any) error
	Serialize      func(any) ([]byte, error)
	ResponseOpCode byte
}

// OpSerializerFor creates an OpSerializer for request type R and response type
// S, with the response opcode set to respOp.
func OpSerializerFor[R, S any](respOp byte) *OpSerializer {
	return &OpSerializer{
		NewRequest:     func() any { return new(R) },
		NewResponse:    func() any { return new(S) },
		Deserialize:    func(p []byte, v any) error { return serialize.DeserializeFromBytes(p, v) },
		Serialize:      func(v any) ([]byte, error) { return serialize.SerializeToBytes(v) },
		ResponseOpCode: respOp,
	}
}

// pendingRPC represents an in-flight RPC call waiting for its response.
type pendingRPC struct {
	result chan rpcResult // cap 1
	stop   func() bool    // context.AfterFunc stop; It is always non-nil.
}

type rpcResult struct {
	resp any
	err  error
}

// ConnectionState mirrors Python's protocol.ConnectionState.
type ConnectionState int32

const (
	ConnectionStateConnecting ConnectionState = iota
	ConnectionStateActive
	ConnectionStateClosed
)

// BaseConn is the shared RPC engine used by cluster connection
// implementations.
//
// Concurrency model:
//
//   - Reader: a single readerLoop owns transport.ReadFrame() and inbound
//     frame processing. No other goroutine reads the transport.
//
//   - Writer: there is no writer loop. All transport.WriteFrame() calls are
//     serialized by writeMu. SendRPC allocates rpc_id while holding writeMu,
//     preserving the ordering between rpc_id allocation and serialized writes.
//
//   - Protocol state (mu): mu protects mutable connection state shared across goroutines:
//     state, nextRPCID, pending, and timedOut.
//
// Lock rules:
//
//   - writeMu may be acquired before mu.
//   - mu must never be held while acquiring writeMu.
//   - mu must never be held during network I/O.
//
// Invariants:
//
//   - Never hold writeMu while waiting for an RPC response.
//   - RPC completion is arbitrated by pending deletion under mu.
//     Each RPC completes exactly once by response, timeout, or connection close.
type BaseConn struct {
	// transport is the frame I/O backend. All writes go through
	// writeMu; all reads through readerLoop.
	transport FrameTransport

	// Configuration. Immutable after construction.
	serializers   map[byte]*OpSerializer
	typedHandlers map[byte]TypedHandler
	nonRPCOps     map[byte]struct{}
	forwarder     func(*wire.Frame) bool

	// -- Protocol state (mu) --
	mu        sync.RWMutex
	state     ConnectionState
	pending   map[uint64]*pendingRPC
	timedOut  map[uint64]struct{}
	nextRPCID uint64

	// Owned by readerLoop. Zero value is the "no request seen yet" sentinel:
	// rpc_id=0 is reserved for non-RPC (fire-and-forget) commands (see
	// SendCommandMeta), so every valid inbound RPC request has rpc_id >= 1 and
	// passes the monotonic check against the zero-value state.
	peerRPCID uint64

	// -- Frame send serialization (writeMu) --
	writeMu sync.Mutex

	// -- Synchronization primitives --
	shutdownOnce sync.Once
	// activeChan signals that startup has completed. The connection may be ACTIVE
	// or already closed; callers must check IsActive().
	activeChan chan struct{}
	closedChan chan struct{} // closed during shutdown

	log log.Logger
}

// NewBaseConn creates a BaseConn from the supplied configuration.
// The caller is responsible for calling Start().
func NewBaseConn(cfg Config) *BaseConn {
	cfg.validate()

	logger := cfg.Logger
	if logger == nil {
		logger = log.Root()
	}

	// Register RPC serializers under both their request and response opcodes;
	// non-RPC serializers are request-only (dummy response opcode ignored).
	serializers := make(map[byte]*OpSerializer, len(cfg.Serializers)*2)
	for opcode, ser := range cfg.Serializers {
		serializers[opcode] = ser

		if _, nonRPC := cfg.NonRPCOps[opcode]; !nonRPC {
			serializers[ser.ResponseOpCode] = ser
		}
	}

	// Copy maps so the caller's maps are not shared.
	nonRPCOps := make(map[byte]struct{}, len(cfg.NonRPCOps))
	for op := range cfg.NonRPCOps {
		nonRPCOps[op] = struct{}{}
	}
	handlers := make(map[byte]TypedHandler, len(cfg.Handlers))
	for op, h := range cfg.Handlers {
		handlers[op] = h
	}

	rc := &BaseConn{
		transport:     cfg.Transport,
		typedHandlers: handlers,
		serializers:   serializers,
		nonRPCOps:     nonRPCOps,
		forwarder:     cfg.Forwarder,
		activeChan:    make(chan struct{}),
		closedChan:    make(chan struct{}),
		pending:       make(map[uint64]*pendingRPC),
		timedOut:      make(map[uint64]struct{}),
		state:         ConnectionStateConnecting,
		log:           logger,
	}
	return rc
}

// -- Public API ---------------------------------------------------------------
//
// The public surface mirrors Python's AbstractConnection:
//
//   SendRPC / SendRPCMeta  -> write_rpc_request (RPC with response tracking)
//   SendCommand / Meta     -> write_command     (fire-and-forget, rpc_id=0)
//   Start                  -> active_and_loop_forever
//   Close                  -> close
//   WaitUntilActive        -> wait_until_active
//   WaitUntilClosed        -> wait_until_closed
//   IsActive / IsClosed    -> is_active / is_closed

// Start transitions the connection to ACTIVE and starts the reader loop.
// If the connection is already started or closed, Start is a no-op.
func (c *BaseConn) Start() {
	c.mu.Lock()
	if c.state != ConnectionStateConnecting {
		c.mu.Unlock()
		return
	}
	c.state = ConnectionStateActive
	close(c.activeChan)
	c.mu.Unlock()

	go c.readerLoop()
}

// Close closes the connection and wakes all pending RPCs.
func (c *BaseConn) Close() {
	c.shutdown(nil)
}

// SendRPC sends a request without metadata and waits for its response.
func (c *BaseConn) SendRPC(ctx context.Context, opcode byte, payload []byte) (any, error) {
	return c.SendRPCMeta(ctx, opcode, payload, wire.ClusterMetadata{})
}

// SendRPCMeta sends a request with metadata and waits for its response.
// Returns the deserialized response object (single-deserialization path).
func (c *BaseConn) SendRPCMeta(
	ctx context.Context,
	opcode byte,
	payload []byte,
	meta wire.ClusterMetadata,
) (any, error) {
	call := &pendingRPC{
		result: make(chan rpcResult, 1),
	}

	c.writeMu.Lock()

	if err := ctx.Err(); err != nil {
		c.writeMu.Unlock()
		return nil, err
	}

	c.mu.Lock()
	if err := c.checkActiveLocked(); err != nil {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return nil, err
	}

	c.nextRPCID++
	rpcID := c.nextRPCID
	c.pending[rpcID] = call

	call.stop = context.AfterFunc(ctx, func() {
		c.cancelRPC(rpcID, ctx.Err())
	})
	c.mu.Unlock()

	frame := &wire.Frame{
		Meta:    meta,
		Opcode:  opcode,
		RPCID:   rpcID,
		Payload: payload,
	}
	err := c.transport.WriteFrame(frame)
	c.writeMu.Unlock()

	if err != nil {
		// The transport is unusable: trigger shutdown (sync.Once,
		// non-blocking). Shutdown completes this RPC with ErrConnectionClosed
		// alongside every other pending RPC.
		c.shutdown(fmt.Errorf("write frame rpc=%d: %w", rpcID, err))
	}

	res := <-call.result
	return res.resp, res.err
}

// SendCommand sends a fire-and-forget command (rpc_id=0, no response
// expected).
func (c *BaseConn) SendCommand(opcode byte, payload []byte) error {
	return c.SendCommandMeta(opcode, payload, wire.ClusterMetadata{})
}

// SendCommandMeta sends a fire-and-forget command with metadata.
func (c *BaseConn) SendCommandMeta(opcode byte, payload []byte, meta wire.ClusterMetadata) error {
	frame := &wire.Frame{
		Meta:    meta,
		Opcode:  opcode,
		RPCID:   0,
		Payload: payload,
	}
	return c.writeFrame(frame)
}

// WriteFrame writes a pre-built frame to the transport, serialized by writeMu
// together with every other outbound frame on this connection. It is the
// low-level "write a complete frame verbatim" entry: unlike SendRPC/SendCommand
// it does not allocate an rpc_id or construct a new frame.
//
// It is exposed so connections that route already-constructed frames from other
// connections can reuse this connection's writeMu for physical serialization
// (e.g. the slave's MasterConn forwarding virtual PeerConn frames).
func (c *BaseConn) WriteFrame(f *wire.Frame) error {
	return c.writeFrame(f)
}

// -- Query methods -----------------------------------------------------------

// RemoteAddr returns the transport's remote address.
func (c *BaseConn) RemoteAddr() string { return c.transport.RemoteAddr() }

// WaitUntilActive returns a channel closed when startup completes.
// The connection may be ACTIVE or already closed; use IsActive() to check.
func (c *BaseConn) WaitUntilActive() <-chan struct{} { return c.activeChan }

// WaitUntilClosed returns a channel closed when shutdown begins.
func (c *BaseConn) WaitUntilClosed() <-chan struct{} { return c.closedChan }

// Logger returns the connection logger.
func (c *BaseConn) Logger() log.Logger { return c.log }

// State returns the current connection state.
func (c *BaseConn) State() ConnectionState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// IsActive reports whether the connection is active.
func (c *BaseConn) IsActive() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state == ConnectionStateActive
}

// IsClosed reports whether the connection is closed.
func (c *BaseConn) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state == ConnectionStateClosed
}

// -- Internal helpers --------------------------------------------------------

// pendingLen returns the number of in-flight RPCs. Used by tests.
func (c *BaseConn) pendingLen() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.pending)
}

// checkActiveLocked maps the connection state to the error an in-flight write
// should return. It must be called with mu held: Active is nil, Closed is
// ErrConnectionClosed, and Connecting is ErrNotActive.
func (c *BaseConn) checkActiveLocked() error {
	switch c.state {
	case ConnectionStateActive:
		return nil
	case ConnectionStateClosed:
		return ErrConnectionClosed
	default:
		return ErrNotActive
	}
}

func rpcTimeoutError(err error) error {
	return fmt.Errorf("rpc timeout: %w", err)
}

// -- Write path ---------------------------------------------------------------
//
// writeFrame serializes transport writes with writeMu. Write failures trigger
// shutdown after the lock is released; shutdown acquires writeMu as a barrier,
// so shutdown must never be entered while writeMu is held.

// writeFrame writes a pre-built frame.
func (c *BaseConn) writeFrame(f *wire.Frame) error {
	c.writeMu.Lock()

	c.mu.Lock()
	if err := c.checkActiveLocked(); err != nil {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return err
	}
	c.mu.Unlock()

	err := c.transport.WriteFrame(f)
	c.writeMu.Unlock()

	if err != nil {
		c.shutdown(fmt.Errorf("write frame: %w", err))
	}
	return err
}

// -- readerLoop --------------------------------------------------------------

// readerLoop reads frames from the transport and dispatches them.
// Read errors trigger shutdown.
func (c *BaseConn) readerLoop() {
	for {
		frame, err := c.transport.ReadFrame()
		if err != nil {
			c.shutdown(normalizeReadErr(err))
			return
		}
		c.handleFrameSafely(frame)
	}
}

// -- handleFrame -------------------------------------------------------------

// handleFrameSafely runs handleFrame with panic isolation: any panic from
// frame processing (the forwarder, response deserialization, serializer
// callbacks, or future extension points) is converted into a connection
// shutdown instead of crashing the process.
func (c *BaseConn) handleFrameSafely(frame *wire.Frame) {
	defer func() {
		if recovered := recover(); recovered != nil {
			c.log.Error("frame processing panic",
				"opcode", frame.Opcode, "rpcid", frame.RPCID,
				"panic", recovered, "stack", string(debug.Stack()))
			c.shutdown(fmt.Errorf("frame processing panic (opcode=0x%x rpc_id %d): %v",
				frame.Opcode, frame.RPCID, recovered))
		}
	}()
	c.handleFrame(frame)
}

func (c *BaseConn) handleFrame(frame *wire.Frame) {
	// Run the forwarder first. A true return means it consumed the frame, so
	// normal dispatch is skipped. The forwarder may call c.Close() directly to
	// shut the connection down; Close is non-blocking and safe to invoke from
	// the reader goroutine.
	if fwd := c.forwarder; fwd != nil && fwd(frame) {
		return
	}

	handler, isRequest := c.typedHandlers[frame.Opcode]
	_, isNonRPC := c.nonRPCOps[frame.Opcode]
	ser := c.serializers[frame.Opcode]

	if isRequest {
		c.handleRequest(frame, handler, ser, isNonRPC)
	} else {
		c.handleResponse(frame, ser)
	}
}

// -- handleResponse (inbound response matching) -------------------------------

// handleResponse matches an inbound response frame to a pending RPC.
// Unknown or malformed responses close the connection regardless of rpc_id.
func (c *BaseConn) handleResponse(frame *wire.Frame, ser *OpSerializer) {
	if ser == nil {
		c.log.Warn("unknown response opcode", "opcode", frame.Opcode)
		c.shutdown(fmt.Errorf("unknown response opcode 0x%x", frame.Opcode))
		return
	}
	resp := ser.NewResponse()
	if err := ser.Deserialize(frame.Payload, resp); err != nil {
		c.log.Warn("malformed response payload", "opcode", frame.Opcode, "err", err)
		c.shutdown(fmt.Errorf("malformed response payload for opcode 0x%x: %w", frame.Opcode, err))
		return
	}

	// The pending deletion under mu decides which of response/timeout/close
	// completes the RPC; only the winner delivers on result.
	c.mu.Lock()
	call, ok := c.pending[frame.RPCID]
	if ok {
		// Responses are matched solely by rpc_id (Python compatibility);
		// the response opcode is not validated.
		delete(c.pending, frame.RPCID)
		c.mu.Unlock()
		call.stop()
		call.result <- rpcResult{resp: resp}
		return
	}

	// Late response for a timed-out RPC: drop it silently.
	_, isTimedOut := c.timedOut[frame.RPCID]
	if isTimedOut {
		delete(c.timedOut, frame.RPCID)
		c.mu.Unlock()
		c.log.Debug("ignoring late rpc response", "rpcid", frame.RPCID)
		return
	}
	c.mu.Unlock()

	// Truly unknown rpc_id — never sent by this connection.
	c.log.Error("unexpected rpc response", "rpcid", frame.RPCID, "opcode", frame.Opcode)
	c.shutdown(fmt.Errorf("unexpected rpc response %d", frame.RPCID))
}

// -- handleRequest (inbound request dispatch) --------------------------------

func (c *BaseConn) handleRequest(frame *wire.Frame, handler TypedHandler, ser *OpSerializer, isNonRPC bool) {
	if ser == nil {
		c.log.Warn("handler without serializer", "opcode", frame.Opcode)
		c.shutdown(fmt.Errorf("handler without serializer for opcode 0x%x", frame.Opcode))
		return
	}
	if isNonRPC && frame.RPCID != 0 {
		c.log.Warn("non-rpc command with non-zero rpc_id", "opcode", frame.Opcode, "rpcid", frame.RPCID)
		c.shutdown(fmt.Errorf("non-rpc command with rpc id %d", frame.RPCID))
		return
	}

	if !isNonRPC {
		// defaultValidateRPCID is owned exclusively by readerLoop, so no lock needed.
		ok := c.defaultValidateRPCID(frame.RPCID)
		if !ok {
			c.log.Warn("incorrect rpc request id sequence", "rpcid", frame.RPCID)
			c.shutdown(fmt.Errorf("incorrect rpc request id sequence"))
			return
		}
	}

	go c.dispatch(frame, handler, ser)
}

// -- dispatch (handler execution + response write) ----------------------------

func (c *BaseConn) dispatch(frame *wire.Frame, handler TypedHandler, ser *OpSerializer) {
	defer func() {
		if recovered := recover(); recovered != nil {
			c.shutdown(fmt.Errorf("handler panic (opcode=0x%x): %v", frame.Opcode, recovered))
		}
	}()

	req := ser.NewRequest()
	if err := ser.Deserialize(frame.Payload, req); err != nil {
		c.shutdown(fmt.Errorf("deserialize failed: %w", err))
		return
	}
	resp, err := handler(req)
	if err != nil {
		c.log.Error("request handler failed", "opcode", frame.Opcode, "err", err)
		c.shutdown(err)
		return
	}

	// Fire-and-forget commands (rpc_id=0) get no response frame.
	if frame.RPCID == 0 {
		return
	}

	respPayload, err := ser.Serialize(resp)
	if err != nil {
		c.shutdown(fmt.Errorf("serialize response failed: %w", err))
		return
	}

	respFrame := &wire.Frame{
		Meta:    frame.Meta,
		Opcode:  ser.ResponseOpCode,
		RPCID:   frame.RPCID,
		Payload: respPayload,
	}

	if err := c.writeFrame(respFrame); err != nil {
		c.log.Debug("response write failed, connection shutting down", "rpcid", frame.RPCID, "err", err)
	}
}

// -- cancelRPC ----------------------------------------------------------------

// cancelRPC completes an RPC with a timeout error. The pending deletion and
// timedOut entry are set under the same mu lock, so a late response always
// sees a consistent view.
func (c *BaseConn) cancelRPC(rpcID uint64, cause error) {
	c.mu.Lock()
	call, ok := c.pending[rpcID]
	if !ok {
		c.mu.Unlock()
		return // already completed by response or close
	}
	delete(c.pending, rpcID)

	c.timedOut[rpcID] = struct{}{}
	c.mu.Unlock()

	call.result <- rpcResult{err: rpcTimeoutError(cause)}
}

// -- Shutdown -----------------------------------------------------------------

// shutdown is the non-blocking internal entry point; sync.Once guarantees
// exactly one execution across all concurrent callers.
func (c *BaseConn) shutdown(cause error) {
	c.shutdownOnce.Do(func() {
		// Collect completions under mu; run stop/send side effects outside.
		var pending []*pendingRPC

		c.mu.Lock()
		wasConnecting := c.state == ConnectionStateConnecting
		if wasConnecting {
			close(c.activeChan)
		}

		c.state = ConnectionStateClosed
		if cause != nil {
			// Mirror Python: the close cause is logged and otherwise
			// discarded (close_with_error's return value is unused).
			c.log.Error("connection closed with error", "err", cause)
		}
		close(c.closedChan)

		for id, call := range c.pending {
			delete(c.pending, id)
			pending = append(pending, call)
		}
		for id := range c.timedOut {
			delete(c.timedOut, id)
		}
		c.mu.Unlock()

		for _, call := range pending {
			call.stop()
			call.result <- rpcResult{err: ErrConnectionClosed}
		}

		// Close transport to interrupt blocked I/O.
		// Writes already accepted by the transport may still complete.
		if err := c.transport.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			c.log.Warn("transport close failed", "err", err)
		}

		c.writeMu.Lock()
		c.writeMu.Unlock()
	})
}

// -- RPC ID validation (default) ---------------------------------------------

func (c *BaseConn) defaultValidateRPCID(rpcID uint64) bool {
	if rpcID <= c.peerRPCID {
		return false
	}
	c.peerRPCID = rpcID
	return true
}

// -- Helpers ------------------------------------------------------------------

func normalizeReadErr(err error) error {
	if errors.Is(err, io.EOF) {
		return nil // clean close — not an error
	}
	return err
}
