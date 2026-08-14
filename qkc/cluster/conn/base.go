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

// ConnectionState mirrors Python's protocol.ConnectionState.
type ConnectionState int32

const (
	ConnectionStateConnecting ConnectionState = iota
	ConnectionStateActive
	ConnectionStateClosed
)

// pendingRPC represents an in-flight RPC call waiting for its response.
type pendingRPC struct {
	result chan rpcResult // cap 1
	stop   func() bool    // context.AfterFunc stop
}

type rpcResult struct {
	frame *wire.Frame
	err   error
}

// BaseConn is the shared RPC engine used by cluster connection
// implementations.
//
// Concurrency model:
//   - mu protects lifecycle, configuration, RPC state, and close state.
//   - writeMu serializes transport writes and RPC ID/send ordering.
//   - shutdownOnce ensures shutdown runs once.
//   - Channels carry lifecycle/event notifications.
//
// Lock ordering: writeMu → mu. Never acquire writeMu while holding mu.
type BaseConn struct {
	FrameTransport

	// Configuration. Mutable only while Connecting; immutable after Active.
	typedHandlers map[byte]TypedHandler
	nonRPCOps     map[byte]struct{}
	// serializers is keyed by both request and response opcodes; each
	// OpSerializer is installed under both keys by RegisterOpSerializers.
	serializers   map[byte]*OpSerializer
	forwarder     func(*wire.Frame) bool
	validateRPCID func(clusterPeerID uint64, rpcID uint64) bool

	// ── Lifecycle + protocol state (mu) ──
	mu        sync.RWMutex
	state     ConnectionState
	pending   map[uint64]*pendingRPC
	timedOut  map[uint64]struct{}
	nextRPCID uint64
	closeErr  error
	// nil until readerLoop starts; closed when readerLoop exits.
	readerDone chan struct{}

	// Owned by readerLoop.
	peerRPCID int64

	// ── Frame send serialization (writeMu) ──
	writeMu sync.Mutex

	// ── Synchronization primitives ──
	shutdownOnce sync.Once
	activeChan   chan struct{} // closed once active, or on shutdown before activation
	closedChan   chan struct{} // closed during shutdown
	errChan      chan error    // cap 1, non-user errors

	log log.Logger
}

// NewBaseConn creates a BaseConn using the supplied frame transport.
func NewBaseConn(tr FrameTransport, logger log.Logger) *BaseConn {
	if logger == nil {
		logger = log.Root()
	}
	rc := &BaseConn{
		FrameTransport: tr,
		activeChan:     make(chan struct{}),
		closedChan:     make(chan struct{}),
		errChan:        make(chan error, 1),
		typedHandlers:  make(map[byte]TypedHandler),
		serializers:    make(map[byte]*OpSerializer),
		pending:        make(map[uint64]*pendingRPC),
		timedOut:       make(map[uint64]struct{}),
		nonRPCOps:      make(map[byte]struct{}),
		peerRPCID:      -1,
		state:          ConnectionStateConnecting,
		log:            logger,
	}
	rc.validateRPCID = rc.defaultValidateRPCID
	return rc
}

// NewBaseConnFromConn wraps a net.Conn with the supplied frame codec.
func NewBaseConnFromConn(
	conn net.Conn,
	readFrame func(io.Reader) (*wire.Frame, error),
	writeFrame func(io.Writer, *wire.Frame) error,
	logger log.Logger,
) *BaseConn {
	return NewBaseConn(newTransport(conn, readFrame, writeFrame), logger)
}

// ── Public API ──────────────────────────────────────────────────────────────

// Start transitions the connection to ACTIVE and starts the reader loop.
// If the connection is already closed, Start is a no-op.
//
// Idempotence is guaranteed by the state machine alone: state starts as
// Connecting and the only transition out of it (to Active) happens here under
// mu. Since no path returns state to Connecting, Start's side effects run at
// most once.
func (c *BaseConn) Start() {
	c.mu.Lock()
	if c.state != ConnectionStateConnecting {
		c.mu.Unlock()
		return
	}
	c.state = ConnectionStateActive
	close(c.activeChan)

	// Allocate the reader's done channel before spawning it. A non-nil
	// readerDone marks that readerLoop has been scheduled; it is closed
	// exactly once when readerLoop exits.
	done := make(chan struct{})
	c.readerDone = done
	c.mu.Unlock()

	go c.readerLoop(done)
}

// Close closes the connection and wakes all pending RPCs.
func (c *BaseConn) Close() error {
	c.initiateShutdown(nil)
	c.mu.RLock()
	done := c.readerDone
	c.mu.RUnlock()
	if done != nil {
		<-done
	}
	c.mu.RLock()
	err := c.closeErr
	c.mu.RUnlock()
	return err
}

// SubmitFrame sends a pre-built frame. The frame's RPCID and metadata are
// preserved as-is; no RPC tracking is created. Returns an error if the
// connection is not active.
func (c *BaseConn) SubmitFrame(f *wire.Frame) error {
	c.writeMu.Lock()

	c.mu.Lock()
	if c.state != ConnectionStateActive {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return ErrConnectionClosed
	}
	c.mu.Unlock()

	err := c.FrameTransport.WriteFrame(f)
	c.writeMu.Unlock()

	if err != nil {
		c.shutdown(fmt.Errorf("submit frame: %w", err))
		return err
	}
	return nil
}

// RegisterTypedHandlers registers handlers before Start is called.
func (c *BaseConn) RegisterTypedHandlers(handlers map[byte]TypedHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != ConnectionStateConnecting {
		panic("handlers must be registered before Start")
	}
	for opcode, handler := range handlers {
		if handler == nil {
			panic("handler must not be nil")
		}
		c.typedHandlers[opcode] = handler
	}
}

// RegisterOpSerializers registers serializers before Start is called.
//
// The input map is keyed by request opcodes. Each OpSerializer is also
// installed under its ResponseOpCode, so the internal serializers map covers
// both directions of every RPC. ResponseOpCode must be set: BaseConn
// deserializes inbound response payloads before rpc_id matching, so an unknown
// or malformed response closes the connection rather than being delivered to
// the caller.
func (c *BaseConn) RegisterOpSerializers(serializers map[byte]*OpSerializer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != ConnectionStateConnecting {
		panic("serializers must be registered before Start")
	}
	for opcode, ser := range serializers {
		if ser == nil {
			panic("serializer must not be nil")
		}
		if ser.NewRequest == nil {
			panic("serializer NewRequest must not be nil")
		}
		if ser.NewResponse == nil {
			panic("serializer NewResponse must not be nil")
		}
		if ser.Deserialize == nil {
			panic("serializer Deserialize must not be nil")
		}
		if ser.Serialize == nil {
			panic("serializer Serialize must not be nil")
		}
		if ser.ResponseOpCode == 0 {
			panic("serializer ResponseOpCode must be set")
		}
		c.serializers[opcode] = ser
		c.serializers[ser.ResponseOpCode] = ser
	}
}

// RegisterNonRPCOps marks opcodes as fire-and-forget before Start is called.
func (c *BaseConn) RegisterNonRPCOps(ops []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != ConnectionStateConnecting {
		panic("non-RPC opcodes must be registered before Start")
	}
	for _, op := range ops {
		c.nonRPCOps[op] = struct{}{}
	}
}

// SetForwarder installs a raw-frame forwarder hook.
func (c *BaseConn) SetForwarder(f func(*wire.Frame) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != ConnectionStateConnecting {
		panic("forwarder must be set before Start")
	}
	c.forwarder = f
}

// SetValidateRPCID installs a custom RPC request ID validation hook.
func (c *BaseConn) SetValidateRPCID(f func(clusterPeerID uint64, rpcID uint64) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != ConnectionStateConnecting {
		panic("validateRPCID must be set before Start")
	}
	c.validateRPCID = f
}

// SendRPC sends a request without metadata and waits for its response.
func (c *BaseConn) SendRPC(ctx context.Context, opcode byte, payload []byte) (*wire.Frame, error) {
	return c.SendRPCMeta(ctx, opcode, payload, wire.ClusterMetadata{})
}

// SendRPCMeta sends a request with metadata and waits for its response.
//
// rpc_id allocation, pending registration, and frame write are serialized
// under writeMu to guarantee rpc_id ordering matches network send order.
func (c *BaseConn) SendRPCMeta(
	ctx context.Context,
	opcode byte,
	payload []byte,
	meta wire.ClusterMetadata,
) (*wire.Frame, error) {
	call := &pendingRPC{
		result: make(chan rpcResult, 1),
	}

	// Phase 1: allocate rpc_id + register pending (writeMu → mu).
	c.writeMu.Lock()

	c.mu.Lock()
	if c.state == ConnectionStateClosed {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return nil, ErrConnectionClosed
	}
	if c.state != ConnectionStateActive {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return nil, ErrNotActive
	}
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return nil, rpcTimeoutError(err)
	}

	c.nextRPCID++
	rpcID := c.nextRPCID
	c.pending[rpcID] = call

	// AfterFunc is registered after pending assignment. mu is held
	// throughout, so if ctx is already done, the cancelRPC goroutine
	// blocks on mu until we unlock — it will always see a valid entry.
	call.stop = context.AfterFunc(ctx, func() {
		c.cancelRPC(rpcID, ctx.Err())
	})
	c.mu.Unlock()

	// Phase 2: recheck under writeMu, then write.
	c.mu.Lock()
	_, stillPending := c.pending[rpcID]
	if !stillPending || c.state != ConnectionStateActive {
		// Cancelled or closed while waiting for write — result already
		// delivered by cancelRPC or shutdown.
		c.mu.Unlock()
		c.writeMu.Unlock()
		res := <-call.result
		return nil, res.err
	}
	c.mu.Unlock()

	frame := &wire.Frame{
		Meta:    meta,
		Opcode:  opcode,
		RPCID:   rpcID,
		Payload: payload,
	}
	err := c.FrameTransport.WriteFrame(frame)
	c.writeMu.Unlock()

	if err != nil {
		c.shutdown(fmt.Errorf("write frame rpc=%d: %w", rpcID, err))
		res := <-call.result
		return nil, res.err
	}

	// Phase 3: wait for response / timeout / close.
	res := <-call.result
	if res.err != nil {
		return nil, res.err
	}
	return res.frame, nil
}

// ── Query methods ────────────────────────────────────────────────────────────

// Error returns connection failures. A caller-initiated Close does not publish
// an error.
func (c *BaseConn) Error() <-chan error { return c.errChan }

// RemoteAddr returns the transport's remote address.
func (c *BaseConn) RemoteAddr() string { return c.FrameTransport.RemoteAddr() }

// WaitUntilActive returns a channel closed after the connection becomes active
// or closes before activation.
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

// ── Internal helpers ─────────────────────────────────────────────────────────

// pendingLen returns the number of in-flight RPCs. Used by tests.
func (c *BaseConn) pendingLen() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.pending)
}

func rpcTimeoutError(err error) error {
	return fmt.Errorf("rpc timeout: %w", err)
}

// ── readerLoop ────────────────────────────────────────────────────────────────

// readerLoop is the single persistent goroutine. It reads frames from the
// transport and dispatches them. Read errors trigger shutdown. done is closed
// exactly once when readerLoop exits.
func (c *BaseConn) readerLoop(done chan struct{}) {
	defer close(done)
	for {
		frame, err := c.FrameTransport.ReadFrame()
		if err != nil {
			c.initiateShutdown(normalizeReadErr(err))
			return
		}
		c.handleFrame(frame)
	}
}

// ── handleFrame ───────────────────────────────────────────────────────────────

func (c *BaseConn) handleFrame(frame *wire.Frame) {
	// Configuration is immutable after Start(), so readerLoop (which only runs
	// once the connection is Active) reads it without any lock.
	fwd := c.forwarder
	handler, isRequest := c.typedHandlers[frame.Opcode]
	_, isNonRPC := c.nonRPCOps[frame.Opcode]
	ser := c.serializers[frame.Opcode]

	if fwd != nil && fwd(frame) {
		return
	}

	if isRequest {
		c.handleRequest(frame, handler, ser, isNonRPC)
	} else {
		c.handleResponse(frame, ser)
	}
}

// ── handleResponse (inbound response matching) ───────────────────────────────

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

	// Claim pattern: delete from pending under mu. Only one path
	// (response, timeout, close) can complete each RPC.
	c.mu.Lock()
	call, ok := c.pending[frame.RPCID]
	if ok {
		// Python compatibility:
		//
		// RPC responses are matched solely by rpc_id.
		//
		// Python's RPCConnection does not verify that the response opcode
		// matches the original request's expected response opcode.
		// If rpc_id matches a pending RPC, the response is delivered to
		// the waiting caller and the connection remains active.
		//
		// Although validating the response opcode would be more defensive,
		// doing so would diverge from Python behavior and break migration
		// compatibility.
		delete(c.pending, frame.RPCID)
		c.mu.Unlock()
		if call.stop != nil {
			call.stop()
		}
		call.result <- rpcResult{frame: frame}
		return
	}

	// Late response: check the timedOut table.
	// TimedOut entries are permanent (matching Python's behaviour where
	// cancelled futures stay in rpc_future_map until a response arrives
	// or the connection closes).  A late response is silently dropped
	// regardless of how much time has passed since the timeout.
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

// ── handleRequest (inbound request dispatch) ─────────────────────────────────

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
		// validateRPCID (and peerRPCID behind the default implementation) is
		// owned exclusively by readerLoop — the sole caller of handleRequest —
		// so no lock is required here.
		ok := c.validateRPCID(frame.Meta.ClusterPeerID, frame.RPCID)
		if !ok {
			c.log.Warn("incorrect rpc request id sequence", "rpcid", frame.RPCID)
			c.shutdown(fmt.Errorf("incorrect rpc request id sequence"))
			return
		}
	}

	go c.dispatch(frame, handler, ser)
}

// ── dispatch (handler execution + response write) ────────────────────────────

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

	// fire-and-forget: no response frame to send.
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

	c.writeMu.Lock()

	c.mu.Lock()
	if c.state != ConnectionStateActive {
		// Connection closed while handler was running — drop the response.
		c.mu.Unlock()
		c.writeMu.Unlock()
		return
	}
	c.mu.Unlock()

	werr := c.FrameTransport.WriteFrame(respFrame)
	c.writeMu.Unlock()

	if werr != nil {
		c.shutdown(fmt.Errorf("write response rpc=%d: %w", frame.RPCID, werr))
	}
}

// ── cancelRPC ─────────────────────────────────────────────────────────────────

// cancelRPC completes an RPC with a timeout error. It atomically removes the
// RPC from pending and adds a timedOut entry to silence any late response —
// both under the same mu lock to prevent a TOCTOU between readerLoop and
// handleResponse.
//
// The timedOut entry lives until a late response arrives (and is silently
// dropped) or the connection closes.  This matches Python's behaviour where
// cancelled futures stay in rpc_future_map indefinitely.
func (c *BaseConn) cancelRPC(rpcID uint64, cause error) {
	c.mu.Lock()
	call, ok := c.pending[rpcID]
	if !ok {
		c.mu.Unlock()
		return // already completed by response or close
	}
	delete(c.pending, rpcID)

	// Set timedOut atomically with the pending deletion so that a late
	// response arriving concurrently always sees a consistent view:
	// either "pending exists" (before cancel) or "timedOut exists"
	// (after cancel). There is no window where both are empty.
	c.timedOut[rpcID] = struct{}{}
	c.mu.Unlock()

	call.result <- rpcResult{err: rpcTimeoutError(cause)}
}

// ── Shutdown ──────────────────────────────────────────────────────────────────

// shutdown is the non-blocking internal entry point. Multiple callers
// (read failure, write failure, handler error/panic) may call concurrently;
// sync.Once guarantees exactly one execution.
func (c *BaseConn) shutdown(cause error) {
	c.initiateShutdown(cause)
}

// initiateShutdown performs the one-time state transition from any state to
// Closed. It wakes all pending RPCs, interrupts blocked I/O, waits for
// in-flight writes, and closes the transport.
//
// Important: it does NOT wait for readerDone inside sync.Once — otherwise
// readerLoop's own initiateShutdown call (triggered by transport.Close
// unblocking ReadFrame) would deadlock. Close() waits for readerDone
// outside sync.Once.
func (c *BaseConn) initiateShutdown(cause error) {
	c.shutdownOnce.Do(func() {
		// Step 1: state transition + wake pending + clear timedOut.
		c.mu.Lock()
		if c.state != ConnectionStateClosed {
			c.state = ConnectionStateClosed
			close(c.closedChan)
			select {
			case <-c.activeChan:
			default:
				close(c.activeChan)
			}

			for id, call := range c.pending {
				delete(c.pending, id)
				if call.stop != nil {
					call.stop()
				}
				call.result <- rpcResult{err: ErrConnectionClosed}
			}
			for id := range c.timedOut {
				delete(c.timedOut, id)
			}

			if cause != nil && c.closeErr == nil {
				c.closeErr = cause
			}
		}
		c.mu.Unlock()

		// Step 2: interrupt blocked I/O.
		if it, ok := c.FrameTransport.(interruptibleTransport); ok {
			_ = it.interrupt()
		}

		// Step 3: wait for in-flight writes to complete (barrier).
		c.writeMu.Lock()
		c.writeMu.Unlock()

		// Step 4: close transport (no concurrent writes).
		if err := c.FrameTransport.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			c.mu.Lock()
			if c.closeErr == nil {
				c.closeErr = err
			}
			c.mu.Unlock()
		}

		// Step 5: publish non-user error.
		if cause != nil {
			select {
			case c.errChan <- cause:
			default:
			}
		}
	})
}

// ── RPC ID validation (default) ──────────────────────────────────────────────

func (c *BaseConn) defaultValidateRPCID(clusterPeerID uint64, rpcID uint64) bool {
	if int64(rpcID) <= c.peerRPCID {
		return false
	}
	c.peerRPCID = int64(rpcID)
	return true
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func normalizeReadErr(err error) error {
	if errors.Is(err, io.EOF) {
		return nil // clean close — not an error
	}
	return err
}
