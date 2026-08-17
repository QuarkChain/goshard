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
//   - mu protects lifecycle, RPC state, and close state.
//   - writeMu serializes transport writes and RPC ID/send ordering.
//   - shutdownOnce ensures shutdown runs once.
//   - Channels carry lifecycle/event notifications.
//
// Lock ordering: writeMu -> mu. Never acquire writeMu while holding mu.
//
// All configuration (handlers, serializers, forwarder, validateRPCID) is
// immutable after construction — set via Config at NewBaseConn time and never
// modified. This eliminates the previous Register*/Set* methods and their
// associated lock contention.
type BaseConn struct {
	// transport is the frame I/O backend. It is a private field — external
	// callers never access it directly. All writes go through the serialized
	// internal path (writeFrame/writeFrameLocked), never through the
	// transport directly.
	transport FrameTransport

	// Configuration. Immutable after construction (set from Config).
	typedHandlers map[byte]TypedHandler
	nonRPCOps     map[byte]struct{}
	// serializers is keyed by both request and response opcodes; each
	// OpSerializer is installed under both keys during construction.
	serializers   map[byte]*OpSerializer
	forwarder     func(*wire.Frame) bool
	validateRPCID func(clusterPeerID uint64, rpcID uint64) bool

	// -- Lifecycle + protocol state (mu) --
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

	// -- Frame send serialization (writeMu) --
	writeMu sync.Mutex

	// -- Synchronization primitives --
	shutdownOnce sync.Once
	activeChan   chan struct{} // closed once active, or on shutdown before activation
	closedChan   chan struct{} // closed during shutdown
	errChan      chan error    // cap 1, non-user errors

	log log.Logger
}

// NewBaseConn creates a BaseConn from the supplied configuration. The
// configuration is validated and then frozen — no post-construction
// mutation is possible.
//
// The caller is responsible for calling Start() to transition the connection
// to ACTIVE and launch the reader loop.
func NewBaseConn(cfg Config) *BaseConn {
	cfg.validate()

	logger := cfg.Logger
	if logger == nil {
		logger = log.Root()
	}

	// Build the serializers map with both request and response opcodes.
	serializers := make(map[byte]*OpSerializer, len(cfg.Serializers)*2)
	for opcode, ser := range cfg.Serializers {
		serializers[opcode] = ser
		serializers[ser.ResponseOpCode] = ser
	}

	// Copy nonRPCOps so the caller's map is not shared.
	nonRPCOps := make(map[byte]struct{}, len(cfg.NonRPCOps))
	for op := range cfg.NonRPCOps {
		nonRPCOps[op] = struct{}{}
	}

	// Copy handlers so the caller's map is not shared.
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
		errChan:       make(chan error, 1),
		pending:       make(map[uint64]*pendingRPC),
		timedOut:      make(map[uint64]struct{}),
		peerRPCID:     -1,
		state:         ConnectionStateConnecting,
		log:           logger,
	}
	if cfg.ValidateRPCID != nil {
		rc.validateRPCID = cfg.ValidateRPCID
	} else {
		rc.validateRPCID = rc.defaultValidateRPCID
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
//
// The raw frame write (Python's write_raw_data) is package-private
// (writeFrame) — it is only used internally and by virtual connections
// within the conn package. External callers must use SendRPC or SendCommand.

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

// SendRPC sends a request without metadata and waits for its response.
// This corresponds to Python's write_rpc_request with empty metadata.
func (c *BaseConn) SendRPC(ctx context.Context, opcode byte, payload []byte) (*wire.Frame, error) {
	return c.SendRPCMeta(ctx, opcode, payload, wire.ClusterMetadata{})
}

// SendRPCMeta sends a request with metadata and waits for its response.
// This corresponds to Python's write_rpc_request.
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

	// Phase 1: allocate rpc_id + register pending (writeMu -> mu).
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
	err := c.writeFrameLocked(frame)
	c.writeMu.Unlock()

	if err != nil {
		// writeFrameLocked does NOT call shutdown (it cannot — writeMu
		// is still held and initiateShutdown needs writeMu as a barrier).
		// Call shutdown here, after writeMu is released.
		if !errors.Is(err, ErrConnectionClosed) {
			c.shutdown(fmt.Errorf("write frame rpc=%d: %w", rpcID, err))
		}
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

// SendCommand sends a fire-and-forget command (rpc_id=0, no response
// expected). This corresponds to Python's write_command with rpc_id=0.
//
// The caller is responsible for serializing the payload. SendCommand does
// not look up serializers — it wraps the payload into a frame with rpc_id=0
// and writes it through the serialized path. The opcode does not need to be
// registered in NonRPCOps (that set is only consulted on the receiving side
// to validate that inbound fire-and-forget frames have rpc_id=0).
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

// -- Query methods -----------------------------------------------------------

// Error returns connection failures. A caller-initiated Close does not publish
// an error.
func (c *BaseConn) Error() <-chan error { return c.errChan }

// RemoteAddr returns the transport's remote address.
func (c *BaseConn) RemoteAddr() string { return c.transport.RemoteAddr() }

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

// -- Internal helpers --------------------------------------------------------

// pendingLen returns the number of in-flight RPCs. Used by tests.
func (c *BaseConn) pendingLen() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.pending)
}

func rpcTimeoutError(err error) error {
	return fmt.Errorf("rpc timeout: %w", err)
}

// -- Write path (package-private) ---------------------------------------------
//
// writeFrame is the single entry point for one-shot frame writes (SendCommand,
// dispatch response write, virtual connection forwarding). It acquires writeMu
// internally.
//
// writeFrameLocked is for callers that already hold writeMu (SendRPCMeta,
// which needs rpc_id allocation and write under the same lock to guarantee
// send ordering).
//
// Both check connection state and write through the transport. writeFrame
// calls shutdown on write failure (after releasing writeMu); writeFrameLocked
// does NOT call shutdown — the caller must release writeMu first, then call
// shutdown. This prevents a deadlock: initiateShutdown acquires writeMu as a
// barrier (step 3), so shutdown cannot be called while writeMu is held.
// External callers never touch these — they use SendRPC or SendCommand.

// writeFrame writes a pre-built frame through the serialized path. It is the
// package-private equivalent of Python's write_raw_data: the frame's rpc_id,
// opcode, and metadata are preserved as-is, and no RPC tracking is created.
func (c *BaseConn) writeFrame(f *wire.Frame) error {
	c.writeMu.Lock()
	err := c.writeFrameLocked(f)
	c.writeMu.Unlock()
	if err != nil && !errors.Is(err, ErrConnectionClosed) {
		c.shutdown(fmt.Errorf("write frame: %w", err))
	}
	return err
}

// writeFrameLocked writes a frame assuming writeMu is already held. This is
// used by SendRPCMeta (which holds writeMu across rpc_id allocation + write
// to guarantee ordering) and by writeFrame (which acquires writeMu first).
//
// It does NOT call shutdown on write failure — the caller must release writeMu
// first, then call shutdown. This is because initiateShutdown needs to acquire
// writeMu as a barrier (step 3), and calling shutdown while writeMu is held
// would deadlock.
func (c *BaseConn) writeFrameLocked(f *wire.Frame) error {
	c.mu.Lock()
	if c.state != ConnectionStateActive {
		c.mu.Unlock()
		return ErrConnectionClosed
	}
	c.mu.Unlock()

	return c.transport.WriteFrame(f)
}

// -- readerLoop --------------------------------------------------------------

// readerLoop is the single persistent goroutine. It reads frames from the
// transport and dispatches them. Read errors trigger shutdown. done is closed
// exactly once when readerLoop exits.
func (c *BaseConn) readerLoop(done chan struct{}) {
	defer close(done)
	for {
		frame, err := c.transport.ReadFrame()
		if err != nil {
			c.initiateShutdown(normalizeReadErr(err))
			return
		}
		c.handleFrame(frame)
	}
}

// -- handleFrame -------------------------------------------------------------

func (c *BaseConn) handleFrame(frame *wire.Frame) {
	// Configuration is immutable after construction, so readerLoop (which
	// only runs once the connection is Active) reads it without any lock.
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

	// writeFrame acquires writeMu internally and checks connection state.
	// On write failure, shutdown is called inside writeFrame.
	if err := c.writeFrame(respFrame); err != nil {
		c.log.Debug("response write failed, connection shutting down", "rpcid", frame.RPCID, "err", err)
	}
}

// -- cancelRPC ----------------------------------------------------------------

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

// -- Shutdown -----------------------------------------------------------------

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
		if it, ok := c.transport.(interruptibleTransport); ok {
			_ = it.interrupt()
		}

		// Step 3: wait for in-flight writes to complete (barrier).
		c.writeMu.Lock()
		c.writeMu.Unlock()

		// Step 4: close transport (no concurrent writes).
		if err := c.transport.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
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

// -- RPC ID validation (default) ---------------------------------------------

func (c *BaseConn) defaultValidateRPCID(clusterPeerID uint64, rpcID uint64) bool {
	if int64(rpcID) <= c.peerRPCID {
		return false
	}
	c.peerRPCID = int64(rpcID)
	return true
}

// -- Helpers ------------------------------------------------------------------

func normalizeReadErr(err error) error {
	if errors.Is(err, io.EOF) {
		return nil // clean close — not an error
	}
	return err
}
