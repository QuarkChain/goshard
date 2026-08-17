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
// Lock ordering: writeMu -> mu. Never acquire writeMu while holding mu.
type BaseConn struct {
	// transport is the frame I/O backend. All writes go through
	// writeFrame/writeFrameLocked.
	transport FrameTransport

	// Configuration. Immutable after construction.
	typedHandlers map[byte]TypedHandler
	nonRPCOps     map[byte]struct{}
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

// NewBaseConn creates a BaseConn from the supplied configuration.
// The caller is responsible for calling Start().
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

// Start transitions the connection to ACTIVE and starts the reader loop.
// If the connection is already closed, Start is a no-op.
func (c *BaseConn) Start() {
	c.mu.Lock()
	if c.state != ConnectionStateConnecting {
		c.mu.Unlock()
		return
	}
	c.state = ConnectionStateActive
	close(c.activeChan)

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
func (c *BaseConn) SendRPC(ctx context.Context, opcode byte, payload []byte) (*wire.Frame, error) {
	return c.SendRPCMeta(ctx, opcode, payload, wire.ClusterMetadata{})
}

// SendRPCMeta sends a request with metadata and waits for its response.
func (c *BaseConn) SendRPCMeta(
	ctx context.Context,
	opcode byte,
	payload []byte,
	meta wire.ClusterMetadata,
) (*wire.Frame, error) {
	call := &pendingRPC{
		result: make(chan rpcResult, 1),
	}

	// Allocate rpc_id and register pending (writeMu -> mu).
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

	call.stop = context.AfterFunc(ctx, func() {
		c.cancelRPC(rpcID, ctx.Err())
	})
	c.mu.Unlock()

	// Recheck state before writing; the result may already be delivered.
	c.mu.Lock()
	_, stillPending := c.pending[rpcID]
	if !stillPending || c.state != ConnectionStateActive {
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
		// writeFrameLocked cannot call shutdown (writeMu still held);
		// call it here after releasing writeMu.
		if !errors.Is(err, ErrConnectionClosed) {
			c.shutdown(fmt.Errorf("write frame rpc=%d: %w", rpcID, err))
		}
		res := <-call.result
		return nil, res.err
	}

	// Wait for response / timeout / close.
	res := <-call.result
	if res.err != nil {
		return nil, res.err
	}
	return res.frame, nil
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
// writeFrame acquires writeMu internally and calls shutdown on write failure
// (after releasing writeMu). writeFrameLocked assumes writeMu is already held
// and does NOT call shutdown — the caller must release writeMu first, since
// initiateShutdown acquires writeMu as a barrier and would deadlock otherwise.

// writeFrame writes a pre-built frame through the serialized path.
func (c *BaseConn) writeFrame(f *wire.Frame) error {
	c.writeMu.Lock()
	err := c.writeFrameLocked(f)
	c.writeMu.Unlock()
	if err != nil && !errors.Is(err, ErrConnectionClosed) {
		c.shutdown(fmt.Errorf("write frame: %w", err))
	}
	return err
}

// writeFrameLocked writes a frame assuming writeMu is already held.
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

// readerLoop reads frames from the transport and dispatches them.
// Read errors trigger shutdown. done is closed exactly once when it exits.
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

	// Only one path (response, timeout, close) can complete each RPC:
	// delete from pending under mu, then deliver.
	c.mu.Lock()
	call, ok := c.pending[frame.RPCID]
	if ok {
		// Responses are matched solely by rpc_id (Python compatibility);
		// the response opcode is not validated.
		delete(c.pending, frame.RPCID)
		c.mu.Unlock()
		if call.stop != nil {
			call.stop()
		}
		call.result <- rpcResult{frame: frame}
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
		// validateRPCID is owned exclusively by readerLoop, so no lock needed.
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
	c.initiateShutdown(cause)
}

// initiateShutdown transitions to Closed, wakes pending RPCs, interrupts
// blocked I/O, waits for in-flight writes, and closes the transport.
//
// It must NOT wait for readerDone here: readerLoop calls initiateShutdown
// itself, and Close() waits for readerDone outside sync.Once.
func (c *BaseConn) initiateShutdown(cause error) {
	c.shutdownOnce.Do(func() {
		// Collect completions under mu; run stop/send side effects outside.
		var pending []*pendingRPC

		c.mu.Lock()
		if c.state != ConnectionStateClosed {
			wasConnecting := c.state == ConnectionStateConnecting

			c.state = ConnectionStateClosed
			close(c.closedChan)

			if wasConnecting {
				close(c.activeChan)
			}

			for id, call := range c.pending {
				delete(c.pending, id)
				pending = append(pending, call)
			}
			for id := range c.timedOut {
				delete(c.timedOut, id)
			}

			if cause != nil && c.closeErr == nil {
				c.closeErr = cause
			}
		}
		c.mu.Unlock()

		for _, call := range pending {
			if call.stop != nil {
				call.stop()
			}
			call.result <- rpcResult{err: ErrConnectionClosed}
		}

		// Close the transport first so blocked reads/writes are interrupted.
		if err := c.transport.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			c.mu.Lock()
			if c.closeErr == nil {
				c.closeErr = err
			}
			c.mu.Unlock()
		}

		c.writeMu.Lock()
		c.writeMu.Unlock()

		// Publish the non-user error, if any.
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
