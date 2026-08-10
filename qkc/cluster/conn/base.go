// Copyright 2026-2027, QuarkChain.

// Package conn provides the generic RPC engine used by cluster connection
// implementations.
package conn

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

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

// BaseConn is the shared RPC engine used by cluster connection
// implementations. Protocol state is owned by one goroutine. Reader and
// writer goroutines only convert transport I/O into owner events.
type BaseConn struct {
	FrameTransport

	events       chan connEvent
	done         chan struct{}
	shutdownDone chan struct{}
	activeChan   chan struct{}
	closedChan   chan struct{}
	errChan      chan error

	ownerOnce sync.Once
	startOnce sync.Once
	submitMu  sync.Mutex
	finished  bool

	configMu      sync.RWMutex
	typedHandlers map[byte]TypedHandler
	nonRPCOps     map[byte]struct{}
	// serializers is keyed by both request and response opcodes; each
	// OpSerializer is installed under both keys by RegisterOpSerializers.
	serializers   map[byte]*OpSerializer
	forwarder     func(*wire.Frame) bool
	validateRPCID func(clusterPeerID uint64, rpcID uint64) bool

	// The following fields are accessed only by ownerLoop, except for the
	// atomic state snapshot used by query helpers.
	state           ConnectionState
	stateSnapshot   atomic.Int32
	pendingCount    atomic.Int64
	pending         map[uint64]*pendingRPC
	timedOut        map[uint64]*time.Timer
	nextRPCID       uint64
	peerRPCID       int64
	started         bool
	shuttingDown    bool
	transportClosed bool
	readerStopped   bool
	writerStopped   bool
	closeErr        error

	writer *frameMailbox
	log    log.Logger
}

// NewBaseConn creates a BaseConn using the supplied frame transport.
func NewBaseConn(tr FrameTransport, logger log.Logger) *BaseConn {
	if logger == nil {
		logger = log.Root()
	}
	rc := &BaseConn{
		FrameTransport: tr,
		events:         make(chan connEvent, 64),
		done:           make(chan struct{}),
		shutdownDone:   make(chan struct{}),
		activeChan:     make(chan struct{}),
		closedChan:     make(chan struct{}),
		errChan:        make(chan error, 1),
		typedHandlers:  make(map[byte]TypedHandler),
		serializers:    make(map[byte]*OpSerializer),
		pending:        make(map[uint64]*pendingRPC),
		timedOut:       make(map[uint64]*time.Timer),
		nonRPCOps:      make(map[byte]struct{}),
		peerRPCID:      -1,
		state:          ConnectionStateConnecting,
		writer:         newFrameMailbox(),
		log:            logger,
	}
	rc.stateSnapshot.Store(int32(ConnectionStateConnecting))
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

// Start transitions the connection to ACTIVE and starts the transport loops.
// If the connection is already closed, Start is a no-op.
func (c *BaseConn) Start() {
	c.ensureOwner()
	c.startOnce.Do(func() {
		c.submitEvent(startEvent{})
	})
}

// Close closes the connection and wakes all pending RPCs.
func (c *BaseConn) Close() error {
	c.ensureOwner()
	c.submitEvent(closeRequestedEvent{})
	<-c.shutdownDone
	return c.closeErr
}

// RegisterTypedHandlers registers handlers before Start is called.
func (c *BaseConn) RegisterTypedHandlers(handlers map[byte]TypedHandler) {
	c.configMu.Lock()
	defer c.configMu.Unlock()
	if c.State() != ConnectionStateConnecting {
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
	c.configMu.Lock()
	defer c.configMu.Unlock()
	if c.State() != ConnectionStateConnecting {
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
	c.configMu.Lock()
	defer c.configMu.Unlock()
	if c.State() != ConnectionStateConnecting {
		panic("non-RPC opcodes must be registered before Start")
	}
	for _, op := range ops {
		c.nonRPCOps[op] = struct{}{}
	}
}

// SetForwarder installs a raw-frame forwarder hook. It is invoked by the owner
// goroutine before local dispatch and must not synchronously wait on this
// connection.
func (c *BaseConn) SetForwarder(f func(*wire.Frame) bool) {
	c.configMu.Lock()
	defer c.configMu.Unlock()
	if c.State() != ConnectionStateConnecting {
		panic("forwarder must be set before Start")
	}
	c.forwarder = f
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
	call := &pendingRPC{result: make(chan rpcResult, 1)}
	c.ensureOwner()
	if !c.submitEvent(outboundRPCEvent{
		ctx:     ctx,
		opcode:  opcode,
		payload: payload,
		meta:    meta,
		call:    call,
	}) {
		call.result <- rpcResult{err: ErrConnectionClosed}
	}

	res := <-call.result
	if res.err != nil {
		return nil, res.err
	}
	if res.frame == nil {
		return nil, ErrConnectionClosed
	}
	return res.frame, nil
}

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
	return ConnectionState(c.stateSnapshot.Load())
}

// IsActive reports whether the connection is active.
func (c *BaseConn) IsActive() bool {
	return c.State() == ConnectionStateActive
}

// IsClosed reports whether the connection is closed.
func (c *BaseConn) IsClosed() bool {
	return c.State() == ConnectionStateClosed
}

// Closed reports whether the connection is closed.
func (c *BaseConn) Closed() bool {
	return c.IsClosed()
}
