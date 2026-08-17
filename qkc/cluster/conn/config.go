// Copyright 2026-2027, QuarkChain.

package conn

import (
	"fmt"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// Config holds all immutable connection configuration. It is validated once
// at construction time (NewBaseConn) and never modified afterward.
//
// This replaces the previous Register*/Set* methods that used runtime mutex
// locks and state-machine panics to enforce immutability. Since all
// configuration is known before Start() is called, it is passed at
// construction time instead — eliminating an entire class of lock-contention
// and state-check code paths.
type Config struct {
	// Transport is the frame I/O backend. Required.
	//
	// BaseConn is agnostic to the transport implementation: it may be a
	// real TCP connection (transport), a virtual connection multiplexing
	// a shared master TCP, or a test double. BaseConn never exposes the
	// transport to external callers — all writes go through the
	// serialized internal write path (writeFrame).
	Transport FrameTransport

	// Handlers maps request opcodes to their handler functions.
	// A handler is invoked when an inbound frame with a matching opcode
	// is received. Optional: a connection with no handlers can still
	// send RPCs.
	Handlers map[byte]TypedHandler

	// Serializers describes how to deserialize requests and serialize
	// responses for each opcode. Each OpSerializer is installed under
	// both its request opcode and its ResponseOpCode so that BaseConn
	// can deserialize both inbound requests and inbound responses.
	//
	// Required if Handlers is non-empty (BaseConn needs a serializer to
	// deserialize inbound request payloads and serialize outbound
	// response payloads).
	Serializers map[byte]*OpSerializer

	// NonRPCOps marks opcodes as fire-and-forget. Frames with these
	// opcodes must arrive with rpc_id=0 and never receive a response.
	// Optional.
	NonRPCOps map[byte]struct{}

	// Forwarder is an optional raw-frame interception hook called before
	// normal dispatch. If it returns true, the frame is consumed (not
	// dispatched to handlers or response matching).
	//
	// Used by master connections to forward frames to peer connections
	// instead of handling them locally. Optional.
	Forwarder func(*wire.Frame) bool

	// ValidateRPCID validates inbound RPC request IDs. If nil, a default
	// monotonic validator is used: peer RPC IDs must be strictly
	// increasing within the connection lifetime.
	//
	// Override this for multiplexed connections that need
	// per-virtual-connection ID validation. Optional.
	ValidateRPCID func(clusterPeerID uint64, rpcID uint64) bool

	// Logger defaults to log.Root() if nil. Optional.
	Logger log.Logger
}

// validate checks the configuration for internal consistency. It panics
// with a descriptive message if the configuration is invalid, failing fast
// at construction time rather than at runtime.
func (cfg *Config) validate() {
	if cfg.Transport == nil {
		panic("conn.Config: Transport must not be nil")
	}
	for op, ser := range cfg.Serializers {
		if ser == nil {
			panic(fmt.Sprintf("conn.Config: serializer for opcode 0x%x must not be nil", op))
		}
		if ser.NewRequest == nil {
			panic(fmt.Sprintf("conn.Config: serializer NewRequest for opcode 0x%x must not be nil", op))
		}
		if ser.NewResponse == nil {
			panic(fmt.Sprintf("conn.Config: serializer NewResponse for opcode 0x%x must not be nil", op))
		}
		if ser.Deserialize == nil {
			panic(fmt.Sprintf("conn.Config: serializer Deserialize for opcode 0x%x must not be nil", op))
		}
		if ser.Serialize == nil {
			panic(fmt.Sprintf("conn.Config: serializer Serialize for opcode 0x%x must not be nil", op))
		}
		if ser.ResponseOpCode == 0 {
			panic(fmt.Sprintf("conn.Config: serializer ResponseOpCode for opcode 0x%x must not be zero", op))
		}
	}
	for op, h := range cfg.Handlers {
		if h == nil {
			panic(fmt.Sprintf("conn.Config: handler for opcode 0x%x must not be nil", op))
		}
	}
}
