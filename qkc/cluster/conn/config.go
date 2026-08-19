// Copyright 2026-2027, QuarkChain.

package conn

import (
	"fmt"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// ForwardResult is the routing decision returned by Config.Forwarder for an
// inbound frame.
type ForwardResult int

const (
	// ForwardPass leaves the frame to this connection's normal dispatch
	// (request handler or RPC response matching).
	ForwardPass ForwardResult = iota

	// ForwardConsumed means the router handled the frame.
	// Local dispatch is skipped.
	ForwardConsumed

	// ForwardClose means the router detected an unrecoverable condition.
	// BaseConn performs the shutdown itself; the router must not directly
	// close the connection.
	ForwardClose
)

// Config holds immutable connection configuration.
type Config struct {
	// Transport is the frame I/O backend. Required.
	Transport FrameTransport

	// Serializers maps request opcodes to their serializers.
	// Each serializer is also registered under its response opcode.
	Serializers map[byte]*OpSerializer

	// Handlers maps request opcodes to their handlers.
	// Must be safe for concurrent use: handlers may be called from multiple
	// dispatch goroutines.
	Handlers map[byte]TypedHandler

	// NonRPCOps marks fire-and-forget opcodes that must use rpc_id=0.
	NonRPCOps map[byte]struct{}

	// Forwarder optionally routes inbound frames before normal dispatch.
	//
	// It runs synchronously on the reader goroutine. The callback should be
	// lightweight and must not block waiting for work that requires this
	// connection's reader loop (for example, sending an RPC and waiting for its
	// response).
	//
	// The callback must not directly close this connection. If it detects an
	// unrecoverable routing or protocol condition, it should return ForwardClose
	// and let BaseConn perform the shutdown.
	//
	// The callback is responsible for synchronizing any shared state it accesses.
	Forwarder func(*wire.Frame) ForwardResult

	// Logger defaults to log.Root() if nil.
	Logger log.Logger
}

// validate checks configuration invariants.
func (cfg *Config) validate() {
	if cfg.Transport == nil {
		panic("conn.Config: Transport must not be nil")
	}

	requestOps := make(map[byte]struct{})
	for op, ser := range cfg.Serializers {
		if ser == nil {
			panic(fmt.Sprintf("conn.Config: serializer for opcode 0x%x must not be nil", op))
		}

		// Non-RPC commands may reuse the same opcode.
		if _, ok := cfg.NonRPCOps[op]; ok {
			continue
		}

		if ser.ResponseOpCode == 0 {
			panic(fmt.Sprintf("conn.Config: serializer ResponseOpCode for opcode 0x%x must not be zero", op))
		}
		requestOps[op] = struct{}{}
	}
	for op, ser := range cfg.Serializers {
		if _, nonRPC := cfg.NonRPCOps[op]; nonRPC {
			continue
		}
		if _, conflict := requestOps[ser.ResponseOpCode]; conflict {
			panic(fmt.Sprintf("conn.Config: response opcode 0x%x conflicts with request opcode", ser.ResponseOpCode))
		}
	}

	for op, h := range cfg.Handlers {
		if h == nil {
			panic(fmt.Sprintf("conn.Config: handler for opcode 0x%x must not be nil", op))
		}
		if _, ok := cfg.Serializers[op]; !ok {
			panic(fmt.Sprintf("conn.Config: serializer for handler opcode 0x%x must be configured", op))
		}
	}
}
