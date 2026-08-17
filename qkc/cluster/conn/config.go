// Copyright 2026-2027, QuarkChain.

package conn

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// Config holds immutable connection configuration.
type Config struct {
	// Transport is the frame I/O backend. Required.
	Transport FrameTransport

	// Handlers maps request opcodes to their handlers.
	// Must be safe for concurrent use: handlers may be called from multiple
	// dispatch goroutines.
	Handlers map[byte]TypedHandler

	// Serializers maps request opcodes to their serializers.
	// Each serializer is also registered under its response opcode.
	Serializers map[byte]*OpSerializer

	// NonRPCOps marks fire-and-forget opcodes that must use rpc_id=0.
	NonRPCOps map[byte]struct{}

	// Forwarder optionally intercepts inbound frames before normal dispatch.
	Forwarder func(*wire.Frame) bool

	// ValidateRPCID optionally validates inbound RPC IDs.
	ValidateRPCID func(clusterPeerID uint64, rpcID uint64) bool

	// Logger defaults to log.Root() if nil.
	Logger log.Logger

	// WriteTimeout bounds how long a single frame write may block on the
	// transport. Zero (default) means no timeout. Only takes effect for
	// transports that support per-write deadlines.
	WriteTimeout time.Duration
}

// validate checks configuration invariants.
func (cfg *Config) validate() {
	if cfg.Transport == nil {
		panic("conn.Config: Transport must not be nil")
	}
	for op, ser := range cfg.Serializers {
		if ser == nil {
			panic(fmt.Sprintf("conn.Config: serializer for opcode 0x%x must not be nil", op))
		}
		if ser.ResponseOpCode == 0 {
			panic(fmt.Sprintf("conn.Config: serializer ResponseOpCode for opcode 0x%x must not be zero", op))
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
