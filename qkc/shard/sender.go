// Copyright 2026-2027, QuarkChain.

package shard

import (
	"context"

	"github.com/ethereum/go-ethereum/qkc/wire"
)

// Sender is the business-owned outbound boundary through which the shard layer
// pushes data to the master and to xshard connections acting on its behalf. It
// is defined here, in the package that consumes it, so business code depends on
// this interface and never on a concrete communicator. It exposes only outbound
// commands — what the shard may volunteer to the master and to other slaves. It
// deliberately excludes any communicator runtime/lifecycle (start, stop,
// listener, connection bookkeeping), which the business layer must never own or
// reach.
//
// A concrete *SlaveBackend satisfies Sender. A real chain will be injected with
// an instance of Sender (through shard.Options), not a live network object.
type Sender interface {
	// To the master (py: SlaveServer.send_minor_block_header[_list]_to_master).
	SendMinorBlockHeaderToMaster(ctx context.Context, req *wire.AddMinorBlockHeaderRequest) (*wire.AddMinorBlockHeaderResponse, error)
	SendMinorBlockHeaderListToMaster(ctx context.Context, req *wire.AddMinorBlockHeaderListRequest) (*wire.AddMinorBlockHeaderListResponse, error)

	// X-shard broadcasts (py: broadcast_xshard_tx_list / batch_broadcast_xshard_tx_list).
	SendXshardTxList(ctx context.Context, branch uint32, req *wire.AddXshardTxListRequest) error
	SendBatchXshardTxList(ctx context.Context, branch uint32, req *wire.BatchAddXshardTxListRequest) error
}
