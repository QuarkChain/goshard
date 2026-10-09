// Copyright 2026-2027, QuarkChain.

package shard

import (
	"context"

	"github.com/ethereum/go-ethereum/qkc/wire"
)

// Sender is the outbound boundary the shard layer uses to reach the master and
// the xshard connections; business code depends on this interface, never on a
// concrete communicator.
type Sender interface {
	// SendMinorBlockHeaderToMaster and SendMinorBlockHeaderListToMaster report
	// new minor block headers to the master.
	SendMinorBlockHeaderToMaster(ctx context.Context, req *wire.AddMinorBlockHeaderRequest) (*wire.AddMinorBlockHeaderResponse, error)
	SendMinorBlockHeaderListToMaster(ctx context.Context, req *wire.AddMinorBlockHeaderListRequest) (*wire.AddMinorBlockHeaderListResponse, error)

	// SendXshardTxList broadcasts req to the connections serving branch;
	// branch is the routing key and must match req.Branch.
	SendXshardTxList(ctx context.Context, branch uint32, req *wire.AddXshardTxListRequest) error

	// SendBatchXshardTxList broadcasts req to the connections serving branch;
	// branch is the routing key; the batch request carries no branch of its own.
	SendBatchXshardTxList(ctx context.Context, branch uint32, req *wire.BatchAddXshardTxListRequest) error
}
