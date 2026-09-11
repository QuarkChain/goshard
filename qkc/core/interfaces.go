// Copyright 2026-2027, QuarkChain.

package core

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// ConnManager owns peer-network communication for a shard. The master service
// connection remains a separate dependency because it uses a different endpoint.
type ConnManager interface {
	SendMinorBlockHeaderToMaster(block *types.MinorBlock, xShardTxCount uint32) error
	SendMinorBlockHeaderListToMaster(blocks []*types.MinorBlock) error
	BroadcastXShardTxList(payload XShardBroadcast) error
	BatchBroadcastXShardTxList(payloads []XShardBroadcast) error
	BroadcastNewTip(minorHeaders []*types.MinorBlockHeader, rootHeader *types.RootBlockHeader, branch uint32) error
	BroadcastTransactions(peerID string, branch uint32, txs []*types.Transaction) error
	BroadcastMinorBlock(peerID string, minorBlock *types.MinorBlock) error
	GetMinorBlocks(headerList []common.Hash, peerID string, branch uint32) ([]*types.MinorBlock, error)
	GetMinorBlockHeaderList(request *wire.GetMinorBlockHeaderListWithSkipRequest) ([]*types.MinorBlockHeader, error)
}

// InsertOptions controls validation-only and known-block replay behavior.
type InsertOptions struct {
	IsCheckDB   bool
	ForceInsert bool
}

// MinorChain exposes the local-chain operations required by shard coordination.
// Its concrete execution and persistence implementation belongs to the next PR.
type MinorChain interface {
	CurrentBlock() *types.MinorBlock
	GetBlock(hash common.Hash) *types.MinorBlock
	GetBlockByNumber(number uint64) *types.MinorBlock
	HasState(root common.Hash) bool
	InsertChainWithXShardInputs(chain []*types.MinorBlock, xShardCursors []*XShardTxCursor, options InsertOptions) (int, [][]*types.CrossShardTransactionDeposit, error)
	SetCanonicalHead(hash common.Hash) error
	Stop()
}
