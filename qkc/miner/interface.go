// Ported verbatim from github.com/QuarkChain/goquarkchain/cluster/miner.

package miner

import (
	"math/big"

	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// MinerAPI is the chain-facing contract the miner drives: what to mine, where
// to put the result, and the data needed to pace and gate mining. A shard's
// MinorBlockChain (or a runner around it) implements it.
type MinerAPI interface {
	GetDefaultCoinbaseAddress() account.Address
	CreateBlockToMine(addr *account.Address) (types.IBlock, *big.Int, uint64, error)
	InsertMinedBlock(types.IBlock) error
	IsSyncing() bool
	GetTip() uint64
}
