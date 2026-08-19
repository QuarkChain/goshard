// Copyright 2026-2027, QuarkChain.

package core

// Gas constants, from quarkchain/evm/opcodes.py. They are repeated here rather
// than taken from go-ethereum's params because these are QuarkChain consensus
// values: upstream is free to reprice its own, and a shared constant would
// carry that repricing into a replay of 2019 mainnet.
const (
	// GTXCost is the base cost of any transaction.
	GTXCost = uint64(21000)
	// CreateContractGas is opcodes.CREATE[3], the surcharge a transaction pays
	// for having no recipient.
	CreateContractGas = uint64(32000)
	GTXDataZero       = uint64(4)
	GTXDataNonZero    = uint64(68)
	// GTXXShardCost is charged on the source shard for a cross-shard deposit,
	// and credited back out of the source shard's fee when the transaction
	// succeeds, so that the target shard's miner is the one paid for it.
	GTXXShardCost = uint64(9000)
	// GSuicideRefund is the refund per distinct self-destructed account.
	GSuicideRefund = uint64(24000)
)

// defaultRefundRate is the only refund rate inside the supported profile: the
// full remaining gas goes back to the sender. Other rates come from pricing a
// non-default gas token, which is refused before it can produce one.
const defaultRefundRate = uint8(100)
