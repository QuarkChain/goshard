// Copyright 2026-2027, QuarkChain.

// The GenesisManager analog (quarkchain/genesis.py): pure functions of the parsed
// config that derive the cluster's genesis blocks — the root block, and each
// shard's minor genesis block with its allocated state. Both live here, as they
// do in pyquarkchain.

package qkc

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/holiman/uint256"
)

// defaultXShardGasLimit is MinorBlockMeta's evm_xshard_gas_limit default
// (quarkchain/core.py:659). The shard genesis does not override it.
const defaultXShardGasLimit = 30000 * 200

// CreateRootBlock derives the genesis root block header purely from
// QUARKCHAIN.ROOT.GENESIS, mirroring pyquarkchain's
// GenesisManager.create_root_block (quarkchain/genesis.py:28). total_difficulty is
// set equal to difficulty; the evm-state root, coinbase, amount map, extra data,
// mixhash, and signature are all empty/zero, exactly as the Python default header.
// The resulting header.Hash() is byte-identical to pyquarkchain's
// create_root_block().header.get_hash().
func CreateRootBlock(qkc *config.QuarkChainConfig) (*types.RootBlockHeader, error) {
	if qkc == nil || qkc.Root == nil || qkc.Root.Genesis == nil {
		return nil, fmt.Errorf("genesis: QUARKCHAIN.ROOT.GENESIS is missing")
	}
	g := qkc.Root.Genesis

	prev, err := parseGenesisHash(g.HashPrevBlock)
	if err != nil {
		return nil, fmt.Errorf("genesis: ROOT.GENESIS.HASH_PREV_BLOCK: %w", err)
	}
	merkle, err := parseGenesisHash(g.HashMerkleRoot)
	if err != nil {
		return nil, fmt.Errorf("genesis: ROOT.GENESIS.HASH_MERKLE_ROOT: %w", err)
	}

	difficulty := new(big.Int)
	if g.Difficulty != nil {
		difficulty.Set(g.Difficulty)
	}
	return &types.RootBlockHeader{
		Version:         g.Version,
		Number:          g.Height,
		ParentHash:      prev,
		MinorHeaderHash: merkle,
		Coinbase:        account.CreatEmptyAddress(0),
		CoinbaseAmount:  qkcCommon.NewEmptyTokenBalances(),
		Time:            g.Timestamp,
		Difficulty:      difficulty,
		TotalDifficulty: new(big.Int).Set(difficulty),
		Nonce:           g.Nonce,
	}, nil
}

// CreateMinorBlock derives one shard's genesis minor block, mirroring
// pyquarkchain's GenesisManager.create_minor_block (quarkchain/genesis.py:43):
// GENESIS.ALLOC is materialized into db and the resulting state root is sealed
// into the block's meta, which the header commits to. The block hash is
// byte-identical to pyquarkchain's create_minor_block().header.get_hash().
//
// Pass an ephemeral database to derive the block without persisting its state —
// the split geth makes between hashing and flushing a genesis allocation
// (core/genesis.go hashAlloc/flushAlloc).
//
// GENESIS.NONCE is deliberately unused: pyquarkchain passes no nonce (nor bloom or
// mixhash) to the genesis header, so all three stay zero.
func CreateMinorBlock(qkc *config.QuarkChainConfig, fullShardID uint32, root *types.RootBlockHeader, db ethdb.Database) (*types.MinorBlock, error) {
	if qkc == nil {
		return nil, fmt.Errorf("genesis: QUARKCHAIN config is missing")
	}
	if root == nil {
		return nil, fmt.Errorf("shard 0x%08x: genesis needs the root genesis header to link", fullShardID)
	}
	shardCfg := qkc.GetShardConfigByFullShardID(fullShardID)
	if shardCfg == nil {
		return nil, fmt.Errorf("shard 0x%08x is not configured in any chain", fullShardID)
	}
	if shardCfg.Genesis == nil {
		return nil, fmt.Errorf("shard 0x%08x: config has no GENESIS", fullShardID)
	}
	g := shardCfg.Genesis

	// A shard with GENESIS.ROOT_HEIGHT > 0 is created lazily once the root chain
	// reaches that height, with its genesis linked to that root block — not the
	// root genesis linked here. Refuse loudly instead of standing the shard on the
	// wrong root linkage.
	if g.RootHeight != 0 {
		return nil, fmt.Errorf("shard 0x%08x: GENESIS.ROOT_HEIGHT = %d is not supported: only shards whose genesis links the root genesis (ROOT_HEIGHT 0) can be stood up",
			fullShardID, g.RootHeight)
	}
	// The genesis block is block 0 by definition; geth refuses the same thing in
	// Genesis.Commit (core/genesis.go).
	if g.Height != 0 {
		return nil, fmt.Errorf("shard 0x%08x: GENESIS.HEIGHT = %d: the genesis block must be block 0", fullShardID, g.Height)
	}
	prevMinor, err := parseGenesisHash(g.HashPrevMinorBlock)
	if err != nil {
		return nil, fmt.Errorf("shard 0x%08x: GENESIS.HASH_PREV_MINOR_BLOCK: %w", fullShardID, err)
	}
	merkle, err := parseGenesisHash(g.HashMerkleRoot)
	if err != nil {
		return nil, fmt.Errorf("shard 0x%08x: GENESIS.HASH_MERKLE_ROOT: %w", fullShardID, err)
	}
	if err := checkAllocOwnership(qkc, fullShardID, g.Alloc); err != nil {
		return nil, err
	}
	coinbaseAmount, err := genesisCoinbaseAmount(qkc, shardCfg)
	if err != nil {
		return nil, fmt.Errorf("shard 0x%08x: %w", fullShardID, err)
	}

	stateRoot, err := commitGenesisAlloc(db, g.Alloc)
	if err != nil {
		return nil, fmt.Errorf("shard 0x%08x: materialize GENESIS.ALLOC: %w", fullShardID, err)
	}

	meta := &types.MinorBlockMeta{
		TxHash:            merkle,
		Root:              stateRoot,
		ReceiptHash:       common.Hash{},
		GasUsed:           &serialize.Uint256{Value: new(big.Int)},
		CrossShardGasUsed: &serialize.Uint256{Value: new(big.Int)},
		// The cross-shard cursor starts at (root_height, 0, 0), matching
		// pyquarkchain (quarkchain/genesis.py:92).
		XShardTxCursor: types.XShardTxCursorInfo{RootBlockHeight: uint64(root.Number)},
		XShardGasLimit: &serialize.Uint256{Value: big.NewInt(defaultXShardGasLimit)},
	}
	header := &types.MinorBlockHeader{
		Version:           g.Version,
		Branch:            account.NewBranch(fullShardID),
		Number:            g.Height,
		Coinbase:          account.CreatEmptyAddress(fullShardID),
		CoinbaseAmount:    coinbaseAmount,
		ParentHash:        prevMinor,
		PrevRootBlockHash: root.Hash(),
		GasLimit:          &serialize.Uint256{Value: new(big.Int).SetUint64(g.GasLimit)},
		MetaHash:          meta.Hash(),
		Time:              g.Timestamp,
		Difficulty:        new(big.Int).SetUint64(g.Difficulty),
		Extra:             bytes.Clone(g.ExtraData),
	}
	return types.NewMinorBlock(header, meta, nil, nil), nil
}

// ShardChainConfig returns the EVM rule set one shard runs: Petersburg-only,
// matching where the QuarkChain networks froze (see
// cmd/geth/testdata/quarkchain-history.json). The chain id is
// BASE_ETH_CHAIN_ID + CHAIN_ID + 1, the derivation pyquarkchain forces on load
// (config.py:363); a configured ETH_CHAIN_ID is accepted only when consistent
// with it (config.py:390).
//
// It is separate from the genesis block on purpose: geth keeps the rule set out
// of the genesis identity too, storing it alongside and reconciling it with
// CheckCompatible (core/genesis.go).
func ShardChainConfig(qkc *config.QuarkChainConfig, shardCfg *config.ShardConfig) (*params.ChainConfig, error) {
	if qkc == nil || shardCfg == nil {
		return nil, fmt.Errorf("chain config: QUARKCHAIN or shard config is missing")
	}
	// Computed in uint64: pyquarkchain's arithmetic is unbounded, so the
	// derivation may exceed uint32 and must not silently wrap.
	ethChainID := uint64(qkc.BaseEthChainID) + uint64(shardCfg.ChainID) + 1
	if shardCfg.EthChainID != 0 && uint64(shardCfg.EthChainID) != ethChainID {
		return nil, fmt.Errorf("chain %d ETH_CHAIN_ID %d != BASE_ETH_CHAIN_ID %d + CHAIN_ID + 1 = %d",
			shardCfg.ChainID, shardCfg.EthChainID, qkc.BaseEthChainID, ethChainID)
	}
	zero := big.NewInt(0)
	return &params.ChainConfig{
		ChainID:             new(big.Int).SetUint64(ethChainID),
		HomesteadBlock:      zero,
		EIP150Block:         zero,
		EIP155Block:         zero,
		EIP158Block:         zero,
		ByzantiumBlock:      zero,
		ConstantinopleBlock: zero,
		PetersburgBlock:     zero,
	}, nil
}

// checkAllocOwnership rejects a GENESIS.ALLOC entry whose address belongs to a
// different shard, the check pyquarkchain performs while writing the genesis
// state (quarkchain/genesis.py:57). Addresses loaded from a separate alloc file
// are already routed per shard by the config loader, but ALLOC written inline in
// the cluster config is not validated anywhere else.
func checkAllocOwnership(qkc *config.QuarkChainConfig, fullShardID uint32, alloc map[account.Address]config.Allocation) error {
	for addr := range alloc {
		owner, err := qkc.GetFullShardIdByFullShardKey(addr.FullShardKey)
		if err != nil {
			return fmt.Errorf("shard 0x%08x: GENESIS.ALLOC address %s: %w", fullShardID, addr.ToHex(), err)
		}
		if owner != fullShardID {
			return fmt.Errorf("shard 0x%08x: GENESIS.ALLOC address %s belongs to shard 0x%08x", fullShardID, addr.ToHex(), owner)
		}
	}
	return nil
}

// genesisCoinbaseAmount is COINBASE_AMOUNT scaled by the local fee rate
// (1 - REWARD_TAX_RATE), recorded on the genesis token — pyquarkchain's
// integer-truncating numerator/denominator arithmetic (quarkchain/genesis.py:95).
func genesisCoinbaseAmount(qkc *config.QuarkChainConfig, shardCfg *config.ShardConfig) (*qkcCommon.TokenBalances, error) {
	if qkc.LocalFeeRate == nil {
		return nil, fmt.Errorf("QUARKCHAIN.REWARD_TAX_RATE is missing, cannot derive the local fee rate")
	}
	amount := new(big.Int)
	if shardCfg.CoinbaseAmount != nil {
		amount = qkcCommon.BigIntMulBigRat(shardCfg.CoinbaseAmount, qkc.LocalFeeRate)
	}
	if amount.Sign() < 0 {
		return nil, fmt.Errorf("COINBASE_AMOUNT × local fee rate is negative (%s)", amount)
	}
	value, overflow := uint256.FromBig(amount)
	if overflow {
		return nil, fmt.Errorf("COINBASE_AMOUNT × local fee rate overflows 256 bits (%s)", amount)
	}
	return qkcCommon.NewTokenBalancesWithMap(map[uint64]*uint256.Int{
		qkc.GetDefaultChainTokenID(): value,
	}), nil
}

// parseGenesisHash decodes a config hash field (a hex string such as "0000…0000",
// optionally "0x"-prefixed; empty means all-zero) into a 32-byte hash. Any other
// length is rejected so a malformed config fails loudly here rather than producing
// a silently wrong genesis hash.
func parseGenesisHash(s string) (common.Hash, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return common.Hash{}, nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return common.Hash{}, fmt.Errorf("invalid hex %q: %w", s, err)
	}
	if len(b) != common.HashLength {
		return common.Hash{}, fmt.Errorf("expected %d bytes, got %d", common.HashLength, len(b))
	}
	return common.BytesToHash(b), nil
}
