// Copyright 2026-2027, QuarkChain.

package shard

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
)

// Genesis is the descriptor for one shard's QuarkChain genesis, built purely from
// the shard's config. It is handed to the ShardChain seam: materializing ALLOC
// into a real state root, committing the genesis block, and the EVM machinery all
// belong to the geth-core shard-chain task, not the slave skeleton.
type Genesis struct {
	FullShardID uint32
	Timestamp   uint64
	Difficulty  uint64
	GasLimit    uint64
	Nonce       uint32
	ExtraData   []byte
	// Alloc carries the parsed GENESIS.ALLOC (per-token balances, code, storage),
	// already filtered down to the addresses that belong to this shard.
	Alloc map[account.Address]config.Allocation
	// ChainConfig is the EVM rule set for this shard: Petersburg-only, matching
	// where the QuarkChain networks froze (see cmd/geth/testdata/quarkchain-history.json).
	ChainConfig *params.ChainConfig
}

// NewGenesis builds the genesis descriptor for shardCfg. The EVM chain id is
// BASE_ETH_CHAIN_ID + CHAIN_ID + 1, the derivation pyquarkchain forces on load
// (config.py:363); a configured ETH_CHAIN_ID is accepted only when consistent
// with it (config.py:390).
func NewGenesis(qkc *config.QuarkChainConfig, shardCfg *config.ShardConfig) (*Genesis, error) {
	if shardCfg == nil || shardCfg.Genesis == nil {
		return nil, fmt.Errorf("shard config has no GENESIS")
	}
	// Computed in uint64: pyquarkchain's arithmetic is unbounded, so the
	// derivation may exceed uint32 and must not silently wrap.
	ethChainID := uint64(qkc.BaseEthChainID) + uint64(shardCfg.ChainID) + 1
	if shardCfg.EthChainID != 0 && uint64(shardCfg.EthChainID) != ethChainID {
		return nil, fmt.Errorf("chain %d ETH_CHAIN_ID %d != BASE_ETH_CHAIN_ID %d + CHAIN_ID + 1 = %d",
			shardCfg.ChainID, shardCfg.EthChainID, qkc.BaseEthChainID, ethChainID)
	}
	g := shardCfg.Genesis
	return &Genesis{
		FullShardID: shardCfg.GetFullShardId(),
		Timestamp:   g.Timestamp,
		Difficulty:  g.Difficulty,
		GasLimit:    g.GasLimit,
		Nonce:       g.Nonce,
		ExtraData:   g.ExtraData,
		Alloc:       g.Alloc,
		ChainConfig: petersburgChainConfig(ethChainID),
	}, nil
}

// petersburgChainConfig returns a chain config with every fork up to and
// including Petersburg active from block 0 and nothing after it.
func petersburgChainConfig(ethChainID uint64) *params.ChainConfig {
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
	}
}

// TODO(#1): remove this surrogate when the real chain commits the QKC minor
// genesis; preserve its chain-rule compatibility check in the native setup path.
// Fingerprint returns a deterministic identity hash of the descriptor. It is what
// the stub chain reports as its genesis hash and what the genesis metadata records
// as ChainGenesisHash, so a config change is caught on reopen. It is not
// pyquarkchain's minor-block genesis hash — reproducing that requires the QKC
// block format and state materialization, both owned by the shard-chain task.
func (g *Genesis) Fingerprint() common.Hash {
	alloc := make(map[string]config.Allocation, len(g.Alloc))
	for addr, a := range g.Alloc {
		alloc[addr.ToHex()] = a
	}
	// json.Marshal sorts map keys, and Allocation's MarshalJSON encodes balances
	// and storage as maps too, so the encoding is canonical.
	enc, err := json.Marshal(struct {
		FullShardID uint32
		// The full EVM rule set (chain id + compiled-in fork schedule): a code
		// change to the rules changes every fingerprint, forcing a loud
		// re-bootstrap instead of silently executing new semantics on an old db.
		ChainConfig *params.ChainConfig
		Timestamp   uint64
		Difficulty  uint64
		GasLimit    uint64
		Nonce       uint32
		ExtraData   string
		Alloc       map[string]config.Allocation
	}{g.FullShardID, g.ChainConfig, g.Timestamp, g.Difficulty, g.GasLimit, g.Nonce, common.Bytes2Hex(g.ExtraData), alloc})
	if err != nil {
		panic(fmt.Sprintf("qkc/shard: encode genesis descriptor for fingerprint: %v", err))
	}
	return crypto.Keccak256Hash(enc)
}
