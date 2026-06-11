// Copyright 2026-2027, QuarkChain.

package state

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/triedb"
)

// QuarkChainClusterGenesisConfig is the minimal subset of a goquarkchain
// cluster config needed by QKC genesis root and historical replay helpers.
type QuarkChainClusterGenesisConfig struct {
	GenesisDir *string                 `json:"GENESIS_DIR"`
	QuarkChain QuarkChainGenesisConfig `json:"QUARKCHAIN"`
}

type QuarkChainGenesisConfig struct {
	GenesisToken                string                   `json:"GENESIS_TOKEN"`
	BaseEthChainID              uint32                   `json:"BASE_ETH_CHAIN_ID"`
	NetworkID                   uint32                   `json:"NETWORK_ID"`
	EnableEIP155SignerTimestamp *uint64                  `json:"ENABLE_EIP155_SIGNER_TIMESTAMP"`
	RewardTaxRate               json.Number              `json:"REWARD_TAX_RATE"`
	Chains                      []QuarkChainGenesisChain `json:"CHAINS"`
}

type QuarkChainGenesisChain struct {
	ChainID           uint32                  `json:"CHAIN_ID"`
	EthChainID        uint32                  `json:"ETH_CHAIN_ID"`
	ShardSize         uint32                  `json:"SHARD_SIZE"`
	DefaultChainToken string                  `json:"DEFAULT_CHAIN_TOKEN"`
	Genesis           *QuarkChainShardGenesis `json:"GENESIS"`
}

type QuarkChainShardGenesis struct {
	Alloc map[QuarkChainGenesisAddress]QuarkChainGenesisAllocation
}

type QuarkChainGenesisAddress struct {
	Recipient    common.Address
	FullShardKey uint32
}

type QuarkChainGenesisAllocation struct {
	Balances map[string]*big.Int
	Code     []byte
	CodeSet  bool
	Storage  map[common.Hash]common.Hash
}

type quarkChainShardGenesisJSON struct {
	Alloc map[string]QuarkChainGenesisAllocation `json:"ALLOC"`
}

func ReadQuarkChainClusterGenesisConfig(r io.Reader) (*QuarkChainClusterGenesisConfig, error) {
	var config QuarkChainClusterGenesisConfig
	if err := json.NewDecoder(r).Decode(&config); err != nil {
		return nil, err
	}
	return &config, nil
}

func ParseQuarkChainClusterGenesisConfig(input []byte) (*QuarkChainClusterGenesisConfig, error) {
	return ReadQuarkChainClusterGenesisConfig(bytes.NewReader(input))
}

func (s *QuarkChainShardGenesis) UnmarshalJSON(input []byte) error {
	var dec quarkChainShardGenesisJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	s.Alloc = make(map[QuarkChainGenesisAddress]QuarkChainGenesisAllocation, len(dec.Alloc))
	for raw, alloc := range dec.Alloc {
		addr, err := ParseQuarkChainGenesisAddress(raw)
		if err != nil {
			return err
		}
		s.Alloc[addr] = alloc
	}
	return nil
}

func (a *QuarkChainGenesisAllocation) UnmarshalJSON(input []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(input, &fields); err != nil {
		return err
	}
	if _, ok := fields["balances"]; !ok {
		if _, ok := fields["code"]; !ok {
			if _, ok := fields["storage"]; !ok {
				var balances map[string]*big.Int
				if err := json.Unmarshal(input, &balances); err != nil {
					return err
				}
				a.Balances = balances
				return nil
			}
		}
	}
	if raw := fields["balances"]; len(raw) != 0 {
		if err := json.Unmarshal(raw, &a.Balances); err != nil {
			return err
		}
	}
	if raw := fields["code"]; len(raw) != 0 {
		var code string
		if err := json.Unmarshal(raw, &code); err != nil {
			return err
		}
		blob, err := quarkChainDecodeHex(code)
		if err != nil {
			return err
		}
		a.Code = blob
		a.CodeSet = true
	}
	if raw := fields["storage"]; len(raw) != 0 {
		var storage map[quarkChainGenesisHash]quarkChainGenesisHash
		if err := json.Unmarshal(raw, &storage); err != nil {
			return err
		}
		a.Storage = make(map[common.Hash]common.Hash, len(storage))
		for key, value := range storage {
			a.Storage[common.Hash(key)] = common.Hash(value)
		}
	}
	return nil
}

// ParseQuarkChainGenesisAddress parses a 24-byte QKC address:
// 20-byte recipient followed by a 4-byte big-endian full shard key.
func ParseQuarkChainGenesisAddress(input string) (QuarkChainGenesisAddress, error) {
	blob, err := quarkChainDecodeHex(input)
	if err != nil {
		return QuarkChainGenesisAddress{}, err
	}
	if len(blob) != common.AddressLength+4 {
		return QuarkChainGenesisAddress{}, fmt.Errorf("invalid qkc genesis address length %d", len(blob))
	}
	return QuarkChainGenesisAddress{
		Recipient:    common.BytesToAddress(blob[:common.AddressLength]),
		FullShardKey: binary.BigEndian.Uint32(blob[common.AddressLength:]),
	}, nil
}

func (a QuarkChainGenesisAddress) FullShardID(shardSize uint32) (uint32, error) {
	if shardSize == 0 || shardSize&(shardSize-1) != 0 {
		return 0, fmt.Errorf("invalid qkc shard size %d", shardSize)
	}
	chainID := a.FullShardKey >> 16
	shardID := a.FullShardKey & (shardSize - 1)
	return chainID<<16 | shardSize | shardID, nil
}

func (c *QuarkChainClusterGenesisConfig) GenesisAccountsByFullShardID() (map[uint32]map[common.Address]QuarkChainAccount, error) {
	if c == nil {
		return nil, errors.New("nil qkc genesis config")
	}
	accountsByShard := make(map[uint32]map[common.Address]QuarkChainAccount)
	for _, chain := range c.QuarkChain.Chains {
		if chain.Genesis == nil {
			continue
		}
		for qkcAddress, alloc := range chain.Genesis.Alloc {
			if got := qkcAddress.FullShardKey >> 16; got != chain.ChainID {
				return nil, fmt.Errorf("qkc genesis address chain id mismatch: address has %d, chain has %d", got, chain.ChainID)
			}
			fullShardID, err := qkcAddress.FullShardID(chain.ShardSize)
			if err != nil {
				return nil, err
			}
			account, err := alloc.toQuarkChainAccount(qkcAddress)
			if err != nil {
				return nil, err
			}
			accounts := accountsByShard[fullShardID]
			if accounts == nil {
				accounts = make(map[common.Address]QuarkChainAccount)
				accountsByShard[fullShardID] = accounts
			}
			if _, exists := accounts[qkcAddress.Recipient]; exists {
				return nil, fmt.Errorf("duplicate qkc genesis recipient %s in full shard %d", qkcAddress.Recipient, fullShardID)
			}
			accounts[qkcAddress.Recipient] = account
		}
	}
	return accountsByShard, nil
}

func (c *QuarkChainClusterGenesisConfig) BuildGenesisStateRoots(db *triedb.Database) (map[uint32]common.Hash, error) {
	accountsByShard, err := c.GenesisAccountsByFullShardID()
	if err != nil {
		return nil, err
	}
	if db == nil {
		db = NewQuarkChainMemoryTrieDB()
	}
	roots := make(map[uint32]common.Hash, len(accountsByShard))
	for fullShardID, accounts := range accountsByShard {
		root, err := BuildQuarkChainStateRoot(accounts, db)
		if err != nil {
			return nil, fmt.Errorf("build qkc genesis state root for full shard %d: %w", fullShardID, err)
		}
		roots[fullShardID] = root
	}
	return roots, nil
}

func (a QuarkChainGenesisAllocation) toQuarkChainAccount(addr QuarkChainGenesisAddress) (QuarkChainAccount, error) {
	balances := make(map[uint64]*big.Int, len(a.Balances))
	for token, balance := range a.Balances {
		tokenID, err := QuarkChainTokenIDEncode(token)
		if err != nil {
			return QuarkChainAccount{}, err
		}
		if balance == nil {
			continue
		}
		balances[tokenID] = new(big.Int).Set(balance)
	}
	var nonce uint64
	if a.CodeSet {
		nonce = 1
	}
	return QuarkChainAccount{
		Nonce:         nonce,
		TokenBalances: balances,
		Storage:       a.Storage,
		Code:          a.Code,
		FullShardKey:  addr.FullShardKey,
	}, nil
}

func QuarkChainTokenIDEncode(token string) (uint64, error) {
	if len(token) == 0 {
		return 0, errors.New("empty qkc token symbol")
	}
	if len(token) >= 13 {
		return 0, fmt.Errorf("qkc token symbol too long: %s", token)
	}
	id, err := quarkChainTokenCharEncode(token[len(token)-1])
	if err != nil {
		return 0, err
	}
	base := uint64(36)
	for i := len(token) - 2; i >= 0; i-- {
		ch, err := quarkChainTokenCharEncode(token[i])
		if err != nil {
			return 0, err
		}
		id += base * (ch + 1)
		base *= 36
		if i == 0 {
			break
		}
	}
	return id, nil
}

func quarkChainTokenCharEncode(ch byte) (uint64, error) {
	switch {
	case ch >= '0' && ch <= '9':
		return uint64(ch - '0'), nil
	case ch >= 'A' && ch <= 'Z':
		return 10 + uint64(ch-'A'), nil
	default:
		return 0, fmt.Errorf("invalid qkc token symbol character %q", ch)
	}
}

type quarkChainGenesisHash common.Hash

func (h *quarkChainGenesisHash) UnmarshalText(text []byte) error {
	text = bytes.TrimPrefix(text, []byte("0x"))
	if len(text) > common.HashLength*2 {
		return fmt.Errorf("too many hex characters in qkc genesis hash %q", text)
	}
	if len(text)%2 == 1 {
		text = append([]byte{'0'}, text...)
	}
	offset := len(h) - len(text)/2
	if _, err := hex.Decode((*h)[offset:], text); err != nil {
		return fmt.Errorf("invalid qkc genesis hash %q", text)
	}
	return nil
}

func quarkChainDecodeHex(input string) ([]byte, error) {
	input = strings.TrimPrefix(input, "0x")
	input = strings.TrimPrefix(input, "0X")
	if len(input)%2 == 1 {
		input = "0" + input
	}
	blob, err := hex.DecodeString(input)
	if err != nil {
		return nil, err
	}
	return blob, nil
}
