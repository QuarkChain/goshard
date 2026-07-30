// Ported verbatim from github.com/QuarkChain/goquarkchain/cluster/config (byte-compatible).

package config

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/qkc/account"
	qcom "github.com/ethereum/go-ethereum/qkc/common"
)

// decodeGenesisHex strictly decodes an optional-"0x"-prefixed hex string. Unlike
// common.FromHex/Hex2Bytes (which silently drop invalid input, so "zz" decodes to
// empty and a malformed genesis passes validation), it reports malformed hex so the
// failure surfaces at config load instead of at shard-genesis creation. An empty
// string is a valid empty value.
func decodeGenesisHex(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return nil, nil
	}
	return hex.DecodeString(s)
}

type ShardGenesis struct {
	RootHeight         uint32                         `json:"ROOT_HEIGHT"`
	Version            uint32                         `json:"VERSION"`
	Height             uint64                         `json:"HEIGHT"`
	HashPrevMinorBlock string                         `json:"HASH_PREV_MINOR_BLOCK"`
	HashMerkleRoot     string                         `json:"HASH_MERKLE_ROOT"`
	ExtraData          []byte                         `json:"-"`
	Timestamp          uint64                         `json:"TIMESTAMP"`
	Difficulty         uint64                         `json:"DIFFICULTY"`
	GasLimit           uint64                         `json:"GAS_LIMIT"`
	Nonce              uint32                         `json:"NONCE"`
	Alloc              map[account.Address]Allocation `json:"-"`
}

func NewShardGenesis() *ShardGenesis {
	return &ShardGenesis{
		RootHeight:         0,
		Version:            0,
		Height:             0,
		HashPrevMinorBlock: "",
		HashMerkleRoot:     "",
		ExtraData:          common.FromHex("497420776173207468652062657374206f662074696d65732c206974207761732074686520776f727374206f662074696d65732c202e2e2e202d20436861726c6573204469636b656e73"),
		Timestamp:          NewRootGenesis().Timestamp,
		Difficulty:         10000,
		GasLimit:           30000 * 400,
		Nonce:              0,
		Alloc:              make(map[account.Address]Allocation),
	}
}

type Allocation struct {
	Balances map[string]*big.Int
	Code     []byte
	// CodePresent distinguishes an omitted code field from explicit empty code,
	// which pyquarkchain treats as a contract allocation with nonce 1.
	CodePresent bool
	Storage     map[common.Hash]common.Hash
}

type AllocMarshalling = struct {
	Balances map[string]*big.Int         `json:"balances"`
	Code     *string                     `json:"code,omitempty"`
	Storage  map[storageJSON]storageJSON `json:"storage"`
}

func (a Allocation) MarshalJSON() ([]byte, error) {
	var jsonConfig AllocMarshalling
	if a.Balances != nil {
		jsonConfig.Balances = a.Balances
	}
	if a.CodePresent || a.Code != nil {
		code := common.Bytes2Hex(a.Code)
		jsonConfig.Code = &code
	}
	if a.Storage != nil {
		jsonConfig.Storage = make(map[storageJSON]storageJSON, len(a.Storage))
		for k, v := range a.Storage {
			jsonConfig.Storage[storageJSON(k)] = storageJSON(v)
		}
	}
	return json.Marshal(jsonConfig)
}

func (a *Allocation) UnmarshalJSON(input []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(input, &fields); err != nil {
		return err
	}
	_, hasBalances := fields["balances"]
	_, hasCode := fields["code"]
	_, hasStorage := fields["storage"]
	if !hasBalances && !hasCode && !hasStorage {
		var jsonConfig map[string]*big.Int
		if err := json.Unmarshal(input, &jsonConfig); err != nil {
			return err
		}
		//# backward compatible:
		//# v1: {addr: {QKC: 1234}}
		//# v2: {addr: {balances: {QKC: 1234}, code: 0x, storage: {0x12: 0x34}}}
		a.Balances = jsonConfig
		return nil
	}

	var jsonConfig AllocMarshalling
	if err := json.Unmarshal(input, &jsonConfig); err != nil {
		return err
	}
	if jsonConfig.Balances != nil {
		a.Balances = jsonConfig.Balances
	}
	if hasCode {
		if jsonConfig.Code == nil {
			return fmt.Errorf("code: expected a string")
		}
		code, err := decodeGenesisHex(*jsonConfig.Code)
		if err != nil {
			return fmt.Errorf("code: %w", err)
		}
		a.Code = code
		a.CodePresent = true
	}
	if jsonConfig.Storage != nil {
		a.Storage = make(map[common.Hash]common.Hash, len(jsonConfig.Storage))
		for k, v := range jsonConfig.Storage {
			a.Storage[common.Hash(k)] = common.Hash(v)
		}
	}
	return nil
}

// storageJSON represents a 256 bit byte array, but allows less than 256 bits when
// unmarshaling from hex.
type storageJSON common.Hash

func (h *storageJSON) UnmarshalText(text []byte) error {
	text = bytes.TrimPrefix(text, []byte("0x"))
	if len(text) > 64 {
		return fmt.Errorf("too many hex characters in storage key/value %q", text)
	}
	offset := len(h) - len(text)/2 // pad on the left
	if _, err := hex.Decode(h[offset:], text); err != nil {
		fmt.Println(err)
		return fmt.Errorf("invalid hex storage key/value %q", text)
	}
	return nil
}

func (h storageJSON) MarshalText() ([]byte, error) {
	return hexutil.Bytes(h[:]).MarshalText()
}

type ShardGenesisAlias ShardGenesis

func (s *ShardGenesis) MarshalJSON() ([]byte, error) {
	alloc := make(map[string]Allocation)
	for addr, val := range s.Alloc {
		alloc[string(addr.ToHex())] = val
	}
	jsonConfig := struct {
		ShardGenesisAlias
		ExtraData string                `json:"EXTRA_DATA"`
		Alloc     map[string]Allocation `json:"ALLOC"`
	}{ShardGenesisAlias(*s), common.Bytes2Hex(s.ExtraData), alloc}
	return json.Marshal(jsonConfig)
}

func (s *ShardGenesis) UnmarshalJSON(input []byte) error {
	var jsonConfig struct {
		ShardGenesisAlias
		ExtraData string                `json:"EXTRA_DATA"`
		Alloc     map[string]Allocation `json:"ALLOC"`
	}
	if err := json.Unmarshal(input, &jsonConfig); err != nil {
		return err
	}
	*s = ShardGenesis(jsonConfig.ShardGenesisAlias)
	extra, err := decodeGenesisHex(jsonConfig.ExtraData)
	if err != nil {
		return fmt.Errorf("EXTRA_DATA: %w", err)
	}
	s.ExtraData = extra
	s.Alloc = make(map[account.Address]Allocation)
	for addr, val := range jsonConfig.Alloc {
		address, err := account.CreatAddressFromBytes(common.FromHex(addr))
		if err != nil {
			return err
		}
		s.Alloc[address] = val
	}
	return nil
}

type ShardConfig struct {
	ShardID    uint32
	rootConfig *RootConfig
	*ChainConfig
}

func NewShardConfig(chainCfg *ChainConfig) *ShardConfig {
	var cfg = new(ChainConfig)
	_ = qcom.DeepCopy(cfg, chainCfg)
	shardConfig := &ShardConfig{
		ShardID:     0,
		ChainConfig: cfg,
	}
	return shardConfig
}

func (s *ShardConfig) SetRootConfig(value *RootConfig) {
	s.rootConfig = value
}

func (s *ShardConfig) GetRootConfig() *RootConfig {
	return s.rootConfig
}

func (s *ShardConfig) MaxBlocksPerShardInOneRootBlock() uint32 {
	return s.rootConfig.ConsensusConfig.TargetBlockTime/
		s.ConsensusConfig.TargetBlockTime + s.ExtraShardBlocksInRootBlock
}

func (s *ShardConfig) MaxStaleMinorBlockHeightDiff() uint64 {
	return s.rootConfig.MaxStaleRootBlockHeightDiff *
		uint64(s.rootConfig.ConsensusConfig.TargetBlockTime) /
		uint64(s.ConsensusConfig.TargetBlockTime)
}

func (s *ShardConfig) MaxMinorBlocksInMemory() uint64 {
	return s.MaxStaleMinorBlockHeightDiff() * 2
}

func (s *ShardConfig) GetFullShardId() uint32 {
	return (s.ChainID << 16) | s.ShardSize | s.ShardID
}
