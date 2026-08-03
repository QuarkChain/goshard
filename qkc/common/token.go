// Copyright 2026-2027, QuarkChain.

// Token balance encoding follows pyquarkchain-compatible QKC wire bytes.

package common

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

// DefaultTokenID is the QKC native token ID (= TokenIDEncode("QKC") = 35760).
// The QKC balance is carried as this token ID inside the multi-token balance
// set when merging into the QuarkChain account trie encoding.
const DefaultTokenID = uint64(35760)

// TokenNameMaxLen is the longest encodable token name: TokenIDEncode(TOKENMAX)
// is TOKENIDMAX, the largest id a uint64 holds.
const TokenNameMaxLen = 12

// ValidateTokenName checks name against pyquarkchain's token-name domain,
// [0-9A-Z]{1,12} (quarkchain/utils.py token_id_encode). TokenIDEncode is a
// verbatim port and panics outside that domain, so anything derived from parsed
// config or a peer message has to come through here first.
func ValidateTokenName(name string) error {
	if name == "" {
		return errors.New("token name is empty")
	}
	if len(name) > TokenNameMaxLen {
		return fmt.Errorf("token name %q is longer than %d characters", name, TokenNameMaxLen)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') {
			continue
		}
		return fmt.Errorf("token name %q has illegal character %q: only 0-9 and A-Z are allowed", name, c)
	}
	return nil
}

// TokenIDEncodeChecked is TokenIDEncode with the domain check reported as an
// error instead of a panic.
func TokenIDEncodeChecked(name string) (uint64, error) {
	if err := ValidateTokenName(name); err != nil {
		return 0, err
	}
	return TokenIDEncode(name), nil
}

// TokenTrieThreshold is the maximum number of non-zero token balances supported
// by this list-only implementation. Accounts with > 16 MNT tokens (goquarkchain's
// 0x01 trie format) are not implemented; supporting them would require a
// database-backed trie.
const TokenTrieThreshold = 16

const (
	tokenBalanceListPrefix = byte(0)
	tokenBalanceTriePrefix = byte(1)
)

type TokenBalancePair struct {
	TokenID uint64
	Balance *uint256.Int
}

// TokenBalances holds QKC account multi-token balances.
//
// Serialization uses list format (0x00 prefix) for up to TokenTrieThreshold
// non-zero balances. Trie format (>16 non-zero balances) is not supported;
// SerializeToBytes returns an error instead.
type TokenBalances struct {
	balances map[uint64]*uint256.Int
}

type TokenBalancesAlias TokenBalances

func (t *TokenBalances) MarshalJSON() ([]byte, error) {
	keys := make([]uint64, 0, len(t.balances))
	for key := range t.balances {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	balances := make([]string, 0, len(keys))
	for _, key := range keys {
		val := t.balances[key]
		balance := "0"
		if val != nil {
			balance = val.Dec()
		}
		balances = append(balances, strconv.FormatUint(key, 10)+":"+balance)
	}
	jsoncfg := struct {
		TokenBalancesAlias
		Balances string `json:"balances"`
	}{TokenBalancesAlias: TokenBalancesAlias(*t), Balances: strings.Join(balances, ",")}
	return json.Marshal(jsoncfg)
}

func (t *TokenBalances) UnmarshalJSON(input []byte) error {
	var jsoncfg struct {
		TokenBalancesAlias
		Balances string `json:"balances"`
	}
	if err := json.Unmarshal(input, &jsoncfg); err != nil {
		return err
	}
	*t = TokenBalances(jsoncfg.TokenBalancesAlias)
	t.balances = make(map[uint64]*uint256.Int)
	if jsoncfg.Balances == "" {
		return nil
	}
	balList := strings.Split(jsoncfg.Balances, ",")
	for _, val := range balList {
		parts := strings.SplitN(val, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid token balance entry: %q", val)
		}
		key, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid token id %q: %w", parts[0], err)
		}
		balance, err := uint256.FromDecimal(parts[1])
		if err != nil {
			return fmt.Errorf("invalid token balance %q: %w", parts[1], err)
		}
		t.balances[key] = balance
	}
	return nil
}

func NewEmptyTokenBalances() *TokenBalances {
	return &TokenBalances{
		balances: make(map[uint64]*uint256.Int),
	}
}

func NewTokenBalancesWithMap(data map[uint64]*uint256.Int) *TokenBalances {
	t := &TokenBalances{
		balances: make(map[uint64]*uint256.Int, len(data)),
	}
	for tokenID, balance := range data {
		if balance == nil {
			continue
		}
		t.balances[tokenID] = new(uint256.Int).Set(balance)
	}
	return t
}

func NewTokenBalances(data []byte) (*TokenBalances, error) {
	tokenBalances := NewEmptyTokenBalances()
	if len(data) == 0 {
		return tokenBalances, nil
	}

	switch data[0] {
	case tokenBalanceListPrefix:
		balanceList := make([]*TokenBalancePair, 0)
		if err := rlp.DecodeBytes(data[1:], &balanceList); err != nil {
			return nil, err
		}
		for _, v := range balanceList {
			if v == nil || v.Balance == nil {
				continue
			}
			tokenBalances.balances[v.TokenID] = new(uint256.Int).Set(v.Balance)
		}
		if nonZeroEntries := tokenBalances.nonZeroEntriesInBalancesCache(); nonZeroEntries > TokenTrieThreshold {
			return nil, fmt.Errorf("token balances exceed supported list size: %d > %d", nonZeroEntries, TokenTrieThreshold)
		}
	case tokenBalanceTriePrefix:
		return nil, errors.New("token balance trie encoding is unsupported")
	default:
		return nil, fmt.Errorf("unknown enum byte in token_balances: %v", data[0])

	}
	return tokenBalances, nil
}

func (t *TokenBalances) Add(other map[uint64]*uint256.Int) error {
	updates := make(map[uint64]*uint256.Int, len(other))
	for tokenID, amount := range other {
		if amount == nil {
			continue
		}
		balance := t.balances[tokenID]
		if balance == nil {
			balance = new(uint256.Int)
		}
		updated, overflow := new(uint256.Int).AddOverflow(balance, amount)
		if overflow {
			return fmt.Errorf("token balance overflow for token %d", tokenID)
		}
		updates[tokenID] = updated
	}
	for tokenID, balance := range updates {
		t.balances[tokenID] = balance
	}
	return nil
}

func (t *TokenBalances) Sub(other map[uint64]*uint256.Int) error {
	updates := make(map[uint64]*uint256.Int, len(other))
	for tokenID, amount := range other {
		if amount == nil {
			continue
		}
		balance := t.balances[tokenID]
		if balance == nil {
			balance = new(uint256.Int)
		}
		updated, underflow := new(uint256.Int).SubOverflow(balance, amount)
		if underflow {
			return fmt.Errorf("token balance underflow for token %d", tokenID)
		}
		updates[tokenID] = updated
	}
	for tokenID, balance := range updates {
		t.balances[tokenID] = balance
	}
	return nil
}

func (t *TokenBalances) SetValue(amount *uint256.Int, tokenID uint64) {
	if amount == nil {
		amount = new(uint256.Int)
	}
	t.balances[tokenID] = new(uint256.Int).Set(amount)
}

func (t *TokenBalances) GetTokenBalance(tokenID uint64) *uint256.Int {
	data, ok := t.balances[tokenID]
	if ok {
		return new(uint256.Int).Set(data)
	}

	return new(uint256.Int)
}

func (t *TokenBalances) GetBalanceMap() map[uint64]*uint256.Int {
	data := t.Copy()
	return data.balances
}

func (t *TokenBalances) Len() int {
	return len(t.balances)
}

func (t *TokenBalances) nonZeroEntriesInBalancesCache() int {
	sum := 0
	for _, v := range t.balances {
		if v != nil && !v.IsZero() {
			sum++
		}
	}
	return sum
}

func (t *TokenBalances) IsBlank() bool {
	return t.nonZeroEntriesInBalancesCache() == 0
}

func (t *TokenBalances) Copy() *TokenBalances {
	balancesCopy := make(map[uint64]*uint256.Int)
	for k, v := range t.balances {
		if v == nil {
			continue
		}
		balancesCopy[k] = new(uint256.Int).Set(v)
	}
	return &TokenBalances{
		balances: balancesCopy,
	}
}

func (t *TokenBalances) SerializeToBytes() ([]byte, error) {
	nonZeroEntries := t.nonZeroEntriesInBalancesCache()
	if nonZeroEntries > TokenTrieThreshold {
		return nil, fmt.Errorf("token balances exceed supported list size: %d > %d", nonZeroEntries, TokenTrieThreshold)
	}
	if t.Len() == 0 {
		return nil, nil
	}
	list := make([]*TokenBalancePair, 0)
	for k, v := range t.balances {
		if v == nil || v.IsZero() {
			continue
		}
		list = append(list, &TokenBalancePair{
			TokenID: k,
			Balance: v,
		})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].TokenID < (list[j].TokenID) })
	rlpData := new(bytes.Buffer)
	rlpData.WriteByte(tokenBalanceListPrefix)
	err := rlp.Encode(rlpData, list)
	if err != nil {
		return nil, err
	}
	return rlpData.Bytes(), nil
}

func (t *TokenBalances) EncodeRLP(w io.Writer) error {
	dataSer, err := t.SerializeToBytes()
	if err != nil {
		return err
	}
	dataRlp, err := rlp.EncodeToBytes(dataSer)
	if err != nil {
		return err
	}
	_, err = w.Write(dataRlp)
	return err
}

func (t *TokenBalances) DecodeRLP(s *rlp.Stream) error {
	dataRawBytes, err := s.Raw()
	if err != nil {
		return err
	}
	dataRlp := new([]byte)
	err = rlp.DecodeBytes(dataRawBytes, dataRlp)
	if err != nil {
		return err
	}
	b, err := NewTokenBalances(*dataRlp)
	if err != nil {
		return err
	}
	(*t).balances = (*b).balances
	return err
}

func (t *TokenBalances) Serialize(w *[]byte) error {
	// Follow pyquarkchain (TokenBalanceMap's PrependedSizeMapSerializer skip_func):
	// omit zero-balance entries from both the count and the body. This keeps
	// Serialize/Deserialize symmetric and preserves pyquarkchain-compatible bytes.
	keys := make([]uint64, 0, t.Len())
	for k, v := range t.balances {
		if v == nil || v.IsZero() {
			continue
		}
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	if err := serialize.Serialize(w, uint32(len(keys))); err != nil {
		return err
	}
	for _, key := range keys {
		v := t.balances[key]
		if err := serialize.Serialize(w, new(big.Int).SetUint64(key)); err != nil {
			return err
		}
		if err := serialize.Serialize(w, v.ToBig()); err != nil {
			return err
		}
	}
	return nil
}

func (t *TokenBalances) Deserialize(bb *serialize.ByteBuffer) error {
	t.balances = make(map[uint64]*uint256.Int)
	num, err := bb.GetUInt32()
	if err != nil {
		return err
	}
	for index := uint32(0); index < num; index++ {
		k := new(big.Int)
		if err := serialize.Deserialize(bb, k); err != nil {
			return err
		}
		v := new(big.Int)

		if err := serialize.Deserialize(bb, v); err != nil {
			return err
		}
		if !k.IsUint64() {
			return fmt.Errorf("token id overflows uint64: %v", k)
		}
		if v.Cmp(common.Big0) == 0 {
			delete(t.balances, k.Uint64())
			continue
		}
		balance, overflow := uint256.FromBig(v)
		if overflow {
			return fmt.Errorf("token balance overflows uint256: %v", v)
		}
		t.balances[k.Uint64()] = balance
	}
	return nil
}
