// Copyright 2026-2027, QuarkChain.

// Token balance encoding follows pyquarkchain-compatible QKC wire bytes.
// Adaptations (serialize form identical):
//   - *trie.Database -> *triedb.Database; trie.NewSecure(root, db, 0) ->
//     trie.NewStateTrie(trie.TrieID(root), db) (modern trie API).
//   - Trie node persistence in Commit goes through triedb.Database.Update;
//     full incremental-commit wiring is part of the deferred execution issue.

package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	qCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

const (
	TokenTrieThreshold = 16
)

type TokenBalancePair struct {
	TokenID uint64
	Balance *uint256.Int
}

type TokenBalances struct {
	db        *triedb.Database
	tokenTrie *trie.SecureTrie
	balances  map[uint64]*uint256.Int
}

type TokenBalancesAlias TokenBalances

func (t *TokenBalances) MarshalJSON() ([]byte, error) {
	balances := ""
	for key, val := range t.balances {
		bal := fmt.Sprintf("%d:%d", key, val.Uint64())
		if balances == "" {
			balances = bal
		} else {
			balances += "," + bal
		}
	}
	jsoncfg := struct {
		TokenBalancesAlias
		Balances string `json:"balances"`
	}{TokenBalancesAlias: TokenBalancesAlias(*t), Balances: balances}
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
		var (
			key     int
			balance int
		)
		_, err := fmt.Fscanf(strings.NewReader(val), "%d:%d", &key, &balance)
		if err != nil {
			return err
		}
		t.balances[uint64(key)] = new(uint256.Int).SetUint64(uint64(balance))
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

func NewTokenBalances(data []byte, db *triedb.Database) (*TokenBalances, error) {
	tokenBalances := NewEmptyTokenBalances()
	tokenBalances.db = db
	if len(data) == 0 {
		return tokenBalances, nil
	}

	switch data[0] {
	case byte(0):
		balanceList := make([]*TokenBalancePair, 0)
		if err := rlp.DecodeBytes(data[1:], &balanceList); err != nil {
			return nil, err
		}
		for _, v := range balanceList {
			tokenBalances.balances[v.TokenID] = v.Balance
		}
	case byte(1):
		var err error
		tokenBalances.tokenTrie, err = trie.NewStateTrie(trie.TrieID(common.BytesToHash(data[1:])), db)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Unknown enum byte in token_balances:%v", data[0])

	}
	return tokenBalances, nil
}

func (t *TokenBalances) Commit() {
	if t.notUsingTrie() {
		return
	}
	if t.tokenTrie == nil {
		var err error
		t.tokenTrie, err = trie.NewStateTrie(trie.TrieID(common.Hash{}), t.db)
		if err != nil {
			panic(err)
		}
	}
	for tokenID, bal := range t.balances {
		k := qCommon.EncodeToByte32(tokenID)
		if bal != nil && !bal.IsZero() {
			val, err := rlp.EncodeToBytes(bal)
			if err != nil {
				panic(err)
			}
			t.tokenTrie.MustUpdate(k, val)
		} else {
			t.tokenTrie.MustDelete(k)
		}
	}
	// TODO(execution-issue): thread the previous trie root through for proper
	// incremental commits; EmptyTrieHash as parent only covers the initial commit.
	root, nodes := t.tokenTrie.Commit(false)
	if nodes != nil {
		if err := t.db.Update(root, EmptyTrieHash, 0, trienode.NewWithNodeSet(nodes), nil); err != nil {
			panic(err)
		}
	}
	newTrie, err := trie.NewStateTrie(trie.TrieID(root), t.db)
	if err != nil {
		panic(err)
	}
	t.tokenTrie = newTrie
	t.balances = make(map[uint64]*uint256.Int, 0)
}

func (t *TokenBalances) Add(other map[uint64]*uint256.Int) {
	//TODO only for test? need to delete
	for k, v := range other {
		if v == nil {
			continue
		}
		if data, ok := t.balances[k]; ok {
			t.balances[k] = new(uint256.Int).Add(v, data)
		} else {
			t.balances[k] = new(uint256.Int).Set(v)
		}
	}
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

	if t.tokenTrie != nil {
		v := t.tokenTrie.MustGet(qCommon.EncodeToByte32(tokenID))
		ret := new(uint256.Int)
		if len(v) != 0 {
			if err := rlp.DecodeBytes(v, ret); err != nil {
				panic(err)
			}
			t.balances[tokenID] = new(uint256.Int).Set(ret)
		}
		return ret
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
	return t.tokenTrie == nil && t.nonZeroEntriesInBalancesCache() == 0
}

func (t *TokenBalances) CopyWithDB() *TokenBalances {
	data := t.Copy()
	data.db = t.db
	data.tokenTrie = t.tokenTrie
	return data
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

func (t *TokenBalances) notUsingTrie() bool {
	return t.tokenTrie == nil && t.nonZeroEntriesInBalancesCache() <= TokenTrieThreshold
}

func (t *TokenBalances) SerializeToBytes() ([]byte, error) {
	readyBeforeSer := func() bool {
		if t.notUsingTrie() {
			return true
		}

		if t.tokenTrie != nil && len(t.balances) == 0 {
			return true
		}
		return false
	}
	if !readyBeforeSer() {
		return nil, errors.New("bug here")
	}
	if t.tokenTrie != nil {
		w := make([]byte, 33)
		w[0] = byte(1)
		copy(w[1:], t.tokenTrie.Hash().Bytes())
		return w, nil
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
	rlpData.WriteByte(byte(0))
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
		panic(err)
	}
	if t.db == nil {
		debug.PrintStack()
		panic("bug here")
	}
	//!!!need to set db before decode
	b, err := NewTokenBalances(*dataRlp, t.db)
	if err != nil {
		return err
	}
	(*t).balances = (*b).balances
	(*t).tokenTrie = (*b).tokenTrie
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
		if v.Cmp(common.Big0) == 0 {
			continue
		}
		if !k.IsUint64() {
			return fmt.Errorf("token id overflows uint64: %v", k)
		}
		balance, overflow := uint256.FromBig(v)
		if overflow {
			return fmt.Errorf("token balance overflows uint256: %v", v)
		}
		t.balances[k.Uint64()] = balance
	}
	return nil
}
