// Copyright 2026-2027, QuarkChain.

package types

import (
	"encoding/hex"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pyqkcVecDrainedToZero is the pyquarkchain encoding of an account whose QKC
// balance was drained to exactly zero by a transaction in the committing block:
//
//	nonce           = 1
//	token_balances  = b"\x00\xc0"
//	storage         = EmptyRootHash
//	code_hash       = EmptyCodeHash
//	full_shard_key  = 1
//	optional        = b""
//
// token_balances is 00c0 rather than b"" because the balance map keeps the zero
// entry. TokenBalances.serialize (quarkchain/evm/state.py:135) gates on map
// length while the zero-skipping only happens when the pair list is built:
//
//	if len(self._balances) == 0:
//	    return b""
//	ls = [TokenBalancePair(k, v) for k, v in self._balances.items() if v > 0]
//	return b"\x00" + rlp.encode(ls)
//
// and set_token_balance writes the zero straight into _balances, so a drained
// account holds {35760: 0} and serializes to b"\x00" + rlp([]) == 00c0. The
// account survives the empty-account sweep because is_blank requires nonce == 0.
// goquarkchain agrees: SubBalance -> SetValue keeps the zero entry and
// SerializeToBytes gates on Len() (core/types/token.go).
var pyqkcVecDrainedToZero = mustHex("f84c018200c0a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470840000000180")

// pyqkcVecNeverHeld is the same account without the entry: an account that never
// held any token serializes token_balances as empty bytes. The two vectors are
// different trie leaves, so a codec that cannot tell them apart forks the state
// root of any block that drains an account.
var pyqkcVecNeverHeld = mustHex("f84a0180a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470840000000180")

// TestStateAccountDrainedToZeroEncoding pins the trie leaf of an account drained
// to exactly zero in the committing block. Balance is a scalar and cannot express
// "present but zero", so the entry is carried by a non-nil MntBalances; without
// it the account would encode like one that never held the token.
func TestStateAccountDrainedToZeroEncoding(t *testing.T) {
	drained := &StateAccount{
		Nonce:        1,
		Balance:      new(uint256.Int),
		Root:         EmptyRootHash,
		CodeHash:     EmptyCodeHash.Bytes(),
		FullShardKey: 1,
		MntBalances:  qkccommon.NewEmptyTokenBalances(),
	}
	encoded, err := rlp.EncodeToBytes(drained)
	require.NoError(t, err)
	assert.Equal(t, hex.EncodeToString(pyqkcVecDrainedToZero), hex.EncodeToString(encoded),
		"a drained account must encode its zero entry as 00c0, like pyquarkchain")

	neverHeld := drained.Copy()
	neverHeld.MntBalances = nil
	encoded, err = rlp.EncodeToBytes(neverHeld)
	require.NoError(t, err)
	assert.Equal(t, hex.EncodeToString(pyqkcVecNeverHeld), hex.EncodeToString(encoded),
		"an account that never held the token must encode empty bytes")

	assert.NotEqual(t, crypto.Keccak256Hash(pyqkcVecDrainedToZero), crypto.Keccak256Hash(pyqkcVecNeverHeld),
		"the two forms are distinct trie leaves; a codec that merges them forks the state root")
}

// TestStateAccountDrainedToZeroFreshAccount is the case the presence marker
// cannot be recovered for: an account with no trie leaf at all, credited and
// then drained inside one block. Decoding cannot seed the marker here — there is
// nothing to decode — so it has to be set when the balance is written, which is
// what stateObject.SetBalance does.
func TestStateAccountDrainedToZeroFreshAccount(t *testing.T) {
	// What a fresh account looks like before anything touches it.
	fresh := NewEmptyStateAccount()
	fresh.FullShardKey = 1
	require.Nil(t, fresh.MntBalances)

	// Credited then drained within the block: pyquarkchain's map holds
	// {35760: 0} and serializes to 00c0.
	fresh.Nonce = 1
	fresh.MntBalances = qkccommon.NewEmptyTokenBalances()
	encoded, err := rlp.EncodeToBytes(fresh)
	require.NoError(t, err)
	assert.Equal(t, hex.EncodeToString(pyqkcVecDrainedToZero), hex.EncodeToString(encoded))
}

// TestStateAccountDrainedToZeroDecoding pins the other half of the rule: the
// entry is NOT recoverable from the wire. pyquarkchain rebuilds _balances from
// the pair list, which carries no zeros, so reading 00c0 back gives an empty map
// that re-serializes to empty bytes. Reproducing 00c0 on read-back would look
// faithful and diverge on the next block that touches the account.
func TestStateAccountDrainedToZeroDecoding(t *testing.T) {
	var wire qkcAccountRLP
	require.NoError(t, rlp.DecodeBytes(pyqkcVecDrainedToZero, &wire))
	require.Equal(t, []byte{0x00, 0xc0}, wire.TokenBal, "vector must carry the zero-entry token balance form")

	var acct StateAccount
	require.NoError(t, rlp.DecodeBytes(pyqkcVecDrainedToZero, &acct))
	assert.True(t, acct.Balance.IsZero())
	assert.EqualValues(t, 1, acct.Nonce)
	assert.Nil(t, acct.MntBalances, "the zero entry is lost on read-back, as it is in pyquarkchain")

	reencoded, err := rlp.EncodeToBytes(&acct)
	require.NoError(t, err)
	assert.Equal(t, hex.EncodeToString(pyqkcVecNeverHeld), hex.EncodeToString(reencoded),
		"an account read back and rewritten drops the zero entry, as pyquarkchain does")
}

// TestStateAccountDrainedToZeroViaTokenBalances derives the same wire bytes
// through the balance-set path the reference clients take: SubBalance leaves a
// zero entry in the map, and serialization gates on the map being non-empty.
func TestStateAccountDrainedToZeroViaTokenBalances(t *testing.T) {
	balances := qkccommon.NewTokenBalancesWithMap(map[uint64]*uint256.Int{
		qkccommon.DefaultTokenID: uint256.NewInt(1000),
	})
	require.NoError(t, balances.Sub(map[uint64]*uint256.Int{
		qkccommon.DefaultTokenID: uint256.NewInt(1000),
	}))
	require.Equal(t, 1, balances.Len(), "the drained token keeps its map entry")
	require.Equal(t, 0, balances.NonZeroLen(), "but contributes nothing to the pair list")

	tokenBal, err := balances.SerializeToBytes()
	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0xc0}, tokenBal)
}
