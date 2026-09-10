// Copyright 2026-2027, QuarkChain.

package state

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/tracing"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMntTestStateDB(t *testing.T) *StateDB {
	t.Helper()
	db := triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil)
	s, err := New(common.Hash{}, NewDatabase(db, nil))
	require.NoError(t, err)
	return s
}

func TestTokenBalanceAPIs(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0x1234")
	s.SetBalance(addr, uint256.NewInt(1000), tracing.BalanceChangeUnspecified)
	s.SetMntBalance(addr, uint256.NewInt(500), 100)

	obj := s.getStateObject(addr)
	require.NotNil(t, obj)
	balances := obj.data.MntBalances.GetBalanceMap()
	assert.Equal(t, uint256.NewInt(1000), balances[qkccommon.DefaultTokenID])
	assert.Equal(t, uint256.NewInt(500), balances[100])
	assert.Equal(t, s.GetBalance(addr), s.GetMntBalance(addr, qkccommon.DefaultTokenID))

	s.AddBalanceByTokenID(addr, uint256.NewInt(3), qkccommon.DefaultTokenID, tracing.BalanceChangeUnspecified)
	s.AddBalanceByTokenID(addr, uint256.NewInt(4), 100, tracing.BalanceChangeUnspecified)
	s.SubBalanceByTokenID(addr, uint256.NewInt(1), qkccommon.DefaultTokenID, tracing.BalanceChangeUnspecified)
	s.SubBalanceByTokenID(addr, uint256.NewInt(2), 100, tracing.BalanceChangeUnspecified)
	assert.Equal(t, uint256.NewInt(1002), s.GetBalanceByTokenID(addr, qkccommon.DefaultTokenID))
	assert.Equal(t, uint256.NewInt(502), s.GetBalanceByTokenID(addr, 100))
}

func TestTokenBalanceJournalRevertWritesZero(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0x2345")
	s.CreateAccount(addr)
	snapshot := s.Snapshot()

	s.SetBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
	s.SetMntBalance(addr, uint256.NewInt(2), 100)
	s.RevertToSnapshot(snapshot)

	balances := s.getStateObject(addr).data.MntBalances
	balanceMap := balances.GetBalanceMap()
	qkc, hasQKC := balanceMap[qkccommon.DefaultTokenID]
	mnt, hasMNT := balanceMap[100]
	assert.True(t, hasQKC)
	assert.True(t, hasMNT)
	assert.True(t, qkc.IsZero())
	assert.True(t, mnt.IsZero())
	encoded, err := balances.SerializeToBytes()
	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0xc0}, encoded)
}

func TestSetUnchangedZeroBalanceDoesNotWriteTokenEntry(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0x2346")
	s.SetNonce(addr, 1, tracing.NonceChangeUnspecified)
	root, err := s.Commit(0, false, false)
	require.NoError(t, err)

	s, err = New(root, s.Database())
	require.NoError(t, err)
	s.SetBalance(addr, new(uint256.Int), tracing.BalanceChangeUnspecified)
	s.SetMntBalance(addr, new(uint256.Int), 100)

	balanceMap := s.getStateObject(addr).data.MntBalances.GetBalanceMap()
	assert.Empty(t, balanceMap)

	unchangedRoot, err := s.Commit(1, false, false)
	require.NoError(t, err)
	assert.Equal(t, root, unchangedRoot)
}

func TestTokenBalancesCommitRoundTrip(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0x3456")
	s.SetBalance(addr, uint256.NewInt(1000), tracing.BalanceChangeUnspecified)
	s.SetMntBalance(addr, uint256.NewInt(500), 100)

	root, err := s.Commit(0, false, false)
	require.NoError(t, err)
	reopened, err := New(root, s.Database())
	require.NoError(t, err)
	assert.Equal(t, uint256.NewInt(1000), reopened.GetBalance(addr))
	assert.Equal(t, uint256.NewInt(500), reopened.GetMntBalance(addr, 100))
}

func TestTokenBalancesCopyDoesNotAlias(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0x4567")
	s.SetBalance(addr, uint256.NewInt(1000), tracing.BalanceChangeUnspecified)
	s.SetMntBalance(addr, uint256.NewInt(500), 100)

	copyState := s.Copy()
	copyState.SetBalance(addr, uint256.NewInt(2000), tracing.BalanceChangeUnspecified)
	copyState.SetMntBalance(addr, uint256.NewInt(700), 100)

	assert.Equal(t, uint256.NewInt(1000), s.GetBalance(addr))
	assert.Equal(t, uint256.NewInt(500), s.GetMntBalance(addr, 100))
	assert.Equal(t, uint256.NewInt(2000), copyState.GetBalance(addr))
	assert.Equal(t, uint256.NewInt(700), copyState.GetMntBalance(addr, 100))
}

func TestSetStoragePreservesAllTokenBalances(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0x4568")
	s.SetBalance(addr, uint256.NewInt(1000), tracing.BalanceChangeUnspecified)
	s.SetMntBalance(addr, uint256.NewInt(500), 100)

	s.SetStorage(addr, map[common.Hash]common.Hash{
		common.HexToHash("0x01"): common.HexToHash("0x02"),
	})

	assert.Equal(t, uint256.NewInt(1000), s.GetBalance(addr))
	assert.Equal(t, uint256.NewInt(500), s.GetMntBalance(addr, 100))
}

func TestMntBalanceRejectsInvalidUpdates(t *testing.T) {
	t.Run("QKC token ID", func(t *testing.T) {
		for _, update := range []func(*StateDB, common.Address){
			func(s *StateDB, addr common.Address) {
				s.SetMntBalance(addr, uint256.NewInt(1), qkccommon.DefaultTokenID)
			},
			func(s *StateDB, addr common.Address) {
				s.AddMntBalance(addr, uint256.NewInt(1), qkccommon.DefaultTokenID)
			},
			func(s *StateDB, addr common.Address) {
				s.SubMntBalance(addr, uint256.NewInt(1), qkccommon.DefaultTokenID)
			},
		} {
			s := newMntTestStateDB(t)
			addr := common.HexToAddress("0x5677")
			update(s, addr)
			require.ErrorContains(t, s.Error(), "QKC token ID")
			_, err := s.Commit(0, false, false)
			require.Error(t, err)
			assert.False(t, s.Exist(addr))
		}
	})

	t.Run("overflow", func(t *testing.T) {
		s := newMntTestStateDB(t)
		addr := common.HexToAddress("0x5678")
		max := new(uint256.Int).Not(new(uint256.Int))
		s.SetMntBalance(addr, max, 100)
		s.AddMntBalance(addr, uint256.NewInt(1), 100)

		require.ErrorContains(t, s.Error(), "overflows 256 bits")
		assert.Equal(t, max, s.GetMntBalance(addr, 100))
		_, err := s.Commit(0, false, false)
		require.Error(t, err)
	})

	t.Run("underflow", func(t *testing.T) {
		s := newMntTestStateDB(t)
		addr := common.HexToAddress("0x5679")
		s.SubMntBalance(addr, uint256.NewInt(1), 100)

		require.ErrorContains(t, s.Error(), "underflows zero")
		assert.False(t, s.Exist(addr))
		_, err := s.Commit(0, false, false)
		require.Error(t, err)
	})
}

func TestMntBalanceKeepsAccountNonEmpty(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0x6789")
	s.SetMntBalance(addr, uint256.NewInt(1), 100)
	assert.False(t, s.Empty(addr))

	s.SetMntBalance(addr, new(uint256.Int), 100)
	assert.True(t, s.Empty(addr))
}

func setMntTestBalances(s *StateDB, addr common.Address, count uint64) {
	for tokenID := uint64(1); tokenID <= count; tokenID++ {
		s.SetMntBalance(addr, uint256.NewInt(tokenID), tokenID)
	}
}

func TestTokenBalanceLimit(t *testing.T) {
	t.Run("checks final state", func(t *testing.T) {
		s := newMntTestStateDB(t)
		addr := common.HexToAddress("0x6790")
		setMntTestBalances(s, addr, qkccommon.TokenTrieThreshold)

		transientTokenID := uint64(qkccommon.TokenTrieThreshold + 1)
		s.SetMntBalance(addr, uint256.NewInt(transientTokenID), transientTokenID)
		s.SetMntBalance(addr, new(uint256.Int), 1)
		require.NoError(t, s.Error())
		_, err := s.Commit(0, false, false)
		require.NoError(t, err)
	})

	t.Run("reverted overflow", func(t *testing.T) {
		s := newMntTestStateDB(t)
		addr := common.HexToAddress("0x6791")
		setMntTestBalances(s, addr, qkccommon.TokenTrieThreshold)
		snapshot := s.Snapshot()
		s.SetMntBalance(addr, uint256.NewInt(17), qkccommon.TokenTrieThreshold+1)
		s.RevertToSnapshot(snapshot)

		require.NoError(t, s.Error())
		_, err := s.Commit(0, false, false)
		require.NoError(t, err)
	})

	t.Run("rejects final overflow", func(t *testing.T) {
		s := newMntTestStateDB(t)
		addr := common.HexToAddress("0x6792")
		setMntTestBalances(s, addr, qkccommon.TokenTrieThreshold+1)
		require.NoError(t, s.Error())

		_, err := s.Commit(0, false, false)
		require.ErrorContains(t, err, "token balances exceed supported list size")
	})
}

func TestFullShardKeyInheritance(t *testing.T) {
	t.Run("new account", func(t *testing.T) {
		s := newMntTestStateDB(t)
		addr := common.HexToAddress("0x789a")
		s.SetFullShardKey(0x12345678)
		s.CreateAccount(addr)
		assert.Equal(t, uint32(0x12345678), s.getStateObject(addr).data.FullShardKey)
	})

	t.Run("first missing-account read", func(t *testing.T) {
		s := newMntTestStateDB(t)
		addr := common.HexToAddress("0x789b")
		s.SetFullShardKey(1)
		assert.True(t, s.GetBalance(addr).IsZero())
		s.SetFullShardKey(2)
		s.AddBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
		assert.Equal(t, uint32(1), s.getStateObject(addr).data.FullShardKey)
	})

	t.Run("current message without earlier read", func(t *testing.T) {
		s := newMntTestStateDB(t)
		addr := common.HexToAddress("0x789c")
		s.SetFullShardKey(1)
		s.SetFullShardKey(2)
		s.AddBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
		assert.Equal(t, uint32(2), s.getStateObject(addr).data.FullShardKey)
	})

	t.Run("first read survives state copy", func(t *testing.T) {
		s := newMntTestStateDB(t)
		addr := common.HexToAddress("0x789d")
		s.SetFullShardKey(1)
		assert.True(t, s.GetBalance(addr).IsZero())
		cpy := s.Copy()
		cpy.SetFullShardKey(2)
		cpy.AddBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
		assert.Equal(t, uint32(1), cpy.getStateObject(addr).data.FullShardKey)
	})

	t.Run("first-read cache clears after commit", func(t *testing.T) {
		s := newMntTestStateDB(t)
		addr := common.HexToAddress("0x789e")
		s.SetFullShardKey(1)
		assert.True(t, s.GetBalance(addr).IsZero())
		_, err := s.Commit(0, false, false)
		require.NoError(t, err)

		s.SetFullShardKey(2)
		s.AddBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
		assert.Equal(t, uint32(2), s.getStateObject(addr).data.FullShardKey)
	})

	t.Run("delete and recreate", func(t *testing.T) {
		s := newMntTestStateDB(t)
		addr := common.HexToAddress("0x789f")
		s.SetFullShardKey(1)
		s.SetBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
		root, err := s.Commit(0, false, false)
		require.NoError(t, err)

		s, err = New(root, s.Database())
		require.NoError(t, err)
		s.SelfDestruct(addr)
		s.Finalise(true)
		s.SetFullShardKey(2)
		s.SetBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
		assert.Equal(t, uint32(1), s.getStateObject(addr).data.FullShardKey)

		root, err = s.Commit(1, false, false)
		require.NoError(t, err)
		s, err = New(root, s.Database())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), s.getStateObject(addr).data.FullShardKey)
	})
}

func TestZeroBalanceDeltaTouchesAndCanonicalizesAccount(t *testing.T) {
	tests := []struct {
		name    string
		tokenID uint64
		apply   func(*StateDB, common.Address, uint64)
	}{
		{
			name:    "add QKC",
			tokenID: qkccommon.DefaultTokenID,
			apply: func(s *StateDB, addr common.Address, _ uint64) {
				s.AddBalance(addr, new(uint256.Int), tracing.BalanceChangeUnspecified)
			},
		},
		{
			name:    "subtract QKC",
			tokenID: qkccommon.DefaultTokenID,
			apply: func(s *StateDB, addr common.Address, _ uint64) {
				s.SubBalance(addr, new(uint256.Int), tracing.BalanceChangeUnspecified)
			},
		},
		{
			name:    "add MNT",
			tokenID: 100,
			apply: func(s *StateDB, addr common.Address, tokenID uint64) {
				s.AddMntBalance(addr, new(uint256.Int), tokenID)
			},
		},
		{
			name:    "subtract MNT",
			tokenID: 100,
			apply: func(s *StateDB, addr common.Address, tokenID uint64) {
				s.SubMntBalance(addr, new(uint256.Int), tokenID)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := newMntTestStateDB(t)
			addr := common.HexToAddress("0x89ab")
			s.SetNonce(addr, 1, tracing.NonceChangeUnspecified)
			snapshot := s.Snapshot()
			s.SetBalanceByTokenID(addr, uint256.NewInt(1), test.tokenID, tracing.BalanceChangeUnspecified)
			s.RevertToSnapshot(snapshot)

			encoded, err := s.getStateObject(addr).data.MntBalances.SerializeToBytes()
			require.NoError(t, err)
			require.Equal(t, []byte{0x00, 0xc0}, encoded)
			nonCanonicalRoot, err := s.Commit(0, false, false)
			require.NoError(t, err)

			s, err = New(nonCanonicalRoot, s.Database())
			require.NoError(t, err)
			test.apply(s, addr, test.tokenID)
			canonicalRoot, err := s.Commit(1, false, false)
			require.NoError(t, err)
			assert.NotEqual(t, nonCanonicalRoot, canonicalRoot)
		})
	}
}
