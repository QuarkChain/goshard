// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/tracing"
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

func TestMntBalanceBasic(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0x1234")
	s.CreateAccount(addr)

	const tokenID = uint64(100)
	s.AddMntBalance(addr, uint256.NewInt(500), tokenID)
	assert.Equal(t, uint256.NewInt(500), s.GetMntBalance(addr, tokenID))

	s.SubMntBalance(addr, uint256.NewInt(200), tokenID)
	assert.Equal(t, uint256.NewInt(300), s.GetMntBalance(addr, tokenID))
}

func TestMntRejectsQKCTokenID(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0x5678")
	s.CreateAccount(addr)

	// SetMntBalance with QKC tokenID (35760) must be a no-op
	s.SetMntBalance(addr, uint256.NewInt(999), defaultTokenID)
	assert.True(t, s.GetMntBalance(addr, defaultTokenID).IsZero())
	assert.True(t, s.GetBalance(addr).IsZero()) // QKC balance unchanged
}

func TestMntJournalRevert(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0xABCD")
	s.CreateAccount(addr)

	const tokenID = uint64(200)
	snap := s.Snapshot()
	s.AddMntBalance(addr, uint256.NewInt(1000), tokenID)
	assert.Equal(t, uint256.NewInt(1000), s.GetMntBalance(addr, tokenID))

	s.RevertToSnapshot(snap)
	assert.True(t, s.GetMntBalance(addr, tokenID).IsZero(), "revert should clear MNT balance")
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0x2222")
	s.CreateAccount(addr)
	s.AddBalance(addr, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified) // QKC balance
	s.AddMntBalance(addr, uint256.NewInt(500), uint64(100))                    // MNT token

	root, err := s.Commit(0, false, false)
	require.NoError(t, err)

	// Re-open state at the committed root and verify balances survive
	s2, err := New(root, s.Database())
	require.NoError(t, err)

	assert.Equal(t, uint256.NewInt(1e18), s2.GetBalance(addr), "QKC balance")
	assert.Equal(t, uint256.NewInt(500), s2.GetMntBalance(addr, 100), "MNT balance")
}

// TestEmptyAccountWithMntNotPruned guards the EIP-158 divergence: an account
// with nonce==0 / QKC==0 / MNT!=0 / no code must NOT be treated as empty, since
// pyquarkchain's is_blank spans all tokens and keeps it. Pruning it here would
// diverge the state root. Removing the MNT balance must flip it back to empty.
func TestEmptyAccountWithMntNotPruned(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0xBEEF")
	s.CreateAccount(addr)

	const tokenID = uint64(100)
	s.AddMntBalance(addr, uint256.NewInt(500), tokenID)

	// nonce==0, QKC==0, MNT!=0, no code → not empty.
	require.True(t, s.GetBalance(addr).IsZero(), "QKC balance must be zero for this case")
	require.Zero(t, s.GetNonce(addr), "nonce must be zero for this case")
	assert.False(t, s.Empty(addr), "account with non-zero MNT balance must not be empty")

	// Finalise with deleteEmptyObjects=true must keep the account.
	s.Finalise(true)
	assert.True(t, s.Exist(addr), "MNT-only account must survive empty-object pruning")
	assert.Equal(t, uint256.NewInt(500), s.GetMntBalance(addr, tokenID), "MNT balance must survive")

	// Draining the last MNT balance makes the account empty/prunable again.
	s.SubMntBalance(addr, uint256.NewInt(500), tokenID)
	assert.True(t, s.Empty(addr), "account with no QKC, no MNT, no code, nonce 0 must be empty")
}

// TestCopyDoesNotAliasMntBalances guards the deepCopy aliasing bug: StateDB.Copy()
// must deep-copy the MntBalances map, or a mutation on the copy corrupts the
// original (and vice versa), diverging the state root.
func TestCopyDoesNotAliasMntBalances(t *testing.T) {
	s := newMntTestStateDB(t)
	addr := common.HexToAddress("0xA11A5")
	s.CreateAccount(addr)
	const tokenID = uint64(777)
	s.AddMntBalance(addr, uint256.NewInt(1000), tokenID)

	cp := s.Copy()
	// Mutate the copy; the original must be unaffected.
	cp.AddMntBalance(addr, uint256.NewInt(500), tokenID)

	assert.Equal(t, uint256.NewInt(1000), s.GetMntBalance(addr, tokenID), "original must not see copy's MNT mutation")
	assert.Equal(t, uint256.NewInt(1500), cp.GetMntBalance(addr, tokenID), "copy must reflect its own mutation")

	// And the reverse direction.
	s.AddMntBalance(addr, uint256.NewInt(1), tokenID)
	assert.Equal(t, uint256.NewInt(1001), s.GetMntBalance(addr, tokenID), "original reflects its own mutation")
	assert.Equal(t, uint256.NewInt(1500), cp.GetMntBalance(addr, tokenID), "copy must not see original's later mutation")
}

