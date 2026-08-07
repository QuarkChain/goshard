// Copyright 2026-2027, QuarkChain.

// The state trie's leaf encoding. A QuarkChain account carries per-token
// balances and the full shard key of the address that owns it, so the leaves
// differ from geth's — the trie machinery underneath is geth's, unchanged.

package state

import "github.com/ethereum/go-ethereum/common"

// Account is one leaf of the QuarkChain state trie, mirroring pyquarkchain's
// _Account (quarkchain/evm/state.py:66). The field order is the RLP encoding.
//
// TODO: this moves to core/types/state_account.go once the shards run on geth's
// core/state, which needs the leaf type where StateAccount lives. Every user's
// import path changes with it.
type Account struct {
	Nonce uint64
	// TokenBalances is the multi-token balance blob produced by
	// qkc/common.TokenBalances.SerializeToBytes: empty, or a 0x00 tag followed by
	// the RLP list of (token id, balance) pairs sorted by token id.
	TokenBalances []byte
	Root          common.Hash // storage trie root
	CodeHash      common.Hash
	// FullShardKey is pyquarkchain's BigEndianInt(4): a fixed four-byte string,
	// not an integer. Encoding it as a uint32 would strip leading zeros and change
	// the leaf bytes for every shard key below 0x01000000 — which is all of them
	// on the singularity networks.
	FullShardKey [4]byte
	// Optional is pyquarkchain's trailing extension field, always empty here.
	Optional []byte
}
