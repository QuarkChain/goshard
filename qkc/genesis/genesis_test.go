// Copyright 2026-2027, QuarkChain.

package genesis

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/config"
)

// TestRootBlockFromFixture pins the root genesis hash derived from each real
// QuarkChain network config (the live mainnet and the running devnet) against
// pyquarkchain's own GenesisManager.create_root_block().header.get_hash(). This is
// the tightest compatibility contract. Regeneration: qkc/config/singularity/README.md.
func TestRootBlockFromFixture(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		hash string
		seal string
	}{
		{
			name: "mainnet",
			path: "../config/singularity/mainnet.json",
			hash: "0x4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51",
			seal: "0xe7dcdecc09e724ad81e493d70dedcd6d9ea0ee830d7ab2528a5648f2a0cf8178",
		},
		{
			name: "devnet",
			path: "../config/singularity/devnet.json",
			hash: "0x5ad443efb7cf5246a3d1bbc1734bd02bf3a5d83bedeccfcfe707d0ebee03780d",
			seal: "0x055a7b410a50c098c52c123983e3596c5914ba227bf9e2c0c93309ff8f650d41",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.LoadClusterConfig(tc.path)
			if err != nil {
				t.Fatalf("load fixture: %v", err)
			}
			header, err := CreateRootBlock(cfg.Quarkchain)
			if err != nil {
				t.Fatalf("CreateRootBlock: %v", err)
			}
			if got := header.Hash(); got != common.HexToHash(tc.hash) {
				t.Errorf("root genesis hash mismatch\n got %s\nwant %s", got.Hex(), tc.hash)
			}
			if got := header.SealHash(); got != common.HexToHash(tc.seal) {
				t.Errorf("root genesis seal hash mismatch\n got %s\nwant %s", got.Hex(), tc.seal)
			}
			// total_difficulty is set equal to difficulty (pyquarkchain genesis.py).
			if header.Difficulty.Cmp(header.TotalDifficulty) != 0 {
				t.Errorf("total_difficulty %d != difficulty %d", header.TotalDifficulty, header.Difficulty)
			}
		})
	}
}

// TestRootBlockSynthetic exercises every ROOT.GENESIS field non-zero, pinned
// against a pyquarkchain RootBlockHeader built with the same fields. See README.md.
func TestRootBlockSynthetic(t *testing.T) {
	qkc := config.NewQuarkChainConfig()
	qkc.Root.Genesis = &config.RootGenesis{
		Version:        7,
		Height:         3,
		HashPrevBlock:  "1111111111111111111111111111111111111111111111111111111111111111",
		HashMerkleRoot: "2222222222222222222222222222222222222222222222222222222222222222",
		Timestamp:      1234567890,
		Difficulty:     big.NewInt(11259375), // 0xabcdef
		Nonce:          99,
	}

	header, err := CreateRootBlock(qkc)
	if err != nil {
		t.Fatalf("CreateRootBlock: %v", err)
	}

	const (
		wantHash = "0x70dc3e4bcfe83d1bb81f95015550c9a741168677fe711128aec2d811423991c2"
		wantSeal = "0xbb6649648cd925271b70a49269257b602366b0a24db14d93038ec04a665ad67e"
	)
	if got := header.Hash(); got != common.HexToHash(wantHash) {
		t.Errorf("synthetic root genesis hash mismatch\n got %s\nwant %s", got.Hex(), wantHash)
	}
	if got := header.SealHash(); got != common.HexToHash(wantSeal) {
		t.Errorf("synthetic root genesis seal hash mismatch\n got %s\nwant %s", got.Hex(), wantSeal)
	}
}

func TestRootBlockRejectsBadHash(t *testing.T) {
	qkc := config.NewQuarkChainConfig()
	qkc.Root.Genesis = &config.RootGenesis{HashPrevBlock: "zznothex"}
	if _, err := CreateRootBlock(qkc); err == nil {
		t.Fatal("expected error for malformed HASH_PREV_BLOCK, got nil")
	}
}
