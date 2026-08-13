// Copyright 2026-2027, QuarkChain.

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/qkc/shard"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/urfave/cli/v2"
)

var datadirFlag = &cli.StringFlag{
	Name:  "datadir",
	Usage: "Directory holding the per-shard chaindbs (the cluster config's DB_PATH_ROOT)",
}

var inspectCommand = &cli.Command{
	Name:      "inspect",
	Usage:     "Print the stored genesis block and head of every shard chaindb under a datadir",
	ArgsUsage: " ",
	Flags: []cli.Flag{
		datadirFlag,
	},
	Description: `Read-only and config-free: scans --datadir for shard chaindb directories
(shard-0x{full_shard_id}/), opens each in read-only mode, and prints the stored
minor genesis block and chain head. A shard that cannot be opened or read is
reported inline without aborting the remaining shards; the exit status is
non-zero if any shard failed. A running slave holds its chaindb locks, so
inspect a stopped node.`,
	Action: runInspect,
}

func runInspect(ctx *cli.Context) error {
	datadir := ctx.String(datadirFlag.Name)
	if datadir == "" {
		return fmt.Errorf("--%s is required", datadirFlag.Name)
	}
	return inspectDataDir(os.Stdout, datadir)
}

// inspectDataDir prints one block per shard chaindb found under datadir, in
// directory-name order. A shard that fails to open or read is reported inline
// and folded into the returned error; the other shards still print.
func inspectDataDir(out io.Writer, datadir string) error {
	entries, err := os.ReadDir(datadir)
	if err != nil {
		return err
	}
	var (
		inspected int
		errs      []error
	)
	for _, entry := range entries {
		id, ok := shard.ParseDBDirName(entry.Name())
		if !ok || !entry.IsDir() {
			continue
		}
		inspected++
		if err := inspectShardDB(out, filepath.Join(datadir, entry.Name()), id); err != nil {
			fmt.Fprintf(out, "shard 0x%08x: %v\n", id, err)
			errs = append(errs, fmt.Errorf("shard 0x%08x: %w", id, err))
		}
	}
	if inspected == 0 {
		return fmt.Errorf("no shard chaindbs (shard-0x{full_shard_id}/) under %s", datadir)
	}
	fmt.Fprintf(out, "%d shard(s) inspected, %d failed\n", inspected, len(errs))
	return errors.Join(errs...)
}

// Modest fixed sizing for a short-lived read-only open.
const (
	inspectDBCacheMB = 16
	inspectDBHandles = 16
)

// inspectShardDB opens one shard chaindb read-only and prints its stored genesis
// block and chain head. An absent block is not an error: it is the expected state
// of a chaindb whose bootstrap was interrupted before the block was committed
// (the next boot re-runs the fresh path).
func inspectShardDB(out io.Writer, path string, id uint32) error {
	kv, err := pebble.New(path, inspectDBCacheMB, inspectDBHandles, fmt.Sprintf("qkc/inspect/0x%08x/", id), true)
	if err != nil {
		return fmt.Errorf("open chaindb %s: %w", path, err)
	}
	defer kv.Close()

	// TODO(#1): the genesis block lives under one scaffolding key until the real
	// shard chain owns block storage; read it through the chain's canonical-hash
	// accessors then, and report the real head alongside it.
	block, err := shard.ReadGenesisBlock(kv)
	if err != nil {
		return fmt.Errorf("read genesis block (db %s): %w", path, err)
	}
	fmt.Fprintf(out, "shard 0x%08x (%s):\n", id, path)
	if block == nil {
		fmt.Fprintln(out, "  genesis block:         none (bootstrap never completed; next boot re-initializes)")
	} else {
		printGenesisBlock(out, block)
	}
	if head := rawdb.ReadHeadBlockHash(kv); head != (common.Hash{}) {
		fmt.Fprintf(out, "  head block:            %s\n", head)
	} else {
		fmt.Fprintln(out, "  head block:            none recorded (stub chain persists no head)")
	}
	// The stored block names its own shard through its branch; a chaindb holding
	// another shard's genesis is a misplaced directory, not a config change.
	if block != nil {
		if storedID := block.Header.Branch.GetFullShardID(); storedID != id {
			return fmt.Errorf("stored genesis belongs to shard 0x%08x but the directory name says 0x%08x (db %s) — misplaced chaindb", storedID, id, path)
		}
	}
	return nil
}

// printGenesisBlock prints the identity a reopened datadir is reconciled against:
// the block hash, the state root its meta commits to, and the root-chain linkage
// and cross-shard cursor the block was derived from.
func printGenesisBlock(out io.Writer, block *types.MinorBlock) {
	h, m := block.Header, block.Meta
	fmt.Fprintf(out, "  genesis block:         %s\n", block.Hash())
	fmt.Fprintf(out, "  height:                %d\n", h.Number)
	fmt.Fprintf(out, "  state root:            %s\n", m.Root)
	fmt.Fprintf(out, "  coinbase:              %s\n", h.Coinbase.ToHex())
	fmt.Fprintf(out, "  coinbase amount:       %s\n", formatTokenBalances(h.CoinbaseAmount))
	fmt.Fprintf(out, "  evm_gas_limit:         %s\n", formatUint256(h.GasLimit))
	fmt.Fprintf(out, "  evm_xshard_gas_limit:  %s\n", formatUint256(m.XShardGasLimit))
	fmt.Fprintf(out, "  hash_prev_root_block:  %s\n", h.PrevRootBlockHash)
	fmt.Fprintf(out, "  xshard cursor:         root=%d minor=%d deposit=%d\n",
		m.XShardTxCursor.RootBlockHeight, m.XShardTxCursor.MinorBlockIndex, m.XShardTxCursor.XShardDepositIndex)
}

// formatTokenBalances renders a coinbase amount map in ascending token-id order,
// so two shards' reports are comparable line by line.
func formatTokenBalances(b *qkcCommon.TokenBalances) string {
	if b == nil || b.Len() == 0 {
		return "none"
	}
	balances := b.GetBalanceMap()
	ids := make([]uint64, 0, len(balances))
	for id := range balances {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("token %d = %s", id, balances[id]))
	}
	return strings.Join(parts, ", ")
}

func formatUint256(v *serialize.Uint256) string {
	if v == nil || v.Value == nil {
		return "unset"
	}
	return v.Value.String()
}
