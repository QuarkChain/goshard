// Copyright 2026-2027, QuarkChain.

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/qkc/shard"
	"github.com/urfave/cli/v2"
)

var datadirFlag = &cli.StringFlag{
	Name:  "datadir",
	Usage: "Directory holding the per-shard chaindbs (the cluster config's DB_PATH_ROOT)",
}

var inspectCommand = &cli.Command{
	Name:      "inspect",
	Usage:     "Print the stored genesis metadata and head of every shard chaindb under a datadir",
	ArgsUsage: " ",
	Flags: []cli.Flag{
		datadirFlag,
	},
	Description: `Read-only and config-free: scans --datadir for shard chaindb directories
(shard-0x{full_shard_id}/), opens each in read-only mode, and prints the stored
genesis metadata record and chain head. A shard that cannot be opened or read is
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

// inspectShardDB opens one shard chaindb read-only and prints its stored genesis
// metadata record and chain head. An absent metadata record is not an error: it
// is the expected state of a chaindb whose bootstrap was interrupted before the
// record was committed (the next boot re-runs the fresh path).
// Modest fixed sizing for a short-lived read-only open.
const (
	inspectDBCacheMB = 16
	inspectDBHandles = 16
)

func inspectShardDB(out io.Writer, path string, id uint32) error {
	kv, err := pebble.New(path, inspectDBCacheMB, inspectDBHandles, fmt.Sprintf("qkc/inspect/0x%08x/", id), true)
	if err != nil {
		return fmt.Errorf("open chaindb %s: %w", path, err)
	}
	defer kv.Close()

	meta, err := shard.ReadGenesisMeta(kv)
	if err != nil {
		return fmt.Errorf("read genesis metadata (db %s): %w", path, err)
	}
	fmt.Fprintf(out, "shard 0x%08x (%s):\n", id, path)
	if meta == nil {
		fmt.Fprintln(out, "  genesis metadata:      none (bootstrap never completed; next boot re-initializes)")
	} else {
		fmt.Fprintf(out, "  meta version:          %d\n", meta.Version)
		fmt.Fprintf(out, "  chain genesis:         %s\n", meta.ChainGenesisHash)
		fmt.Fprintf(out, "  root genesis:          %s\n", meta.RootGenesisHash)
		fmt.Fprintf(out, "  hash_prev_root_block:  %s\n", meta.HashPrevRootBlock)
		fmt.Fprintf(out, "  xshard cursor:         root=%d minor=%d deposit=%d\n",
			meta.XShardCursor.RootBlockHeight, meta.XShardCursor.MinorBlockIndex, meta.XShardCursor.XShardDepositIndex)
	}
	if head := rawdb.ReadHeadBlockHash(kv); head != (common.Hash{}) {
		fmt.Fprintf(out, "  head block:            %s\n", head)
	} else {
		fmt.Fprintln(out, "  head block:            none recorded (stub chain persists no head)")
	}
	if meta != nil && meta.FullShardID != id {
		return fmt.Errorf("metadata records shard 0x%08x but the directory name says 0x%08x (db %s) — misplaced chaindb", meta.FullShardID, id, path)
	}
	return nil
}
