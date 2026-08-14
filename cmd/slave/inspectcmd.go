// Copyright 2026-2027, QuarkChain.

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/params"
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
minor genesis block and chain head once the block is shown to hold together. A
shard that cannot be opened, read, or validated is reported inline without
aborting the remaining shards; the exit status is non-zero if any shard failed. A
running slave holds its chaindb locks, so inspect a stopped node.`,
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
// directory-name order. A shard that fails to open, read or validate is reported
// inline and folded into the returned error; the other shards still print.
func inspectDataDir(out io.Writer, datadir string) error {
	entries, err := os.ReadDir(datadir)
	if err != nil {
		return err
	}
	report := &reportWriter{w: out}
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
		if err := inspectShardDB(report, filepath.Join(datadir, entry.Name()), id); err != nil {
			fmt.Fprintf(report, "shard 0x%08x: %v\n", id, err)
			errs = append(errs, fmt.Errorf("shard 0x%08x: %w", id, err))
		}
		// A report that did not reach the reader is not a report: stop rather than
		// walk the remaining shards and summarize findings nobody can see.
		if err := report.err; err != nil {
			return fmt.Errorf("write report: %w", err)
		}
	}
	if inspected == 0 {
		return fmt.Errorf("no shard chaindbs (shard-0x{full_shard_id}/) under %s", datadir)
	}
	fmt.Fprintf(report, "%d shard(s) inspected, %d failed\n", inspected, len(errs))
	if err := report.err; err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return errors.Join(errs...)
}

// reportWriter latches the first write error so the printers below stay free of
// error plumbing while a truncated report still fails the command. Nothing in
// this file writes to the caller's writer directly.
type reportWriter struct {
	w   io.Writer
	err error
}

func (r *reportWriter) Write(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.w.Write(p)
	if err != nil {
		r.err = err
	}
	return n, err
}

// Modest fixed sizing for a short-lived read-only open.
const (
	inspectDBCacheMB = 16
	inspectDBHandles = 16
)

// inspectShardDB opens one shard chaindb read-only and prints its stored genesis
// block and chain head. An absent block is not an error on its own: it is the
// expected state of a chaindb whose bootstrap was interrupted before the block was
// committed (the next boot re-runs the fresh path).
//
// Nothing is printed until everything the report would assert has been checked, so
// a database that fails is described by its error alone. Printing a block's fields
// first and reporting the trouble after would put a state root nobody should trust
// under a block hash that looks authentic.
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
	head, err := readHeadBlockHash(kv)
	if err != nil {
		return fmt.Errorf("read head block hash (db %s): %w", path, err)
	}

	if block == nil {
		// The slave writes the genesis block last, once the chain stands, so a missing
		// block means an interrupted bootstrap — but only on a database that holds
		// nothing else. A head pointer with no genesis under it is not a state this
		// lifecycle can produce; it is a database that lost its genesis, or one that
		// was never this shard's.
		//
		// TODO(#1): the real chain writes block 0 and its head pointer together, ahead
		// of the scaffolding key, which makes this pairing a legitimate interrupted
		// state — this check moves into the chain's own reopen path then.
		if head != (common.Hash{}) {
			return fmt.Errorf("head block %s is recorded with no genesis block under it (db %s) — not an interrupted bootstrap; corrupt or foreign chaindb", head, path)
		}
	} else {
		if err := checkGenesisSelfConsistent(block); err != nil {
			return fmt.Errorf("%w (db %s) — corrupt chaindb", err, path)
		}
		// The stored block names its own shard through its branch; a chaindb holding
		// another shard's genesis is a misplaced directory, not a config change.
		if storedID := block.Header.Branch.GetFullShardID(); storedID != id {
			return fmt.Errorf("stored genesis belongs to shard 0x%08x but the directory name says 0x%08x (db %s) — misplaced chaindb", storedID, id, path)
		}
	}

	// The rule set is keyed by the genesis hash, so there is one to look for only once
	// the block it hangs off has been read and vouched for.
	var rules storedRules
	if block != nil {
		if rules, err = readChainConfig(kv, block.Hash()); err != nil {
			return fmt.Errorf("read chain config (db %s): %w", path, err)
		}
	}

	fmt.Fprintf(out, "shard 0x%08x (%s):\n", id, path)
	if block == nil {
		fmt.Fprintln(out, "  genesis block:         none (bootstrap never completed; next boot re-initializes)")
	} else {
		printGenesisBlock(out, block)
		printChainConfig(out, rules)
	}
	if head != (common.Hash{}) {
		fmt.Fprintf(out, "  head block:            %s\n", head)
	} else {
		fmt.Fprintln(out, "  head block:            none recorded (stub chain persists no head)")
	}
	return nil
}

// checkGenesisSelfConsistent verifies what a reader without the cluster config can:
// that the stored block holds together. Whether it is the genesis a given config
// derives is a different question, and the boot path answers it by comparing the
// stored encoding (shard.ReconcileGenesisBlock); these invariants hold for every QKC
// minor genesis block regardless of config.
//
// The meta hash is the check that earns its place. A minor block's hash is its
// header's hash alone, so it says nothing about the meta hanging off that header: a
// database whose meta was replaced still reports the original block hash next to the
// substituted state root, and inspect has no config-derived encoding to compare
// against. Recomputing the meta's hash is what closes that gap.
func checkGenesisSelfConsistent(block *types.MinorBlock) error {
	// Every field read below hangs off one of these two. A decode that succeeded
	// should have allocated both, but this is the one place that answers to bytes
	// nobody derived, so it does not assume it.
	if block.Header == nil || block.Meta == nil {
		return errors.New("stored genesis has no header or no meta")
	}
	if block.Header.Number != 0 {
		return fmt.Errorf("stored genesis is block %d, not block 0", block.Header.Number)
	}
	if got, want := block.Meta.Hash(), block.Header.MetaHash; got != want {
		return fmt.Errorf("stored genesis meta hashes to %s but its header commits to %s", got, want)
	}
	// The meta's tx merkle root comes from GENESIS.HASH_MERKLE_ROOT rather than from
	// the body, so it cannot be recomputed here; that the body is empty is the part
	// this can check, and pyquarkchain seals no transactions into block 0.
	if len(block.Transactions) != 0 || len(block.TrackingData) != 0 {
		return fmt.Errorf("stored genesis carries %d transaction(s) and %d tracking byte(s), but the genesis body is empty",
			len(block.Transactions), len(block.TrackingData))
	}
	return nil
}

// storedRules is the EVM rule set found under a shard's genesis hash: the parsed
// config, and the encoding it was parsed from. Both are kept because the schedule is
// rendered from the encoding rather than from a fixed list of fork fields.
type storedRules struct {
	config *params.ChainConfig
	raw    []byte
}

// configPrefix mirrors geth's unexported rawdb.configPrefix (core/rawdb/schema.go).
//
// rawdb.ReadChainConfig answers a failed read and a malformed encoding alike with a
// nil config, which inspect would print as "none stored" — the same false claim about
// a database it could not read that ReadHeadBlockHash would make about the head.
var configPrefix = []byte("ethereum-config-")

// readChainConfig returns the rule set stored under genesisHash, or a zero
// storedRules if none is stored.
func readChainConfig(db ethdb.KeyValueReader, genesisHash common.Hash) (storedRules, error) {
	key := append(bytes.Clone(configPrefix), genesisHash.Bytes()...)
	has, err := db.Has(key)
	if err != nil || !has {
		return storedRules{}, err
	}
	data, err := db.Get(key)
	if err != nil {
		return storedRules{}, err
	}
	config := new(params.ChainConfig)
	if err := json.Unmarshal(data, config); err != nil {
		return storedRules{}, fmt.Errorf("decode chain config: %w", err)
	}
	return storedRules{config: config, raw: data}, nil
}

// printChainConfig reports the rule set a reopen is checked against. It is stored
// apart from the genesis block, so it can be missing on a datadir initialized before
// it was written — recoverable, and the boot path answers it by warning and writing
// one, but worth saying out loud here rather than leaving to a log line.
func printChainConfig(out io.Writer, rules storedRules) {
	if rules.config == nil {
		fmt.Fprintln(out, "  rule set:              none stored (recoverable; the next boot warns and writes it)")
		return
	}
	fmt.Fprintf(out, "  chain id:              %s\n", rules.config.ChainID)
	fmt.Fprintf(out, "  fork schedule:         %s\n", formatForkSchedule(rules.raw))
}

// formatForkSchedule renders the fork activations out of the stored encoding rather
// than a fixed field list, so a rule set carrying a fork this build does not know
// about is still shown instead of silently dropped. Ordering is by activation, which
// is how a schedule is read; every QKC shard fork sits at block 0.
func formatForkSchedule(raw []byte) string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return "unreadable"
	}
	type fork struct {
		name string
		at   uint64
	}
	var forks []fork
	for name, value := range fields {
		if name == "chainId" {
			continue
		}
		var at uint64
		// Anything that is not a plain number is not a scheduled fork: an engine
		// section, a boolean switch, a total difficulty.
		if err := json.Unmarshal(value, &at); err != nil {
			continue
		}
		forks = append(forks, fork{strings.TrimSuffix(name, "Block"), at})
	}
	if len(forks) == 0 {
		return "none scheduled"
	}
	sort.Slice(forks, func(i, j int) bool {
		if forks[i].at != forks[j].at {
			return forks[i].at < forks[j].at
		}
		return forks[i].name < forks[j].name
	})
	parts := make([]string, 0, len(forks))
	for _, f := range forks {
		parts = append(parts, fmt.Sprintf("%s=%d", f.name, f.at))
	}
	return strings.Join(parts, " ")
}

// headBlockKey mirrors geth's unexported rawdb.headBlockKey (core/rawdb/schema.go).
//
// rawdb.ReadHeadBlockHash discards the error from its Get and answers a failed read
// with the zero hash, which inspect would print as "none recorded" — a claim about a
// database it could not actually read. A short value would likewise be padded into a
// plausible-looking hash. This key is part of the on-disk schema, so reproducing it
// is stable in the way that accessor's error handling is not.
var headBlockKey = []byte("LastBlock")

// readHeadBlockHash returns the recorded head block hash, the zero hash if none is
// recorded, and an error if the database cannot answer.
func readHeadBlockHash(db ethdb.KeyValueReader) (common.Hash, error) {
	has, err := db.Has(headBlockKey)
	if err != nil {
		return common.Hash{}, err
	}
	if !has {
		return common.Hash{}, nil
	}
	data, err := db.Get(headBlockKey)
	if err != nil {
		return common.Hash{}, err
	}
	if len(data) != common.HashLength {
		return common.Hash{}, fmt.Errorf("head block hash is %d bytes, want %d", len(data), common.HashLength)
	}
	return common.BytesToHash(data), nil
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
