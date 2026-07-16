// Copyright 2026-2027, QuarkChain.

package main

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/urfave/cli/v2"
)

var configCommand = &cli.Command{
	Name:      "config",
	Usage:     "Parse, validate, and print a normalized cluster config summary for a slave",
	ArgsUsage: " ",
	Flags: []cli.Flag{
		clusterConfigFlag,
		nodeIDFlag,
	},
	Description: `Loads and validates a pyquarkchain cluster_config.json, resolves the shards owned
by --node_id, prints a normalized summary, and reports "config OK". Exits non-zero
on an invalid config or an unknown node id.`,
	Action: runConfig,
}

func runConfig(ctx *cli.Context) error {
	cfg, err := loadClusterConfig(ctx)
	if err != nil {
		return err
	}
	nodeID := ctx.String(nodeIDFlag.Name)
	if nodeID == "" {
		return fmt.Errorf("--%s is required (e.g. S0)", nodeIDFlag.Name)
	}
	slaveCtx, err := cfg.ResolveSlave(nodeID)
	if err != nil {
		return err
	}
	printConfigSummary(os.Stdout, slaveCtx)
	fmt.Fprintln(os.Stdout, "config OK")
	return nil
}

func printConfigSummary(out io.Writer, c *config.SlaveContext) {
	fmt.Fprintf(out, "slave %s @ %s:%d\n", c.ID, c.Slave.IP, c.Slave.Port)
	fmt.Fprintf(out, "db path root: %s\n", c.DBPathRoot)
	fmt.Fprintf(out, "network id:   %d\n", c.Quarkchain.NetworkID)
	fmt.Fprintf(out, "owns %d shard(s):\n", len(c.Slave.FullShardList))

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  FULL_SHARD_ID\tCHAIN\tSHARD\tCONSENSUS\tBLOCK_TIME\tGENESIS_TIME\tDIFFICULTY\tGAS_LIMIT\tALLOC")
	for _, shard := range c.ShardConfigs() {
		blockTime := "-"
		if shard.ConsensusConfig != nil {
			blockTime = fmt.Sprintf("%ds", shard.ConsensusConfig.TargetBlockTime)
		}
		fmt.Fprintf(tw, "  0x%08x\t%d\t%d/%d\t%s\t%s\t%d\t%d\t%d\t%d\n",
			shard.GetFullShardId(),
			shard.ChainID,
			shard.ShardID, shard.ShardSize,
			shard.ConsensusType,
			blockTime,
			shard.Genesis.Timestamp,
			shard.Genesis.Difficulty,
			shard.Genesis.GasLimit,
			len(shard.Genesis.Alloc),
		)
	}
	tw.Flush()
}
