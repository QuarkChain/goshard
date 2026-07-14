// Copyright 2026-2027, QuarkChain.

// Command slave is the goshard slave node. It boots from a pyquarkchain-compatible
// cluster_config.json and hosts the shards assigned to one slave identity.
package main

import (
	"fmt"
	"os"
	"slices"

	"github.com/ethereum/go-ethereum/internal/debug"
	"github.com/ethereum/go-ethereum/internal/flags"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/urfave/cli/v2"
)

// Flags shared by the default run action and the subcommands. The names match how
// pyquarkchain's cluster.py launches a slave:
// `slave --cluster_config=<file> --node_id=<id>`.
//
// Neither flag is marked Required even though the actions insist on it: they are
// registered both globally (for the default action) and on the subcommands, and
// cli enforces a required global flag even when the value is passed after the
// subcommand name. The actions validate presence instead.
var (
	clusterConfigFlag = &cli.StringFlag{
		Name:  "cluster_config",
		Usage: "Path to the pyquarkchain cluster_config.json",
	}
	nodeIDFlag = &cli.StringFlag{
		Name:  "node_id",
		Usage: "Slave identity (e.g. S0) selecting which shards this process owns",
	}
)

func main() {
	app := flags.NewApp("the goshard slave node")
	app.Name = "slave"
	app.Flags = slices.Concat([]cli.Flag{clusterConfigFlag, nodeIDFlag}, debug.Flags)
	app.Before = func(ctx *cli.Context) error {
		flags.MigrateGlobalFlags(ctx)
		return debug.Setup(ctx)
	}
	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		return nil
	}
	app.Action = runSlave
	app.Commands = []*cli.Command{
		configCommand,
		genesisCommand,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// loadClusterConfig loads and validates the config named by --cluster_config,
// insisting the flag is set.
func loadClusterConfig(ctx *cli.Context) (*config.ClusterConfig, error) {
	path := ctx.String(clusterConfigFlag.Name)
	if path == "" {
		return nil, fmt.Errorf("--%s is required", clusterConfigFlag.Name)
	}
	return config.LoadClusterConfig(path)
}
