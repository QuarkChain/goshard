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
	"github.com/urfave/cli/v2"
)

// Flags shared by the slave subcommands. The names match how pyquarkchain's
// cluster.py launches a slave: `slave --cluster_config=<file> --node_id=<id>`.
var (
	clusterConfigFlag = &cli.StringFlag{
		Name:     "cluster_config",
		Usage:    "Path to the pyquarkchain cluster_config.json",
		Required: true,
	}
	nodeIDFlag = &cli.StringFlag{
		Name:  "node_id",
		Usage: "Slave identity (e.g. S0) selecting which shards this process owns",
	}
)

func main() {
	app := flags.NewApp("the goshard slave node")
	app.Name = "slave"
	app.Flags = slices.Concat(
		[]cli.Flag{},
		debug.Flags,
	)
	app.Before = func(ctx *cli.Context) error {
		flags.MigrateGlobalFlags(ctx)
		return debug.Setup(ctx)
	}
	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		return nil
	}
	app.Commands = []*cli.Command{
		configCommand,
		genesisCommand,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
