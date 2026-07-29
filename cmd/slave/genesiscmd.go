// Copyright 2026-2027, QuarkChain.

package main

import (
	"fmt"
	"io"
	"os"

	"github.com/ethereum/go-ethereum/qkc"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/urfave/cli/v2"
)

var genesisCommand = &cli.Command{
	Name:      "genesis",
	Usage:     "Derive and print the root genesis block and its hash",
	ArgsUsage: " ",
	Flags: []cli.Flag{
		clusterConfigFlag,
	},
	Description: `Derives the cluster's root genesis block purely from QUARKCHAIN.ROOT.GENESIS and
prints its fields and hash. The hash is byte-identical to pyquarkchain's
GenesisManager.create_root_block().header.get_hash().`,
	Action: runGenesis,
}

func runGenesis(ctx *cli.Context) error {
	cfg, err := loadClusterConfig(ctx)
	if err != nil {
		return err
	}
	header, err := qkc.CreateRootBlock(cfg.Quarkchain)
	if err != nil {
		return err
	}
	printRootGenesis(os.Stdout, header)
	return nil
}

func printRootGenesis(out io.Writer, h *types.RootBlockHeader) {
	fmt.Fprintln(out, "root genesis block:")
	fmt.Fprintf(out, "  version:           %d\n", h.Version)
	fmt.Fprintf(out, "  height:            %d\n", h.Number)
	fmt.Fprintf(out, "  timestamp:         %d\n", h.Time)
	fmt.Fprintf(out, "  difficulty:        %d\n", h.Difficulty)
	fmt.Fprintf(out, "  total_difficulty:  %d\n", h.TotalDifficulty)
	fmt.Fprintf(out, "  nonce:             %d\n", h.Nonce)
	fmt.Fprintf(out, "  hash_prev_block:   %s\n", h.ParentHash.Hex())
	fmt.Fprintf(out, "  hash_merkle_root:  %s\n", h.MinorHeaderHash.Hex())
	fmt.Fprintf(out, "  seal_hash:         %s\n", h.SealHash().Hex())
	fmt.Fprintf(out, "  hash:              %s\n", h.Hash().Hex())
}
