// Copyright 2026-2027, QuarkChain.

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	qkcstate "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/quarkchain/replay"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var (
		pyDB          = flag.String("pyquarkchain-db", "", "PyQuarkChain DB root containing shard-*.db directories")
		clusterConfig = flag.String("cluster-config", "", "PyQuarkChain cluster config JSON")
		fullShardKey  = flag.String("full-shard-key", "", "full shard key or full shard id, hex or decimal")
		start         = flag.Uint64("start", 0, "first height to report")
		end           = flag.Uint64("end", 0, "last height to verify")
	)
	flag.Parse()

	if *clusterConfig == "" {
		return errors.New("--cluster-config is required")
	}
	if *fullShardKey == "" {
		return errors.New("--full-shard-key is required")
	}
	if *pyDB == "" {
		return errors.New("--pyquarkchain-db is required")
	}
	if *end < *start {
		return fmt.Errorf("--end (%d) must be >= --start (%d)", *end, *start)
	}
	key, err := parseUint32Flag(*fullShardKey)
	if err != nil {
		return fmt.Errorf("parse --full-shard-key: %w", err)
	}
	clusterConfigPath := *clusterConfig
	if abs, err := filepath.Abs(clusterConfigPath); err == nil {
		clusterConfigPath = abs
	}
	file, err := os.Open(clusterConfigPath)
	if err != nil {
		return err
	}
	defer file.Close()
	config, err := qkcstate.ReadQuarkChainClusterGenesisConfig(file)
	if err != nil {
		return err
	}
	verifier, err := replay.NewVerifier(config, key)
	if err != nil {
		return err
	}

	fetchStart := uint64(0)
	if *start > 0 {
		fetchStart = 1
	}
	source := replay.NewPyDBSource(*pyDB, clusterConfigPath)
	blocks, err := source.FetchMinorBlocks(verifier.FullShardID(), fetchStart, *end)
	if err != nil {
		return err
	}
	for _, block := range blocks {
		result, err := verifier.VerifyBlock(block)
		if err != nil {
			var mismatch *replay.MismatchError
			if errors.As(err, &mismatch) {
				fmt.Fprintf(os.Stderr, "mismatch fullShard=0x%08x height=%d block=%s expected=%s got=%s\n",
					mismatch.Result.FullShardID,
					mismatch.Result.Height,
					mismatch.Result.BlockHash,
					mismatch.Result.ExpectedStateRoot,
					mismatch.Result.GotStateRoot,
				)
			}
			return err
		}
		if block.Height >= *start {
			fmt.Printf("ok fullShard=0x%08x height=%d block=%s root=%s\n",
				result.FullShardID,
				result.Height,
				result.BlockHash,
				result.GotStateRoot,
			)
		}
	}
	return nil
}

func parseUint32Flag(input string) (uint32, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return 0, errors.New("empty integer")
	}
	var value uint64
	var err error
	if strings.HasPrefix(input, "0x") || strings.HasPrefix(input, "0X") {
		value, err = strconv.ParseUint(input[2:], 16, 32)
	} else {
		value, err = strconv.ParseUint(input, 10, 32)
	}
	if err != nil {
		return 0, err
	}
	return uint32(value), nil
}
