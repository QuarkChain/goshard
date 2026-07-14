// Copyright 2026-2027, QuarkChain.

package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/genesis"
	"github.com/ethereum/go-ethereum/qkc/shard"
	"github.com/ethereum/go-ethereum/qkc/slave"
	"github.com/urfave/cli/v2"
)

// runSlave is the default action: boot every shard owned by --node_id and run
// until interrupted, then shut down cleanly.
func runSlave(ctx *cli.Context) error {
	cfg, err := loadClusterConfig(ctx)
	if err != nil {
		return err
	}
	backend, err := bootSlave(cfg, ctx.String(nodeIDFlag.Name))
	if err != nil {
		return err
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("slave running", "node_id", backend.ID, "shards", len(backend.Shards()))
	<-sigCtx.Done()
	// Restore default signal handling before the blocking shutdown, so a second
	// signal force-quits the process instead of waiting.
	stop()
	log.Info("slave shutting down", "node_id", backend.ID)
	return backend.Stop()
}

// bootSlave narrows the cluster config to the slave identified by nodeID, derives
// the root genesis, and boots every owned shard.
func bootSlave(cfg *config.ClusterConfig, nodeID string) (*slave.SlaveBackend, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("--%s is required (e.g. S0)", nodeIDFlag.Name)
	}
	slaveCtx, err := cfg.ResolveSlave(nodeID)
	if err != nil {
		return nil, err
	}
	root, err := genesis.RootBlock(cfg.Quarkchain)
	if err != nil {
		return nil, err
	}
	return slave.New(slaveCtx, root, shard.Options{})
}
