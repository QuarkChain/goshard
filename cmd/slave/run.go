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
	// Catch SIGINT/SIGTERM before any resource opens, so a signal that lands
	// during config load or shard boot still funnels into the blocking Stop()
	// below instead of the OS default termination. The watcher restores default
	// signal handling the moment the first signal lands, so a second signal
	// force-quits the process even while boot or the shutdown drain is still
	// in flight.
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-sigCtx.Done()
		stop()
	}()

	log.Info("slave booting", "node_id", ctx.String(nodeIDFlag.Name))
	cfg, err := loadClusterConfig(ctx)
	if err != nil {
		return err
	}
	backend, err := bootSlave(cfg, ctx.String(nodeIDFlag.Name))
	if err != nil {
		return err
	}

	if sigCtx.Err() == nil {
		log.Info("slave running", "node_id", backend.ID, "shards", len(backend.Shards()))
		<-sigCtx.Done()
	}
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
