// Copyright 2026-2027, QuarkChain.

package main

import (
	"bufio"
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/internal/reexec"
	"github.com/ethereum/go-ethereum/qkc/config"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	reexec.Register("slave-test", main)
	if reexec.Init() {
		return
	}
	// No ignore list: with metrics disabled (no --metrics flag registered at
	// all) neither geth nor pebble leaves a background goroutine behind after
	// Stop(). When bootSlave wires the real chain, fix its Stop path instead of
	// allowlisting slave-owned goroutines here.
	goleak.VerifyTestMain(m)
}

// TestRunHonorsSignalDuringStartup sends SIGTERM as soon as the run action
// reports its signal handler installed ("slave booting"), while shard boot is
// typically still in flight. Whichever window the signal actually lands in,
// the process must exit 0 through the clean shutdown path and leave a datadir
// that reopens without complaint.
func TestRunHonorsSignalDuringStartup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX signals are not available on windows")
	}
	cfgPath := writeFixtureWithTempDBRoot(t, fixtures[0].path)

	cmd := exec.Command(reexec.Self(), "--cluster_config", cfgPath, "--node_id", "S0")
	cmd.Args[0] = "slave-test"
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	watchdog := time.AfterFunc(30*time.Second, func() { cmd.Process.Kill() })
	defer watchdog.Stop()

	var out strings.Builder
	signalled := false
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		line := scanner.Text()
		out.WriteString(line)
		out.WriteByte('\n')
		if !signalled && strings.Contains(line, "slave booting") {
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatalf("SIGTERM: %v", err)
			}
			signalled = true
		}
	}
	err = cmd.Wait()
	if !signalled {
		t.Fatalf("never saw %q in output:\n%s", "slave booting", out.String())
	}
	if err != nil {
		t.Fatalf("slave exited with %v, want exit 0; output:\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "slave shutting down") {
		t.Errorf("clean shutdown path not taken; output:\n%s", out.String())
	}

	cfg, err := config.LoadClusterConfig(cfgPath)
	if err != nil {
		t.Fatalf("reload config: %v", err)
	}
	backend, err := bootSlave(cfg, "S0")
	if err != nil {
		t.Fatalf("reopen datadir after SIGTERM: %v", err)
	}
	if err := backend.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TODO(real shard chain): retain this binary-level contract with a real QKC
// block 0, and add a case where chain rules change without changing block 0.
// TestRunGenesisMismatchExitsLoudly initializes a datadir from the mainnet
// config, then reruns the slave against the same datadir with the devnet
// config: the run must exit 1 and say which genesis is stored, which one the
// config derives, and which db holds the stored one.
func TestRunGenesisMismatchExitsLoudly(t *testing.T) {
	dbRoot := t.TempDir()
	initDataDir(t, fixtures[0].path, dbRoot)
	devnetCfg := writeFixtureWithDBRoot(t, fixtures[1].path, dbRoot)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, reexec.Self(), "--cluster_config", devnetCfg, "--node_id", "S0")
	cmd.Args[0] = "slave-test"
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("slave exited with %v, want exit 1; output:\n%s", err, out)
	}
	for _, want := range []string{
		"stored genesis 0x",
		"does not match config genesis 0x",
		"cluster config changed since initialization",
		filepath.Join(dbRoot, "shard-0x00000001"),
	} {
		if !strings.Contains(string(out), want) {
			t.Errorf("mismatch report missing %q:\n%s", want, out)
		}
	}
}
