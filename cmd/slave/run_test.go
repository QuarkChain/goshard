// Copyright 2026-2027, QuarkChain.

package main

import (
	"bufio"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/internal/reexec"
	"github.com/ethereum/go-ethereum/qkc/config"
)

func TestMain(m *testing.M) {
	reexec.Register("slave-test", main)
	if reexec.Init() {
		return
	}
	os.Exit(m.Run())
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
