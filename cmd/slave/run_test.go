// Copyright 2026-2027, QuarkChain.

package main

import (
	"bufio"
	"encoding/json"
	"os"
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

// writeFixtureWithDBRoot copies a fixture with its DB_PATH_ROOT redirected to
// dbRoot and returns the rewritten config's path.
func writeFixtureWithDBRoot(t *testing.T, path, dbRoot string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	doc["DB_PATH_ROOT"], _ = json.Marshal(dbRoot)
	rewritten, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	tmpPath := filepath.Join(t.TempDir(), "cluster_config.json")
	if err := os.WriteFile(tmpPath, rewritten, 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return tmpPath
}

// writeFixtureWithTempDBRoot is writeFixtureWithDBRoot into a fresh t.TempDir(),
// so booting from it never writes into the repo tree.
func writeFixtureWithTempDBRoot(t *testing.T, path string) string {
	t.Helper()
	return writeFixtureWithDBRoot(t, path, t.TempDir())
}

func loadFixtureWithTempDBRoot(t *testing.T, path string) *config.ClusterConfig {
	t.Helper()
	cfg, err := config.LoadClusterConfig(writeFixtureWithTempDBRoot(t, path))
	if err != nil {
		t.Fatalf("load rewritten fixture: %v", err)
	}
	return cfg
}

// TestBootSlave drives the default action's boot pipeline end to end from each
// real network config: resolve S0, derive the root genesis, start both owned
// shards, stop, and boot again from the same datadir.
func TestBootSlave(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			cfg := loadFixtureWithTempDBRoot(t, f.path)

			backend, err := bootSlave(cfg, "S0")
			if err != nil {
				t.Fatalf("bootSlave: %v", err)
			}
			if got := len(backend.Shards()); got != 2 {
				t.Errorf("booted %d shards, want 2", got)
			}
			if err := backend.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}

			backend, err = bootSlave(cfg, "S0")
			if err != nil {
				t.Fatalf("bootSlave(reopen): %v", err)
			}
			if err := backend.Stop(); err != nil {
				t.Fatalf("Stop(reopen): %v", err)
			}
		})
	}
}

func TestBootSlaveRejectsBadNodeID(t *testing.T) {
	cfg := loadFixtureWithTempDBRoot(t, fixtures[0].path)
	if _, err := bootSlave(cfg, ""); err == nil || !strings.Contains(err.Error(), "--node_id is required") {
		t.Errorf("bootSlave(\"\") err = %v, want required-flag error", err)
	}
	if _, err := bootSlave(cfg, "S9"); err == nil || !strings.Contains(err.Error(), `unknown node id "S9"`) {
		t.Errorf("bootSlave(S9) err = %v, want unknown node id", err)
	}
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
