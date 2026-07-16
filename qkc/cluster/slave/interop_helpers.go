// Copyright 2026-2027, QuarkChain.

//go:build interop

package slave

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/internal/testlog"
	"github.com/ethereum/go-ethereum/log"
)

// requirePyquarkchain returns the pyquarkchain root path.
// Skips the test if PYQUARKCHAIN is not set or the directory doesn't exist.
func requirePyquarkchain(t *testing.T) string {
	t.Helper()
	root := os.Getenv("PYQUARKCHAIN")
	if root == "" {
		t.Skip("PYQUARKCHAIN not set")
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("PYQUARKCHAIN directory not found: %v", err)
	}
	return root
}

// requirePython3 skips the test if python3 is not available.
func requirePython3(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
}

// helperScript returns the absolute path to the Python master helper script.
func helperScript(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine source file location")
	}
	return filepath.Join(filepath.Dir(thisFile), "testdata", "py_master_helper.py")
}

// safeBuffer is a goroutine-safe bytes.Buffer.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startPythonHelper launches the Python helper asynchronously and returns the
// command handle, a thread-safe output buffer, and a cancel function. The
// caller must call cancel and wait for the process to exit.
func startPythonHelper(t *testing.T, args ...string) (*exec.Cmd, *safeBuffer, context.CancelFunc) {
	t.Helper()

	requirePython3(t)
	pyRoot := requirePyquarkchain(t)

	script := helperScript(t)
	if _, err := os.Stat(script); err != nil {
		t.Skipf("python helper script not found: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	cmd := exec.CommandContext(ctx, "python3", append([]string{script}, args...)...)
	cmd.Env = append(os.Environ(), "PYQUARKCHAIN="+pyRoot)
	var out safeBuffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	// Let CommandContext handle process termination via os.Kill on timeout.
	// Do not override cmd.Cancel to avoid test hangs if Python ignores SIGINT.

	if err := cmd.Start(); err != nil {
		t.Fatalf("python helper start: %v", err)
	}
	return cmd, &out, cancel
}

// runPythonHelper executes the helper with the given arguments and returns its
// combined output. It fails the test if Python is unavailable or the helper
// exits non-zero.
func runPythonHelper(t *testing.T, args ...string) []byte {
	t.Helper()

	cmd, out, cancel := startPythonHelper(t, args...)
	defer cancel()

	if err := cmd.Wait(); err != nil {
		t.Fatalf("python helper failed: %v\noutput:\n%s", err, out.String())
	}
	return []byte(out.String())
}

// freePort returns an unused TCP port on 127.0.0.1.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// startTestSlave starts a SlaveServer configured with the given identity.
func startTestSlave(t *testing.T, id string, shards []uint32) (*SlaveServer, int) {
	t.Helper()
	port := freePort(t)
	cfg := SlaveConfig{
		ID:              []byte(id),
		FullShardIDList: append([]uint32(nil), shards...),
		Port:            port,
		MaxPayloadSize:  0,
	}
	logger := testlog.Logger(t, log.LvlInfo)
	srv, err := NewSlaveServer(cfg, logger)
	if err != nil {
		t.Fatalf("new slave server: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("start slave server: %v", err)
	}
	t.Cleanup(func() { srv.Stop() })
	return srv, port
}

// shardStr returns a comma-separated string of shard ids.
func shardStr(shards []uint32) string {
	parts := make([]string, len(shards))
	for i, s := range shards {
		parts[i] = strconv.Itoa(int(s))
	}
	return strings.Join(parts, ",")
}
