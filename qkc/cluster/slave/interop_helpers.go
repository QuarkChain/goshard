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

// startTestSlave starts a SlaveComm configured with the given identity.
func startTestSlave(t *testing.T, id string, shards []uint32) (*SlaveComm, int) {
	t.Helper()
	port := freePort(t)
	cfg := SlaveConfig{
		ID:              []byte(id),
		FullShardIDList: append([]uint32(nil), shards...),
		Port:            port,
		MaxPayloadSize:  0,
	}
	logger := testlog.Logger(t, log.LvlInfo)
	srv, err := NewSlaveComm(cfg, logger)
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

// runPythonScript executes a Python script (relative to testdata/) with the
// given arguments and returns its stdout. Stderr is logged via t.Log.
func runPythonScript(t *testing.T, scriptName string, args ...string) []byte {
	t.Helper()

	requirePython3(t)
	pyRoot := requirePyquarkchain(t)

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine source file location")
	}
	script := filepath.Join(filepath.Dir(thisFile), "testdata", scriptName)
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("script not found: %s", script)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "python3", append([]string{script}, args...)...)
	cmd.Env = append(os.Environ(), "PYQUARKCHAIN="+pyRoot)
	var stdout, stderr safeBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("python script %s start: %v", scriptName, err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("python script %s failed: %v\nstderr:\n%s\nstdout:\n%s",
			scriptName, err, stderr.String(), stdout.String())
	}
	if s := stderr.String(); s != "" {
		t.Logf("python script %s stderr:\n%s", scriptName, s)
	}
	return []byte(stdout.String())
}
