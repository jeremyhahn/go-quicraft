// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build unix || windows

package quicraft

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	childHostWALDirEnv  = "QUICRAFT_CHILD_HOST_WALDIR"
	childHostNodeDirEnv = "QUICRAFT_CHILD_HOST_NODEHOSTDIR"
)

// TestNewHost_ChildOpener is the second opener of
// TestNewHost_RefusedToAnotherProcess, run in a re-executed test binary. It
// does nothing in an ordinary run. Its exit status reports the outcome: 0
// started, 3 ErrDirLocked, 4 any other error.
func TestNewHost_ChildOpener(t *testing.T) {
	walDir, nodeDir := os.Getenv(childHostWALDirEnv), os.Getenv(childHostNodeDirEnv)
	if walDir == "" || nodeDir == "" {
		t.Skip("child opener; run by TestNewHost_RefusedToAnotherProcess")
	}
	cfg := testHostConfig(t)
	cfg.WALDir, cfg.NodeHostDir = walDir, nodeDir
	h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
	switch {
	case errors.Is(err, ErrDirLocked):
		os.Exit(3)
	case err != nil:
		os.Exit(4)
	}
	if err := h.Close(); err != nil {
		os.Exit(4)
	}
	os.Exit(0)
}

func newDirLockTestHost(t *testing.T, walDir, nodeDir string, deploymentID uint64) (*Host, error) {
	t.Helper()
	cfg := testHostConfig(t)
	cfg.WALDir, cfg.NodeHostDir, cfg.DeploymentID = walDir, nodeDir, deploymentID
	return NewHost(cfg, WithoutTransport(), WithoutEncryption())
}

func mustCloseHost(t *testing.T, h *Host) {
	t.Helper()
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestNewHost_SecondOpenerRefusedInProcess verifies a live host's
// directories cannot be opened by a second host in the same process, by
// NodeHostDir or by WALDir, and can be once the first is closed.
func TestNewHost_SecondOpenerRefusedInProcess(t *testing.T) {
	walDir, nodeDir := t.TempDir(), t.TempDir()
	h, err := newDirLockTestHost(t, walDir, nodeDir, 1)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}

	_, err = newDirLockTestHost(t, t.TempDir(), nodeDir, 1)
	if !errors.Is(err, ErrDirLocked) {
		h.Close()
		t.Fatalf("second host over the same NodeHostDir = %v, want ErrDirLocked", err)
	}
	if want := filepath.Join(nodeDir, hostLockFileName); !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not name %s", err, want)
	}
	_, err = newDirLockTestHost(t, walDir, t.TempDir(), 1)
	if !errors.Is(err, ErrDirLocked) {
		h.Close()
		t.Fatalf("second host over the same WALDir = %v, want ErrDirLocked", err)
	}

	mustCloseHost(t, h)
	h2, err := newDirLockTestHost(t, walDir, nodeDir, 1)
	if err != nil {
		t.Fatalf("NewHost after Close: %v", err)
	}
	mustCloseHost(t, h2)
}

// TestNewHost_RefusedToAnotherProcess verifies the lock holds across
// processes: a re-executed test binary cannot open a live host's
// directories, and can once the host is closed.
func TestNewHost_RefusedToAnotherProcess(t *testing.T) {
	walDir, nodeDir := t.TempDir(), t.TempDir()
	h, err := newDirLockTestHost(t, walDir, nodeDir, 1)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	child := func() int {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestNewHost_ChildOpener$")
		cmd.Env = append(os.Environ(), childHostWALDirEnv+"="+walDir, childHostNodeDirEnv+"="+nodeDir)
		err := cmd.Run()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode()
		}
		if err != nil {
			t.Fatalf("running the child: %v", err)
		}
		return 0
	}
	if code := child(); code != 3 {
		h.Close()
		t.Fatalf("child opener exit status = %d while the host is live, want 3 (ErrDirLocked)", code)
	}
	mustCloseHost(t, h)
	if code := child(); code != 0 {
		t.Fatalf("child opener exit status = %d after Close, want 0", code)
	}
}

// TestNewHost_FailedStartReleasesDirectories verifies a start that fails
// after the WAL is open releases both directories: here the host ID file
// cannot be written because a directory occupies its name.
func TestNewHost_FailedStartReleasesDirectories(t *testing.T) {
	walDir, nodeDir := t.TempDir(), t.TempDir()
	idPath := filepath.Join(nodeDir, hostIDFileName)
	if err := os.Mkdir(idPath, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := newDirLockTestHost(t, walDir, nodeDir, 1); err == nil {
		t.Fatal("NewHost succeeded with a directory at the host ID path")
	}
	if err := os.Remove(idPath); err != nil {
		t.Fatalf("remove: %v", err)
	}
	h, err := newDirLockTestHost(t, walDir, nodeDir, 1)
	if err != nil {
		t.Fatalf("NewHost after a failed start: %v", err)
	}
	mustCloseHost(t, h)
}

// TestNewHost_IdentityMismatchRefused verifies a NodeHostDir first started
// under one DeploymentID refuses another, naming both, releases its lock on
// the refusal, and starts again under its own.
func TestNewHost_IdentityMismatchRefused(t *testing.T) {
	walDir, nodeDir := t.TempDir(), t.TempDir()
	h, err := newDirLockTestHost(t, walDir, nodeDir, 41)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	mustCloseHost(t, h)

	_, err = newDirLockTestHost(t, walDir, nodeDir, 42)
	if !errors.Is(err, ErrHostIdentityMismatch) {
		t.Fatalf("NewHost under another DeploymentID = %v, want ErrHostIdentityMismatch", err)
	}
	for _, v := range []uint64{41, 42} {
		if !strings.Contains(err.Error(), strconv.FormatUint(v, 10)) {
			t.Errorf("error %q does not name deployment ID %d", err, v)
		}
	}

	h, err = newDirLockTestHost(t, walDir, nodeDir, 41)
	if err != nil {
		t.Fatalf("NewHost under the recorded DeploymentID: %v", err)
	}
	mustCloseHost(t, h)
}

// TestNewHost_IdentityRestarts verifies a restart under the same identity
// succeeds, and that a changed RaftAddress is not refused: a member may
// restart at a new address.
func TestNewHost_IdentityRestarts(t *testing.T) {
	walDir, nodeDir := t.TempDir(), t.TempDir()
	for i, addr := range []string{"127.0.0.1:5000", "127.0.0.1:5000", "127.0.0.1:5001"} {
		cfg := testHostConfig(t)
		cfg.WALDir, cfg.NodeHostDir = walDir, nodeDir
		cfg.RaftAddress, cfg.ListenAddress = addr, addr
		h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
		if err != nil {
			t.Fatalf("start %d at %s: %v", i, addr, err)
		}
		mustCloseHost(t, h)
	}
	data, err := os.ReadFile(filepath.Join(nodeDir, hostIdentityFileName))
	if err != nil {
		t.Fatalf("read identity: %v", err)
	}
	id, err := parseHostIdentity(data)
	if err != nil {
		t.Fatalf("parse identity: %v", err)
	}
	if id.deploymentID != 1 || id.raftAddress != "127.0.0.1:5000" {
		t.Errorf("identity = %+v, want deployment 1 at the first address", id)
	}
}

// TestNewHost_MalformedIdentityRefused verifies an identity file that does
// not parse fails the start rather than being replaced.
func TestNewHost_MalformedIdentityRefused(t *testing.T) {
	walDir, nodeDir := t.TempDir(), t.TempDir()
	h, err := newDirLockTestHost(t, walDir, nodeDir, 1)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	mustCloseHost(t, h)
	path := filepath.Join(nodeDir, hostIdentityFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read identity: %v", err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, hostIDFilePerms); err != nil {
		t.Fatalf("write identity: %v", err)
	}
	if _, err := newDirLockTestHost(t, walDir, nodeDir, 1); !errors.Is(err, errHostIdentityMalformed) {
		t.Fatalf("NewHost over a corrupt identity file = %v, want errHostIdentityMalformed", err)
	}
}

func TestHostIdentity_RoundTrip(t *testing.T) {
	for _, want := range []hostIdentity{
		{deploymentID: 0, raftAddress: ""},
		{deploymentID: 1<<64 - 1, raftAddress: "[::1]:63000"},
	} {
		buf, err := marshalHostIdentity(want)
		if err != nil {
			t.Fatalf("marshal %+v: %v", want, err)
		}
		got, err := parseHostIdentity(buf)
		if err != nil || got != want {
			t.Fatalf("round trip %+v = %+v, %v", want, got, err)
		}
		for _, bad := range [][]byte{buf[:len(buf)-1], append(append([]byte{}, buf...), 0), []byte("QRHOSTIX" + string(buf[8:]))} {
			if _, err := parseHostIdentity(bad); !errors.Is(err, errHostIdentityMalformed) {
				t.Errorf("parse of a damaged record = %v, want errHostIdentityMalformed", err)
			}
		}
	}
	if _, err := marshalHostIdentity(hostIdentity{raftAddress: strings.Repeat("a", 1<<16)}); err == nil {
		t.Error("marshal of an oversized address succeeded")
	}
}
