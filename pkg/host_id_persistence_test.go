// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quicraft

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// lowerHex32 matches a host ID as HostConfig.HostID documents it: 16 random
// bytes hex-encoded, 32 lowercase hex characters.
var lowerHex32 = regexp.MustCompile(`^[0-9a-f]{32}$`)

// newHostIDPersistHost starts a Host on nodeHostDir with the given HostID
// override and returns its ID() after closing it.
func newHostIDPersistHost(t *testing.T, nodeHostDir, hostID string) string {
	t.Helper()
	cfg := testHostConfig(t)
	cfg.NodeHostDir = nodeHostDir
	cfg.HostID = hostID
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost(NodeHostDir=%s, HostID=%q) = %v", nodeHostDir, hostID, err)
	}
	id := h.ID()
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return id
}

// readHostIDFile returns the content and permission bits of dir/host.id.
func readHostIDFile(t *testing.T, dir string) (string, os.FileMode) {
	t.Helper()
	p := filepath.Join(dir, hostIDFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	info, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat %s: %v", p, err)
	}
	return string(data), info.Mode().Perm()
}

// TestHostIDPersistAcrossRestart covers (a) through (d): a generated ID is
// 32 lowercase hex characters stored in host.id with mode 0600, survives a
// restart on the same NodeHostDir, differs on another directory, and a
// HostID override is reported without touching the stored file.
func TestHostIDPersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	// (a) generated ID and its file.
	first := newHostIDPersistHost(t, dir, "")
	if !lowerHex32.MatchString(first) {
		t.Fatalf("ID() = %q, want 32 lowercase hex characters", first)
	}
	stored, perm := readHostIDFile(t, dir)
	if stored != first {
		t.Fatalf("host.id = %q, want ID() %q", stored, first)
	}
	if perm != 0o600 {
		t.Fatalf("host.id mode = %04o, want 0600", perm)
	}

	// (b) restart on the same directory reports the same ID.
	if again := newHostIDPersistHost(t, dir, ""); again != first {
		t.Fatalf("ID() after restart = %q, want %q", again, first)
	}

	// (c) another empty directory gets a different ID.
	other := newHostIDPersistHost(t, t.TempDir(), "")
	if !lowerHex32.MatchString(other) {
		t.Fatalf("ID() on another directory = %q, want 32 lowercase hex characters", other)
	}
	if other == first {
		t.Fatalf("ID() on another directory = %q, same as the first directory", other)
	}

	// (d) an override is reported and host.id is left unchanged.
	const override = "operator-assigned-host-id"
	if got := newHostIDPersistHost(t, dir, override); got != override {
		t.Fatalf("ID() with HostID set = %q, want %q", got, override)
	}
	if after, afterPerm := readHostIDFile(t, dir); after != first || afterPerm != 0o600 {
		t.Fatalf("host.id after override = %q mode %04o, want %q mode 0600", after, afterPerm, first)
	}

	// The stored ID is still the one reported once the override is dropped.
	if again := newHostIDPersistHost(t, dir, ""); again != first {
		t.Fatalf("ID() after override run = %q, want %q", again, first)
	}
}

// TestHostIDPersistMalformedFile covers (e): a host.id that is not 32 hex
// characters is replaced by a new valid ID, and the Host never reports the
// malformed content.
func TestHostIDPersistMalformedFile(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"empty", ""},
		{"short", "0123456789abcdef"},
		{"long", "0123456789abcdef0123456789abcdef00"},
		{"non-hex", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
		{"trailing newline", "0123456789abcdef0123456789abcdef\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := filepath.Join(dir, hostIDFileName)
			if err := os.WriteFile(p, []byte(tc.content), 0o644); err != nil {
				t.Fatalf("write %s: %v", p, err)
			}

			id := newHostIDPersistHost(t, dir, "")
			if id == tc.content {
				t.Fatalf("ID() = %q, the malformed file content", id)
			}
			if !lowerHex32.MatchString(id) {
				t.Fatalf("ID() = %q, want 32 lowercase hex characters", id)
			}
			stored, _ := readHostIDFile(t, dir)
			if stored != id {
				t.Fatalf("host.id = %q, want the regenerated ID %q", stored, id)
			}

			if again := newHostIDPersistHost(t, dir, ""); again != id {
				t.Fatalf("ID() after restart = %q, want the regenerated ID %q", again, id)
			}
		})
	}
}
