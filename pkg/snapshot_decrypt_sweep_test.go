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

package quicraft

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
)

// plaintextMarker is the state-machine content a crash-orphaned decrypt scratch
// file would expose in the clear.
var plaintextMarker = []byte("SECRET-STATE-MACHINE-PLAINTEXT")

// orphanDecryptTemp writes a scratch file with the given prefix into dir,
// simulating a process killed before it could remove its own temp file. It is
// deliberately NOT created through the in-process decrypt path: the bug being
// guarded is that nothing reclaims a file whose owner never returns.
func orphanDecryptTemp(t *testing.T, dir, prefix string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dir, err)
	}
	f, err := os.CreateTemp(dir, prefix+"*"+engine.DecryptTempSuffix)
	if err != nil {
		t.Fatalf("CreateTemp in %s: %v", dir, err)
	}
	if _, err := f.Write(plaintextMarker); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close orphan: %v", err)
	}
	return f.Name()
}

// TestStartShard_ReclaimsCrashOrphanedSendDecryptPlaintext is the regression
// guard for the send-path plaintext leak.
//
// transportSender.decryptSnapshotForSend decrypts the ENTIRE snapshot into a
// scratch file created as a SIBLING of the encrypted snapshot.dat. A leader
// killed mid-send (SIGKILL/OOM/power loss) never runs the deferred removal, so
// a COMPLETE PLAINTEXT COPY of the state machine is left next to the
// ciphertext. Snapshot directories are retained indefinitely, so the copy
// survives restarts and backups — while an operator spot-checking snapshot.dat
// still sees ciphertext.
//
// The send scratch file previously used a prefix the orphan sweeper did not
// match, and no other cleaner existed. This asserts a crash-ORPHANED file (not
// one whose in-process owner removes it) is reclaimed at the next StartShard.
func TestStartShard_ReclaimsCrashOrphanedSendDecryptPlaintext(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	const shardID, replicaID uint64 = 4, 2
	base := shardSnapshotBaseDir(cfg.NodeHostDir, "", shardID)

	// Two retained snapshots. The leader was streaming the OLDER one when it
	// died, which is exactly the case a sweep scoped to the newest recovered
	// directory misses.
	oldDir := engine.SnapshotSaveDir(base, shardID, replicaID, 100)
	newDir := engine.SnapshotSaveDir(base, shardID, replicaID, 200)

	sendOrphan := orphanDecryptTemp(t, oldDir, engine.SendDecryptTempPrefix)
	recoverOrphan := orphanDecryptTemp(t, newDir, engine.RecoverDecryptTempPrefix)

	// Canonical artifacts that must survive the sweep untouched.
	keep := []string{
		filepath.Join(oldDir, "snapshot.dat"),
		filepath.Join(oldDir, "snapshot.meta"),
		filepath.Join(newDir, "snapshot.dat"),
	}
	for _, p := range keep {
		if writeErr := os.WriteFile(p, []byte("ciphertext"), 0o600); writeErr != nil {
			t.Fatalf("write %s: %v", p, writeErr)
		}
	}

	if startErr := h.StartShard(map[uint64]string{replicaID: "127.0.0.1:5000"},
		false, testCreateFunc, testShardConfig(shardID, replicaID)); startErr != nil {
		t.Fatalf("StartShard failed: %v", startErr)
	}

	for _, orphan := range []string{sendOrphan, recoverOrphan} {
		data, readErr := os.ReadFile(orphan)
		if readErr == nil {
			t.Fatalf("crash-orphaned decrypt scratch file %s survived StartShard "+
				"and still contains %q: plaintext state machine left on disk",
				filepath.Base(orphan), data)
		}
		if !os.IsNotExist(readErr) {
			t.Fatalf("unexpected error stating %s: %v", orphan, readErr)
		}
	}

	for _, p := range keep {
		if _, statErr := os.Stat(p); statErr != nil {
			t.Fatalf("canonical snapshot file %s was destroyed by the sweep: %v", p, statErr)
		}
	}
}

// TestSendDecryptTempPatternIsSwept is the structural guard: whatever pattern
// the send path decrypts into must be a pattern the sweeper matches. It fails
// the moment someone introduces a new, unswept scratch-file name — which is how
// the leak was introduced in the first place.
func TestSendDecryptTempPatternIsSwept(t *testing.T) {
	for _, pattern := range []string{sendDecryptTempPattern, recoverDecryptTempPattern} {
		// os.CreateTemp substitutes the random component for the last "*".
		name := strings.Replace(pattern, "*", "123456789", 1)
		if !engine.IsSnapshotDecryptTemp(name) {
			t.Fatalf("decrypt scratch pattern %q produces %q, which the orphan "+
				"sweeper does not match: a crash leaves plaintext on disk forever",
				pattern, name)
		}
	}
}
