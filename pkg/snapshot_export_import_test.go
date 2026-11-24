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
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

// These tests drive the operator's backup-and-restore path end to end with a
// real state machine: entries are applied through raft, a snapshot is taken,
// exported to a file, and imported into a Host that has never seen the data.
// The artifact framing itself is covered on synthetic payloads in
// snapshot_artifact_trailer_test.go; here the claim is about STATE — the
// restored replica must hash to exactly what the source replica held.

const exportImportShardID = 1

var exportImportMembers = map[uint64]string{1: "127.0.0.1:5000"}

// exportShardWithEntries starts a KV shard on a fresh Host, applies n puts,
// snapshots it, and exports the snapshot to a file in a temp directory. It
// returns the file path and the source replica's state hash, taken at the
// snapshot's applied index.
func exportShardWithEntries(t *testing.T, n int) (string, uint64) {
	t.Helper()
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost (source): %v", err)
	}
	defer func() { _ = h.Close() }()
	if startErr := h.StartShard(exportImportMembers, false, kv.NewMemoryCreateFunc(),
		testShardConfig(exportImportShardID, 1)); startErr != nil {
		t.Fatalf("StartShard (source): %v", startErr)
	}
	waitForLeader(t, h, exportImportShardID)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := range n {
		cmd, encErr := kv.EncodePut([]byte(fmt.Sprintf("key-%04d", i)),
			[]byte(fmt.Sprintf("value-%04d-exported-state", i)))
		if encErr != nil {
			t.Fatalf("EncodePut: %v", encErr)
		}
		if _, propErr := h.SyncPropose(ctx, exportImportShardID, cmd); propErr != nil {
			t.Fatalf("SyncPropose[%d]: %v", i, propErr)
		}
	}
	snapIndex, err := h.SyncRequestSnapshot(ctx, exportImportShardID, SnapshotOption{})
	if err != nil {
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}
	hash, applied, ok, err := h.StateMachineHash(exportImportShardID)
	if err != nil || !ok {
		t.Fatalf("StateMachineHash (source) = ok %v, err %v", ok, err)
	}
	if applied != snapIndex {
		t.Fatalf("source applied index %d moved past the snapshot at %d", applied, snapIndex)
	}

	path := filepath.Join(t.TempDir(), "shard-1.snapshot")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create export file: %v", err)
	}
	if expErr := h.ExportSnapshot(ctx, exportImportShardID, f); expErr != nil {
		_ = f.Close()
		t.Fatalf("ExportSnapshot: %v", expErr)
	}
	if closeErr := f.Close(); closeErr != nil {
		t.Fatalf("close export file: %v", closeErr)
	}
	return path, hash
}

// freshImportHost returns a Host with its own new directories on which the
// shard has been started once (so its layout is recorded) and stopped, the
// state an operator restores into.
func freshImportHost(t *testing.T) (*Host, config.HostConfig) {
	t.Helper()
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost (target): %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if startErr := h.StartShard(exportImportMembers, false, kv.NewMemoryCreateFunc(),
		testShardConfig(exportImportShardID, 1)); startErr != nil {
		t.Fatalf("StartShard (target): %v", startErr)
	}
	waitForLeader(t, h, exportImportShardID)
	if stopErr := h.StopShard(exportImportShardID); stopErr != nil {
		t.Fatalf("StopShard (target): %v", stopErr)
	}
	return h, cfg
}

// treeDigest maps every path under the roots to its kind and, for regular
// files, a digest of the contents, so two calls compare byte for byte.
func treeDigest(t *testing.T, roots ...string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, root := range roots {
		walkErr := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.Type().IsRegular() {
				out[p] = d.Type().String()
				return nil
			}
			b, readErr := os.ReadFile(p)
			if readErr != nil {
				return readErr
			}
			out[p] = fmt.Sprintf("%x", sha256.Sum256(b))
			return nil
		})
		if walkErr != nil {
			t.Fatalf("walk %s: %v", root, walkErr)
		}
	}
	return out
}

func TestExportImport_FreshHostSameStateHash(t *testing.T) {
	path, srcHash := exportShardWithEntries(t, 64)

	empty, ok := kv.NewMemoryCreateFunc()(exportImportShardID, 1).(interface{ GetHash() (uint64, error) })
	if !ok {
		t.Fatal("the KV memory store does not implement GetHash")
	}
	emptyHash, err := empty.GetHash()
	if err != nil {
		t.Fatalf("GetHash (empty): %v", err)
	}
	if srcHash == emptyHash {
		t.Fatal("source state hashes like an empty store; the test would prove nothing")
	}

	h, _ := freshImportHost(t)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open export file: %v", err)
	}
	defer func() { _ = f.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if importErr := h.ImportSnapshot(ctx, exportImportShardID, f); importErr != nil {
		t.Fatalf("ImportSnapshot: %v", importErr)
	}
	if startErr := h.StartShard(exportImportMembers, false, kv.NewMemoryCreateFunc(),
		testShardConfig(exportImportShardID, 1)); startErr != nil {
		t.Fatalf("StartShard after import: %v", startErr)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		got, _, hashOK, hashErr := h.StateMachineHash(exportImportShardID)
		if hashErr == nil && hashOK && got == srcHash {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("imported replica state hash = %#x (ok %v, err %v), want the source's %#x",
				got, hashOK, hashErr, srcHash)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestExportImport_FlippedByteRefusedTargetUnchanged(t *testing.T) {
	path, _ := exportShardWithEntries(t, 64)
	artifact, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read export file: %v", err)
	}
	// The middle of the file is payload: 64 KV pairs dwarf the header and
	// the fixed-size trailer.
	artifact[len(artifact)/2] ^= 0xFF
	if writeErr := os.WriteFile(path, artifact, 0o600); writeErr != nil {
		t.Fatalf("rewrite export file: %v", writeErr)
	}

	h, cfg := freshImportHost(t)
	before := treeDigest(t, cfg.NodeHostDir, cfg.WALDir)

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open export file: %v", err)
	}
	defer func() { _ = f.Close() }()
	importErr := h.ImportSnapshot(context.Background(), exportImportShardID, f)
	var artErr *SnapshotArtifactError
	if !errors.As(importErr, &artErr) {
		t.Fatalf("ImportSnapshot of a flipped byte = %v (%T), want *SnapshotArtifactError", importErr, importErr)
	}
	if !errors.Is(importErr, ErrSnapshotCorrupt) {
		t.Fatalf("ImportSnapshot = %v, want it to match ErrSnapshotCorrupt", importErr)
	}

	after := treeDigest(t, cfg.NodeHostDir, cfg.WALDir)
	for p, d := range before {
		if after[p] != d {
			t.Errorf("refused import changed %s: %q -> %q", p, d, after[p])
		}
	}
	for p := range after {
		if _, had := before[p]; !had {
			t.Errorf("refused import left %s behind", p)
		}
	}
}
