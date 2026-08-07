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
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestImportSnapshot_ReachesDEKPurgeFloorAndGC is the regression guard for the
// import-path key-destruction bug.
//
// ImportSnapshot used to hand-roll its on-disk path as
// <NodeHostDir>/snapshots/shard-N/replica-M/snapshot-..., but the shard's
// canonical BASE directory already ends in shard-N and the engine appends
// shard-N/replica-M/snapshot-... to it. The import therefore landed one level
// ABOVE the canonical tree, invisible to listSnapshotDirs. Two consequences,
// both serious:
//
//  1. The DEK purge floor is computed by SCANNING snapshot directories, not
//     from LogDB records. An invisible snapshot contributes no floor, so
//     PurgeEpochsBefore legitimately drops the DEK the imported snapshot was
//     re-encrypted under and StartShard then fails closed on an undecryptable
//     artifact — for a disaster-recovery restore, total loss.
//  2. Snapshot GC never enumerates the directory, so every import leaks.
//
// The assertion is on the FLOOR, not on the path string: the floor is produced
// by the very scan (listSnapshotDirs + readSnapshotEpoch) that GC and epoch
// purging use, so it proves the artifact is genuinely reachable rather than
// merely well-named.
func TestImportSnapshot_ReachesDEKPurgeFloorAndGC(t *testing.T) {
	tests := []struct {
		name    string
		dataDir bool
	}{
		{name: "default NodeHostDir layout"},
		{name: "per-shard DataDir layout", dataDir: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const shardID, replicaID uint64 = 7, 1
			payload := []byte("imported disaster-recovery state")

			cfg := testHostConfig(t)
			barrier := createTestBarrier(t)
			db := newMockLogDB()
			db.mu.Lock()
			db.nodeInfos = []logdb.NodeInfo{{ShardID: shardID, ReplicaID: replicaID}}
			db.mu.Unlock()

			h, err := NewHost(cfg, WithLogDB(db), WithBarrier(barrier), WithoutTransport())
			if err != nil {
				t.Fatalf("NewHost failed: %v", err)
			}
			defer h.Close()

			shardCfg := testShardConfig(shardID, replicaID)
			if tt.dataDir {
				shardCfg.DataDir = t.TempDir()
			}
			// The shard was never started on this host, so there is no
			// bootstrap record to discover the layout from: the caller has to
			// state it, and ImportSnapshot fails closed if it does not.
			opts := []ImportOption{WithImportDataDir(shardCfg.DataDir)}

			artifact := buildSnapshotArtifact(t, 900, 5, 0, proto.Membership{},
				snapshotEnvelope(payload))
			if impErr := h.ImportSnapshot(context.Background(), shardID,
				bytes.NewReader(artifact), opts...); impErr != nil {
				t.Fatalf("ImportSnapshot failed: %v", impErr)
			}

			imported, getErr := db.GetSnapshot(shardID, replicaID)
			if getErr != nil {
				t.Fatalf("GetSnapshot failed: %v", getErr)
			}
			// A barrier is configured, so the artifact was re-encrypted at rest
			// and its epoch is exactly what must survive a DEK purge.
			if imported.Epoch == 0 {
				t.Fatal("imported snapshot Epoch = 0; the at-rest re-encryption did not happen, " +
					"so this test cannot observe the purge floor it is guarding")
			}

			// The path must be the SAME construction every other snapshot
			// writer uses, honouring the shard's DataDir — including the
			// directory name, which is the snapshot's own index (900 here).
			base := shardSnapshotBaseDir(cfg.NodeHostDir, shardCfg.DataDir, shardID)
			wantDir := engine.SnapshotSaveDir(base, shardID, replicaID, 900)
			if imported.Filepath != wantDir {
				t.Fatalf("imported snapshot Filepath = %q, want the canonical %q",
					imported.Filepath, wantDir)
			}

			// Start the shard so the engine has a node to enumerate, then ask
			// for the floor the DEK purge would use.
			memSM := &recordingMemSM{want: payload}
			create := func(_, _ uint64) interface{} { return memSM }
			if startErr := h.StartShard(map[uint64]string{replicaID: "127.0.0.1:5000"},
				false, create, shardCfg); startErr != nil {
				t.Fatalf("StartShard after import failed: %v", startErr)
			}
			if !memSM.recoverCalled.Load() {
				t.Fatal("RecoverFromSnapshot was not called: the import was not recovered")
			}

			// The engine's node must root its snapshot tree at the same base the
			// import wrote into — this is what makes GC enumerate the directory.
			node := h.engine.GetNode(shardID)
			if node == nil {
				t.Fatal("engine node missing after StartShard")
			}
			if node.SnapshotDir() != base {
				t.Fatalf("engine node SnapshotDir = %q, import base = %q: the import "+
					"is outside the tree GC and the purge-floor scan enumerate",
					node.SnapshotDir(), base)
			}

			floor, ok := h.engine.GlobalSafePurgeFloor()
			if !ok {
				t.Fatal("GlobalSafePurgeFloor reported no floor although an encrypted " +
					"imported snapshot is retained: its DEK would be purged and the " +
					"restored state permanently undecryptable")
			}
			if floor != imported.Epoch {
				t.Fatalf("purge floor = %d, want %d (the imported snapshot's epoch)",
					floor, imported.Epoch)
			}
		})
	}
}

// TestImportSnapshot_WritesEpochMetadata pins the other half of the same
// failure: the purge-floor scan reads the epoch from snapshot.meta, and a
// MISSING metadata file is read as "no snapshot here, imposes no floor". An
// import that writes no metadata is therefore just as invisible to the purge
// floor as one written to the wrong directory, even when the path is correct.
func TestImportSnapshot_WritesEpochMetadata(t *testing.T) {
	const shardID, replicaID uint64 = 8, 1

	cfg := testHostConfig(t)
	barrier := createTestBarrier(t)
	db := newMockLogDB()
	db.mu.Lock()
	db.nodeInfos = []logdb.NodeInfo{{ShardID: shardID, ReplicaID: replicaID}}
	db.mu.Unlock()

	h, err := NewHost(cfg, WithLogDB(db), WithBarrier(barrier), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	artifact := buildSnapshotArtifact(t, 123, 4, 0, proto.Membership{},
		snapshotEnvelope([]byte("payload")))
	if impErr := h.ImportSnapshot(context.Background(), shardID,
		bytes.NewReader(artifact), WithImportDataDir("")); impErr != nil {
		t.Fatalf("ImportSnapshot failed: %v", impErr)
	}

	imported, getErr := db.GetSnapshot(shardID, replicaID)
	if getErr != nil {
		t.Fatalf("GetSnapshot failed: %v", getErr)
	}

	epoch, index, term := readSnapshotMetaFields(t, imported.Filepath)
	if epoch != imported.Epoch {
		t.Fatalf("snapshot.meta epoch = %d, want %d (the LogDB record's epoch); "+
			"a mismatched or absent epoch lets PurgeEpochsBefore destroy the DEK",
			epoch, imported.Epoch)
	}
	if index != 123 {
		t.Errorf("snapshot.meta index = %d, want 123", index)
	}
	if term != 4 {
		t.Errorf("snapshot.meta term = %d, want 4", term)
	}
}

// readSnapshotMetaFields reads the epoch, index and term out of the canonical
// snapshot.meta layout written beside a snapshot artifact.
func readSnapshotMetaFields(t *testing.T, dir string) (epoch, index, term uint64) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "snapshot.meta"))
	if err != nil {
		t.Fatalf("snapshot.meta unreadable in %s: %v (the purge-floor scan reads "+
			"a missing file as \"imposes no floor\")", dir, err)
	}
	meta, parseErr := engine.ParseSnapshotMeta(data)
	if parseErr != nil {
		t.Fatalf("snapshot.meta in %s is not a record the purge-floor scan can read: %v",
			dir, parseErr)
	}
	if !meta.EpochRecorded {
		t.Fatalf("snapshot.meta in %s does not carry the epoch field the purge-floor "+
			"scan reads", dir)
	}
	return meta.Epoch, meta.Index, meta.Term
}

// H3. The DataDir layout must be DISCOVERED, not declared.
//
// WithImportDataDir was documented as required whenever Config.DataDir is
// non-empty, but nothing validated it and ImportSnapshot could not detect the
// omission. Omitting it wrote the artifact under NodeHostDir while StartShard
// pointed the shard's SnapshotDir at DataDir/shard-N, so listSnapshotDirs
// never enumerated it: the artifact contributed no floor to the DEK purge and
// the next PurgeEpochsBefore destroyed the key it needed. That is exactly the
// bug TestImportSnapshot_ReachesDEKPurgeFloorAndGC covers, still live, behind
// a silent caller opt-in.
//
// StartShard now records Config.DataDir in the shard's LogDB bootstrap record,
// so an import of a shard this host has started resolves the layout itself.
func TestImportSnapshot_DiscoversDataDirFromBootstrapRecord(t *testing.T) {
	const shardID, replicaID uint64 = 41, 1
	dataDir := t.TempDir()

	cfg := testHostConfig(t)
	db := newMockLogDB()
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	shardCfg := testShardConfig(shardID, replicaID)
	shardCfg.DataDir = dataDir
	if startErr := h.StartShard(map[uint64]string{replicaID: "127.0.0.1:5000"},
		false, testCreateFunc, shardCfg); startErr != nil {
		t.Fatalf("StartShard failed: %v", startErr)
	}
	if stopErr := h.StopShard(shardID); stopErr != nil {
		t.Fatalf("StopShard failed: %v", stopErr)
	}
	// The mock LogDB does not derive ListNodeInfo from its records.
	db.mu.Lock()
	db.nodeInfos = []logdb.NodeInfo{{ShardID: shardID, ReplicaID: replicaID}}
	db.mu.Unlock()

	bs, ok, bsErr := db.GetBootstrap(shardID, replicaID)
	if bsErr != nil || !ok {
		t.Fatalf("GetBootstrap: ok=%v err=%v", ok, bsErr)
	}
	if bs.DataDir != dataDir {
		t.Fatalf("StartShard recorded DataDir %q, want %q: without it the "+
			"import cannot discover the shard's snapshot tree", bs.DataDir, dataDir)
	}

	// NO WithImportDataDir: the layout must come from the record.
	artifact := buildSnapshotArtifact(t, 900, 5, 0, proto.Membership{},
		snapshotEnvelope([]byte("imported disaster-recovery state")))
	if impErr := h.ImportSnapshot(context.Background(), shardID,
		bytes.NewReader(artifact)); impErr != nil {
		t.Fatalf("ImportSnapshot failed: %v", impErr)
	}

	imported, getErr := db.GetSnapshot(shardID, replicaID)
	if getErr != nil {
		t.Fatalf("GetSnapshot failed: %v", getErr)
	}
	base := shardSnapshotBaseDir(cfg.NodeHostDir, dataDir, shardID)
	if rel, relErr := filepath.Rel(base, imported.Filepath); relErr != nil ||
		rel == ".." || len(rel) > 2 && rel[:3] == ".."+string(filepath.Separator) {
		t.Fatalf("imported snapshot landed at %q, outside the shard's snapshot "+
			"tree %q: snapshot GC and the DEK purge-floor scan never enumerate "+
			"it, so the next PurgeEpochsBefore destroys its key",
			imported.Filepath, base)
	}
}

// A WithImportDataDir that contradicts the recorded value is refused rather
// than silently resolved: one of the two is wrong, the import cannot tell
// which, and choosing wrongly costs the artifact's DEK.
func TestImportSnapshot_RejectsDataDirContradictingTheRecord(t *testing.T) {
	const shardID, replicaID uint64 = 42, 1
	dataDir := t.TempDir()

	cfg := testHostConfig(t)
	db := newMockLogDB()
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	shardCfg := testShardConfig(shardID, replicaID)
	shardCfg.DataDir = dataDir
	if startErr := h.StartShard(map[uint64]string{replicaID: "127.0.0.1:5000"},
		false, testCreateFunc, shardCfg); startErr != nil {
		t.Fatalf("StartShard failed: %v", startErr)
	}
	if stopErr := h.StopShard(shardID); stopErr != nil {
		t.Fatalf("StopShard failed: %v", stopErr)
	}
	db.mu.Lock()
	db.nodeInfos = []logdb.NodeInfo{{ShardID: shardID, ReplicaID: replicaID}}
	db.mu.Unlock()

	artifact := buildSnapshotArtifact(t, 900, 5, 0, proto.Membership{},
		snapshotEnvelope([]byte("payload")))

	// The omission the old contract could not detect: no DataDir declared for
	// a DataDir shard. It must no longer land under NodeHostDir.
	if impErr := h.ImportSnapshot(context.Background(), shardID,
		bytes.NewReader(artifact), WithImportDataDir("")); !errorsIsImportMismatch(impErr) {
		t.Fatalf("declaring no DataDir for a DataDir shard: err = %v, want "+
			"ErrImportDataDirMismatch", impErr)
	}

	// A different DataDir is equally refused.
	if impErr := h.ImportSnapshot(context.Background(), shardID,
		bytes.NewReader(artifact), WithImportDataDir(t.TempDir())); !errorsIsImportMismatch(impErr) {
		t.Fatalf("declaring the wrong DataDir: err = %v, want ErrImportDataDirMismatch", impErr)
	}

	// Nothing was written for either refusal.
	if _, getErr := db.GetSnapshot(shardID, replicaID); getErr == nil {
		t.Fatal("a refused import still recorded a snapshot in LogDB")
	}
}

// A shard with NO bootstrap record (started with join=true, or restored onto a
// host that has never run it) cannot have its layout discovered. The import
// must fail closed rather than assume NodeHostDir: assuming wrongly is silent
// and destroys the DEK.
func TestImportSnapshot_FailsClosedWhenLayoutIsUnknown(t *testing.T) {
	const shardID, replicaID uint64 = 43, 1
	cfg := testHostConfig(t)
	db := newMockLogDB()
	db.mu.Lock()
	db.nodeInfos = []logdb.NodeInfo{{ShardID: shardID, ReplicaID: replicaID}}
	db.mu.Unlock()

	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	artifact := buildSnapshotArtifact(t, 900, 5, 0, proto.Membership{},
		snapshotEnvelope([]byte("payload")))
	impErr := h.ImportSnapshot(context.Background(), shardID, bytes.NewReader(artifact))
	if !errorsIsImportUnknown(impErr) {
		t.Fatalf("import with no recorded layout: err = %v, want "+
			"ErrImportLayoutUnknown; assuming NodeHostDir here writes the "+
			"artifact outside a DataDir shard's tree and the next "+
			"PurgeEpochsBefore destroys its DEK", impErr)
	}
	if _, getErr := db.GetSnapshot(shardID, replicaID); getErr == nil {
		t.Fatal("a refused import still recorded a snapshot in LogDB")
	}

	// An explicit assertion unblocks it.
	if impErr := h.ImportSnapshot(context.Background(), shardID,
		bytes.NewReader(artifact), WithImportDataDir("")); impErr != nil {
		t.Fatalf("import with an explicit layout assertion failed: %v", impErr)
	}
}

// errorsIsImportMismatch and errorsIsImportUnknown keep the assertions above
// readable while still matching on the sentinel rather than on message text.
func errorsIsImportMismatch(err error) bool {
	return errors.Is(err, ErrImportDataDirMismatch)
}

func errorsIsImportUnknown(err error) bool {
	return errors.Is(err, ErrImportLayoutUnknown)
}
