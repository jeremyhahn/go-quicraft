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

package engine

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// assertRecordedFileSizeMatchesDisk fails unless the recorded FileSize equals
// the size of snapshot.dat inside the recorded snapshot directory.
func assertRecordedFileSizeMatchesDisk(t *testing.T, dir string, recorded uint64) {
	t.Helper()
	fi, err := os.Stat(filepath.Join(dir, snapshotDataFile))
	if err != nil {
		t.Fatalf("stat snapshot.dat: %v", err)
	}
	if uint64(fi.Size()) != recorded {
		t.Fatalf("recorded FileSize = %d, on-disk size = %d", recorded, fi.Size())
	}
}

// setSealed toggles the tenant barrier's sealed state, modelling an operator
// EmergencySeal landing between a snapshot receive and its (asynchronous,
// retried) recovery.
func (b *tenantBarrier) setSealed(v bool) {
	b.mu.Lock()
	b.sealed = v
	b.mu.Unlock()
}

// setTestShardBarrier points the pool at a per-shard barrier resolver, but
// ONLY for a barrier that exists.
//
// A resolver that returns nil is NOT the same deployment as no resolver at
// all: with per-shard barriers configured and no process-wide fallback, an
// unresolved shard fails closed (ErrShardBarrierUnresolved) rather than being
// written in plaintext beside encrypted tenants. These fixtures model the
// plaintext deployment — no resolver, no barrier — so installing a
// nil-returning resolver would make them assert the unresolved-shard error
// instead of whatever the test is actually about.
func setTestShardBarrier(p *snapshotPool, barrier SnapshotBarrier) {
	if barrier == nil {
		return
	}
	p.shardBarriers = func(uint64) SnapshotBarrier { return barrier }
}

// saveEncryptedTestSnapshot saves one snapshot for shard/replica under the
// given barrier and returns the pool that wrote it, so a later pool can be
// pointed at a different (or absent) barrier for the recovery half.
func saveEncryptedTestSnapshot(t *testing.T, baseDir string, ldb *snapshotTestLogDB,
	barrier SnapshotBarrier, shardID, replicaID, state uint64, compression config.CompressionType) {
	t.Helper()
	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	setTestShardBarrier(p, barrier)
	var wg sync.WaitGroup
	p.start(&wg)

	ss := newSnapshotTestSnapshotter(state)
	cfg := config.Config{
		ShardID:                 shardID,
		ReplicaID:               replicaID,
		CompactionOverhead:      50,
		SnapshotCompressionType: compression,
	}
	node := NewNode(nil, nil, nil, cfg, state, ss, baseDir, nil)
	if err := p.requestSnapshot(snapshotRequest{
		shardID: shardID, replicaID: replicaID, node: node, save: true,
	}); err != nil {
		t.Fatalf("save request: %v", err)
	}
	select {
	case res := <-p.CompletedC():
		if res.err != nil {
			t.Fatalf("save failed: %v", res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("save timeout")
	}
}

// recoverTestSnapshot runs one recovery and returns its result error plus the
// state the snapshotter ended up holding.
func recoverTestSnapshot(t *testing.T, baseDir string, ldb *snapshotTestLogDB,
	barrier SnapshotBarrier, shardID, replicaID uint64, compression config.CompressionType) (error, uint64) {
	t.Helper()
	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	setTestShardBarrier(p, barrier)
	var wg sync.WaitGroup
	p.start(&wg)

	ss := newSnapshotTestSnapshotter(0)
	cfg := config.Config{
		ShardID:                 shardID,
		ReplicaID:               replicaID,
		CompactionOverhead:      50,
		SnapshotCompressionType: compression,
	}
	node := NewNode(nil, nil, nil, cfg, 0, ss, baseDir, nil)
	if err := p.requestSnapshot(snapshotRequest{
		shardID: shardID, replicaID: replicaID, node: node, save: false,
	}); err != nil {
		t.Fatalf("recover request: %v", err)
	}
	select {
	case res := <-p.CompletedC():
		return res.err, ss.state.Load()
	case <-time.After(2 * time.Second):
		t.Fatal("recover timeout")
	}
	return nil, 0
}

// TestSnapshotPool_RecoverSnapshot_FailsClosedWithoutBarrier is the
// fail-closed mirror of Host.StartShard. An artifact recorded with Epoch > 0
// whose barrier is absent or sealed used to fall through the decrypt
// condition silently: recoverPath stayed the ENCRYPTED file and the raw
// ciphertext was handed to RecoverFromSnapshot with no error and no log.
// Recovery must fail so the leader re-sends instead.
func TestSnapshotPool_RecoverSnapshot_FailsClosedWithoutBarrier(t *testing.T) {
	cases := []struct {
		name    string
		barrier func(*tenantBarrier) SnapshotBarrier
	}{
		{
			name:    "no barrier configured",
			barrier: func(*tenantBarrier) SnapshotBarrier { return nil },
		},
		{
			name: "sealed barrier",
			barrier: func(b *tenantBarrier) SnapshotBarrier {
				b.setSealed(true)
				return b
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			ldb := newSnapshotTestLogDB()
			b := newTenantBarrier(1, 2)
			saveEncryptedTestSnapshot(t, baseDir, ldb, b, 5, 1, 200, config.NoCompression)

			snap, _ := ldb.GetSnapshot(5, 1)
			if snap.Epoch == 0 {
				t.Fatal("saved snapshot recorded Epoch 0; the fail-closed path is not exercised")
			}

			err, state := recoverTestSnapshot(t, baseDir, ldb, tc.barrier(b), 5, 1, config.NoCompression)
			if !errors.Is(err, ErrSnapshotEncryptedBarrierUnavailable) {
				t.Fatalf("recover error = %v, want ErrSnapshotEncryptedBarrierUnavailable; "+
					"an undecryptable snapshot must not be recovered from ciphertext", err)
			}
			if state != 0 {
				t.Fatalf("state machine state = %d, want 0 (nothing may be recovered)", state)
			}
		})
	}
}

// TestSnapshotPool_RecoverSnapshot_UsesArtifactCompression verifies the reader
// chain is built from the ARTIFACT's recorded compression, not from the
// recovering node's own config. Both directions matter: a node whose config
// says Snappy must still read an uncompressed artifact verbatim, and a node
// whose config says NoCompression must still decompress a Snappy artifact.
func TestSnapshotPool_RecoverSnapshot_UsesArtifactCompression(t *testing.T) {
	cases := []struct {
		name      string
		saved     config.CompressionType
		recoverer config.CompressionType
	}{
		{name: "snappy_artifact_uncompressed_config", saved: config.Snappy, recoverer: config.NoCompression},
		{name: "uncompressed_artifact_snappy_config", saved: config.NoCompression, recoverer: config.Snappy},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			ldb := newSnapshotTestLogDB()
			saveEncryptedTestSnapshot(t, baseDir, ldb, nil, 7, 1, 321, tc.saved)

			snap, _ := ldb.GetSnapshot(7, 1)
			if snap.Compression != tc.saved {
				t.Fatalf("recorded Compression = %v, want %v (the save path must describe the artifact)",
					snap.Compression, tc.saved)
			}

			err, state := recoverTestSnapshot(t, baseDir, ldb, nil, 7, 1, tc.recoverer)
			if err != nil {
				t.Fatalf("recover failed: %v", err)
			}
			if state != 321 {
				t.Fatalf("recovered state = %d, want 321", state)
			}
		})
	}
}

// TestSaveSnapshot_RecordsOnDiskFileSize pins the LogDB record's FileSize to
// the artifact it points at. A zero FileSize beside a non-empty Filepath makes
// the replica stream an empty snapshot once it becomes leader.
func TestSaveSnapshot_RecordsOnDiskFileSize(t *testing.T) {
	baseDir := t.TempDir()
	ldb := newSnapshotTestLogDB()
	saveEncryptedTestSnapshot(t, baseDir, ldb, nil, 11, 1, 400, config.NoCompression)

	snap, err := ldb.GetSnapshot(11, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snap.FileSize == 0 {
		t.Fatal("recorded FileSize = 0 for a non-empty snapshot artifact")
	}
	assertRecordedFileSizeMatchesDisk(t, snap.Filepath, snap.FileSize)
}
