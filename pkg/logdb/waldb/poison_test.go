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

package waldb

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// failingSyncFile wraps a File and fails Sync after failAfter successful
// calls, simulating an fsync failure (e.g. EIO) mid-life.
type failingSyncFile struct {
	File
	syncErr   error
	failAfter int
	calls     int
}

func (f *failingSyncFile) Sync() error {
	f.calls++
	if f.calls > f.failAfter {
		return f.syncErr
	}
	return f.File.Sync()
}

// TestFsyncFailurePoisonsDB verifies the fail-stop contract: when the
// sync phase of a batched write fails, the DB is poisoned — the failing
// write returns ErrDBPoisoned (with the underlying cause preserved) and
// every subsequent write is refused with ErrDBPoisoned. Retrying after a
// failed fdatasync has undefined durability semantics (fsyncgate), so a
// retryable error here would be a correctness bug.
func TestFsyncFailurePoisonsDB(t *testing.T) {
	dir := t.TempDir()
	// NoSync must be OFF so the sync phase actually runs.
	db, err := Open(dir, WithBlockSize(4096), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// First write succeeds and creates the active segment for the shard.
	if err := db.SaveEntries(1, 1, makeEntries(1, 1, 1)); err != nil {
		t.Fatalf("initial SaveEntries: %v", err)
	}

	// Inject an fsync failure into the shard's active segment.
	simulated := errors.New("simulated EIO on fdatasync")
	shard := db.getShard(1)
	shard.mu.Lock()
	shard.activeSegment.f = &failingSyncFile{
		File:    shard.activeSegment.f,
		syncErr: simulated,
	}
	shard.mu.Unlock()

	// The write whose sync phase fails must poison the DB and surface
	// both the fail-stop sentinel and the underlying cause.
	err = db.SaveEntries(1, 1, makeEntries(2, 1, 1))
	if err == nil {
		t.Fatal("expected error from SaveEntries with failing fsync")
	}
	if !errors.Is(err, ErrDBPoisoned) {
		t.Fatalf("err = %v, want ErrDBPoisoned in chain", err)
	}
	if !errors.Is(err, simulated) {
		t.Fatalf("err = %v, want underlying cause in chain", err)
	}
	if !db.poisoned.Load() {
		t.Fatal("DB must be marked poisoned after fsync failure")
	}

	// ALL subsequent writes must be refused — including ones that touch
	// other (healthy) shards, because durability of the whole DB can no
	// longer be reasoned about.
	if err := db.SaveEntries(2, 1, makeEntries(1, 1, 1)); !errors.Is(err, ErrDBPoisoned) {
		t.Fatalf("SaveEntries on other shard = %v, want ErrDBPoisoned", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 1}); !errors.Is(err, ErrDBPoisoned) {
		t.Fatalf("SaveNodeState = %v, want ErrDBPoisoned", err)
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 1, Term: 1}); !errors.Is(err, ErrDBPoisoned) {
		t.Fatalf("SaveSnapshot = %v, want ErrDBPoisoned", err)
	}
	if err := db.SaveBootstrap(1, 1, logdb.Bootstrap{}); !errors.Is(err, ErrDBPoisoned) {
		t.Fatalf("SaveBootstrap = %v, want ErrDBPoisoned", err)
	}
	if err := db.RemoveNodeData(1, 1); !errors.Is(err, ErrDBPoisoned) {
		t.Fatalf("RemoveNodeData = %v, want ErrDBPoisoned", err)
	}

	// Reads of the pre-poison committed state remain served.
	entries, err := db.GetEntries(1, 1, 1, 2, 0)
	if err != nil {
		t.Fatalf("GetEntries after poison: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("GetEntries returned %d entries, want 1", len(entries))
	}
}

// TestNonBatchFsyncFailurePoisonsDB verifies the same fail-stop applies
// to the non-batched write path (writeRecord's inline sync), exercised
// here via SaveSnapshot.
func TestNonBatchFsyncFailurePoisonsDB(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithBlockSize(4096), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 1, Term: 1}); err != nil {
		t.Fatalf("initial SaveSnapshot: %v", err)
	}

	simulated := errors.New("simulated EIO on fdatasync")
	shard := db.getShard(1)
	shard.mu.Lock()
	shard.activeSegment.f = &failingSyncFile{
		File:    shard.activeSegment.f,
		syncErr: simulated,
	}
	shard.mu.Unlock()

	err = db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 2, Term: 1})
	if !errors.Is(err, simulated) {
		t.Fatalf("SaveSnapshot = %v, want simulated sync error", err)
	}
	if !db.poisoned.Load() {
		t.Fatal("DB must be poisoned after non-batch fsync failure")
	}
	if err := db.SaveSnapshot(2, 1, logdb.Snapshot{Index: 1, Term: 1}); !errors.Is(err, ErrDBPoisoned) {
		t.Fatalf("subsequent SaveSnapshot = %v, want ErrDBPoisoned", err)
	}
}
