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
	"io/fs"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// shardOpenFailFS fails OpenFile for segment files under one specific WAL shard
// directory once armed, simulating an I/O error that aborts a SaveState batch
// in Phase 1 (segment creation) and forces a rollback of the sibling shards
// already written in the same batch. It yields the scheduler a few times before
// returning the error to widen the window during which a concurrent committer
// could land on the shared WAL shard between the failing batch's checkpoint
// capture and its rollback.
type shardOpenFailFS struct {
	FS
	failDir string // e.g. "shard-1"
	armed   atomic.Bool
}

func (f *shardOpenFailFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	if f.armed.Load() && filepath.Base(filepath.Dir(name)) == f.failDir {
		for i := 0; i < 4; i++ {
			runtime.Gosched()
		}
		return nil, errors.New("injected segment open failure")
	}
	return f.FS.OpenFile(name, flag, perm)
}

// TestSaveStateConcurrentRollbackPreservesCommitted is a regression test.
//
// Two Raft shards (0 and 16) hash to the SAME WAL shard (0). When NumWorkers
// does not divide DefaultShardCount they are served by different commit workers,
// so their SaveState calls run concurrently against the one shared WAL segment.
// Writer B's batch writes the shared WAL shard 0 and THEN a second WAL shard
// whose segment open is rigged to fail, forcing a Phase-1 failure and a rollback
// that truncates WAL shard 0 back to B's pre-batch checkpoint.
//
// If SaveState released the shard lock between its durable write and its
// rollback, writer A could commit a record to WAL shard 0 inside that window
// and B's absolute-offset rollback would silently erase A's already
// acknowledged committed record. SaveState holds the WAL-shard lock across the
// whole batch lifecycle, making each batch atomic per segment. This test asserts
// that every record A observed as committed survives a reopen + WAL replay.
func TestSaveStateConcurrentRollbackPreservesCommitted(t *testing.T) {
	dir := t.TempDir()
	ff := &shardOpenFailFS{FS: defaultFS(), failDir: shardDirName(1)}
	// Real fsync (no WithNoSync) so committed records are durable and the
	// rollback truncation operates on on-disk bytes that replay must recover.
	db, err := Open(dir, WithFS(ff), WithBlockSize(4096), WithMaxFileSize(256*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const (
		raftA    = uint64(0)  // WAL shard 0
		raftB    = uint64(16) // WAL shard 0 (collides with raftA)
		raftFail = uint64(1)  // WAL shard 1 (B's batch fails here -> rolls back WAL 0)
		iters    = 400
	)

	// Prime WAL shard 0 so its segment exists before the fault is armed, and
	// establish A's first committed entry.
	if err := db.SaveEntries(raftA, 1, makeEntries(1, 1, 1)); err != nil {
		t.Fatalf("prime: %v", err)
	}
	ff.armed.Store(true)

	var committedA atomic.Uint64
	committedA.Store(1)
	var wg sync.WaitGroup
	wg.Add(2)

	// Writer A: commit a growing, contiguous entry log to raftA on WAL shard 0.
	go func() {
		defer wg.Done()
		for i := uint64(2); i <= iters+1; i++ {
			if err := db.SaveEntries(raftA, 1, makeEntries(i, 1, 1)); err == nil {
				committedA.Store(i)
			}
		}
	}()

	// Writer B: each batch writes raftB (WAL shard 0) then raftFail (WAL shard
	// 1, whose segment open fails), forcing a Phase-1 failure and a rollback of
	// WAL shard 0. Every B batch is expected to fail; none of B's records commit.
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= iters; i++ {
			upd := []logdb.Update{
				{ShardID: raftB, ReplicaID: 1, Entries: makeEntries(i, 1, 1)},
				{ShardID: raftFail, ReplicaID: 1, Entries: makeEntries(i, 1, 1)},
			}
			if err := db.SaveState(upd); err == nil {
				t.Errorf("writer B batch %d unexpectedly succeeded; fault not injected", i)
				return
			}
		}
	}()
	wg.Wait()

	want := committedA.Load()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen WITHOUT the fault and replay from the WAL. Every entry A observed
	// as committed must still be present and contiguous; a truncation of the
	// shared segment below A's tail shows up as missing/gapped entries here.
	db2, err := Open(dir, WithBlockSize(4096), WithMaxFileSize(256*1024))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	got, err := db2.GetEntries(raftA, 1, 1, want+1, 0)
	if err != nil {
		t.Fatalf("GetEntries after reopen: %v", err)
	}
	if uint64(len(got)) != want {
		t.Fatalf("raftA committed %d entries but replay recovered %d: a concurrent rollback truncated committed data",
			want, len(got))
	}
	for i, e := range got {
		if e.Index != uint64(i+1) {
			t.Fatalf("entry at position %d has index %d, want %d: gap from rollback truncation", i, e.Index, i+1)
		}
	}
}
