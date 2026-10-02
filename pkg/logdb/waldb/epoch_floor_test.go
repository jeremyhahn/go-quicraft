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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// shardEpochState reads a shard's epoch floor and per-segment epoch map the
// way the lock-free readers do.
func shardEpochState(s *walShard) (minEpoch uint64, segEpochs map[uint64]uint64) {
	s.dataMu.RLock()
	defer s.dataMu.RUnlock()
	segEpochs = make(map[uint64]uint64, len(s.segmentEpochs))
	for id, e := range s.segmentEpochs {
		segEpochs[id] = e
	}
	return s.minWriteEpoch, segEpochs
}

// rotateBarrierTo advances the test barrier to the requested write epoch.
func rotateBarrierTo(t *testing.T, db *DB, want uint64) {
	t.Helper()
	type rotator interface {
		Rotate(ctx context.Context) (uint64, error)
	}
	r, ok := db.barrier.(rotator)
	if !ok {
		t.Fatalf("barrier %T does not support Rotate", db.barrier)
	}
	for i := uint64(1); i < want; i++ {
		if _, err := r.Rotate(context.Background()); err != nil {
			t.Fatalf("barrier.Rotate: %v", err)
		}
	}
}

// TestSaveState_RolledBackBatchDoesNotLowerEpochFloor pins the epoch floor to
// DURABLE records only.
//
// Applying the floor (minWriteEpoch / segmentEpochs) inside writeRecord, in
// SaveState's Phase 1 — before the fsync and before the batch is known to
// commit — would leave a rolled-back batch's epoch behind permanently:
// rollbackShard drops segmentEpochs only for the intermediate segments it
// unlinks, and never restores minWriteEpoch or recomputes the checkpoint
// segment's entry. MinLiveEpoch would then report a floor pinned to an epoch
// no live record was ever encrypted under, and the engine could never purge
// that DEK.
func TestSaveState_RolledBackBatchDoesNotLowerEpochFloor(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()
	db, err := Open(dir,
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const shardID, replicaID = uint64(1), uint64(1)
	shard := db.getShard(shardID)

	// Materialize the segment WITHOUT committing a record, so the shard's
	// epoch floor is still 0 and any epoch observed afterwards can only have
	// come from the batch below.
	shard.mu.Lock()
	ensErr := shard.ensureActiveSegment()
	shard.mu.Unlock()
	if ensErr != nil {
		t.Fatalf("ensureActiveSegment: %v", ensErr)
	}

	// Fail the first block flush. Small records live entirely in the block
	// buffer and never flush, so the first update below lands successfully and
	// only the second — large enough to span blocks — fails. That is the shape
	// the property needs: a record that WAS written (and whose epoch an eager
	// floor update would apply on the spot) inside a batch that then rolls back.
	simulated := errors.New("simulated EIO on block flush")
	shard.mu.Lock()
	realFile := shard.activeSegment.f
	shard.activeSegment.f = &failingWriteAtFile{
		File: realFile, writeErr: simulated, failAfter: 0,
	}
	shard.mu.Unlock()

	err = db.SaveState([]logdb.Update{
		{
			ShardID:   shardID,
			ReplicaID: replicaID,
			State:     logdb.State{Term: 1, Vote: 1, Commit: 1},
			Entries:   makeEntries(1, 3, 1),
		},
		{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Entries: []logdb.Entry{
				{Index: 4, Term: 1, Cmd: bytes.Repeat([]byte("x"), 8*1024)},
			},
		},
	})
	if !errors.Is(err, simulated) {
		t.Fatalf("SaveState = %v, want the injected write error", err)
	}

	shard.mu.Lock()
	shard.activeSegment.f = realFile
	shard.mu.Unlock()

	minEpoch, segEpochs := shardEpochState(shard)
	if minEpoch != 0 {
		t.Fatalf("minWriteEpoch = %d after a rolled-back batch, want 0: the "+
			"floor was applied to an ATTEMPTED write that never committed", minEpoch)
	}
	if len(segEpochs) != 0 {
		t.Fatalf("segmentEpochs = %v after a rolled-back batch, want empty", segEpochs)
	}
	if epoch, err := db.MinLiveEpoch(); err != nil || epoch != 0 {
		t.Fatalf("MinLiveEpoch = %d, %v after a rolled-back batch; want 0, nil", epoch, err)
	}

	// The symptom in full: rotate the barrier and commit a real batch. Every
	// record that exists is now epoch 2, so the floor must be exactly 2. With
	// the rolled-back batch's epoch still registered, the floor stays pinned
	// at 1 and epoch 1's DEK can never be purged.
	rotateBarrierTo(t, db, 2)
	if err := db.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		State:     logdb.State{Term: 2, Vote: 1, Commit: 1},
		Entries:   makeEntries(1, 3, 2),
	}}); err != nil {
		t.Fatalf("SaveState after rollback: %v", err)
	}
	epoch, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch: %v", err)
	}
	if epoch != 2 {
		t.Fatalf("MinLiveEpoch = %d, want 2: only epoch-2 records exist, but the "+
			"rolled-back batch left epoch %d pinned forever", epoch, epoch)
	}
}

// TestEpochFloorTracksCommittedWrites is the counterweight to
// TestSaveState_RolledBackBatchDoesNotLowerEpochFloor: deferring the epoch
// update to each writer's durability point must not stop tracking epochs.
// Every path that writes a WAL record is covered, because each applies the
// floor at its own commit point.
func TestEpochFloorTracksCommittedWrites(t *testing.T) {
	const shardID, replicaID = uint64(1), uint64(1)

	// Each case runs against a fresh DB whose barrier has been rotated to
	// epoch 3 BEFORE any write, so the observed floor can only come from the
	// records the operation itself wrote.
	cases := []struct {
		name string
		op   func(*testing.T, *DB)
	}{
		{
			name: "SaveState entries state and snapshot",
			op: func(t *testing.T, db *DB) {
				if err := db.SaveState([]logdb.Update{{
					ShardID:   shardID,
					ReplicaID: replicaID,
					State:     logdb.State{Term: 3, Vote: 1, Commit: 2},
					Entries:   makeEntries(1, 4, 3),
					Snapshot:  logdb.Snapshot{Index: 2, Term: 3},
				}}); err != nil {
					t.Fatalf("SaveState: %v", err)
				}
			},
		},
		{
			name: "SaveSnapshot",
			op: func(t *testing.T, db *DB) {
				if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{Index: 7, Term: 3}); err != nil {
					t.Fatalf("SaveSnapshot: %v", err)
				}
			},
		},
		{
			name: "SaveBootstrap",
			op: func(t *testing.T, db *DB) {
				if err := db.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
					Addresses: map[uint64]string{replicaID: "node1:9000"},
				}); err != nil {
					t.Fatalf("SaveBootstrap: %v", err)
				}
			},
		},
		{
			name: "RemoveEntriesTo",
			op: func(t *testing.T, db *DB) {
				if err := db.RemoveEntriesTo(shardID, replicaID, 5); err != nil {
					t.Fatalf("RemoveEntriesTo: %v", err)
				}
			},
		},
		// RemoveNodeData is absent: it writes no WAL record. The removal is
		// kept in the retired-replica registry beside the WAL, which is not
		// encrypted under any epoch and needs no DEK.
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			barrier := newTestBarrier(t)
			db, err := Open(t.TempDir(),
				WithBlockSize(1024),
				WithMaxFileSize(64*1024),
				WithBarrier(barrier),
			)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer db.Close()

			rotateBarrierTo(t, db, 3)
			if epoch, err := db.MinLiveEpoch(); err != nil || epoch != 0 {
				t.Fatalf("MinLiveEpoch before the write = %d, %v; want 0, nil", epoch, err)
			}

			tc.op(t, db)

			epoch, err := db.MinLiveEpoch()
			if err != nil {
				t.Fatalf("MinLiveEpoch: %v", err)
			}
			if epoch != 3 {
				t.Fatalf("MinLiveEpoch = %d after a committed write, want 3: the "+
					"durable record's epoch was never folded into the floor, so its "+
					"DEK can be purged out from under recovery", epoch)
			}

			shard := db.getShard(shardID)
			shard.mu.Lock()
			activeSegID := shard.activeSegmentID
			shard.mu.Unlock()
			_, segEpochs := shardEpochState(shard)
			if got, ok := segEpochs[activeSegID]; !ok || got != 3 {
				t.Fatalf("segmentEpochs[%d] = %d, present=%v; want 3, true",
					activeSegID, got, ok)
			}
		})
	}
}

// TestEpochFloorSurvivesRestartAfterRollback verifies the floor the writers
// maintain in memory agrees with what recovery derives from the ciphertext
// headers on disk, which is the ground truth. A rolled-back batch leaves
// nothing on disk, so replay must land on the same floor the live shard
// reports.
func TestEpochFloorSurvivesRestartAfterRollback(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()
	db, err := Open(dir,
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const shardID, replicaID = uint64(1), uint64(1)
	shard := db.getShard(shardID)

	shard.mu.Lock()
	ensErr := shard.ensureActiveSegment()
	shard.mu.Unlock()
	if ensErr != nil {
		t.Fatalf("ensureActiveSegment: %v", ensErr)
	}

	simulated := errors.New("simulated EIO on block flush")
	shard.mu.Lock()
	realFile := shard.activeSegment.f
	shard.activeSegment.f = &failingWriteAtFile{
		File: realFile, writeErr: simulated, failAfter: 0,
	}
	shard.mu.Unlock()

	if err := db.SaveState([]logdb.Update{
		{
			ShardID:   shardID,
			ReplicaID: replicaID,
			State:     logdb.State{Term: 1, Vote: 1, Commit: 1},
			Entries:   makeEntries(1, 3, 1),
		},
		{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Entries: []logdb.Entry{
				{Index: 4, Term: 1, Cmd: bytes.Repeat([]byte("x"), 8*1024)},
			},
		},
	}); !errors.Is(err, simulated) {
		t.Fatalf("SaveState = %v, want the injected write error", err)
	}

	shard.mu.Lock()
	shard.activeSegment.f = realFile
	shard.mu.Unlock()

	rotateBarrierTo(t, db, 2)
	if err := db.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		State:     logdb.State{Term: 2, Vote: 1, Commit: 1},
		Entries:   makeEntries(1, 3, 2),
	}}); err != nil {
		t.Fatalf("SaveState after rollback: %v", err)
	}

	liveEpoch, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(dir,
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	recoveredEpoch, err := reopened.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch after reopen: %v", err)
	}
	if recoveredEpoch != liveEpoch {
		t.Fatalf("MinLiveEpoch = %d live but %d after replay: the in-memory floor "+
			"disagrees with the ciphertext headers on disk", liveEpoch, recoveredEpoch)
	}
	if recoveredEpoch != 2 {
		t.Fatalf("MinLiveEpoch after replay = %d, want 2", recoveredEpoch)
	}
}
