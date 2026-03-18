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

package proto

import (
	"errors"
	"testing"
)

func TestUpdateCommitMarshalUnmarshalRoundTrip(t *testing.T) {
	uc := UpdateCommit{
		StableLogTo:      100,
		StableLogTerm:    5,
		StableSnapshotTo: 50,
		Processed:        true,
		LastApplied:      99,
		ReadyToRead:      3,
	}
	buf := make([]byte, uc.Size())
	n, err := uc.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != updateCommitSize {
		t.Fatalf("MarshalTo: wrote %d, want %d", n, updateCommitSize)
	}

	var uc2 UpdateCommit
	consumed, err := uc2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != updateCommitSize {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, updateCommitSize)
	}
	assertUpdateCommitEqual(t, &uc, &uc2)
}

func TestUpdateCommitMarshalUnmarshalZero(t *testing.T) {
	uc := UpdateCommit{}
	buf := make([]byte, uc.Size())
	uc.MarshalTo(buf)
	var uc2 UpdateCommit
	uc2.UnmarshalFrom(buf)
	if uc2.StableLogTo != 0 || uc2.StableLogTerm != 0 || uc2.StableSnapshotTo != 0 ||
		uc2.Processed || uc2.LastApplied != 0 || uc2.ReadyToRead != 0 {
		t.Fatal("Zero UpdateCommit round-trip failed")
	}
}

func TestUpdateCommitSize(t *testing.T) {
	uc := UpdateCommit{}
	if uc.Size() != 48 {
		t.Fatalf("Size: got %d, want 48", uc.Size())
	}
}

func TestUpdateCommitBufferTooSmallMarshal(t *testing.T) {
	uc := UpdateCommit{StableLogTo: 1}
	buf := make([]byte, updateCommitSize-1)
	_, err := uc.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo: got %v, want ErrBufferTooSmall", err)
	}
}

func TestUpdateCommitBufferTooSmallUnmarshal(t *testing.T) {
	buf := make([]byte, updateCommitSize-1)
	var uc UpdateCommit
	_, err := uc.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestUpdateCommitReset(t *testing.T) {
	uc := UpdateCommit{
		StableLogTo:      100,
		StableLogTerm:    5,
		StableSnapshotTo: 50,
		Processed:        true,
		LastApplied:      99,
		ReadyToRead:      3,
	}
	uc.Reset()
	if uc.StableLogTo != 0 || uc.StableLogTerm != 0 || uc.StableSnapshotTo != 0 ||
		uc.Processed || uc.LastApplied != 0 || uc.ReadyToRead != 0 {
		t.Fatal("Reset did not clear all fields")
	}
}

func TestUpdateReset(t *testing.T) {
	u := Update{
		ShardID:          1,
		ReplicaID:        2,
		State:            State{Term: 5, Vote: 3, Commit: 100},
		FirstIndex:       10,
		LastIndex:        20,
		EntriesToSave:    []Entry{{Term: 5, Index: 10}},
		CommittedEntries: []Entry{{Term: 5, Index: 9}},
		Messages:         []Message{{Type: Heartbeat}},
		Snapshot:         Snapshot{Index: 50, Term: 4},
		ReadyToRead:      []ReadyToRead{{Key: 2, Index: 100}},
		UpdateCommit:     UpdateCommit{StableLogTo: 20, Processed: true},
		LastApplied:      9,
	}
	u.Reset()
	if u.ShardID != 0 || u.ReplicaID != 0 || !u.State.IsEmpty() ||
		u.FirstIndex != 0 || u.LastIndex != 0 ||
		u.EntriesToSave != nil || u.CommittedEntries != nil ||
		u.Messages != nil || !u.Snapshot.IsEmpty() ||
		u.ReadyToRead != nil || u.LastApplied != 0 {
		t.Fatal("Update.Reset did not clear all fields")
	}
}

func TestUpdateCommitProcessedBoolRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		processed bool
	}{
		{"false", false},
		{"true", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := UpdateCommit{Processed: tt.processed}
			buf := make([]byte, uc.Size())
			uc.MarshalTo(buf)
			var uc2 UpdateCommit
			uc2.UnmarshalFrom(buf)
			if uc2.Processed != tt.processed {
				t.Fatalf("Processed: got %v, want %v", uc2.Processed, tt.processed)
			}
		})
	}
}

func TestUpdate_ReleaseCommittedEntries_ReturnsPooledSlice(t *testing.T) {
	pooled := GetEntrySlice()
	*pooled = append(*pooled,
		Entry{Term: 1, Index: 1, Cmd: []byte("cmd1")},
		Entry{Term: 1, Index: 2, Cmd: []byte("cmd2")},
	)
	u := Update{
		CommittedEntries: *pooled,
	}
	u.SetCommittedEntriesPooled(pooled)

	// Release should nil out CommittedEntries and return the slice.
	u.ReleaseCommittedEntries()
	if u.CommittedEntries != nil {
		t.Fatal("CommittedEntries should be nil after release")
	}
	if u.committedEntriesPooled != nil {
		t.Fatal("committedEntriesPooled should be nil after release")
	}

	// Verify the pool returned the slice by getting a new one.
	sp := GetEntrySlice()
	defer PutEntrySlice(sp)
	if len(*sp) != 0 {
		t.Fatalf("expected empty pooled slice, got length %d", len(*sp))
	}
}

func TestUpdate_ReleaseCommittedEntries_NoopWithoutPool(t *testing.T) {
	entries := []Entry{
		{Term: 1, Index: 1, Cmd: []byte("cmd1")},
	}
	u := Update{
		CommittedEntries: entries,
	}
	// No SetCommittedEntriesPooled call. Release should be a no-op.
	u.ReleaseCommittedEntries()
	if u.CommittedEntries == nil {
		t.Fatal("CommittedEntries should not be nil when no pool was set")
	}
}

func TestUpdate_ReleaseCommittedEntries_Idempotent(t *testing.T) {
	pooled := GetEntrySlice()
	*pooled = append(*pooled, Entry{Term: 1, Index: 1})
	u := Update{
		CommittedEntries: *pooled,
	}
	u.SetCommittedEntriesPooled(pooled)

	// First release returns the slice.
	u.ReleaseCommittedEntries()
	// Second release is a no-op (must not panic or double-free).
	u.ReleaseCommittedEntries()
}

func TestUpdate_ResetReleasesPooledEntries(t *testing.T) {
	pooled := GetEntrySlice()
	*pooled = append(*pooled,
		Entry{Term: 3, Index: 10, Cmd: []byte("data")},
	)
	u := Update{
		ShardID:          1,
		CommittedEntries: *pooled,
	}
	u.SetCommittedEntriesPooled(pooled)

	u.Reset()
	if u.CommittedEntries != nil {
		t.Fatal("CommittedEntries should be nil after Reset")
	}
	if u.committedEntriesPooled != nil {
		t.Fatal("committedEntriesPooled should be nil after Reset")
	}
	if u.ShardID != 0 {
		t.Fatal("ShardID should be 0 after Reset")
	}
}

func assertUpdateCommitEqual(t *testing.T, want, got *UpdateCommit) {
	t.Helper()
	if got.StableLogTo != want.StableLogTo {
		t.Fatalf("StableLogTo: got %d, want %d", got.StableLogTo, want.StableLogTo)
	}
	if got.StableLogTerm != want.StableLogTerm {
		t.Fatalf("StableLogTerm: got %d, want %d", got.StableLogTerm, want.StableLogTerm)
	}
	if got.StableSnapshotTo != want.StableSnapshotTo {
		t.Fatalf("StableSnapshotTo: got %d, want %d", got.StableSnapshotTo, want.StableSnapshotTo)
	}
	if got.Processed != want.Processed {
		t.Fatalf("Processed: got %v, want %v", got.Processed, want.Processed)
	}
	if got.LastApplied != want.LastApplied {
		t.Fatalf("LastApplied: got %d, want %d", got.LastApplied, want.LastApplied)
	}
	if got.ReadyToRead != want.ReadyToRead {
		t.Fatalf("ReadyToRead: got %d, want %d", got.ReadyToRead, want.ReadyToRead)
	}
}
