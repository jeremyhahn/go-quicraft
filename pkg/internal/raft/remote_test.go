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

package raft

import (
	"testing"
)

// Unit tests of the per-peer replication state machine. The snapshot
// transfer outcomes (snapshotDone, snapshotFailed and the countdown that
// follows them) are tested through step in raft_test.go
// (TestRaft_SnapshotStatus_*) and raft_safety_test.go
// (TestSafety_SnapshotFailureRecovery, TestSafety_RemoteState*).

// ---------------------------------------------------------------------------
// newRemote and String
// ---------------------------------------------------------------------------

func TestNewRemoteInitializesCorrectly(t *testing.T) {
	r := newRemote(10, 5)
	if r.next != 10 {
		t.Errorf("expected next 10, got %d", r.next)
	}
	if r.match != 0 {
		t.Errorf("expected match 0, got %d", r.match)
	}
	if r.state != remoteProbe {
		t.Errorf("expected probe state, got %s", r.state)
	}
	if r.active {
		t.Errorf("expected not active")
	}
	if r.inflights == nil {
		t.Errorf("expected inflights to be initialized")
	}
	if r.inflights.size != 5 {
		t.Errorf("expected inflights size 5, got %d", r.inflights.size)
	}
	if r.snapshotIndex != 0 {
		t.Errorf("expected snapshotIndex 0, got %d", r.snapshotIndex)
	}
	if r.probeSent {
		t.Errorf("expected probeSent false")
	}
}

func TestRemoteString(t *testing.T) {
	r := newRemote(11, 5)
	r.match = 10
	r.snapshotIndex = 50
	r.snapshotWaitTicks = 3
	s := r.String()
	if s != "match:10,next:11,state:Probe,snapshot:50,snapshotwait:3" {
		t.Errorf("unexpected String(): %s", s)
	}
}

func TestRemoteStateString(t *testing.T) {
	tests := []struct {
		state remoteState
		name  string
	}{
		{remoteReplicate, "Replicate"},
		{remoteProbe, "Probe"},
		{remoteSnapshot, "Snapshot"},
		{remoteState(99), "Unknown(99)"},
	}
	for _, tt := range tests {
		if tt.state.String() != tt.name {
			t.Errorf("expected %q for state %d, got %q", tt.name, tt.state, tt.state.String())
		}
	}
}

// ---------------------------------------------------------------------------
// State transitions
// ---------------------------------------------------------------------------

func TestBecomeReplicateFromProbe(t *testing.T) {
	r := newRemote(20, 5)
	r.match = 10
	r.probeSent = true
	mustAdd(t, r.inflights, 15)
	mustAdd(t, r.inflights, 16)
	r.becomeReplicate()
	if r.state != remoteReplicate {
		t.Errorf("expected replicate state, got %s", r.state)
	}
	if r.next != 11 {
		t.Errorf("expected next=match+1=11, got %d", r.next)
	}
	if r.probeSent {
		t.Errorf("expected probeSent cleared")
	}
	if r.snapshotIndex != 0 {
		t.Errorf("expected snapshotIndex 0, got %d", r.snapshotIndex)
	}
	if r.inflights.Count() != 0 {
		t.Errorf("expected inflights reset, got count %d", r.inflights.Count())
	}
}

// TestBecomeProbe verifies next is match+1 from replicate and
// max(match+1, snapshotIndex+1) from snapshot, and that the snapshot
// fields, the wait countdown and the inflights are cleared.
func TestBecomeProbe(t *testing.T) {
	tests := []struct {
		name          string
		state         remoteState
		match         uint64
		snapshotIndex uint64
		expNext       uint64
	}{
		{"from_replicate", remoteReplicate, 10, 0, 11},
		{"from_snapshot_uses_snapshot_index", remoteSnapshot, 10, 100, 101},
		{"from_snapshot_match_higher", remoteSnapshot, 200, 100, 201},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRemote(5, 5)
			r.match = tt.match
			r.state = tt.state
			r.snapshotIndex = tt.snapshotIndex
			if tt.state == remoteSnapshot {
				r.snapshotDone(10)
			}
			mustAdd(t, r.inflights, 15)
			r.becomeProbe()
			if r.state != remoteProbe {
				t.Errorf("expected probe state, got %s", r.state)
			}
			if r.next != tt.expNext {
				t.Errorf("expected next=%d, got %d", tt.expNext, r.next)
			}
			if r.snapshotIndex != 0 {
				t.Errorf("expected snapshotIndex cleared, got %d", r.snapshotIndex)
			}
			if r.snapshotWaitTicks != 0 {
				t.Errorf("expected wait countdown cleared, got %d", r.snapshotWaitTicks)
			}
			if r.inflights.Count() != 0 {
				t.Errorf("expected inflights reset, count=%d", r.inflights.Count())
			}
		})
	}
}

func TestBecomeSnapshotRecordsIndex(t *testing.T) {
	tests := []remoteState{remoteReplicate, remoteProbe, remoteSnapshot}
	for _, st := range tests {
		r := newRemote(11, 5)
		r.match = 10
		r.state = st
		mustAdd(t, r.inflights, 12)
		r.becomeSnapshot(50, 1)
		if r.state != remoteSnapshot {
			t.Errorf("from %s: expected snapshot state, got %s", st, r.state)
		}
		if r.snapshotIndex != 50 {
			t.Errorf("from %s: expected snapshotIndex 50, got %d", st, r.snapshotIndex)
		}
		if r.match != 10 {
			t.Errorf("from %s: match should not change, got %d", st, r.match)
		}
		if r.probeSent {
			t.Errorf("from %s: expected probeSent cleared", st)
		}
		if r.inflights.Count() != 0 {
			t.Errorf("from %s: expected inflights reset, count=%d", st, r.inflights.Count())
		}
	}
}

// ---------------------------------------------------------------------------
// update / optimisticUpdate
// ---------------------------------------------------------------------------

func TestUpdate(t *testing.T) {
	tests := []struct {
		name     string
		match    uint64
		next     uint64
		index    uint64
		updated  bool
		expMatch uint64
		expNext  uint64
	}{
		{"advances_match_and_next", 10, 11, 15, true, 15, 16},
		{"stale_index_changes_nothing", 15, 20, 10, false, 15, 20},
		{"index_at_match_advances_next_only", 10, 8, 10, false, 10, 11},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRemote(tt.next, 5)
			r.match = tt.match
			if got := r.update(tt.index); got != tt.updated {
				t.Errorf("expected updated=%v, got %v", tt.updated, got)
			}
			if r.match != tt.expMatch {
				t.Errorf("expected match=%d, got %d", tt.expMatch, r.match)
			}
			if r.next != tt.expNext {
				t.Errorf("expected next=%d, got %d", tt.expNext, r.next)
			}
		})
	}
}

// TestOptimisticUpdate verifies next becomes index+1, also below the
// current next, and match never moves.
func TestOptimisticUpdate(t *testing.T) {
	tests := []struct {
		name    string
		next    uint64
		index   uint64
		expNext uint64
	}{
		{"advances_next", 11, 20, 21},
		{"lower_index_moves_next_back", 20, 5, 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRemote(tt.next, 5)
			r.match = 10
			r.optimisticUpdate(tt.index)
			if r.next != tt.expNext {
				t.Errorf("expected next=%d, got %d", tt.expNext, r.next)
			}
			if r.match != 10 {
				t.Errorf("expected match=10 unchanged, got %d", r.match)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// respondedTo
// ---------------------------------------------------------------------------

func TestRespondedToTableDriven(t *testing.T) {
	tests := []struct {
		name          string
		state         remoteState
		match         uint64
		next          uint64
		snapshotIndex uint64
		expectedState remoteState
		expectedNext  uint64
	}{
		{
			name:          "probe_to_replicate",
			state:         remoteProbe,
			match:         10,
			next:          12,
			expectedState: remoteReplicate,
			expectedNext:  11, // match+1
		},
		{
			name:          "replicate_no_change",
			state:         remoteReplicate,
			match:         10,
			next:          12,
			expectedState: remoteReplicate,
			expectedNext:  12,
		},
		{
			name:          "snapshot_caught_up",
			state:         remoteSnapshot,
			match:         10,
			next:          12,
			snapshotIndex: 8,
			expectedState: remoteProbe,
			expectedNext:  11, // max(match+1, snapshotIndex+1) = max(11, 9) = 11
		},
		{
			name:          "snapshot_not_caught_up",
			state:         remoteSnapshot,
			match:         10,
			next:          12,
			snapshotIndex: 50,
			expectedState: remoteSnapshot,
			expectedNext:  12,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRemote(tt.next, 5)
			r.match = tt.match
			r.state = tt.state
			r.snapshotIndex = tt.snapshotIndex
			if tt.state == remoteProbe {
				r.probeSent = true
			}
			if tt.state == remoteSnapshot {
				r.snapshotDone(10)
			}
			r.respondedTo()
			if r.state != tt.expectedState {
				t.Errorf("expected state %s, got %s", tt.expectedState, r.state)
			}
			if r.next != tt.expectedNext {
				t.Errorf("expected next %d, got %d", tt.expectedNext, r.next)
			}
			if tt.state == remoteProbe && r.probeSent {
				t.Errorf("expected probeSent cleared after respondedTo")
			}
			if tt.state == remoteSnapshot && tt.expectedState == remoteProbe && r.snapshotWaitTicks != 0 {
				t.Errorf("expected wait countdown cleared on ack, got %d", r.snapshotWaitTicks)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// declineFrom
// ---------------------------------------------------------------------------

func TestDeclineFromReplicateTableDriven(t *testing.T) {
	tests := []struct {
		name     string
		match    uint64
		next     uint64
		rejected uint64
		changed  bool
		expNext  uint64
	}{
		{"stale_below_match", 10, 15, 9, false, 15},
		{"stale_at_match", 10, 15, 10, false, 15},
		{"valid_above_match", 10, 15, 12, true, 11},
		{"valid_at_next_minus_1", 10, 15, 14, true, 11},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRemote(tt.next, 5)
			r.match = tt.match
			r.state = remoteReplicate
			changed := r.declineFrom(tt.rejected, 100)
			if changed != tt.changed {
				t.Errorf("expected changed=%v, got %v", tt.changed, changed)
			}
			if r.next != tt.expNext {
				t.Errorf("expected next=%d, got %d", tt.expNext, r.next)
			}
		})
	}
}

// TestDeclineFromNonReplicateTableDriven verifies that in probe and
// snapshot state only a rejection of next-1 counts, next becomes
// max(1, min(rejected, hint+1)) and an accepted rejection clears probeSent.
func TestDeclineFromNonReplicateTableDriven(t *testing.T) {
	tests := []struct {
		name      string
		next      uint64
		rejected  uint64
		lastIndex uint64
		changed   bool
		expNext   uint64
	}{
		{"stale_rejected_not_matching", 15, 20, 100, false, 15},
		{"valid_uses_rejected", 15, 14, 100, true, 14},
		{"valid_uses_hint_when_lower", 15, 14, 10, true, 11},
		{"never_below_one", 2, 1, 0, true, 1},
	}
	for _, tt := range tests {
		for _, st := range []remoteState{remoteProbe, remoteSnapshot} {
			t.Run(tt.name+"_"+st.String(), func(t *testing.T) {
				r := newRemote(tt.next, 5)
				r.state = st
				r.probeSent = true
				changed := r.declineFrom(tt.rejected, tt.lastIndex)
				if changed != tt.changed {
					t.Errorf("expected changed=%v, got %v", tt.changed, changed)
				}
				if r.next != tt.expNext {
					t.Errorf("expected next=%d, got %d", tt.expNext, r.next)
				}
				if r.probeSent == tt.changed {
					t.Errorf("probeSent=%v after changed=%v, want cleared only when changed",
						r.probeSent, changed)
				}
			})
		}
	}
}

// ---------------------------------------------------------------------------
// isPaused and the snapshot wait countdown
// ---------------------------------------------------------------------------

// TestIsPausedTableDriven covers the rows no step-level test reaches: a full
// inflight window and an invalid state. Probe and snapshot pausing are
// asserted through sendAppend in TestSafety_RemoteStateRetry and
// TestSafety_RemoteStateSnapshot (raft_safety_test.go), replicate with room
// in TestSafety_RemoteStateReplicate.
func TestIsPausedTableDriven(t *testing.T) {
	tests := []struct {
		name     string
		state    remoteState
		inflFull bool
		expected bool
	}{
		{"replicate_full", remoteReplicate, true, true},
		// An invalid state is paused, not a panic; the remote is retried
		// on the next heartbeat cycle.
		{"invalid_state", remoteState(99), false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxInfl := 5
			if tt.inflFull {
				maxInfl = 1
			}
			r := newRemote(11, maxInfl)
			r.state = tt.state
			if tt.inflFull {
				mustAdd(t, r.inflights, 11)
			}
			if r.isPaused() != tt.expected {
				t.Errorf("expected isPaused=%v, got %v", tt.expected, r.isPaused())
			}
		})
	}
}

// TestTickSnapshotWaitNotArmed verifies the countdown is inert while the
// transfer is in flight and outside snapshot state.
func TestTickSnapshotWaitNotArmed(t *testing.T) {
	r := newRemote(5, 5)
	r.state = remoteSnapshot
	r.snapshotIndex = 50
	if r.tickSnapshotWait() {
		t.Fatal("countdown must not fire while transfer is in flight")
	}
	r.becomeProbe()
	if r.tickSnapshotWait() {
		t.Fatal("countdown must not fire outside snapshot state")
	}
}
