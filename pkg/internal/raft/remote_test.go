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

// ---------------------------------------------------------------------------
// newRemote
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

func TestNewRemoteWithZeroInflight(t *testing.T) {
	r := newRemote(1, 0)
	if r.inflights.size != 0 {
		t.Errorf("expected inflights size 0, got %d", r.inflights.size)
	}
}

// ---------------------------------------------------------------------------
// String
// ---------------------------------------------------------------------------

func TestRemoteString(t *testing.T) {
	r := newRemote(11, 5)
	r.match = 10
	r.snapshotIndex = 50
	s := r.String()
	if s != "match:10,next:11,state:Probe,snapshot:50" {
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
	}
	for _, tt := range tests {
		if tt.state.String() != tt.name {
			t.Errorf("expected %q for state %d, got %q", tt.name, tt.state, tt.state.String())
		}
	}
}

func TestRemoteStateStringUnknown(t *testing.T) {
	s := remoteState(99).String()
	if s != "Unknown(99)" {
		t.Errorf("expected Unknown(99), got %q", s)
	}
}

// ---------------------------------------------------------------------------
// becomeReplicate
// ---------------------------------------------------------------------------

func TestBecomeReplicateFromProbe(t *testing.T) {
	r := newRemote(20, 5)
	r.match = 10
	r.probeSent = true
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
}

func TestBecomeReplicateResetsInflights(t *testing.T) {
	r := newRemote(20, 5)
	r.match = 10
	mustAdd(t, r.inflights, 15)
	mustAdd(t, r.inflights, 16)
	r.becomeReplicate()
	if r.inflights.Count() != 0 {
		t.Errorf("expected inflights reset, got count %d", r.inflights.Count())
	}
}

// ---------------------------------------------------------------------------
// becomeProbe
// ---------------------------------------------------------------------------

func TestBecomeProbeFromReplicate(t *testing.T) {
	r := newRemote(20, 5)
	r.match = 10
	r.state = remoteReplicate
	mustAdd(t, r.inflights, 15)
	r.becomeProbe()
	if r.state != remoteProbe {
		t.Errorf("expected probe state, got %s", r.state)
	}
	if r.next != 11 {
		t.Errorf("expected next=match+1=11, got %d", r.next)
	}
	if r.snapshotIndex != 0 {
		t.Errorf("expected snapshotIndex 0, got %d", r.snapshotIndex)
	}
	if r.inflights.Count() != 0 {
		t.Errorf("expected inflights reset, count=%d", r.inflights.Count())
	}
}

func TestBecomeProbeFromSnapshot(t *testing.T) {
	r := newRemote(5, 5)
	r.match = 10
	r.state = remoteSnapshot
	r.snapshotIndex = 100
	r.becomeProbe()
	if r.state != remoteProbe {
		t.Errorf("expected probe state, got %s", r.state)
	}
	// next should be max(match+1, snapshotIndex+1) = max(11, 101) = 101
	if r.next != 101 {
		t.Errorf("expected next=101, got %d", r.next)
	}
	if r.snapshotIndex != 0 {
		t.Errorf("expected snapshotIndex cleared, got %d", r.snapshotIndex)
	}
}

func TestBecomeProbeFromSnapshotMatchHigherThanSnapshot(t *testing.T) {
	r := newRemote(5, 5)
	r.match = 200
	r.state = remoteSnapshot
	r.snapshotIndex = 100
	r.becomeProbe()
	// next should be max(201, 101) = 201
	if r.next != 201 {
		t.Errorf("expected next=201, got %d", r.next)
	}
}

// ---------------------------------------------------------------------------
// becomeSnapshot
// ---------------------------------------------------------------------------

func TestBecomeSnapshotRecordsIndex(t *testing.T) {
	tests := []remoteState{remoteReplicate, remoteProbe, remoteSnapshot}
	for _, st := range tests {
		r := newRemote(11, 5)
		r.match = 10
		r.state = st
		r.becomeSnapshot(50)
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
	}
}

func TestBecomeSnapshotResetsInflights(t *testing.T) {
	r := newRemote(11, 5)
	r.state = remoteReplicate
	mustAdd(t, r.inflights, 12)
	r.becomeSnapshot(50)
	if r.inflights.Count() != 0 {
		t.Errorf("expected inflights reset, count=%d", r.inflights.Count())
	}
}

// ---------------------------------------------------------------------------
// update
// ---------------------------------------------------------------------------

func TestUpdateAdvancesMatchAndNext(t *testing.T) {
	r := newRemote(11, 5)
	r.match = 10
	updated := r.update(15)
	if !updated {
		t.Errorf("expected updated=true")
	}
	if r.match != 15 {
		t.Errorf("expected match=15, got %d", r.match)
	}
	if r.next != 16 {
		t.Errorf("expected next=16, got %d", r.next)
	}
}

func TestUpdateDoesNotGoBackward(t *testing.T) {
	r := newRemote(20, 5)
	r.match = 15
	updated := r.update(10)
	if updated {
		t.Errorf("expected updated=false for stale index")
	}
	if r.match != 15 {
		t.Errorf("expected match=15 unchanged, got %d", r.match)
	}
	if r.next != 20 {
		t.Errorf("expected next=20 unchanged, got %d", r.next)
	}
}

func TestUpdateAdvancesNextWhenBehind(t *testing.T) {
	r := newRemote(5, 5)
	r.match = 3
	updated := r.update(10)
	if !updated {
		t.Errorf("expected updated=true")
	}
	if r.match != 10 {
		t.Errorf("expected match=10, got %d", r.match)
	}
	if r.next != 11 {
		t.Errorf("expected next=11, got %d", r.next)
	}
}

func TestUpdateOnlyAdvancesNextNotMatch(t *testing.T) {
	r := newRemote(5, 5)
	r.match = 10
	r.next = 8
	updated := r.update(10) // same as match
	if updated {
		t.Errorf("expected updated=false when index == match")
	}
	// next should advance to 11 since next < index+1
	if r.next != 11 {
		t.Errorf("expected next=11, got %d", r.next)
	}
}

// ---------------------------------------------------------------------------
// optimisticUpdate
// ---------------------------------------------------------------------------

func TestOptimisticUpdateAdvancesNextOnly(t *testing.T) {
	r := newRemote(11, 5)
	r.match = 10
	r.optimisticUpdate(20)
	if r.next != 21 {
		t.Errorf("expected next=21, got %d", r.next)
	}
	if r.match != 10 {
		t.Errorf("expected match=10 unchanged, got %d", r.match)
	}
}

func TestOptimisticUpdateDoesNotRequireHigherIndex(t *testing.T) {
	r := newRemote(20, 5)
	r.match = 10
	r.optimisticUpdate(5) // lower than current next
	if r.next != 6 {
		t.Errorf("expected next=6, got %d", r.next)
	}
}

// ---------------------------------------------------------------------------
// respondedTo
// ---------------------------------------------------------------------------

func TestRespondedToFromProbeTransitionsToReplicate(t *testing.T) {
	r := newRemote(11, 5)
	r.match = 10
	r.probeSent = true
	r.respondedTo()
	if r.state != remoteReplicate {
		t.Errorf("expected replicate state, got %s", r.state)
	}
	if r.next != 11 {
		t.Errorf("expected next=11, got %d", r.next)
	}
}

func TestRespondedToFromSnapshotTransitionsToProbeWhenCaughtUp(t *testing.T) {
	r := newRemote(5, 5)
	r.match = 50
	r.state = remoteSnapshot
	r.snapshotIndex = 50
	r.respondedTo()
	if r.state != remoteProbe {
		t.Errorf("expected probe state, got %s", r.state)
	}
	// next should be max(match+1, snapshotIndex+1) = max(51, 51) = 51
	if r.next != 51 {
		t.Errorf("expected next=51, got %d", r.next)
	}
}

func TestRespondedToFromSnapshotStaysInSnapshotWhenBehind(t *testing.T) {
	r := newRemote(5, 5)
	r.match = 10
	r.state = remoteSnapshot
	r.snapshotIndex = 50
	r.respondedTo()
	if r.state != remoteSnapshot {
		t.Errorf("expected snapshot state, got %s", r.state)
	}
}

func TestRespondedToFromReplicateNoChange(t *testing.T) {
	r := newRemote(11, 5)
	r.match = 10
	r.state = remoteReplicate
	r.respondedTo()
	if r.state != remoteReplicate {
		t.Errorf("expected replicate state, got %s", r.state)
	}
	if r.next != 11 {
		t.Errorf("expected next unchanged, got %d", r.next)
	}
}

func TestRespondedToResetsProbeSent(t *testing.T) {
	r := newRemote(11, 5)
	r.match = 10
	r.probeSent = true
	r.respondedTo() // transitions to replicate, which clears probeSent
	if r.probeSent {
		t.Errorf("expected probeSent cleared after respondedTo")
	}
}

// ---------------------------------------------------------------------------
// declineFrom
// ---------------------------------------------------------------------------

func TestDeclineFromReplicateStateStale(t *testing.T) {
	r := newRemote(15, 5)
	r.match = 10
	r.state = remoteReplicate
	// rejected index <= match: stale
	changed := r.declineFrom(9, 100)
	if changed {
		t.Errorf("expected no change for stale rejection")
	}
	if r.next != 15 {
		t.Errorf("expected next unchanged, got %d", r.next)
	}
}

func TestDeclineFromReplicateStateAtMatch(t *testing.T) {
	r := newRemote(15, 5)
	r.match = 10
	r.state = remoteReplicate
	changed := r.declineFrom(10, 100)
	if changed {
		t.Errorf("expected no change for rejection at match")
	}
}

func TestDeclineFromReplicateStateFallsBack(t *testing.T) {
	r := newRemote(15, 5)
	r.match = 10
	r.state = remoteReplicate
	changed := r.declineFrom(12, 100)
	if !changed {
		t.Errorf("expected changed")
	}
	if r.next != 11 {
		t.Errorf("expected next=match+1=11, got %d", r.next)
	}
}

func TestDeclineFromProbeStateStale(t *testing.T) {
	r := newRemote(15, 5)
	r.match = 10
	r.state = remoteProbe
	// Stale: rejected != next-1
	changed := r.declineFrom(20, 100)
	if changed {
		t.Errorf("expected no change for stale rejection")
	}
}

func TestDeclineFromProbeStateUsesHint(t *testing.T) {
	r := newRemote(15, 5)
	r.match = 10
	r.state = remoteProbe
	r.probeSent = true
	changed := r.declineFrom(14, 12)
	if !changed {
		t.Errorf("expected changed")
	}
	// next = min(rejectedIndex, lastIndex+1) = min(14, 13) = 13
	if r.next != 13 {
		t.Errorf("expected next=13, got %d", r.next)
	}
	if r.probeSent {
		t.Errorf("expected probeSent cleared")
	}
}

func TestDeclineFromProbeStateHintLastIndexLow(t *testing.T) {
	r := newRemote(15, 5)
	r.match = 10
	r.state = remoteProbe
	// lastIndex is very low
	changed := r.declineFrom(14, 5)
	if !changed {
		t.Errorf("expected changed")
	}
	// next = min(14, 6) = 6
	if r.next != 6 {
		t.Errorf("expected next=6, got %d", r.next)
	}
}

func TestDeclineFromProbeStateNeverBelowOne(t *testing.T) {
	r := newRemote(2, 5)
	r.state = remoteProbe
	changed := r.declineFrom(1, 0)
	if !changed {
		t.Errorf("expected changed")
	}
	if r.next != 1 {
		t.Errorf("expected next=1 (minimum), got %d", r.next)
	}
}

func TestDeclineFromSnapshotStateStale(t *testing.T) {
	r := newRemote(15, 5)
	r.state = remoteSnapshot
	// rejected != next-1: stale
	changed := r.declineFrom(20, 100)
	if changed {
		t.Errorf("expected no change for stale rejection in snapshot state")
	}
}

// ---------------------------------------------------------------------------
// isPaused
// ---------------------------------------------------------------------------

func TestIsPausedProbeNotSent(t *testing.T) {
	r := newRemote(11, 5)
	r.state = remoteProbe
	r.probeSent = false
	if r.isPaused() {
		t.Errorf("expected not paused in probe with no pending probe")
	}
}

func TestIsPausedProbeSent(t *testing.T) {
	r := newRemote(11, 5)
	r.state = remoteProbe
	r.probeSent = true
	if !r.isPaused() {
		t.Errorf("expected paused in probe with pending probe")
	}
}

func TestIsPausedReplicateNotFull(t *testing.T) {
	r := newRemote(11, 5)
	r.state = remoteReplicate
	if r.isPaused() {
		t.Errorf("expected not paused in replicate with empty inflights")
	}
}

func TestIsPausedReplicateFull(t *testing.T) {
	r := newRemote(11, 3)
	r.state = remoteReplicate
	mustAdd(t, r.inflights, 11)
	mustAdd(t, r.inflights, 12)
	mustAdd(t, r.inflights, 13)
	if !r.isPaused() {
		t.Errorf("expected paused in replicate with full inflights")
	}
}

func TestIsPausedSnapshot(t *testing.T) {
	r := newRemote(11, 5)
	r.state = remoteSnapshot
	if !r.isPaused() {
		t.Errorf("expected paused in snapshot state")
	}
}

func TestIsPausedTableDriven(t *testing.T) {
	tests := []struct {
		name      string
		state     remoteState
		probeSent bool
		inflFull  bool
		expected  bool
	}{
		{"probe_not_sent", remoteProbe, false, false, false},
		{"probe_sent", remoteProbe, true, false, true},
		{"replicate_not_full", remoteReplicate, false, false, false},
		{"replicate_full", remoteReplicate, false, true, true},
		{"snapshot", remoteSnapshot, false, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxInfl := 5
			if tt.inflFull {
				maxInfl = 1
			}
			r := newRemote(11, maxInfl)
			r.state = tt.state
			r.probeSent = tt.probeSent
			if tt.inflFull {
				mustAdd(t, r.inflights, 11)
			}
			if r.isPaused() != tt.expected {
				t.Errorf("expected isPaused=%v, got %v", tt.expected, r.isPaused())
			}
		})
	}
}

func TestIsPausedReturnsTrueForInvalidState(t *testing.T) {
	r := newRemote(11, 5)
	r.state = remoteState(99)
	// Invalid state should return true (paused) without panicking.
	// The remote will retry on the next heartbeat cycle.
	if !r.isPaused() {
		t.Errorf("expected paused=true for invalid state, got false")
	}
}

func TestIsPausedInvalidStateDoesNotPanic(t *testing.T) {
	r := newRemote(11, 5)
	r.state = remoteState(255)
	// Must not panic; should return true.
	result := r.isPaused()
	if !result {
		t.Errorf("expected paused=true for invalid state 255, got false")
	}
}

// ---------------------------------------------------------------------------
// snapshotDone
// ---------------------------------------------------------------------------

func TestSnapshotDoneTransitionsToProbe(t *testing.T) {
	r := newRemote(5, 5)
	r.match = 10
	r.state = remoteSnapshot
	r.snapshotIndex = 50
	r.snapshotDone()
	if r.state != remoteProbe {
		t.Errorf("expected probe state, got %s", r.state)
	}
	// next = max(match+1, snapshotIndex+1) = max(11, 51) = 51
	if r.next != 51 {
		t.Errorf("expected next=51, got %d", r.next)
	}
	if r.snapshotIndex != 0 {
		t.Errorf("expected snapshotIndex cleared, got %d", r.snapshotIndex)
	}
}

func TestSnapshotDoneWhenMatchExceedsSnapshot(t *testing.T) {
	r := newRemote(5, 5)
	r.match = 100
	r.state = remoteSnapshot
	r.snapshotIndex = 50
	r.snapshotDone()
	if r.state != remoteProbe {
		t.Errorf("expected probe state, got %s", r.state)
	}
	if r.next != 101 {
		t.Errorf("expected next=101, got %d", r.next)
	}
}

// ---------------------------------------------------------------------------
// snapshotFailed
// ---------------------------------------------------------------------------

func TestSnapshotFailedTransitionsToProbe(t *testing.T) {
	r := newRemote(5, 5)
	r.match = 10
	r.state = remoteSnapshot
	r.snapshotIndex = 50
	r.snapshotFailed()
	if r.state != remoteProbe {
		t.Errorf("expected probe state, got %s", r.state)
	}
	if r.snapshotIndex != 0 {
		t.Errorf("expected snapshotIndex cleared, got %d", r.snapshotIndex)
	}
}

func TestSnapshotFailedResetsInflights(t *testing.T) {
	r := newRemote(5, 5)
	r.state = remoteSnapshot
	r.snapshotIndex = 50
	r.snapshotFailed()
	if r.inflights.Count() != 0 {
		t.Errorf("expected inflights reset")
	}
}

// ---------------------------------------------------------------------------
// State transitions: probe -> replicate -> snapshot -> probe cycle
// ---------------------------------------------------------------------------

func TestStateCycleProbeReplicateSnapshotProbe(t *testing.T) {
	r := newRemote(11, 5)
	r.match = 10

	// Start in probe
	if r.state != remoteProbe {
		t.Fatalf("expected initial probe state, got %s", r.state)
	}

	// probe -> replicate
	r.becomeReplicate()
	if r.state != remoteReplicate {
		t.Errorf("expected replicate, got %s", r.state)
	}
	if r.next != 11 {
		t.Errorf("expected next=11, got %d", r.next)
	}

	// replicate -> snapshot
	r.becomeSnapshot(100)
	if r.state != remoteSnapshot {
		t.Errorf("expected snapshot, got %s", r.state)
	}
	if r.snapshotIndex != 100 {
		t.Errorf("expected snapshotIndex=100, got %d", r.snapshotIndex)
	}

	// snapshot -> probe (via snapshotDone)
	r.snapshotDone()
	if r.state != remoteProbe {
		t.Errorf("expected probe, got %s", r.state)
	}
	if r.snapshotIndex != 0 {
		t.Errorf("expected snapshotIndex cleared")
	}
}

// ---------------------------------------------------------------------------
// Active tracking
// ---------------------------------------------------------------------------

func TestActiveTracking(t *testing.T) {
	r := newRemote(11, 5)
	if r.isActive() {
		t.Errorf("expected not active initially")
	}
	r.setActive()
	if !r.isActive() {
		t.Errorf("expected active after setActive")
	}
	r.setNotActive()
	if r.isActive() {
		t.Errorf("expected not active after setNotActive")
	}
}

func TestActiveDoesNotAffectState(t *testing.T) {
	r := newRemote(11, 5)
	r.match = 10
	r.state = remoteReplicate
	r.setActive()
	if r.state != remoteReplicate {
		t.Errorf("setActive changed state, got %s", r.state)
	}
	if r.match != 10 || r.next != 11 {
		t.Errorf("setActive changed match/next")
	}
	r.setNotActive()
	if r.state != remoteReplicate {
		t.Errorf("setNotActive changed state, got %s", r.state)
	}
}

// ---------------------------------------------------------------------------
// respondedTo table-driven
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
			r.respondedTo()
			if r.state != tt.expectedState {
				t.Errorf("expected state %s, got %s", tt.expectedState, r.state)
			}
			if r.next != tt.expectedNext {
				t.Errorf("expected next %d, got %d", tt.expectedNext, r.next)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// declineFrom table-driven for replicate state
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

// ---------------------------------------------------------------------------
// declineFrom table-driven for non-replicate states
// ---------------------------------------------------------------------------

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
				changed := r.declineFrom(tt.rejected, tt.lastIndex)
				if changed != tt.changed {
					t.Errorf("expected changed=%v, got %v", tt.changed, changed)
				}
				if r.next != tt.expNext {
					t.Errorf("expected next=%d, got %d", tt.expNext, r.next)
				}
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Edge case: rapid state transitions
// ---------------------------------------------------------------------------

func TestRapidStateTransitions(t *testing.T) {
	r := newRemote(11, 5)
	r.match = 10

	// Rapid transitions should not corrupt state
	r.becomeReplicate()
	r.becomeProbe()
	r.becomeSnapshot(50)
	r.becomeProbe()
	r.becomeReplicate()

	if r.state != remoteReplicate {
		t.Errorf("expected replicate, got %s", r.state)
	}
	if r.snapshotIndex != 0 {
		t.Errorf("expected snapshotIndex 0, got %d", r.snapshotIndex)
	}
	if r.probeSent {
		t.Errorf("expected probeSent false")
	}
}

// ---------------------------------------------------------------------------
// Integration: update with inflights
// ---------------------------------------------------------------------------

func TestUpdateFreesInflights(t *testing.T) {
	r := newRemote(11, 5)
	r.match = 10
	r.becomeReplicate()

	// Add some in-flight entries
	mustAdd(t, r.inflights, 11)
	mustAdd(t, r.inflights, 12)
	mustAdd(t, r.inflights, 13)
	if r.inflights.Count() != 3 {
		t.Fatalf("expected 3 inflights, got %d", r.inflights.Count())
	}

	// Simulate acknowledgment: update match and free inflights
	r.update(12)
	r.inflights.freeTo(12)
	if r.inflights.Count() != 1 {
		t.Errorf("expected 1 inflight after freeTo(12), got %d", r.inflights.Count())
	}
	if r.match != 12 {
		t.Errorf("expected match=12, got %d", r.match)
	}
}

// ---------------------------------------------------------------------------
// Edge case: match at zero
// ---------------------------------------------------------------------------

func TestRemoteWithZeroMatch(t *testing.T) {
	r := newRemote(1, 5)
	if r.match != 0 {
		t.Errorf("expected match=0, got %d", r.match)
	}
	if r.next != 1 {
		t.Errorf("expected next=1, got %d", r.next)
	}

	r.becomeReplicate()
	// match=0, so next=match+1=1
	if r.next != 1 {
		t.Errorf("expected next=1 after becomeReplicate, got %d", r.next)
	}
}
