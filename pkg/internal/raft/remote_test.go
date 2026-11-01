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
	}
}

func TestBecomeSnapshotResetsInflights(t *testing.T) {
	r := newRemote(11, 5)
	r.state = remoteReplicate
	mustAdd(t, r.inflights, 12)
	r.becomeSnapshot(50, 1)
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

// ---------------------------------------------------------------------------
// isPaused
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// snapshotDone
// ---------------------------------------------------------------------------

// TestSnapshotDoneStaysPausedUntilAck verifies that a successful transfer
// report does NOT un-pause the peer: it stays in remoteSnapshot with an
// armed wait countdown until the peer acknowledges (respondedTo) or the
// countdown expires. Probing before the peer's ack would draw a rejection
// that backtracks into compacted territory and re-sends the snapshot.
func TestSnapshotDoneStaysPausedUntilAck(t *testing.T) {
	r := newRemote(5, 5)
	r.match = 10
	r.state = remoteSnapshot
	r.snapshotIndex = 50
	r.snapshotDone(3)
	if r.state != remoteSnapshot {
		t.Fatalf("expected snapshot state after snapshotDone, got %s", r.state)
	}
	if !r.isPaused() {
		t.Fatal("expected peer to stay paused after snapshotDone")
	}
	// Countdown: expires exactly on the waitTicks-th tick. Each call IS one
	// tick, so the pre-expiry ticks are asserted one statement at a time —
	// folding them into a single `||` would hide which tick expired early
	// behind short-circuit evaluation.
	if r.tickSnapshotWait() {
		t.Fatal("countdown expired on tick 1, before waitTicks ticks")
	}
	if r.tickSnapshotWait() {
		t.Fatal("countdown expired on tick 2, before waitTicks ticks")
	}
	if !r.tickSnapshotWait() {
		t.Fatal("countdown must expire on the final tick")
	}
	// Caller falls back to probe on expiry:
	// next = max(match+1, snapshotIndex+1) = max(11, 51) = 51.
	r.becomeProbe()
	if r.state != remoteProbe {
		t.Errorf("expected probe state, got %s", r.state)
	}
	if r.next != 51 {
		t.Errorf("expected next=51, got %d", r.next)
	}
	if r.snapshotIndex != 0 {
		t.Errorf("expected snapshotIndex cleared, got %d", r.snapshotIndex)
	}
	// Expired countdown must not fire again.
	if r.tickSnapshotWait() {
		t.Fatal("countdown must not fire after expiry")
	}
}

// TestSnapshotDoneAckViaRespondedTo verifies the normal exit path: the
// peer's ReplicateResp advances match past the snapshot index and
// respondedTo transitions snapshot -> probe.
func TestSnapshotDoneAckViaRespondedTo(t *testing.T) {
	r := newRemote(5, 5)
	r.state = remoteSnapshot
	r.snapshotIndex = 50
	r.snapshotDone(10)
	r.update(100)
	r.respondedTo()
	if r.state != remoteProbe {
		t.Errorf("expected probe state after ack, got %s", r.state)
	}
	if r.next != 101 {
		t.Errorf("expected next=101, got %d", r.next)
	}
	if r.snapshotWaitTicks != 0 {
		t.Errorf("expected wait countdown cleared, got %d", r.snapshotWaitTicks)
	}
}

// TestTickSnapshotWaitNotArmed verifies the countdown is inert while the
// transfer is still in flight (snapshotDone not yet reported) and outside
// snapshot state.
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

// ---------------------------------------------------------------------------
// snapshotFailed
// ---------------------------------------------------------------------------

// TestSnapshotFailedArmsRetryBackoff verifies that a failed transfer keeps
// the peer paused with the countdown armed as a retry backoff — probing
// immediately would re-stream the full snapshot back-to-back on persistent
// failure. Expiry falls back to probe via the caller.
func TestSnapshotFailedArmsRetryBackoff(t *testing.T) {
	r := newRemote(5, 5)
	r.match = 10
	r.state = remoteSnapshot
	r.snapshotIndex = 50
	r.snapshotFailed(2)
	if r.state != remoteSnapshot {
		t.Errorf("expected snapshot state (paused backoff), got %s", r.state)
	}
	if !r.isPaused() {
		t.Error("expected peer to stay paused during retry backoff")
	}
	if r.tickSnapshotWait() {
		t.Fatal("backoff must not expire before waitTicks ticks")
	}
	if !r.tickSnapshotWait() {
		t.Fatal("backoff must expire on the final tick")
	}
	r.becomeProbe()
	if r.state != remoteProbe {
		t.Errorf("expected probe state after backoff expiry, got %s", r.state)
	}
	if r.snapshotIndex != 0 {
		t.Errorf("expected snapshotIndex cleared, got %d", r.snapshotIndex)
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
	r.becomeSnapshot(100, 1)
	if r.state != remoteSnapshot {
		t.Errorf("expected snapshot, got %s", r.state)
	}
	if r.snapshotIndex != 100 {
		t.Errorf("expected snapshotIndex=100, got %d", r.snapshotIndex)
	}

	// snapshot -> (paused wait) -> probe: transfer success arms the wait
	// countdown; expiry falls back to probe.
	r.snapshotDone(1)
	if r.state != remoteSnapshot {
		t.Errorf("expected snapshot (paused wait) after snapshotDone, got %s", r.state)
	}
	if !r.tickSnapshotWait() {
		t.Error("expected wait countdown to expire")
	}
	r.becomeProbe()
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
