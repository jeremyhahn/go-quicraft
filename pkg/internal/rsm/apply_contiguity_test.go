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

package rsm

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// maxBridgedEntries bounds how large a hole applyContiguous fills with real entries
// before it resumes the state machine at the index instead. It is generous
// enough that every test which walks a sparse index range still walks the exact
// checkpoint sequence a contiguous log would produce.
const maxBridgedEntries = 1 << 21

// applyContiguous applies entries the way the engine's apply worker does: against a
// CONTIGUOUS committed log.
//
// A raft log has no holes, so entries[0].Index is always lastApplied+1 in
// production and Apply rejects anything else (see the contiguity guard in
// Apply). Tests, though, routinely jump the index to make a session look idle
// for hundreds of entries without writing hundreds of entries, and that jump is
// not the condition the guard exists to catch. applyContiguous closes the gap the same
// way the log would: with the empty EntryNormal entries raft commits between
// two user commands, which the classification loop skips, which reach no user
// state machine, and which drive EXACTLY the expiry checkpoints a single sparse
// Apply would drive. The observable behaviour of the call is therefore the
// same; only the index stream is made legal.
//
// A hole too large to bridge (a test reaching for 1<<40 to prove a threshold is
// unreachable) is closed by RESUMING the state machine at that index, which is
// what Host.StartShard does for a restarted replica. That skips the intervening
// checkpoints, so it is used only where the test's own threshold makes them
// vacuous.
func applyContiguous(t *testing.T, s *StateMachine, entries []proto.Entry, results []sm.Result) error {
	t.Helper()
	if len(entries) == 0 {
		return s.Apply(entries, results)
	}
	first := entries[0].Index
	prev := s.LastApplied()
	if first > prev+1 && first-prev-1 > maxBridgedEntries {
		s.SetLastApplied(first - 1)
		prev = first - 1
	}
	const chunk = 4096
	filler := make([]proto.Entry, 0, chunk)
	fillerResults := make([]sm.Result, chunk)
	for prev+1 < first {
		filler = filler[:0]
		for idx := prev + 1; idx < first && len(filler) < chunk; idx++ {
			filler = append(filler, proto.Entry{Index: idx, Type: proto.EntryNormal})
		}
		if err := s.Apply(filler, fillerResults[:len(filler)]); err != nil {
			t.Fatalf("bridging the log to index %d: %v", first, err)
		}
		prev = s.LastApplied()
	}
	return s.Apply(entries, results)
}

// TestApply_RejectsHoleBelowTheBatch is the guard's own unit test. A state
// machine handed a batch that starts above lastApplied+1 must fail rather than
// apply it: the hole means either committed entries were dropped or the applied
// marker was never seeded from the index the shard resumed at, and both make
// this replica evaluate session expiry over a different index range than the
// peers that applied the same log contiguously.
func TestApply_RejectsHoleBelowTheBatch(t *testing.T) {
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	s, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 10), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer s.Close()

	entries := []proto.Entry{{Index: 5, Cmd: makeCmd("k", "v")}}
	if err := s.Apply(entries, make([]sm.Result, 1)); !errors.Is(err, ErrApplyGap) {
		t.Fatalf("Apply(index 5) on a state machine at lastApplied 0 = %v, want "+
			"ErrApplyGap: indices 1-4 were never applied here, so the expiry "+
			"checkpoint walk would run over a range this replica did not apply", err)
	}
	if got := s.LastApplied(); got != 0 {
		t.Fatalf("lastApplied = %d, want 0: a rejected batch must not advance the "+
			"marker", got)
	}
}

// TestApply_AcceptsContiguousAndReplayedBatches pins the two shapes the guard
// must NOT reject, so it cannot be tightened into an equality check that breaks
// the documented on-disk replay contract.
func TestApply_AcceptsContiguousAndReplayedBatches(t *testing.T) {
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	s, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 10), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer s.Close()

	entries := []proto.Entry{
		{Index: 1, Cmd: makeCmd("a", "1")},
		{Index: 2, Cmd: makeCmd("b", "2")},
		{Index: 3, Cmd: makeCmd("c", "3")},
	}
	if err := s.Apply(entries, make([]sm.Result, len(entries))); err != nil {
		t.Fatalf("contiguous Apply from index 1 = %v", err)
	}
	if got := s.LastApplied(); got != 3 {
		t.Fatalf("lastApplied = %d, want 3", got)
	}

	// An on-disk state machine may be re-offered entries it already applied.
	// That is a replay, not a hole, and stays a no-op.
	replay := []proto.Entry{{Index: 2, Cmd: makeCmd("b", "2")}}
	if err := s.Apply(replay, make([]sm.Result, 1)); err != nil {
		t.Fatalf("replay of an already-applied index = %v, want nil: the guard "+
			"must reject holes, not replays", err)
	}
	if got := s.LastApplied(); got != 3 {
		t.Fatalf("lastApplied = %d after replay, want 3 (must not decrease)", got)
	}
}
