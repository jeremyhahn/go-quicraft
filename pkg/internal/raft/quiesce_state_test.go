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
	"context"
	"testing"
)

// quiesceStateTestElectionTimeout is the election timeout used by the
// quiesceState tests; the derived threshold is 4*quiesceThresholdMultiplier.
const quiesceStateTestElectionTimeout = 4

// enterQuiesceStateForTest drives an enabled quiesceState through exactly
// threshold idle ticks and fails the test unless the last one enters quiesce.
func enterQuiesceStateForTest(ctx context.Context, t *testing.T, q *quiesceState) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before entering quiesce: %v", err)
	}
	for i := 1; i < q.threshold; i++ {
		if q.tryEnter() {
			t.Fatalf("tryEnter call %d of threshold %d entered quiesce early", i, q.threshold)
		}
	}
	if !q.tryEnter() {
		t.Fatalf("tryEnter call %d did not enter quiesce", q.threshold)
	}
	if !q.active {
		t.Fatal("quiesceState not active after entering")
	}
}

// assertQuiesceStateReset fails the test unless q is inactive with all
// counters at zero.
func assertQuiesceStateReset(ctx context.Context, t *testing.T, q *quiesceState, what string) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done: %v", err)
	}
	if q.active {
		t.Errorf("%s: active = true, want false", what)
	}
	if q.tick != 0 {
		t.Errorf("%s: tick = %d, want 0", what, q.tick)
	}
	if q.livenessTick != 0 {
		t.Errorf("%s: livenessTick = %d, want 0", what, q.livenessTick)
	}
}

func TestQuiesceState_New_DerivesThresholdAndLivenessFromElectionTimeout(t *testing.T) {
	q := newQuiesceState(true, quiesceStateTestElectionTimeout)
	if !q.enabled {
		t.Error("enabled = false, want true")
	}
	if want := quiesceStateTestElectionTimeout * quiesceThresholdMultiplier; q.threshold != want {
		t.Errorf("threshold = %d, want %d", q.threshold, want)
	}
	if q.threshold != 40 {
		t.Errorf("threshold = %d, want 40", q.threshold)
	}
	if want := quiesceStateTestElectionTimeout * quiesceLivenessMultiplier; q.livenessInterval != want {
		t.Errorf("livenessInterval = %d, want %d", q.livenessInterval, want)
	}
	if q.livenessInterval != 40 {
		t.Errorf("livenessInterval = %d, want 40", q.livenessInterval)
	}
	assertQuiesceStateReset(context.Background(), t, &q, "new state")
}

func TestQuiesceState_TryEnter_DisabledNeverCounts(t *testing.T) {
	q := newQuiesceState(false, quiesceStateTestElectionTimeout)
	for i := 1; i <= 1000; i++ {
		if q.tryEnter() {
			t.Fatalf("tryEnter call %d on a disabled state returned true", i)
		}
	}
	if q.tick != 0 {
		t.Errorf("tick = %d after 1000 calls on a disabled state, want 0", q.tick)
	}
	if q.active {
		t.Error("disabled state became active")
	}
}

func TestQuiesceState_TryEnter_EntersOnExactlyThresholdthTick(t *testing.T) {
	q := newQuiesceState(true, quiesceStateTestElectionTimeout)
	q.livenessTick = 7
	for i := 1; i < q.threshold; i++ {
		if q.tryEnter() {
			t.Fatalf("tryEnter call %d returned true before threshold %d", i, q.threshold)
		}
		if q.tick != i {
			t.Fatalf("tick = %d after call %d, want %d", q.tick, i, i)
		}
		if q.active {
			t.Fatalf("active after call %d, before threshold", i)
		}
	}
	if !q.tryEnter() {
		t.Fatalf("tryEnter call %d (threshold) returned false", q.threshold)
	}
	if !q.active {
		t.Error("active = false after entering, want true")
	}
	if q.livenessTick != 0 {
		t.Errorf("livenessTick = %d after entering, want 0", q.livenessTick)
	}
	if q.tick != 40 {
		t.Errorf("tick = %d after entering, want 40", q.tick)
	}
	if q.tryEnter() {
		t.Error("tryEnter after entering returned true, want false")
	}
	if q.tick != 40 {
		t.Errorf("tick = %d after a call while active, want 40 (no counting)", q.tick)
	}
	if !q.active {
		t.Error("active = false after a call while active, want true")
	}
}

func TestQuiesceState_RecordActivity_RestartsCountWhenInactive(t *testing.T) {
	q := newQuiesceState(true, quiesceStateTestElectionTimeout)
	q.tick = 25
	q.recordActivity()
	if q.tick != 0 {
		t.Errorf("tick = %d after recordActivity, want 0", q.tick)
	}
	if q.active {
		t.Error("active = true after recordActivity on an inactive state")
	}
	for i := 1; i < q.threshold; i++ {
		if q.tryEnter() {
			t.Fatalf("tryEnter call %d after recordActivity entered quiesce, want full threshold %d", i, q.threshold)
		}
	}
	if !q.tryEnter() {
		t.Errorf("tryEnter call %d after recordActivity did not enter quiesce", q.threshold)
	}
}

func TestQuiesceState_RecordActivity_ExitsWhenActive(t *testing.T) {
	ctx := context.Background()
	q := newQuiesceState(true, quiesceStateTestElectionTimeout)
	enterQuiesceStateForTest(ctx, t, &q)
	q.livenessTick = 9
	q.recordActivity()
	assertQuiesceStateReset(ctx, t, &q, "recordActivity on active state")
}

func TestQuiesceState_Exit_ResetsActiveState(t *testing.T) {
	ctx := context.Background()
	q := newQuiesceState(true, quiesceStateTestElectionTimeout)
	enterQuiesceStateForTest(ctx, t, &q)
	q.livenessTick = 9
	q.exit()
	assertQuiesceStateReset(ctx, t, &q, "exit on active state")
}
