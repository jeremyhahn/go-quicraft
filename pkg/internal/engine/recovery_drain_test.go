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
	"sync/atomic"
	"testing"
	"time"
)

// TestNode_WaitForApplyDrain_BlocksUntilInFlightApplyCompletes is the
// regression test for C3: snapshot recovery must not touch the state machine
// while an apply closure that already passed the recovering gate is still
// executing. WaitForApplyDrain must block until applyInFlight reaches 0.
func TestNode_WaitForApplyDrain_BlocksUntilInFlightApplyCompletes(t *testing.T) {
	n := &Node{}
	stopC := make(chan struct{})

	// Simulate an apply closure that has already entered (incremented before
	// the recovering gate) and is mid-SM.Apply.
	n.applyInFlight.Add(1)

	var drained atomic.Bool
	go func() {
		n.SetRecovering(true)
		ok := n.WaitForApplyDrain(stopC)
		if ok {
			drained.Store(true)
		}
	}()

	// The drain must NOT complete while the apply is still in flight.
	time.Sleep(20 * time.Millisecond)
	if drained.Load() {
		t.Fatal("WaitForApplyDrain returned while an apply was still in flight (C3 race)")
	}

	// Complete the in-flight apply; the drain must now finish promptly.
	n.applyInFlight.Add(-1)

	deadline := time.After(2 * time.Second)
	for !drained.Load() {
		select {
		case <-deadline:
			t.Fatal("WaitForApplyDrain did not complete after apply drained")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// TestNode_WaitForApplyDrain_StopCAborts verifies the drain returns false and
// does not hang when the node is stopped while an apply is wedged.
func TestNode_WaitForApplyDrain_StopCAborts(t *testing.T) {
	n := &Node{}
	stopC := make(chan struct{})
	n.applyInFlight.Add(1) // never decremented

	done := make(chan bool, 1)
	go func() {
		n.SetRecovering(true)
		done <- n.WaitForApplyDrain(stopC)
	}()

	close(stopC)
	select {
	case ok := <-done:
		if ok {
			t.Fatal("expected WaitForApplyDrain to return false on stopC")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForApplyDrain did not return after stopC closed")
	}
}
