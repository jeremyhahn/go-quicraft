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

package transport

import (
	"sync"
	"sync/atomic"
	"testing"
)

// TestMsgRecvBudget_ReserveReleaseBounds is the H2 regression test for the
// aggregate message-receive memory budget: reservations succeed only up to the
// cap, a rejected reservation restores the budget exactly, and releases free
// capacity for subsequent frames.
func TestMsgRecvBudget_ReserveReleaseBounds(t *testing.T) {
	tr := &QUICTransport{}
	tr.msgRecvBudget.Store(1000)

	if !tr.reserveMsgRecvBudget(600) {
		t.Fatal("first reserve(600) should succeed")
	}
	if !tr.reserveMsgRecvBudget(400) {
		t.Fatal("reserve(400) up to the cap should succeed")
	}
	// Budget exhausted (0 remaining): any further reservation must fail and
	// leave the budget unchanged (the rejection restores its deduction).
	if tr.reserveMsgRecvBudget(1) {
		t.Fatal("reserve(1) past the cap must fail")
	}
	if got := tr.msgRecvBudget.Load(); got != 0 {
		t.Fatalf("budget after rejected reserve = %d, want 0 (rejection must restore)", got)
	}
	// Release frees capacity for the next frame.
	tr.releaseMsgRecvBudget(400)
	if !tr.reserveMsgRecvBudget(400) {
		t.Fatal("reserve(400) after release should succeed")
	}
	tr.releaseMsgRecvBudget(600)
	tr.releaseMsgRecvBudget(400)
	if got := tr.msgRecvBudget.Load(); got != 1000 {
		t.Fatalf("budget fully released = %d, want 1000", got)
	}
}

// TestMsgRecvBudget_ConcurrentNeverOverCommits verifies that under concurrent
// reservation attempts the budget never grants more than its cap — the property
// that bounds the OOM DoS — and is fully restored after all reservations are
// released.
func TestMsgRecvBudget_ConcurrentNeverOverCommits(t *testing.T) {
	const (
		slots   = 40
		each    = 4096
		workers = 200
	)
	tr := &QUICTransport{}
	tr.msgRecvBudget.Store(slots * each)

	var attempted sync.WaitGroup
	attempted.Add(workers)
	var done sync.WaitGroup
	done.Add(workers)
	var granted atomic.Int64
	release := make(chan struct{})

	for i := 0; i < workers; i++ {
		go func() {
			defer done.Done()
			ok := tr.reserveMsgRecvBudget(each)
			attempted.Done()
			if ok {
				granted.Add(1)
				<-release // hold the reservation so concurrent grants are visible
				tr.releaseMsgRecvBudget(each)
			}
		}()
	}

	// Once every worker has attempted its reservation, the grant count is stable
	// and must not exceed the budget.
	attempted.Wait()
	if g := granted.Load(); g > slots {
		t.Fatalf("granted %d reservations of %d bytes, exceeds budget of %d slots (over-commit)", g, each, slots)
	}

	close(release)
	done.Wait()
	if got := tr.msgRecvBudget.Load(); got != slots*each {
		t.Fatalf("budget after all releases = %d, want %d (leak)", got, slots*each)
	}
}
