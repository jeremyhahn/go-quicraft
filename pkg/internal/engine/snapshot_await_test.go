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
	"context"
	"fmt"
	"testing"
	"time"
)

// deadlineReportingGrace is how long before the test binary's own deadline a
// wait on the snapshot pool gives up, so the failure is reported by the test
// that was waiting ("save (shard 5): no snapshotResult ...") instead of by the
// harness killing the binary with a goroutine dump at the same instant. It is
// a margin for printing the failure, not a budget for the operation.
const deadlineReportingGrace = time.Second

// testDeadlineContext returns a context that ends deadlineReportingGrace
// before the test binary's deadline (go test -timeout; 900s in CI). A binary
// run without a deadline (-timeout 0) gets a context that ends only with the
// test, exactly as the harness itself is unbounded in that configuration.
func testDeadlineContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	deadline, ok := t.Deadline()
	if !ok {
		return context.WithCancel(t.Context())
	}
	return context.WithDeadline(t.Context(), deadline.Add(-deadlineReportingGrace))
}

// awaitSnapshotResult receives the pool's completion for the one request the
// caller has in flight and returns it.
//
// The snapshotResult on CompletedC is the pool's own completion mark: the
// worker sends it after handleSnapshot has returned, after OnSnapshotCompleted,
// after the inFlight slot is released and after the node's snapshotting flag
// is cleared. The channel receive happens-after that send, so every value a
// caller reads once this returns — the LogDB record, the artifact on disk, the
// snapshotter's state, the node's flags — is pinned to the finished operation.
//
// A snapshot save or recovery has no intrinsic time bound: the pool carries no
// timeout, and a save takes as long as the state machine, the barrier and the
// disk take. Budgeting it with a wall-clock literal treats "2s have elapsed"
// as if it were "the save did not complete", which under a saturated runner
// it is not. The only budget that honestly describes the wait is the
// test binary's own, so the wait ends at the harness deadline less a grace
// for reporting.
//
// A wait that ends without a result names the shards the pool still holds in
// flight, because a timeout on its own does not distinguish in-progress from
// never-started: a shard still in flight means the worker has not finished
// its request; an empty set means the request completed and its result was
// dropped (CompletedC full and unread), or was never accepted.
//
// format and args name the request being waited for, printf-style.
func awaitSnapshotResult(t *testing.T, p *snapshotPool, format string, args ...any) snapshotResult {
	t.Helper()
	ctx, cancel := testDeadlineContext(t)
	defer cancel()
	select {
	case res := <-p.CompletedC():
		return res
	case <-ctx.Done():
		var inFlight []uint64
		p.inFlight.Range(func(k, _ any) bool {
			inFlight = append(inFlight, k.(uint64))
			return true
		})
		t.Fatalf("%s: no snapshotResult before the test deadline (%v); shards still in flight: %v",
			fmt.Sprintf(format, args...), ctx.Err(), inFlight)
	}
	return snapshotResult{}
}
