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

// Tests read snapshot outcomes from the completedC tap, so every test pool records them; production pools never do.
func init() { recordSnapshotResults = true }

// CompletedC returns the pool's completedC tap: one result per finished
// request, dropped with a warning while the buffer is full.
func (p *snapshotPool) CompletedC() <-chan snapshotResult {
	return p.completedC
}

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
