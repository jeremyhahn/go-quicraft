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

package quicraft

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// TestStopShard_ConcurrentClaimDecrementsOnce is the H3 regression test. Many
// goroutines call StopShard for the same shard concurrently. Teardown — and the
// activeNodes decrement — must happen exactly once (claimed via CompareAndSwap),
// or activeNodes drifts negative and a later monitoring call panics in
// make([]ShardInfo, 0, n) with "makeslice: cap out of range".
func TestStopShard_ConcurrentClaimDecrementsOnce(t *testing.T) {
	h := newTestHost(t)
	const shardID, replicaID = uint64(7), uint64(1)
	addTestNodeState(h, shardID, replicaID)

	if got := h.ShardCount(); got != 1 {
		t.Fatalf("ShardCount after add = %d, want 1", got)
	}

	const goroutines = 16
	var wg sync.WaitGroup
	var successes, notFound atomic.Int64
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			err := h.StopShard(shardID)
			switch {
			case err == nil:
				successes.Add(1)
			case errors.Is(err, ErrShardNotFound):
				notFound.Add(1)
			default:
				t.Errorf("StopShard returned unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if s := successes.Load(); s != 1 {
		t.Fatalf("StopShard succeeded %d times, want exactly 1 (teardown must be claimed once)", s)
	}
	if nf := notFound.Load(); nf != goroutines-1 {
		t.Fatalf("StopShard returned ErrShardNotFound %d times, want %d", nf, goroutines-1)
	}
	if got := h.activeNodes.Load(); got != 0 {
		t.Fatalf("activeNodes after concurrent StopShard = %d, want 0 (double-decrement regression)", got)
	}
	if got := h.ShardCount(); got != 0 {
		t.Fatalf("ShardCount = %d, want 0", got)
	}
	// A monitoring call must not panic on the (zero, never negative) count.
	_ = h.GetNodeHostInfo(NodeHostInfoOption{})
	_ = h.HealthCheck()
}

// TestClampNonNegative covers the defense-in-depth slice-capacity floor.
func TestClampNonNegative(t *testing.T) {
	cases := []struct {
		in   int64
		want int
	}{{-5, 0}, {-1, 0}, {0, 0}, {3, 3}, {1000, 1000}}
	for _, c := range cases {
		if got := clampNonNegative(c.in); got != c.want {
			t.Fatalf("clampNonNegative(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}
