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

	"github.com/jeremyhahn/go-quicraft/pkg/internal/server"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// newRateLimitedInMemory builds an in-memory log wired to a per-shard limiter
// and the SHARED global limiter, the way newRaft wires them.
func newRateLimitedInMemory(global *server.RateLimiter) *inMemory {
	im := newInMemory(0)
	im.rl = server.NewInMemRateLimiter(1 << 30)
	im.globalRL = global
	return &im
}

// newGlobalOnlyInMemory builds an in-memory log with ONLY the host-wide limiter
// configured (MaxTotalInMemLogSize set, MaxInMemLogSize left at its default 0).
func newGlobalOnlyInMemory(global *server.RateLimiter) *inMemory {
	im := newInMemory(0)
	im.globalRL = global
	return &im
}

func rlEntries(first, count uint64) []proto.Entry {
	entries := make([]proto.Entry, count)
	for i := range entries {
		entries[i] = proto.Entry{Term: 1, Index: first + uint64(i), Cmd: []byte("payload")}
	}
	return entries
}

// TestGlobalRateLimiter_IsSharedAndUpdatedByDelta pins the accounting model of
// the global (cross-shard) limiter. It is ONE counter shared by every shard on
// the host, so a shard may only ever apply DELTAS to it. Writing an absolute
// size — which is correct for the per-shard limiter — silently overwrites every
// other shard's contribution, so the host-wide total no longer reflects the
// memory actually held and MaxTotalInMemLogSize stops applying backpressure.
//
// The snapshot-restore path is the sharpest case: one shard restoring would
// zero the total for all of them.
func TestGlobalRateLimiter_IsSharedAndUpdatedByDelta(t *testing.T) {
	global := server.NewRateLimiter(1 << 30)
	shardA := newRateLimitedInMemory(global)
	shardB := newRateLimitedInMemory(global)

	a := rlEntries(1, 4)
	b := rlEntries(1, 6)
	shardA.merge(a)
	shardB.merge(b)

	sizeA := getEntrySliceInMemSize(a)
	sizeB := getEntrySliceInMemSize(b)
	if got, want := global.Get(), sizeA+sizeB; got != want {
		t.Fatalf("global size = %d, want %d (both shards' entries)", got, want)
	}

	// Shard B restores from a snapshot and drops its whole in-memory log.
	// Shard A is untouched and must keep its share of the global total.
	shardB.restore(proto.Snapshot{Index: 20, Term: 2})

	if got := global.Get(); got != sizeA {
		t.Fatalf("global size = %d after shard B restored, want %d (shard A's entries).\n"+
			"A shard that restores from a snapshot wrote its own absolute size to the "+
			"SHARED counter, discarding every other shard's contribution: the host-wide "+
			"total now under-reports the memory actually held and MaxTotalInMemLogSize "+
			"stops applying backpressure.", got, sizeA)
	}

	// Shard B refills; the total must again be the sum, not just B's share.
	b2 := rlEntries(21, 3)
	shardB.merge(b2)
	if got, want := global.Get(), sizeA+getEntrySliceInMemSize(b2); got != want {
		t.Fatalf("global size = %d after shard B refilled, want %d", got, want)
	}
}

// TestGlobalRateLimiter_ShardCannotConsumeAnotherShardsShare pins the clamp on
// the decrease path. A shard must never subtract more from the shared counter
// than it put in; doing so drives the global total below the real footprint and
// disables backpressure host-wide.
func TestGlobalRateLimiter_ShardCannotConsumeAnotherShardsShare(t *testing.T) {
	global := server.NewRateLimiter(1 << 30)
	shardA := newRateLimitedInMemory(global)
	shardB := newRateLimitedInMemory(global)

	a := rlEntries(1, 5)
	shardA.merge(a)
	sizeA := getEntrySliceInMemSize(a)

	// Shard B never contributed anything, so it can take nothing away.
	shardB.subGlobalSize(sizeA)

	if got := global.Get(); got != sizeA {
		t.Fatalf("global size = %d, want %d: a shard subtracted another shard's "+
			"share from the shared counter", got, sizeA)
	}
}

// TestGlobalRateLimiter_WorksWithoutThePerShardLimiter pins that the two limits
// are INDEPENDENT knobs. MaxInMemLogSize and MaxTotalInMemLogSize both default
// to 0 and are not cross-validated, so configuring only the host-wide cap is
// both legal and the documented way to enable it. Nesting the global accounting
// inside the per-shard limiter's nil check meant that configuration allocated a
// limiter that was consulted on every propose and permanently read zero — the
// cap silently did nothing.
func TestGlobalRateLimiter_WorksWithoutThePerShardLimiter(t *testing.T) {
	global := server.NewRateLimiter(1 << 30)
	shard := newGlobalOnlyInMemory(global)
	if shard.rl != nil {
		t.Fatal("fixture: the per-shard limiter must be disabled")
	}

	entries := rlEntries(1, 4)
	shard.merge(entries)

	size := getEntrySliceInMemSize(entries)
	if got := global.Get(); got != size {
		t.Fatalf("global size = %d, want %d: with only MaxTotalInMemLogSize set, "+
			"the host-wide limiter never saw the shard's in-memory log at all, so "+
			"the configured cap applies no backpressure", got, size)
	}

	// And it comes back down as entries are applied.
	shard.appliedLogTo(4)
	if got := global.Get(); got != 0 {
		t.Fatalf("global size = %d after applying every entry, want 0", got)
	}
}

// TestGlobalRateLimiter_ReleasedOnShardTeardown pins the teardown half of the
// shared-counter contract. Each shard tracks its own share so it can apply
// deltas without disturbing the others, but that share lives on the raft log
// and dies with the Peer. Without an explicit release, a stopped shard's bytes
// stay counted against a shard that no longer exists, and every
// StartShard/StopShard cycle leaks a little more until the accumulated orphan
// total crosses MaxTotalInMemLogSize — at which point every propose on the host
// fails with ErrSystemBusy, permanently, with no memory actually held.
func TestGlobalRateLimiter_ReleasedOnShardTeardown(t *testing.T) {
	global := server.NewRateLimiter(1 << 30)
	survivor := newRateLimitedInMemory(global)
	survivorEntries := rlEntries(1, 3)
	survivor.merge(survivorEntries)
	survivorSize := getEntrySliceInMemSize(survivorEntries)

	const cycles = 20
	for range cycles {
		shard := newRateLimitedInMemory(global)
		shard.merge(rlEntries(1, 8))
		// Teardown: the engine releases the shard's share before the peer is
		// dropped (stepWorker.unloadNode -> Peer.ReleaseGlobalRateLimit).
		shard.setGlobalSize(0)
	}

	if got := global.Get(); got != survivorSize {
		t.Fatalf("global size = %d after %d shard start/stop cycles, want %d "+
			"(only the surviving shard): stopped shards left their bytes in the "+
			"shared counter", got, cycles, survivorSize)
	}
}
