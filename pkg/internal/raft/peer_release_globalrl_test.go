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

	"github.com/jeremyhahn/go-quicraft/pkg/internal/server"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// newGlobalRLPeer builds a Peer with NewPeer on the shared global limiter.
func newGlobalRLPeer(ctx context.Context, t *testing.T, shardID uint64, global *server.RateLimiter) *Peer {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before peer creation: %v", err)
	}
	cfg := testConfig()
	cfg.ShardID = shardID
	p, err := NewPeer(cfg, newTestLogReader(nil, proto.State{}), global)
	if err != nil {
		t.Fatalf("NewPeer(shard %d) failed: %v", shardID, err)
	}
	return p
}

// fillGlobalRLPeer merges count entries into the peer's in-memory log and
// returns the bytes that log now holds in the shared limiter.
func fillGlobalRLPeer(ctx context.Context, t *testing.T, p *Peer, count uint64) uint64 {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before fill: %v", err)
	}
	entries := rlEntries(p.r.log.inmem.markerIndex, count)
	p.r.log.inmem.merge(entries)
	share := getEntrySliceInMemSize(entries)
	if share == 0 {
		t.Fatalf("filled %d entries but their in-memory size is zero", count)
	}
	return share
}

func TestPeer_ReleaseGlobalRateLimit_LeavesOtherShardsShare(t *testing.T) {
	ctx := context.Background()
	global := server.NewRateLimiter(1 << 30)
	a := newGlobalRLPeer(ctx, t, 1, global)
	b := newGlobalRLPeer(ctx, t, 2, global)
	shareA := fillGlobalRLPeer(ctx, t, a, 4)
	shareB := fillGlobalRLPeer(ctx, t, b, 7)
	if got := global.Get(); got != shareA+shareB {
		t.Fatalf("global size = %d with both shards filled, want %d+%d", got, shareA, shareB)
	}

	a.ReleaseGlobalRateLimit()
	if got := global.Get(); got != shareB {
		t.Fatalf("global size = %d after releasing shard 1, want shard 2's share %d", got, shareB)
	}
}

func TestPeer_ReleaseGlobalRateLimit_IsIdempotentAndReleasesToZero(t *testing.T) {
	ctx := context.Background()
	global := server.NewRateLimiter(1 << 30)
	a := newGlobalRLPeer(ctx, t, 1, global)
	b := newGlobalRLPeer(ctx, t, 2, global)
	fillGlobalRLPeer(ctx, t, a, 5)
	shareB := fillGlobalRLPeer(ctx, t, b, 3)

	a.ReleaseGlobalRateLimit()
	a.ReleaseGlobalRateLimit()
	if got := global.Get(); got != shareB {
		t.Fatalf("global size = %d after releasing shard 1 twice, want shard 2's share %d", got, shareB)
	}

	b.ReleaseGlobalRateLimit()
	if got := global.Get(); got != 0 {
		t.Fatalf("global size = %d after releasing both shards, want 0", got)
	}
}

func TestPeer_ReleaseGlobalRateLimit_StartStopCyclesLeaveNoOrphanedBytes(t *testing.T) {
	ctx := context.Background()
	probe := newGlobalRLPeer(ctx, t, 1, server.NewRateLimiter(1<<30))
	oneShare := fillGlobalRLPeer(ctx, t, probe, 8)
	// A limit just under one shard's share: a cycle that left any of it
	// behind would rate-limit the host by the end.
	global := server.NewRateLimiter(oneShare - 1)

	const cycles = 20
	for i := range cycles {
		p := newGlobalRLPeer(ctx, t, uint64(i)+1, global)
		fillGlobalRLPeer(ctx, t, p, 8)
		if !global.RateLimited() {
			t.Fatalf("cycle %d: limiter not rate-limited with %d bytes over a %d limit",
				i, global.Get(), global.MaxSize())
		}
		p.ReleaseGlobalRateLimit()
	}

	if got := global.Get(); got != 0 {
		t.Fatalf("global size = %d after %d start/fill/release cycles, want 0", got, cycles)
	}
	if global.RateLimited() {
		t.Fatalf("limiter still rate-limited after %d start/fill/release cycles", cycles)
	}
}
