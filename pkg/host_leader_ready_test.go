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
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// readyRecorder collects the OnLeaderReady and OnLeaderLost notifications a
// host delivers.
type readyRecorder struct {
	mu    sync.Mutex
	ready []config.LeaderInfo
	lost  int
}

func (r *readyRecorder) listener() *config.EventListener {
	return &config.EventListener{
		OnLeaderReady: func(info config.LeaderInfo) {
			r.mu.Lock()
			r.ready = append(r.ready, info)
			r.mu.Unlock()
		},
		OnLeaderLost: func(config.EventShardInfo) {
			r.mu.Lock()
			r.lost++
			r.mu.Unlock()
		},
	}
}

func (r *readyRecorder) snapshot() ([]config.LeaderInfo, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]config.LeaderInfo(nil), r.ready...), r.lost
}

func newReadyRecorderHost(t *testing.T) (*Host, *readyRecorder) {
	t.Helper()
	rec := &readyRecorder{}
	cfg := testHostConfig(t)
	cfg.EventListener = rec.listener()
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h, rec
}

func assertMirror(t *testing.T, ns *nodeState, leaderID, term uint64, ready bool) {
	t.Helper()
	l, tm, _, r := ns.leaderTermReady()
	if l != leaderID || tm != term || r != ready {
		t.Fatalf("mirror = (%d, %d, ready=%v), want (%d, %d, ready=%v)", l, tm, r, leaderID, term, ready)
	}
}

// The host's readiness mirror belongs to exactly the (leader, term) pair it
// was delivered for: every leader update ends it — including a same-leader
// re-election, which OnLeaderLost does not report — and a readiness for a
// pair that is no longer current is refused rather than applied to its
// successor.
func TestHostLeaderReady_MirrorFollowsTheDeliveredPair(t *testing.T) {
	h, rec := newReadyRecorderHost(t)
	addTestNodeState(h, 1, 1)
	ns := testLoadNode(h, 1)
	cb := &hostEngineCallback{h: h}

	cb.OnLeaderUpdated(1, 1, 3)
	assertMirror(t, ns, 1, 3, false)
	cb.OnLeaderReady(1, 1, 1, 3)
	assertMirror(t, ns, 1, 3, true)

	// Re-elected at a higher term: not ready until the new term's own commit.
	cb.OnLeaderUpdated(1, 1, 4)
	assertMirror(t, ns, 1, 4, false)

	// A readiness for the superseded pair is not applied to the current one.
	cb.OnLeaderReady(1, 1, 1, 3)
	assertMirror(t, ns, 1, 4, false)

	cb.OnLeaderReady(1, 1, 1, 4)
	assertMirror(t, ns, 1, 4, true)

	// CheckQuorum step-down: no leader in the same term.
	cb.OnLeaderUpdated(1, 0, 4)
	assertMirror(t, ns, 0, 4, false)

	ready, lost := rec.snapshot()
	want := []config.LeaderInfo{
		{ShardID: 1, ReplicaID: 1, LeaderID: 1, Term: 3},
		{ShardID: 1, ReplicaID: 1, LeaderID: 1, Term: 4},
	}
	if len(ready) != len(want) || ready[0] != want[0] || ready[1] != want[1] {
		t.Fatalf("OnLeaderReady deliveries = %+v, want %+v", ready, want)
	}
	if lost != 1 {
		t.Fatalf("OnLeaderLost fired %d times, want 1: only the step-down lost "+
			"leadership, the re-election did not", lost)
	}
}

// A witness stores no log and cannot serve reads: it is never marked ready,
// never announces readiness, and the query refuses it as ReadIndex does.
func TestHostLeaderReady_WitnessIsNeverReady(t *testing.T) {
	h, rec := newReadyRecorderHost(t)
	addTestNodeState(h, 1, 1)
	ns := testLoadNode(h, 1)
	ns.cfg.IsWitness = true
	cb := &hostEngineCallback{h: h}

	cb.OnLeaderUpdated(1, 2, 3)
	cb.OnLeaderReady(1, 1, 2, 3)
	assertMirror(t, ns, 2, 3, false)
	if ready, _ := rec.snapshot(); len(ready) != 0 {
		t.Fatalf("witness announced readiness: %+v", ready)
	}
	if _, _, _, err := h.ShardReadReady(1); !errors.Is(err, ErrInvalidOperation) {
		t.Fatalf("ShardReadReady on a witness = %v, want ErrInvalidOperation", err)
	}
}

// ShardReadReady refuses with the same errors ReadIndex refuses with before
// reaching Raft.
func TestShardReadReady_Refusals(t *testing.T) {
	h := newTestHost(t)
	if _, _, _, err := h.ShardReadReady(9); !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("unknown shard: %v, want ErrShardNotFound", err)
	}
	// Registered with the host but not loaded into the engine.
	addTestNodeState(h, 1, 1)
	if _, _, _, err := h.ShardReadReady(1); !errors.Is(err, ErrShardNotReady) {
		t.Fatalf("shard not in the engine: %v, want ErrShardNotReady", err)
	}
	h.draining.Store(true)
	if _, _, _, err := h.ShardReadReady(1); !errors.Is(err, ErrDraining) {
		t.Fatalf("draining host: %v, want ErrDraining", err)
	}
	h.draining.Store(false)
	h.Close()
	if _, _, _, err := h.ShardReadReady(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed host: %v, want ErrClosed", err)
	}
}

// End to end on a running shard: the consumer waits on OnLeaderReady — no
// polling — and from then on the query agrees and a linearizable read is
// served. Once the failure latch trips, the query reports the failure as
// ReadIndex does.
func TestShardReadReady_EventThenQueryThenRead(t *testing.T) {
	readyC := make(chan config.LeaderInfo, 8)
	failed := make(chan config.ShardFailedInfo, 1)
	cfg := testHostConfig(t)
	cfg.EventListener = &config.EventListener{
		OnLeaderReady: func(info config.LeaderInfo) {
			select {
			case readyC <- info:
			default:
			}
		},
		OnShardFailed: func(info config.ShardFailedInfo) {
			select {
			case failed <- info:
			default:
			}
		},
	}
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	machine := &poisonApplySM{}
	create := func(shardID, replicaID uint64) interface{} { return machine }
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}

	var info config.LeaderInfo
	select {
	case info = <-readyC:
	case <-time.After(10 * time.Second):
		t.Fatal("OnLeaderReady never fired for a single-voter shard")
	}
	if info.ShardID != 1 || info.ReplicaID != 1 || info.LeaderID != 1 || info.Term == 0 {
		t.Fatalf("OnLeaderReady info = %+v", info)
	}
	leaderID, term, ready, err := h.ShardReadReady(1)
	if err != nil || !ready || leaderID != info.LeaderID || term != info.Term {
		t.Fatalf("ShardReadReady after OnLeaderReady = (%d, %d, %v, %v), want (%d, %d, true, nil)",
			leaderID, term, ready, err, info.LeaderID, info.Term)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := h.SyncRead(ctx, 1, "any-key"); err != nil {
		t.Fatalf("SyncRead after OnLeaderReady: %v", err)
	}

	latchTheShard(t, h, failed)
	if _, _, _, err := h.ShardReadReady(1); !errors.Is(err, ErrShardFailed) {
		t.Fatalf("ShardReadReady on a latched shard = %v, want ErrShardFailed", err)
	}
	select {
	case extra := <-readyC:
		t.Fatalf("OnLeaderReady fired twice in one term: %+v", extra)
	default:
	}
}
