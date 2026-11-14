// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"maps"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// asyncMembershipCase is one async membership call paired with its Sync
// form. setup, when set, runs (through the Sync API) before the call under
// test; applied reports whether the membership shows the call's effect.
type asyncMembershipCase struct {
	name    string
	replica uint64
	setup   func(ctx context.Context, h *Host, shardID uint64) error
	async   func(ctx context.Context, h *Host, shardID, cci uint64) (*RequestState, error)
	sync    func(ctx context.Context, h *Host, shardID, cci uint64) error
	applied func(m *Membership) bool
}

func asyncMembershipCases() []asyncMembershipCase {
	const (
		observerID = 10
		witnessID  = 20
		voterID    = 30
	)
	inNone := func(m *Membership, id uint64) bool {
		_, n := m.Nodes[id]
		_, o := m.Observers[id]
		_, w := m.Witnesses[id]
		return !n && !o && !w
	}
	return []asyncMembershipCase{
		{
			name:    "AddObserver",
			replica: observerID,
			async: func(ctx context.Context, h *Host, shardID, cci uint64) (*RequestState, error) {
				return h.RequestAddObserver(ctx, shardID, observerID, "127.0.0.1:6010", cci)
			},
			sync: func(ctx context.Context, h *Host, shardID, cci uint64) error {
				return h.SyncRequestAddObserver(ctx, shardID, observerID, "127.0.0.1:6010", cci)
			},
			applied: func(m *Membership) bool {
				_, ok := m.Observers[observerID]
				_, voter := m.Nodes[observerID]
				return ok && !voter
			},
		},
		{
			name:    "RemoveNode",
			replica: observerID,
			setup: func(ctx context.Context, h *Host, shardID uint64) error {
				m, err := h.SyncGetShardMembership(ctx, shardID)
				if err != nil {
					return err
				}
				return h.SyncRequestAddObserver(ctx, shardID, observerID, "127.0.0.1:6010", m.ConfigChangeID)
			},
			async: func(ctx context.Context, h *Host, shardID, cci uint64) (*RequestState, error) {
				return h.RequestRemoveNode(ctx, shardID, observerID, cci)
			},
			sync: func(ctx context.Context, h *Host, shardID, cci uint64) error {
				return h.SyncRequestRemoveNode(ctx, shardID, observerID, cci)
			},
			applied: func(m *Membership) bool {
				_, removed := m.Removed[observerID]
				return removed && inNone(m, observerID)
			},
		},
		{
			name:    "AddWitness",
			replica: witnessID,
			async: func(ctx context.Context, h *Host, shardID, cci uint64) (*RequestState, error) {
				return h.RequestAddWitness(ctx, shardID, witnessID, "127.0.0.1:6020", cci)
			},
			sync: func(ctx context.Context, h *Host, shardID, cci uint64) error {
				return h.SyncRequestAddWitness(ctx, shardID, witnessID, "127.0.0.1:6020", cci)
			},
			applied: func(m *Membership) bool {
				_, ok := m.Witnesses[witnessID]
				_, voter := m.Nodes[witnessID]
				_, obs := m.Observers[witnessID]
				return ok && !voter && !obs
			},
		},
		{
			name:    "AddNode",
			replica: voterID,
			async: func(ctx context.Context, h *Host, shardID, cci uint64) (*RequestState, error) {
				return h.RequestAddNode(ctx, shardID, voterID, "127.0.0.1:6030", cci)
			},
			sync: func(ctx context.Context, h *Host, shardID, cci uint64) error {
				return h.SyncRequestAddNode(ctx, shardID, voterID, "127.0.0.1:6030", cci)
			},
			applied: func(m *Membership) bool {
				_, ok := m.Nodes[voterID]
				_, obs := m.Observers[voterID]
				_, wit := m.Witnesses[voterID]
				return ok && !obs && !wit
			},
		},
	}
}

// startAsyncMembershipShard starts a fresh single-voter shard on h and waits
// for it to elect itself.
func startAsyncMembershipShard(t *testing.T, h *Host, shardID uint64) {
	t.Helper()
	scfg := testShardConfig(shardID, 1)
	scfg.CheckQuorum = config.BoolPtr(false)
	// A witness beside the one voter leaves full replicas without a majority
	// of the voting set, which the default witness policy refuses.
	scfg.Witness.RequireFullReplicaMajority = config.BoolPtr(false)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard(%d): %v", shardID, err)
	}
	waitForLeader(t, h, shardID)
}

// awaitLocalMembership polls the local ShardMembership until cond holds. A
// voting add leaves a single-voter shard without a quorum, so the
// linearizable SyncGetShardMembership cannot be used after one.
func awaitLocalMembership(ctx context.Context, t *testing.T, h *Host, shardID uint64, cond func(*Membership) bool) *Membership {
	t.Helper()
	var last *Membership
	for {
		m, err := h.ShardMembership(shardID)
		if err == nil {
			last = m
			if cond(m) {
				return m
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("shard %d membership never reached the expected state: last=%+v err=%v", shardID, last, err)
			return nil
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// sameRoles reports whether two memberships hold the same replicas in the
// same roles, at the same addresses.
func sameRoles(a, b *Membership) bool {
	return maps.Equal(a.Nodes, b.Nodes) &&
		maps.Equal(a.Observers, b.Observers) &&
		maps.Equal(a.Witnesses, b.Witnesses) &&
		maps.Equal(a.Removed, b.Removed)
}

// TestAsyncMembership_CompletesWithTheSyncEffect drives each async membership
// call through its RequestState on a fresh shard, and its Sync form on a twin
// shard, and checks both reach the same membership.
func TestAsyncMembership_CompletesWithTheSyncEffect(t *testing.T) {
	h := newTestHost(t)
	shardID := uint64(100)
	for _, tc := range asyncMembershipCases() {
		asyncShard, syncShard := shardID, shardID+1
		shardID += 2
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			results := make(map[uint64]*Membership, 2)
			for _, sid := range []uint64{asyncShard, syncShard} {
				startAsyncMembershipShard(t, h, sid)
				if tc.setup != nil {
					if err := tc.setup(ctx, h, sid); err != nil {
						t.Fatalf("setup on shard %d: %v", sid, err)
					}
				}
				before, err := h.SyncGetShardMembership(ctx, sid)
				if err != nil {
					t.Fatalf("SyncGetShardMembership(%d): %v", sid, err)
				}
				if tc.applied(before) {
					t.Fatalf("shard %d already shows the effect before the call: %+v", sid, before)
				}

				if sid == asyncShard {
					rs, err := tc.async(ctx, h, sid, before.ConfigChangeID)
					if err != nil {
						t.Fatalf("async %s: %v", tc.name, err)
					}
					if rs == nil {
						t.Fatalf("async %s returned a nil *RequestState with no error", tc.name)
					}
					if _, err := rs.Result(); err != nil {
						t.Fatalf("async %s Result: %v", tc.name, err)
					}
					rs.Release()
				} else {
					if err := tc.sync(ctx, h, sid, before.ConfigChangeID); err != nil {
						t.Fatalf("sync %s: %v", tc.name, err)
					}
				}

				after := awaitLocalMembership(ctx, t, h, sid, tc.applied)
				if after.ConfigChangeID <= before.ConfigChangeID {
					t.Fatalf("shard %d ConfigChangeID did not advance: before=%d after=%d",
						sid, before.ConfigChangeID, after.ConfigChangeID)
				}
				results[sid] = after
			}

			if !sameRoles(results[asyncShard], results[syncShard]) {
				t.Fatalf("async and sync %s differ:\nasync=%+v\nsync=%+v",
					tc.name, results[asyncShard], results[syncShard])
			}
		})
	}
}

// TestAsyncMembership_StaleConfigChangeIndexRefusedAtOnce checks that each
// async form called with a configChangeIndex older than the current one is
// refused with *StaleConfigChangeError and a nil *RequestState, and changes
// nothing.
func TestAsyncMembership_StaleConfigChangeIndexRefusedAtOnce(t *testing.T) {
	h := newTestHost(t)
	const shardID = 200
	startAsyncMembershipShard(t, h, shardID)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	m0, err := h.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}
	// Two observer adds: the index after the first is non-zero and older
	// than the current one (zero would skip the check).
	if err := h.SyncRequestAddObserver(ctx, shardID, 10, "127.0.0.1:6010", m0.ConfigChangeID); err != nil {
		t.Fatalf("first SyncRequestAddObserver: %v", err)
	}
	m1, err := h.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}
	if err := h.SyncRequestAddObserver(ctx, shardID, 11, "127.0.0.1:6011", m1.ConfigChangeID); err != nil {
		t.Fatalf("second SyncRequestAddObserver: %v", err)
	}
	current, err := h.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}
	stale := m1.ConfigChangeID
	if stale == 0 || stale >= current.ConfigChangeID {
		t.Fatalf("no usable stale index: stale=%d current=%d", stale, current.ConfigChangeID)
	}

	for _, tc := range asyncMembershipCases() {
		t.Run(tc.name, func(t *testing.T) {
			rs, err := tc.async(ctx, h, shardID, stale)
			if rs != nil {
				rs.Release()
				t.Fatalf("stale %s returned a non-nil *RequestState", tc.name)
			}
			var se *StaleConfigChangeError
			if !errors.As(err, &se) {
				t.Fatalf("stale %s: got %v (%T), want *StaleConfigChangeError", tc.name, err, err)
			}
			if se.ShardID != shardID || se.Got != stale || se.Expected != current.ConfigChangeID {
				t.Fatalf("stale %s: error fields %+v, want shard=%d got=%d expected=%d",
					tc.name, se, shardID, stale, current.ConfigChangeID)
			}
		})
	}

	after, err := h.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}
	if after.ConfigChangeID != current.ConfigChangeID || !sameRoles(after, current) {
		t.Fatalf("stale calls changed the membership:\nbefore=%+v\nafter=%+v", current, after)
	}
}
