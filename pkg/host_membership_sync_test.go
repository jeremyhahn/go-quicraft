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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// startSingleVoterShard starts shard 1 with replica 1 as its only voter and
// waits for it to elect itself leader. CheckQuorum is disabled so a membership
// change that raises the voter count does not depose the leader before the
// test finishes.
func startSingleVoterShard(t *testing.T) *Host {
	t.Helper()
	h := newTestHost(t)
	scfg := testShardConfig(1, 1)
	scfg.CheckQuorum = config.BoolPtr(false)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)
	return h
}

// ---------------------------------------------------------------------------
// SyncRequestDeleteNode
// ---------------------------------------------------------------------------

// TestSyncRequestDeleteNode_RemovesObserver verifies the committed path of the
// public synchronous membership removal: an observer that was added through
// consensus is removed through consensus, the call returns only once the
// change is applied, and the shard membership no longer lists the replica.
// Observers do not count toward quorum, so the removal commits on the single
// voter without any peer being reachable.
func TestSyncRequestDeleteNode_RemovesObserver(t *testing.T) {
	h := startSingleVoterShard(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := h.SyncRequestAddObserver(ctx, 1, 10, "127.0.0.1:6000", 0); err != nil {
		t.Fatalf("SyncRequestAddObserver: %v", err)
	}

	added, err := h.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership after add: %v", err)
	}
	if _, ok := added.Observers[10]; !ok {
		t.Fatalf("observer 10 missing from membership after add: %+v", added.Observers)
	}

	if err := h.SyncRequestDeleteNode(ctx, 1, 10, added.ConfigChangeID); err != nil {
		t.Fatalf("SyncRequestDeleteNode: %v", err)
	}

	removed, err := h.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership after delete: %v", err)
	}
	if _, ok := removed.Observers[10]; ok {
		t.Fatalf("observer 10 still present after SyncRequestDeleteNode: %+v", removed.Observers)
	}
	if _, ok := removed.Nodes[10]; ok {
		t.Fatalf("replica 10 reappeared as a voter after removal: %+v", removed.Nodes)
	}
	if removed.ConfigChangeID == added.ConfigChangeID {
		t.Fatalf("ConfigChangeID did not advance across the removal (%d)", removed.ConfigChangeID)
	}
}

// TestSyncRequestDeleteNode_ContextEndsUncommittedWait verifies that a removal
// which cannot commit does not block the caller forever: the call is bounded
// by the caller's context and reports ErrCanceled / ErrTimeout accordingly.
//
// The setup makes the removal genuinely uncommittable: replica 4 is added as a
// voter (the add commits under the OLD single-voter quorum), after which the
// shard needs 2 of 2 acks and replica 4 does not exist.
func TestSyncRequestDeleteNode_ContextEndsUncommittedWait(t *testing.T) {
	tests := []struct {
		name    string
		ctxFn   func() (context.Context, context.CancelFunc)
		wantErr error
	}{
		{
			name: "canceled",
			ctxFn: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			wantErr: ErrCanceled,
		},
		{
			name: "deadline exceeded",
			ctxFn: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			wantErr: ErrTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := startSingleVoterShard(t)

			setupCtx, setupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer setupCancel()
			if err := h.SyncRequestAddNode(setupCtx, 1, 4, "127.0.0.1:6001", 0); err != nil {
				t.Fatalf("SyncRequestAddNode: %v", err)
			}
			// Read locally: with two voters and one of them absent, a
			// linearizable read can no longer reach quorum.
			members, err := h.GetShardMembership(1)
			if err != nil {
				t.Fatalf("GetShardMembership: %v", err)
			}
			if _, ok := members.Nodes[4]; !ok {
				t.Fatalf("replica 4 not a voter, removal would be rejected early: %+v", members.Nodes)
			}

			ctx, cancel := tt.ctxFn()
			defer cancel()

			err = h.SyncRequestDeleteNode(ctx, 1, 4, 0)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("SyncRequestDeleteNode error = %v, want %v", err, tt.wantErr)
			}

			// The uncommitted change must not have mutated local membership.
			after, err := h.GetShardMembership(1)
			if err != nil {
				t.Fatalf("GetShardMembership after abandoned removal: %v", err)
			}
			if _, ok := after.Nodes[4]; !ok {
				t.Fatalf("replica 4 removed from membership by an uncommitted config change: %+v", after.Nodes)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// waitResultCtx
// ---------------------------------------------------------------------------

// TestWaitResultCtx verifies the bridge used by the session APIs: a resolved
// request returns its result (and its embedded error) verbatim, while a
// context that ends first unblocks the caller with the mapped error rather
// than waiting on a request that will never resolve.
func TestWaitResultCtx(t *testing.T) {
	pool := NewRequestStatePool()

	t.Run("result delivered", func(t *testing.T) {
		rs := newRequestState(pool, 1, time.Time{}, false)
		defer rs.Release()
		rs.complete(RequestResult{Value: 42})

		result, err := waitResultCtx(context.Background(), rs)
		if err != nil {
			t.Fatalf("waitResultCtx error = %v, want nil", err)
		}
		if result.Value != 42 {
			t.Fatalf("result.Value = %d, want 42", result.Value)
		}
	})

	t.Run("result error returned", func(t *testing.T) {
		rs := newRequestState(pool, 2, time.Time{}, false)
		defer rs.Release()
		rs.complete(RequestResult{Err: ErrNotLeader})

		result, err := waitResultCtx(context.Background(), rs)
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("waitResultCtx error = %v, want ErrNotLeader", err)
		}
		if !errors.Is(result.Err, ErrNotLeader) {
			t.Fatalf("result.Err = %v, want ErrNotLeader", result.Err)
		}
	})

	t.Run("context canceled", func(t *testing.T) {
		rs := newRequestState(pool, 3, time.Time{}, false)
		defer rs.Release()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		result, err := waitResultCtx(ctx, rs)
		if !errors.Is(err, ErrCanceled) {
			t.Fatalf("waitResultCtx error = %v, want ErrCanceled", err)
		}
		if result.Value != 0 || result.Data != nil || result.Err != nil {
			t.Fatalf("result = %+v, want the zero value on cancellation", result)
		}
	})

	t.Run("context deadline exceeded", func(t *testing.T) {
		rs := newRequestState(pool, 4, time.Time{}, false)
		defer rs.Release()

		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		result, err := waitResultCtx(ctx, rs)
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("waitResultCtx error = %v, want ErrTimeout", err)
		}
		if result.Value != 0 || result.Data != nil || result.Err != nil {
			t.Fatalf("result = %+v, want the zero value on timeout", result)
		}
	})
}
