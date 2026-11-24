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
)

// StaleRead delegates to QueryLocalNode; these tests assert the two agree on every branch.

// staleReadCase is one input both read entry points must answer identically.
type staleReadCase struct {
	name string
	// host builds a host in whatever state the branch needs.
	host func(t *testing.T) *Host
	ctx  func(t *testing.T) (context.Context, context.CancelFunc)
	// shard and query are the arguments handed to both functions.
	shard uint64
	query interface{}
	// wantErr is the sentinel both must return, or nil for the success path.
	wantErr error
	// wantResult is the value both must return on the success path.
	wantResult interface{}
}

// runningShardHost starts shard 1 with a single voter and waits for it to
// serve, so the state machine behind it answers a Lookup.
func runningShardHost(t *testing.T) *Host {
	t.Helper()
	h := newTestHost(t)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
	return h
}

func backgroundCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.Background(), func() {}
}

var staleReadDelegationCases = []staleReadCase{
	{
		name:       "success",
		host:       runningShardHost,
		ctx:        backgroundCtx,
		shard:      1,
		query:      "echo-me",
		wantResult: "echo-me",
	},
	{
		name:    "shard not found",
		host:    runningShardHost,
		ctx:     backgroundCtx,
		shard:   999,
		query:   "echo-me",
		wantErr: ErrShardNotFound,
	},
	{
		name: "closed host",
		host: func(t *testing.T) *Host {
			t.Helper()
			h := runningShardHost(t)
			h.Close()
			return h
		},
		ctx:     backgroundCtx,
		shard:   1,
		query:   "echo-me",
		wantErr: ErrClosed,
	},
	{
		name: "shard not ready",
		host: func(t *testing.T) *Host {
			t.Helper()
			h := newTestHost(t)
			addTestNodeState(h, 1, 1)
			testLoadNode(h, 1).rsm = nil
			return h
		},
		ctx:     backgroundCtx,
		shard:   1,
		query:   "echo-me",
		wantErr: ErrShardNotReady,
	},
	{
		// An already-cancelled context on a live echoing shard.
		name: "context already canceled",
		host: runningShardHost,
		ctx: func(t *testing.T) (context.Context, context.CancelFunc) {
			t.Helper()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, cancel
		},
		shard:   1,
		query:   "echo-me",
		wantErr: ErrCanceled,
	},
	{
		name: "context deadline already exceeded",
		host: runningShardHost,
		ctx: func(t *testing.T) (context.Context, context.CancelFunc) {
			t.Helper()
			return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		},
		shard:   1,
		query:   "echo-me",
		wantErr: ErrTimeout,
	},
}

// TestStaleReadAndQueryLocalNodeAgree requires both entry points to give the same, expected answer on every branch.
func TestStaleReadAndQueryLocalNodeAgree(t *testing.T) {
	for _, tc := range staleReadDelegationCases {
		t.Run(tc.name, func(t *testing.T) {
			h := tc.host(t)
			ctx, cancel := tc.ctx(t)
			defer cancel()

			staleResult, staleErr := h.StaleRead(ctx, tc.shard, tc.query)
			queryResult, queryErr := h.QueryLocalNode(ctx, tc.shard, tc.query)

			if (staleErr == nil) != (queryErr == nil) {
				t.Fatalf("StaleRead err = %v, QueryLocalNode err = %v: one "+
					"refused and the other did not", staleErr, queryErr)
			}
			if staleErr != nil {
				if !errors.Is(staleErr, queryErr) || !errors.Is(queryErr, staleErr) {
					t.Fatalf("StaleRead err = %v, QueryLocalNode err = %v: the two "+
						"entry points have drifted apart", staleErr, queryErr)
				}
				if staleErr.Error() != queryErr.Error() {
					t.Fatalf("StaleRead err text %q != QueryLocalNode err text %q",
						staleErr.Error(), queryErr.Error())
				}
			}
			if staleResult != queryResult {
				t.Fatalf("StaleRead result = %v, QueryLocalNode result = %v",
					staleResult, queryResult)
			}

			if tc.wantErr != nil {
				if !errors.Is(staleErr, tc.wantErr) {
					t.Fatalf("err = %v, want %v", staleErr, tc.wantErr)
				}
				if staleResult != nil {
					t.Fatalf("result = %v on the %s branch, want nil",
						staleResult, tc.name)
				}
				return
			}
			if staleErr != nil {
				t.Fatalf("err = %v, want nil", staleErr)
			}
			if staleResult != tc.wantResult {
				t.Fatalf("result = %v, want %v", staleResult, tc.wantResult)
			}
		})
	}
}
