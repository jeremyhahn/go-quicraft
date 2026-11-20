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

// expiredUndoneCtx is a context whose deadline has passed but whose Done
// channel has not closed yet: the instant the sweeper expires a request at
// the caller's deadline, before the context's own timer fires.
type expiredUndoneCtx struct {
	context.Context
	deadline time.Time
}

func (c expiredUndoneCtx) Deadline() (time.Time, bool) { return c.deadline, true }
func (c expiredUndoneCtx) Done() <-chan struct{}       { return nil }
func (c expiredUndoneCtx) Err() error                  { return nil }

// TestAwaitResult_SweeperExpiryKeepsDeadlineExceeded: when the sweeper
// completes a request with ErrTimeout at the caller's context deadline, the
// result branch wins the wait, and the error must still match
// context.DeadlineExceeded (pkg/errors.go ErrTimeout), bare or inside a
// MaybeAppliedError.
func TestAwaitResult_SweeperExpiryKeepsDeadlineExceeded(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		maybe bool
	}{
		{"bare ErrTimeout", ErrTimeout, false},
		{"maybe-applied ErrTimeout", &MaybeAppliedError{Err: ErrTimeout}, true},
	}
	pool := NewRequestStatePool()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deadline := time.Now().Add(-time.Millisecond)
			rs := newRequestState(pool, 1, deadline, false)
			defer rs.Release()
			rs.complete(RequestResult{Err: tc.err})

			ctx := expiredUndoneCtx{Context: context.Background(), deadline: deadline}
			result, err := awaitResult(ctx, rs)
			checkSweptDeadlineErr(t, err, tc.maybe)
			if !errors.Is(result.Err, context.DeadlineExceeded) {
				t.Fatalf("result.Err = %v, want it to match context.DeadlineExceeded", result.Err)
			}
		})
	}
}

// TestRequestStateResult_TimeoutHasNoContextCause pins the asynchronous half
// of the ErrTimeout contract: RequestState.Result has no caller context, so a
// timeout it reports, whether its own deadline expiry or a swept result,
// matches ErrTimeout but not context.DeadlineExceeded. Only the synchronous
// wrappers (awaitResult and withDeadlineCause) add the context cause.
func TestRequestStateResult_TimeoutHasNoContextCause(t *testing.T) {
	pool := NewRequestStatePool()
	cases := []struct {
		name     string
		complete bool
	}{
		{"expired deadline", false},
		{"swept ErrTimeout", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rs := newRequestState(pool, 1, time.Now().Add(-time.Millisecond), false)
			defer rs.Release()
			if tc.complete {
				rs.complete(RequestResult{Err: ErrTimeout})
			}
			_, err := rs.Result()
			if !errors.Is(err, ErrTimeout) {
				t.Fatalf("Result() err = %v, want ErrTimeout", err)
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Result() err = %v matches context.DeadlineExceeded; only the synchronous wrappers add it", err)
			}
		})
	}
}

// TestAwaitResult_ExpiredContextEitherBranch: with the request swept and
// the caller's context expired, both select branches are ready; whichever
// wins, the error matches ErrTimeout and context.DeadlineExceeded.
func TestAwaitResult_ExpiredContextEitherBranch(t *testing.T) {
	pool := NewRequestStatePool()
	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
		dl, _ := ctx.Deadline()
		rs := newRequestState(pool, uint64(i)+1, dl, false)
		rs.complete(RequestResult{Err: ErrTimeout})
		_, err := awaitResult(ctx, rs)
		cancel()
		rs.Release()
		checkSweptDeadlineErr(t, err, false)
	}
}

// TestAwaitResult_LeavesOtherResultsAlone: only a timeout reached at or
// past the caller's own deadline gains context.DeadlineExceeded; any other
// delivered result is returned unchanged.
func TestAwaitResult_LeavesOtherResultsAlone(t *testing.T) {
	past := expiredUndoneCtx{Context: context.Background(), deadline: time.Now().Add(-time.Second)}
	future, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	cases := []struct {
		name string
		ctx  context.Context
		err  error
	}{
		{"nil error", past, nil},
		{"other error", past, ErrRejected},
		{"deadline not reached", future, ErrTimeout},
		{"no deadline", context.Background(), ErrTimeout},
	}
	pool := NewRequestStatePool()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rs := newRequestState(pool, 1, time.Time{}, false)
			defer rs.Release()
			rs.complete(RequestResult{Err: tc.err})
			if _, got := awaitResult(tc.ctx, rs); got != tc.err {
				t.Fatalf("awaitResult with result %v = %v, want it unchanged", tc.err, got)
			}
		})
	}
}

func checkSweptDeadlineErr(t *testing.T, err error, maybe bool) {
	t.Helper()
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("err = %v, want it to match ErrTimeout", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want it to match context.DeadlineExceeded", err)
	}
	var mae *MaybeAppliedError
	if got := errors.As(err, &mae); got != maybe {
		t.Fatalf("err = %v: MaybeAppliedError = %v, want %v", err, got, maybe)
	}
}
