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

	"github.com/jeremyhahn/go-quicraft/pkg/internal/rsm"
)

// TestAudit_MapLocalReadError_StateMachineClosed checks the StopShard
// interaction of the local read paths: a read that passed the stopped check
// and then found the state machine closed by StopShard reports the host's
// ErrClosed, as the stopped check itself does, not the internal rsm error
// callers cannot match.
func TestAudit_MapLocalReadError_StateMachineClosed(t *testing.T) {
	err := mapLocalReadError(rsm.ErrClosed)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("mapLocalReadError(rsm.ErrClosed) = %v, want errors.Is ErrClosed", err)
	}
}

// TestAudit_MapLocalReadError_ContextAndPassthrough checks the request
// deadline interaction: a raw context error from a read abandoned behind an
// Update carries both the quicraft sentinel and the context error, an error
// already carrying a sentinel is returned unchanged, and nil stays nil.
func TestAudit_MapLocalReadError_ContextAndPassthrough(t *testing.T) {
	if err := mapLocalReadError(nil); err != nil {
		t.Fatalf("mapLocalReadError(nil) = %v, want nil", err)
	}
	for _, tc := range []struct {
		in       error
		sentinel error
	}{
		{context.Canceled, ErrCanceled},
		{context.DeadlineExceeded, ErrTimeout},
	} {
		got := mapLocalReadError(tc.in)
		if !errors.Is(got, tc.sentinel) || !errors.Is(got, tc.in) {
			t.Fatalf("mapLocalReadError(%v) = %v, want errors.Is %v and %v", tc.in, got, tc.sentinel, tc.in)
		}
	}
	already := mapContextError(context.DeadlineExceeded)
	if got := mapLocalReadError(already); got != already {
		t.Fatalf("mapLocalReadError(mapped) = %v, want it unchanged", got)
	}
	other := errors.New("user lookup failed")
	if got := mapLocalReadError(other); got != other {
		t.Fatalf("mapLocalReadError(user error) = %v, want it unchanged", got)
	}
}

// TestAudit_WithDeadlineCause checks the request-deadline mapping of the
// Sync* waits: an ErrTimeout returned once the caller's deadline has passed
// also matches context.DeadlineExceeded (a *MaybeAppliedError keeps its
// type); a timeout without a deadline, or under a context that was
// cancelled rather than timed out, is unchanged.
func TestAudit_WithDeadlineCause(t *testing.T) {
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	got := withDeadlineCause(expired, ErrTimeout)
	if !errors.Is(got, ErrTimeout) || !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("withDeadlineCause(expired, ErrTimeout) = %v, want both ErrTimeout and DeadlineExceeded", got)
	}
	mae := &MaybeAppliedError{Err: ErrTimeout}
	got = withDeadlineCause(expired, mae)
	var gotMAE *MaybeAppliedError
	if !errors.As(got, &gotMAE) || !errors.Is(got, context.DeadlineExceeded) || !errors.Is(got, ErrTimeout) {
		t.Fatalf("withDeadlineCause(expired, MaybeApplied) = %T %v, want *MaybeAppliedError with both causes", got, got)
	}
	if got := withDeadlineCause(context.Background(), ErrTimeout); got != ErrTimeout {
		t.Fatalf("withDeadlineCause(no deadline) = %v, want ErrTimeout unchanged", got)
	}
	cancelled, cancelNow := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	cancelNow()
	if got := withDeadlineCause(cancelled, ErrTimeout); got != ErrTimeout {
		t.Fatalf("withDeadlineCause(cancelled) = %v, want ErrTimeout unchanged", got)
	}
	if got := withDeadlineCause(expired, ErrCanceled); got != ErrCanceled {
		t.Fatalf("withDeadlineCause(expired, ErrCanceled) = %v, want it unchanged", got)
	}
}

// TestAudit_LocalReadParkedBehindUpdateAcrossStopShard checks the StopShard
// interaction end to end: a QueryLocalNode parked behind a held Update when
// StopShard runs returns once the Update is released, with its result or the
// host's ErrClosed, never an internal error and never a hang.
func TestAudit_LocalReadParkedBehindUpdateAcrossStopShard(t *testing.T) {
	h, block, release := startBlockedShard(t)
	block()

	ctx, cancel := context.WithTimeout(context.Background(), contextCancelBound)
	defer cancel()
	readErr := make(chan error, 1)
	go func() {
		_, err := h.QueryLocalNode(ctx, 1, []byte("k"))
		readErr <- err
	}()
	// Let the read pass its stopped check and park behind the Update.
	time.Sleep(50 * time.Millisecond)

	stopErr := make(chan error, 1)
	go func() { stopErr <- h.StopShard(1) }()
	time.Sleep(50 * time.Millisecond)
	release()

	select {
	case err := <-stopErr:
		if err != nil {
			t.Fatalf("StopShard = %v", err)
		}
	case <-time.After(contextCancelBound):
		t.Fatal("StopShard did not return after the Update was released")
	}
	select {
	case err := <-readErr:
		if err != nil && !errors.Is(err, ErrClosed) {
			t.Fatalf("QueryLocalNode across StopShard = %v, want a result or ErrClosed", err)
		}
	case <-time.After(contextCancelBound):
		t.Fatal("QueryLocalNode parked behind the Update never returned after StopShard")
	}
}
