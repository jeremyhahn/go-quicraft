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

package proposer

import (
	"errors"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// These tests pin the ONE loop every exported form is built on. They script
// the attempt rather than standing up a cluster, so they run untagged under
// go test ./... and guard the loop on every CI job; the netfault fixture in
// test/integration/host proves the same rule against a real deposed leader.

// host is a resolvable target the scripted attempts never dereference.
var host = &quicraft.Host{}

// preflightNotLeader is the refusal handleFollowerReadIndex and the propose
// path produce when the addressed replica does not know the leader.
func preflightNotLeader() error {
	return &quicraft.NotLeaderError{ShardID: 1}
}

// postDeliveryNotLeader is what a pending request sees when the leader it was
// delivered to loses the shard.
func postDeliveryNotLeader() error {
	return &quicraft.MaybeAppliedError{Err: &quicraft.NotLeaderError{ShardID: 1, LeaderID: 2}}
}

// countingResolver returns host every time and counts how often it is asked.
func countingResolver(calls *int) Resolver {
	return func(uint64) *quicraft.Host {
		*calls++
		return host
	}
}

// scripted returns each error in turn, then succeeds with value.
func scripted(attempts *int, value string, errs ...error) func(*quicraft.Host, time.Duration) (string, error) {
	return func(*quicraft.Host, time.Duration) (string, error) {
		i := *attempts
		*attempts++
		if i < len(errs) {
			return "", errs[i]
		}
		return value, nil
	}
}

// TestDrive_ReResolvesBeforeEveryAttempt is the mutation guard for the class
// this package exists to end. Hoisting the resolve out of the loop makes the
// second attempt go to the first attempt's replica; this test sees that as a
// resolver asked once for two attempts.
func TestDrive_ReResolvesBeforeEveryAttempt(t *testing.T) {
	var resolves, attempts int
	got, err := drive(countingResolver(&resolves), 1, time.Second, IsNotLeaderRejection,
		scripted(&attempts, "ok", preflightNotLeader()))
	if err != nil {
		t.Fatalf("drive returned %v, want success after one not-leader rejection", err)
	}
	if got != "ok" {
		t.Fatalf("drive returned %q, want %q", got, "ok")
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (one refusal, one success)", attempts)
	}
	if resolves != attempts {
		t.Fatalf("resolver asked %d time(s) for %d attempt(s): the leader must be "+
			"resolved before EVERY attempt, not once before the loop", resolves, attempts)
	}
}

// TestDrive_PostDeliveryNotLeaderIsRetried covers the form a pending read or
// proposal sees when the leader loses the shard underneath it, which is wrapped in MaybeAppliedError and
// must still be recognised as the not-leader rejection.
func TestDrive_PostDeliveryNotLeaderIsRetried(t *testing.T) {
	var resolves, attempts int
	_, err := drive(countingResolver(&resolves), 1, time.Second, IsNotLeaderRejection,
		scripted(&attempts, "ok", postDeliveryNotLeader()))
	if err != nil {
		t.Fatalf("drive returned %v, want success after a post-delivery not-leader", err)
	}
	if attempts != 2 || resolves != 2 {
		t.Fatalf("attempts = %d, resolves = %d, want 2 and 2", attempts, resolves)
	}
}

// TestDrive_RetriesOnlyTheNotLeaderRejection: every other error is a real
// result from the replica that genuinely leads, and re-resolving cannot change
// it, so it reaches the caller on its first occurrence.
func TestDrive_RetriesOnlyTheNotLeaderRejection(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"timeout", quicraft.ErrTimeout},
		{"read index not ready", quicraft.ErrReadIndexNotReady},
		{"system busy", quicraft.ErrSystemBusy},
		{"rejected", quicraft.ErrRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var resolves, attempts int
			_, err := drive(countingResolver(&resolves), 1, time.Second, IsNotLeaderRejection,
				scripted(&attempts, "never", tc.err, tc.err))
			if !errors.Is(err, tc.err) {
				t.Fatalf("drive returned %v, want %v unchanged", err, tc.err)
			}
			if attempts != 1 {
				t.Fatalf("attempts = %d, want 1: %v is not the not-leader rejection "+
					"and must not be retried", attempts, tc.err)
			}
		})
	}
}

// TestDrive_NoLeaderExhaustsBudgetWithoutAnAttempt: a resolver that never
// names a leader is itself a not-leader condition, reported as ErrNotLeader
// once the budget is spent and without ever calling attempt.
func TestDrive_NoLeaderExhaustsBudgetWithoutAnAttempt(t *testing.T) {
	var attempts int
	none := func(uint64) *quicraft.Host { return nil }
	start := time.Now()
	_, err := drive(none, 1, 100*time.Millisecond, IsNotLeaderRejection,
		scripted(&attempts, "never"))
	if !errors.Is(err, quicraft.ErrNotLeader) {
		t.Fatalf("drive returned %v, want ErrNotLeader", err)
	}
	if attempts != 0 {
		t.Fatalf("attempts = %d, want 0: there was never a host to attempt against", attempts)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("drive gave up after %v, before the %v budget", elapsed, 100*time.Millisecond)
	}
}

// TestDrive_BudgetBelowMinAttemptIsNotStarted: an attempt that would time out
// on arrival would overwrite the real cause with ErrTimeout, so the loop
// reports the last not-leader instead of starting it.
func TestDrive_BudgetBelowMinAttemptIsNotStarted(t *testing.T) {
	var resolves, attempts int
	_, err := drive(countingResolver(&resolves), 1, minAttempt/2, IsNotLeaderRejection,
		scripted(&attempts, "never"))
	if !errors.Is(err, quicraft.ErrNotLeader) {
		t.Fatalf("drive returned %v, want ErrNotLeader", err)
	}
	if attempts != 0 {
		t.Fatalf("attempts = %d, want 0 with a budget under minAttempt", attempts)
	}
}

// TestIsNotLeaderRejection pins the predicate's edges: both not-leader forms
// in, everything else out.
func TestIsNotLeaderRejection(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"sentinel", quicraft.ErrNotLeader, true},
		{"pre-flight", preflightNotLeader(), true},
		{"post-delivery", postDeliveryNotLeader(), true},
		{"timeout", quicraft.ErrTimeout, false},
		{"read index not ready", quicraft.ErrReadIndexNotReady, false},
		{"closed", quicraft.ErrClosed, false},
	} {
		if got := IsNotLeaderRejection(tc.err); got != tc.want {
			t.Errorf("IsNotLeaderRejection(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestDrive_PinnedReadPolicyRetriesWhatOnlyWaitingFixes is the policy
// TryReadWith exists to serve.
//
// A pinned caller's remedy is "wait, and ask the SAME replica again", so its
// retryable is quicraft.Temporary rather than IsNotLeaderRejection. The
// difference is load-bearing for syncReadExact: right after an election the new
// leader rejects ReadIndex with ErrReadIndexNotReady until it commits its
// term's no-op, and that error is Temporary but is NOT a not-leader rejection.
// A TryReadWith wired to IsNotLeaderRejection instead of the caller's predicate
// would fail on the first such refusal.
func TestDrive_PinnedReadPolicyRetriesWhatOnlyWaitingFixes(t *testing.T) {
	notReady := quicraft.ErrReadIndexNotReady
	if IsNotLeaderRejection(notReady) {
		t.Fatal("ErrReadIndexNotReady is a not-leader rejection, so this test " +
			"cannot tell the two policies apart any more")
	}
	if !quicraft.Temporary(notReady) {
		t.Fatal("ErrReadIndexNotReady is not Temporary, so the pinned policy " +
			"would not retry it and syncReadExact's wait is unfounded")
	}

	var attempts int
	got, err := drive(Pinned(host), 1, time.Second, quicraft.Temporary,
		scripted(&attempts, "value", notReady, notReady))
	if err != nil {
		t.Fatalf("pinned read with quicraft.Temporary: %v", err)
	}
	if got != "value" || attempts != 3 {
		t.Fatalf("got %q after %d attempt(s), want \"value\" after 3", got, attempts)
	}

	// The re-resolving policy is the control: it must NOT retry this, or the
	// two Retryables are the same one and Pinned buys nothing.
	attempts = 0
	if _, err := drive(Pinned(host), 1, time.Second, IsNotLeaderRejection,
		scripted(&attempts, "value", notReady)); err == nil {
		t.Fatal("IsNotLeaderRejection retried ErrReadIndexNotReady; the pinned " +
			"and re-resolving policies must differ on exactly this")
	}
}
