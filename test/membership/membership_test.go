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

package membership_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/test/membership"
)

// ---------------------------------------------------------------------------
// Mutation tests for the retry decision
//
// These drive membership.Drive with scripted attempts and a scripted
// membership, so every branch the idempotence gate turns on is pinned WITHOUT
// a cluster or a loaded box. The two that define the line are
// TestRetry_AlreadyExistsAtDifferentAddressStillFails (a fix that confirms the
// replica ID without the address passes every other test and fails that one)
// and the two *WithoutSubmissionStillFails tests (a fix that swallows the
// refusal without the submission bit passes every other test and fails those).
// ---------------------------------------------------------------------------

// scriptedAttempt returns an attempt function that yields errs in order and
// then repeats the last one, recording how many times it was called.
func scriptedAttempt(calls *int, errs ...error) func(context.Context) error {
	return func(context.Context) error {
		i := *calls
		*calls++
		if i >= len(errs) {
			i = len(errs) - 1
		}
		return errs[i]
	}
}

// fixedMembership returns a readMembership function that always reports m.
func fixedMembership(m *quicraft.Membership) func(context.Context) (*quicraft.Membership, error) {
	return func(context.Context) (*quicraft.Membership, error) { return m, nil }
}

// quicraftAlreadyExists and quicraftNodeNotFound reproduce the two engine
// refusals the gate is about.
//
// They are constructed by text rather than imported on purpose: the driver
// deliberately does NOT branch on these identities. It decides from the
// submission bit and a linearizable membership read, so any non-retryable
// refusal exercises the same path these do, and a test that used the exported
// sentinels would be pinning a dependency the driver does not have.
func quicraftAlreadyExists() error {
	return errors.New("raft: node already exists in membership")
}

func quicraftNodeNotFound() error {
	return errors.New("raft: target node not found in membership")
}

// postDeliveryNotLeader is the error the engine hands a caller whose config
// change was ALREADY delivered to raft when leadership moved. Host builds
// exactly this shape in OnLeaderChanged.
func postDeliveryNotLeader() error {
	return &quicraft.MaybeAppliedError{
		Err: &quicraft.NotLeaderError{ShardID: 1, LeaderID: 2},
	}
}

// preflightNotLeader is the refusal produced BEFORE anything is submitted. It
// is textually indistinguishable from postDeliveryNotLeader and must not be
// treated as evidence that a change may have landed.
func preflightNotLeader() error {
	return &quicraft.NotLeaderError{ShardID: 1, LeaderID: 2}
}

// TestRetry_AlreadyExistsAfterPostDeliveryLossIsSuccess is the benign
// reading of "already exists": the first add was delivered, leadership moved, the retry was
// correctly rejected, and the membership shows exactly what was asked for.
func TestRetry_AlreadyExistsAfterPostDeliveryLossIsSuccess(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:59436"}}

	err := membership.Drive("add", 2*time.Second,
		scriptedAttempt(&calls, postDeliveryNotLeader(), quicraftAlreadyExists()),
		fixedMembership(m),
		membership.AddNodeLanded(11, "127.0.0.1:59436"))
	if err != nil {
		t.Fatalf("a retry of an add that already committed must succeed, got: %v", err)
	}
}

// TestRetry_TimeoutIsASubmission pins the second producer of the submission
// bit: SyncRequestAddNode can only return ErrTimeout after the entry was
// handed to the engine, so a timeout followed by "already exists" at the
// requested address is a landed retry.
func TestRetry_TimeoutIsASubmission(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:59436"}}

	err := membership.Drive("add", 2*time.Second,
		scriptedAttempt(&calls, quicraft.ErrTimeout, quicraftAlreadyExists()),
		fixedMembership(m),
		membership.AddNodeLanded(11, "127.0.0.1:59436"))
	if err != nil {
		t.Fatalf("an 'already exists' after a post-delivery timeout must succeed, got: %v", err)
	}
}

// scriptedMembership returns a readMembership function that fails with errs in
// order, one per read, and then reports m.
func scriptedMembership(m *quicraft.Membership, errs ...error) func(context.Context) (*quicraft.Membership, error) {
	reads := 0
	return func(context.Context) (*quicraft.Membership, error) {
		i := reads
		reads++
		if i < len(errs) {
			return nil, errs[i]
		}
		return m, nil
	}
}

// TestRetry_AlreadyExistsWithAFailedReadKeepsLooking is the applied-but-
// unacknowledged add on a loaded cluster: the add is delivered and its wait
// lost, the retry is refused "already exists" because the first add
// committed, and the reads that would confirm it time out. A failed read
// decides nothing, so the refusal must not fail the loop; the next read that
// succeeds shows the add in effect.
func TestRetry_AlreadyExistsWithAFailedReadKeepsLooking(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:59436"}}

	err := membership.Drive("add", 2*time.Second,
		scriptedAttempt(&calls, quicraft.ErrTimeout, quicraftAlreadyExists()),
		scriptedMembership(m, quicraft.ErrTimeout, quicraft.ErrTimeout),
		membership.AddNodeLanded(11, "127.0.0.1:59436"))
	if err != nil {
		t.Fatalf("an 'already exists' whose confirming read failed must be read again, got: %v", err)
	}
}

// TestRetry_SubmittedRefusalNeverConfirmedIsExhausted: keeping on looking is
// not success. When no read ever succeeds the change is never shown in effect,
// and the loop ends exhausted with the refusal as its last error.
func TestRetry_SubmittedRefusalNeverConfirmedIsExhausted(t *testing.T) {
	calls := 0
	unreadable := func(context.Context) (*quicraft.Membership, error) { return nil, quicraft.ErrTimeout }

	err := membership.Drive("add", 300*time.Millisecond,
		scriptedAttempt(&calls, quicraft.ErrTimeout, quicraftAlreadyExists()),
		unreadable,
		membership.AddNodeLanded(11, "127.0.0.1:59436"))
	var exhausted *membership.ExhaustedError
	if !errors.As(err, &exhausted) {
		t.Fatalf("a submitted add never confirmed must be exhausted, got: %v", err)
	}
	if !strings.Contains(exhausted.Last.Error(), "already exists") {
		t.Fatalf("exhausted with last error %v, want the refusal", exhausted.Last)
	}
}

// TestRetry_SubmittedRefusalShownNotInEffectFails: once a read succeeds and
// the change is not in effect, the refusal is real and fails at once.
func TestRetry_SubmittedRefusalShownNotInEffectFails(t *testing.T) {
	calls := 0
	err := membership.Drive("add", 2*time.Second,
		scriptedAttempt(&calls, quicraft.ErrTimeout, quicraftAlreadyExists()),
		fixedMembership(&quicraft.Membership{Nodes: map[uint64]string{}}),
		membership.AddNodeLanded(11, "127.0.0.1:59436"))
	var failed *membership.FailedError
	if !errors.As(err, &failed) {
		t.Fatalf("a refusal a successful read does not explain must fail, got: %v", err)
	}
}

// TestRetry_AlreadyExistsAtDifferentAddressStillFails is THE mutation guard.
// Peer.validateAddNode answers ErrNodeAlreadyExists from the replica ID
// alone, so "already exists" is also what a genuine replica-ID collision looks
// like. Confirming the ID without the address would make this pass, which is
// precisely the blanket swallow the gate exists to avoid.
func TestRetry_AlreadyExistsAtDifferentAddressStillFails(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:1234"}}

	err := membership.Drive("add", 2*time.Second,
		scriptedAttempt(&calls, postDeliveryNotLeader(), quicraftAlreadyExists()),
		fixedMembership(m),
		membership.AddNodeLanded(11, "127.0.0.1:59436"))
	if err == nil {
		t.Fatal("an 'already exists' for a DIFFERENT address is a membership conflict and must fail")
	}
	var conflict *membership.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("the failure must be a *ConflictError, got %T: %v", err, err)
	}
	if conflict.Addr != "127.0.0.1:1234" || conflict.Requested != "127.0.0.1:59436" {
		t.Errorf("conflict must carry the holding and requested addresses, got %+v", conflict)
	}
	if !strings.Contains(err.Error(), "127.0.0.1:1234") {
		t.Errorf("the failure must name the address actually holding the replica ID, got: %v", err)
	}
}

// TestRetry_AlreadyExistsWithoutSubmissionStillFails covers the other half of
// the gate: nothing this loop sent ever reached raft, so an "already exists"
// is somebody else's member, not our landed retry — even when the address
// matches.
func TestRetry_AlreadyExistsWithoutSubmissionStillFails(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:59436"}}

	err := membership.Drive("add", 2*time.Second,
		scriptedAttempt(&calls, preflightNotLeader(), quicraftAlreadyExists()),
		fixedMembership(m),
		membership.AddNodeLanded(11, "127.0.0.1:59436"))
	if err == nil {
		t.Fatal("an 'already exists' with no prior in-flight submission must fail")
	}
	var failed *membership.FailedError
	if !errors.As(err, &failed) {
		t.Fatalf("expected a *FailedError, got %T: %v", err, err)
	}
}

// TestRetry_CanceledIsNotASubmission: ErrCanceled is retryable but is
// produced before anything is submitted, so it must not arm the gate.
func TestRetry_CanceledIsNotASubmission(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:59436"}}

	err := membership.Drive("add", 2*time.Second,
		scriptedAttempt(&calls, quicraft.ErrCanceled, quicraftAlreadyExists()),
		fixedMembership(m),
		membership.AddNodeLanded(11, "127.0.0.1:59436"))
	if err == nil {
		t.Fatal("a cancellation submits nothing; the following 'already exists' must fail")
	}
}

// TestRetry_AddThatNeverLandsStillFails is the issue's own verification
// clause: make the add genuinely fail, and the loop must still go red. The
// attempt keeps reporting a post-delivery loss and the membership never shows
// the replica.
func TestRetry_AddThatNeverLandsStillFails(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{1: "127.0.0.1:1"}}

	err := membership.Drive("add", 400*time.Millisecond,
		scriptedAttempt(&calls, postDeliveryNotLeader()),
		fixedMembership(m),
		membership.AddNodeLanded(11, "127.0.0.1:59436"))
	if err == nil {
		t.Fatal("an add that never appears in the membership must fail, not be swallowed")
	}
	var exhausted *membership.ExhaustedError
	if !errors.As(err, &exhausted) {
		t.Fatalf("expected the budget-exhausted failure, got %T: %v", err, err)
	}
	if _, ok := quicraft.NotLeader(exhausted.Last); !ok {
		t.Errorf("the exhausted error must carry the last attempt's error, got: %v", exhausted.Last)
	}
	if calls < 2 {
		t.Errorf("expected the loop to retry, it attempted %d time(s)", calls)
	}
}

// TestRetry_WrongRoleIsAConflict pins the remaining shapes of "present, but
// not what I asked for".
func TestRetry_WrongRoleIsAConflict(t *testing.T) {
	addr := "127.0.0.1:59436"
	cases := map[string]*quicraft.Membership{
		"observer": {Observers: map[uint64]string{11: addr}},
		"witness":  {Witnesses: map[uint64]string{11: addr}},
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			landed, conflict := membership.AddNodeLanded(11, addr)(m)
			if landed {
				t.Fatalf("a %s is not the full voting member the add requested", name)
			}
			var c *membership.ConflictError
			if !errors.As(conflict, &c) {
				t.Fatalf("a %s occupying the replica ID must be reported as a *ConflictError, got %v", name, conflict)
			}
			if c.Role != name {
				t.Errorf("conflict role = %q, want %q", c.Role, name)
			}
		})
	}
}

// TestRetry_NilMembershipIsNotEvidence: a read that yields nothing confirms
// nothing, in either direction.
func TestRetry_NilMembershipIsNotEvidence(t *testing.T) {
	if landed, conflict := membership.AddNodeLanded(11, "127.0.0.1:1")(nil); landed || conflict != nil {
		t.Errorf("AddNodeLanded(nil) = (%v, %v), want (false, nil)", landed, conflict)
	}
	if landed, conflict := membership.DeleteNodeLanded(11)(nil); landed || conflict != nil {
		t.Errorf("DeleteNodeLanded(nil) = (%v, %v), want (false, nil)", landed, conflict)
	}
}

// TestRetry_DeleteNotFoundAfterPostDeliveryLossIsSuccess is the remove path's
// mirror of the post-delivery loss, observed as "raft: target node not found
// in membership".
func TestRetry_DeleteNotFoundAfterPostDeliveryLossIsSuccess(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{2: "127.0.0.1:2"}}

	err := membership.Drive("delete", 2*time.Second,
		scriptedAttempt(&calls, postDeliveryNotLeader(), quicraftNodeNotFound()),
		fixedMembership(m),
		membership.DeleteNodeLanded(3))
	if err != nil {
		t.Fatalf("a retry of a remove that already committed must succeed, got: %v", err)
	}
}

// TestRetry_DeleteWithoutSubmissionStillFails: a remove that was never
// submitted, against a replica that was never a member, must not be read as
// success just because the replica is absent.
func TestRetry_DeleteWithoutSubmissionStillFails(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{2: "127.0.0.1:2"}}

	err := membership.Drive("delete", 2*time.Second,
		scriptedAttempt(&calls, quicraftNodeNotFound()),
		fixedMembership(m),
		membership.DeleteNodeLanded(3))
	if err == nil {
		t.Fatal("a remove that was never submitted must not be confirmed by mere absence")
	}
}

// TestRetry_DeleteStillPresentFails: the confirm must not accept a remove
// while the replica is still a member, in any role.
func TestRetry_DeleteStillPresentFails(t *testing.T) {
	cases := map[string]*quicraft.Membership{
		"voter":    {Nodes: map[uint64]string{3: "127.0.0.1:3"}},
		"observer": {Observers: map[uint64]string{3: "127.0.0.1:3"}},
		"witness":  {Witnesses: map[uint64]string{3: "127.0.0.1:3"}},
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			calls := 0
			err := membership.Drive("delete", 400*time.Millisecond,
				scriptedAttempt(&calls, postDeliveryNotLeader()),
				fixedMembership(m),
				membership.DeleteNodeLanded(3))
			if err == nil {
				t.Fatalf("a remove whose target is still a %s must fail", name)
			}
		})
	}
}

// TestRetry_PendingConfigChangeIsRetried covers the add arriving while the preceding remove is
// still committing, which the engine documents as retryable.
func TestRetry_PendingConfigChangeIsRetried(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:59436"}}

	err := membership.Drive("add", 2*time.Second,
		scriptedAttempt(&calls, quicraft.ErrRejected, nil),
		fixedMembership(m),
		membership.AddNodeLanded(11, "127.0.0.1:59436"))
	if err != nil {
		t.Fatalf("ErrRejected on a membership change is retryable, got: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected the rejection to be retried once, attempts = %d", calls)
	}
}

// TestRetry_FailedReadIsNotEvidence: a membership read that errors neither
// confirms nor refutes; the loop keeps going and lands on the next read.
func TestRetry_FailedReadIsNotEvidence(t *testing.T) {
	calls := 0
	reads := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:59436"}}
	read := func(context.Context) (*quicraft.Membership, error) {
		reads++
		if reads == 1 {
			return nil, quicraft.ErrNotLeader
		}
		return m, nil
	}

	err := membership.Drive("add", 2*time.Second,
		scriptedAttempt(&calls, postDeliveryNotLeader(), quicraftAlreadyExists()),
		read,
		membership.AddNodeLanded(11, "127.0.0.1:59436"))
	if err != nil {
		t.Fatalf("a failed read must be retried, not reported, got: %v", err)
	}
	if reads < 2 {
		t.Errorf("expected the read to be retried after failing, reads = %d", reads)
	}
}

// TestWasSubmitted pins the classifier the whole gate rests on. A pre-flight
// refusal and a post-delivery loss print identically; only the
// MaybeAppliedError wrapper separates them.
func TestWasSubmitted(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"post-delivery not-leader", postDeliveryNotLeader(), true},
		{"pre-flight not-leader", preflightNotLeader(), false},
		{"timeout", quicraft.ErrTimeout, true},
		{"canceled", quicraft.ErrCanceled, false},
		{"system busy", quicraft.ErrSystemBusy, false},
		{"shard not ready", quicraft.ErrShardNotReady, false},
		{"pending config change", quicraft.ErrRejected, false},
		{"already exists", quicraftAlreadyExists(), false},
		{"not found", quicraftNodeNotFound(), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := membership.WasSubmitted(tc.err); got != tc.want {
				t.Errorf("WasSubmitted(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestIsRetryable pins the retry set. The refusals the gate exists for are NOT
// in it: they reach Drive's decision only through the submission bit and a
// membership read, never through a retry.
func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"pre-flight not-leader", preflightNotLeader(), true},
		{"post-delivery not-leader", postDeliveryNotLeader(), true},
		{"system busy", quicraft.ErrSystemBusy, true},
		{"timeout", quicraft.ErrTimeout, true},
		{"canceled", quicraft.ErrCanceled, true},
		{"shard not ready", quicraft.ErrShardNotReady, true},
		{"pending config change", quicraft.ErrRejected, true},
		{"context deadline", context.DeadlineExceeded, true},
		{"context canceled", context.Canceled, true},
		// The real sentinel, not a look-alike built from the same text.
		// IsRetryable matches by sentinel, so a fixture that only LOOKS like
		// a transfer refusal is correctly not retried.
		{"leadership transfer in progress", quicraft.ErrTransferInProgress, true},
		{"already exists", quicraftAlreadyExists(), false},
		{"not found", quicraftNodeNotFound(), false},
		{"last voter", quicraft.ErrRemoveLastVoter, false},
		{"closed", quicraft.ErrClosed, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := membership.IsRetryable(tc.err); got != tc.want {
				t.Errorf("IsRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestTry_NoLeaderExhaustsBudgetWithoutAHost pins the resolver wiring: while
// no replica reports leadership nothing is called on any Host, the loop keeps
// asking rather than sampling once, and the exhaustion names the reason.
func TestTry_NoLeaderExhaustsBudgetWithoutAHost(t *testing.T) {
	resolves := 0
	noLeader := func(uint64) *quicraft.Host {
		resolves++
		return nil
	}

	for name, try := range map[string]func() error{
		"add": func() error {
			return membership.TryAddNode(noLeader, 1, 11, "127.0.0.1:1", "spki:11", 300*time.Millisecond)
		},
		"delete": func() error { return membership.TryDeleteNode(noLeader, 1, 11, 300*time.Millisecond) },
		"add observer": func() error {
			return membership.TryAddObserver(noLeader, 1, 11, "127.0.0.1:1", "spki:11", 300*time.Millisecond)
		},
		"promote": func() error {
			return membership.TryPromoteNode(noLeader, 1, 11, "127.0.0.1:1", "spki:11", 300*time.Millisecond)
		},
	} {
		t.Run(name, func(t *testing.T) {
			resolves = 0
			err := try()
			var exhausted *membership.ExhaustedError
			if !errors.As(err, &exhausted) {
				t.Fatalf("expected *ExhaustedError, got %T: %v", err, err)
			}
			if _, ok := quicraft.NotLeader(err); !ok {
				t.Errorf("the exhaustion must report the not-leader condition, got: %v", err)
			}
			if resolves < 2 {
				t.Errorf("the leader must be re-resolved on every attempt, resolved %d time(s)", resolves)
			}
		})
	}
}

// TestAddObserverLanded pins the observer add's confirmation: only an observer
// at the requested address has landed, and every other occupant of the replica
// ID is a conflict naming its role.
func TestAddObserverLanded(t *testing.T) {
	const addr = "127.0.0.1:59436"
	tests := []struct {
		name     string
		m        *quicraft.Membership
		landed   bool
		conflict string
	}{
		{name: "nil membership", m: nil},
		{name: "absent", m: &quicraft.Membership{}},
		{name: "observer at addr", m: &quicraft.Membership{Observers: map[uint64]string{11: addr}}, landed: true},
		{name: "observer elsewhere", m: &quicraft.Membership{Observers: map[uint64]string{11: "127.0.0.1:1"}}, conflict: "observer"},
		{name: "voter", m: &quicraft.Membership{Nodes: map[uint64]string{11: addr}}, conflict: "voting member"},
		{name: "witness", m: &quicraft.Membership{Witnesses: map[uint64]string{11: addr}}, conflict: "witness"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			landed, conflict := membership.AddObserverLanded(11, addr)(tt.m)
			assertConfirm(t, landed, conflict, tt.landed, tt.conflict)
		})
	}
}

// TestPromoteNodeLanded pins the promotion's confirmation. The observer being
// promoted, at its own address, is the starting state and must read as "not
// yet" — AddNodeLanded reports it as a conflict, which would fail a promotion
// whose first attempt was delivered but not yet applied.
func TestPromoteNodeLanded(t *testing.T) {
	const addr = "127.0.0.1:59436"
	tests := []struct {
		name     string
		m        *quicraft.Membership
		landed   bool
		conflict string
	}{
		{name: "nil membership", m: nil},
		{name: "still the observer", m: &quicraft.Membership{Observers: map[uint64]string{11: addr}}},
		{name: "voter at addr", m: &quicraft.Membership{Nodes: map[uint64]string{11: addr}}, landed: true},
		{name: "voter elsewhere", m: &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:1"}}, conflict: "voting member"},
		{name: "observer elsewhere", m: &quicraft.Membership{Observers: map[uint64]string{11: "127.0.0.1:1"}}, conflict: "observer"},
		{name: "witness", m: &quicraft.Membership{Witnesses: map[uint64]string{11: addr}}, conflict: "witness"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			landed, conflict := membership.PromoteNodeLanded(11, addr)(tt.m)
			assertConfirm(t, landed, conflict, tt.landed, tt.conflict)
		})
	}
}

// assertConfirm checks one Confirm verdict: landed, and either no conflict or
// a *ConflictError naming wantRole.
func assertConfirm(t *testing.T, landed bool, conflict error, wantLanded bool, wantRole string) {
	t.Helper()
	if landed != wantLanded {
		t.Fatalf("landed = %v, want %v", landed, wantLanded)
	}
	if wantRole == "" {
		if conflict != nil {
			t.Fatalf("unexpected conflict: %v", conflict)
		}
		return
	}
	var c *membership.ConflictError
	if !errors.As(conflict, &c) {
		t.Fatalf("conflict = %v, want a *ConflictError", conflict)
	}
	if c.Role != wantRole {
		t.Fatalf("conflict role = %q, want %q", c.Role, wantRole)
	}
}
