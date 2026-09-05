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

//go:build integration

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// ---------------------------------------------------------------------------
// Idempotent membership changes
//
// This file exists because of #112: TestE2E_Partition_HealAndReconverge failed
// under load with
//
//	SyncRequestAddNode(replica=11) failed: raft: node already exists in membership
//
// on a replica ID the test asks for exactly once. The second add came from the
// suite's own retry loop, and it was rejected CORRECTLY, because the first
// attempt's config change had already committed.
//
// # The class
//
// This is the same flake class as #84, #88, #90, #91, #103 and #107: a value
// that is not pinned, treated as if it were. There the unpinned value was
// leadership, read on one line and used on another. Here it is WHETHER A
// CONFIG CHANGE COMMITTED. The suite's retry loop retried every transient
// error, which reads "no answer" as "did not happen" — and for a membership
// change that inference is simply false. The engine says so itself, in
// Host.OnLeaderChanged:
//
//	Wrapped in MaybeAppliedError because every request this fails was ALREADY
//	delivered to raft: [...] anything still in it at a leadership change is in
//	the log and can still commit under the new leader (Leader Completeness
//	guarantees it if it reached a quorum).
//
// So *quicraft.MaybeAppliedError wraps a *NotLeaderError, errors.Is finds
// ErrNotLeader through it, isTransientLeadershipError answers true, and the
// suite retried a change that was already on its way to committing. The same
// hole exists for ErrTimeout, which SyncRequestAddNode can only return after
// RequestAddNode has already handed the entry to the engine.
//
// # The fix, and what it deliberately does NOT do
//
// The retry is made idempotent, not forgiving. "already exists in membership"
// is never swallowed on its own. It is accepted only when BOTH hold:
//
//  1. an earlier attempt in this same loop was actually submitted to Raft —
//     quicraft.MaybeApplied(err), or an ErrTimeout that can only be produced
//     after delivery. A pre-flight refusal (not leader, shard not ready, rate
//     limited) submits nothing and never sets this bit; and
//  2. a LINEARIZABLE read of the membership shows the exact change asked for —
//     for an add, the replica ID present as a full voter at the address this
//     call requested.
//
// An "already exists" for a different address, for a replica in the wrong
// role, or with no prior in-flight submission still fails the test. So does a
// change that never lands: (2) is a positive observation of the requested
// state, not the absence of an error.
//
// Retrying an add is safe against double-application for a second reason
// worth recording: Host.requestConfigChange pre-validates every membership
// change against the published membership (Peer.ValidateConfigChange) before
// delivering it, so a duplicate add is refused rather than proposed twice.
// ---------------------------------------------------------------------------

// membershipAttemptTimeout bounds ONE membership RPC.
//
// It is deliberately shorter than membershipTimeout, which is the budget for
// the whole retry loop. The two used to be the same value, so an attempt that
// ran to its context deadline consumed the entire budget and the "retry" loop
// got exactly one attempt. A config change commits in milliseconds when it
// commits at all; ten seconds is already an eternity, and what the remaining
// twenty buys is the retries this loop is named after.
const membershipAttemptTimeout = 10 * time.Second

// membershipRetryInterval is how long the loop waits before re-attempting.
const membershipRetryInterval = 50 * time.Millisecond

// membershipConfirm answers "is the change I asked for present in m?" against
// a membership snapshot.
//
// It returns three distinguishable outcomes, and the third is the point of the
// type: landed=true (the requested state is there), landed=false with a nil
// conflict (not there yet — keep trying), and a non-nil conflict, meaning the
// membership holds something INCOMPATIBLE with the request. A conflict is a
// hard failure and must never be retried away: it is what separates "my change
// landed" from "somebody else's change occupies the replica ID I asked for".
type membershipConfirm func(m *quicraft.Membership) (landed bool, conflict error)

// addNodeLanded confirms an AddNode: replicaID must be a FULL VOTER at exactly
// addr.
//
// The address comparison is the load-bearing half. Peer.validateAddNode
// answers ErrNodeAlreadyExists from m.isVoter(replicaID) alone, without
// looking at the address, so "already exists" is also what a caller sees when
// a DIFFERENT host has taken the replica ID it wanted. Confirming the replica
// ID alone would turn that collision — a real membership bug — into a green
// test.
func addNodeLanded(replicaID uint64, addr string) membershipConfirm {
	return func(m *quicraft.Membership) (bool, error) {
		if m == nil {
			return false, nil
		}
		if got, ok := m.Nodes[replicaID]; ok {
			if got != addr {
				return false, fmt.Errorf(
					"replica %d is a voting member at %q, not the %q this call requested: "+
						"the replica ID is held by a different node, which is a membership "+
						"conflict and not a landed retry",
					replicaID, got, addr)
			}
			return true, nil
		}
		if got, ok := m.Observers[replicaID]; ok {
			return false, fmt.Errorf(
				"replica %d is an OBSERVER at %q; this call requested a full voting member at %q",
				replicaID, got, addr)
		}
		if got, ok := m.Witnesses[replicaID]; ok {
			return false, fmt.Errorf(
				"replica %d is a WITNESS at %q; this call requested a full voting member at %q",
				replicaID, got, addr)
		}
		return false, nil
	}
}

// deleteNodeLanded confirms a RemoveNode: replicaID must hold no role at all.
//
// Absence alone is weak evidence — a replica that was never a member is also
// absent — which is exactly why driveMembershipChange consults a confirm only
// after an attempt was actually submitted to Raft. Read the two together.
func deleteNodeLanded(replicaID uint64) membershipConfirm {
	return func(m *quicraft.Membership) (bool, error) {
		if m == nil {
			return false, nil
		}
		if _, ok := m.Nodes[replicaID]; ok {
			return false, nil
		}
		if _, ok := m.Observers[replicaID]; ok {
			return false, nil
		}
		if _, ok := m.Witnesses[replicaID]; ok {
			return false, nil
		}
		return true, nil
	}
}

// membershipChangeWasSubmitted reports whether err's config change had already
// been delivered to Raft when it failed, so the entry may still commit.
//
// Two errors carry that meaning and no others:
//
//   - anything quicraft.MaybeApplied answers true for. The engine wraps
//     precisely the post-delivery failures in *MaybeAppliedError; the
//     pre-flight refusals that are otherwise textually identical are not
//     wrapped. This is the engine's own bit, not a guess made out here.
//   - ErrTimeout. Sync{Add,Delete}Node return it only from the select on
//     ctx.Done() after RequestX returned a live RequestState, or when the
//     host's deadline sweep completes an already-registered pending proposal.
//     Both are strictly after DeliverProposal succeeded.
//
// ErrCanceled is deliberately NOT here. requestConfigChange refuses a context
// that is already done BEFORE submitting anything, and this loop cancels an
// attempt's context only after that attempt has returned — so a cancellation
// seen here submitted nothing.
func membershipChangeWasSubmitted(err error) bool {
	if err == nil {
		return false
	}
	if quicraft.MaybeApplied(err) {
		return true
	}
	return errors.Is(err, quicraft.ErrTimeout)
}

// membershipChangeIsRetryable widens isTransientLeadershipError by exactly one
// error, and only for membership calls: ErrRejected.
//
// On a membership RPC that error has one producer, and the engine documents it
// as retryable in the same breath as it maps it (Host.translateProposalError):
//
//	A config change proposed while another is still uncommitted [...] It IS
//	retryable: nothing entered the log, so re-proposing once the pending change
//	commits succeeds.
//
// The partition test issues a remove and an add back to back, so the add
// routinely arrives while the remove is still committing. Treating that as
// fatal reported a failure the cluster never had — 2 runs in 240 at 6x
// oversubscription, alongside the 9 of #112's own shape.
//
// The widening is scoped here rather than folded into
// isTransientLeadershipError because ErrRejected does NOT mean the same thing
// on the propose path, where it is also what an expired client session maps
// to. A config change carries no session, so on this path the ambiguity does
// not arise.
func membershipChangeIsRetryable(err error) bool {
	if isTransientLeadershipError(err) {
		return true
	}
	return errors.Is(err, quicraft.ErrRejected)
}

// driveMembershipChange runs attempt against the current leader until the
// requested membership change is in effect, the budget runs out, or something
// non-retryable comes back.
//
// It is separated from the cluster it usually drives so the decision logic can
// be tested directly — see TestMembershipRetry_* below, which is where the
// "already exists for a DIFFERENT address must still fail" mutation is pinned.
//
// attempt performs one membership RPC. readMembership performs one
// LINEARIZABLE membership read. Both resolve the leader themselves at the
// point of use, and both may report a transient error, which is treated as
// "ask again".
func driveMembershipChange(
	desc string,
	budget time.Duration,
	attempt func(context.Context) error,
	readMembership func(context.Context) (*quicraft.Membership, error),
	confirm membershipConfirm,
) error {
	deadline := time.Now().Add(budget)
	var lastErr error
	submitted := false

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), attemptBudget(deadline))
		err := attempt(ctx)
		cancel()
		if err == nil {
			return nil
		}

		// Latch the bit for the whole loop, not just this attempt: the entry
		// an earlier attempt submitted is what a later "already exists" is
		// reporting.
		if membershipChangeWasSubmitted(err) {
			submitted = true
		}

		if submitted {
			landed, conflict := observeMembership(deadline, readMembership, confirm)
			if conflict != nil {
				return fmt.Errorf("%s: %w", desc, conflict)
			}
			if landed {
				return nil
			}
		}

		if !membershipChangeIsRetryable(err) {
			return fmt.Errorf("%s failed: %w", desc, err)
		}
		lastErr = err
		waitBriefly(membershipRetryInterval)
	}

	if lastErr == nil {
		lastErr = quicraft.ErrTimeout
	}
	return fmt.Errorf("%s never succeeded within %v: %w", desc, budget, lastErr)
}

// attemptBudget is how long one RPC gets: the shorter of the per-attempt cap
// and whatever is left of the loop's budget, never less than the retry
// interval so a nearly-expired budget still makes a bounded call rather than
// one with an already-dead context.
func attemptBudget(deadline time.Time) time.Duration {
	remaining := time.Until(deadline)
	if remaining > membershipAttemptTimeout {
		remaining = membershipAttemptTimeout
	}
	if remaining < membershipRetryInterval {
		remaining = membershipRetryInterval
	}
	return remaining
}

// observeMembership takes one linearizable membership read and asks confirm
// about it. A read that fails is not evidence of anything, so it reports
// "not landed" and lets the caller try again.
func observeMembership(
	deadline time.Time,
	readMembership func(context.Context) (*quicraft.Membership, error),
	confirm membershipConfirm,
) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), attemptBudget(deadline))
	defer cancel()

	m, err := readMembership(ctx)
	if err != nil {
		return false, nil
	}
	return confirm(m)
}

// waitBriefly sleeps without time.Sleep, which this repo does not use.
func waitBriefly(d time.Duration) {
	timer := time.NewTimer(d)
	<-timer.C
	timer.Stop()
}

// clusterMembershipAttempt returns the two closures driveMembershipChange
// needs for a testCluster: both resolve the leader at the point of use, so
// neither pins a leadership observation across a call.
func clusterMembershipAttempt(
	cluster *testCluster,
	shardID uint64,
	call func(context.Context, *quicraft.Host) error,
) (func(context.Context) error, func(context.Context) (*quicraft.Membership, error)) {
	attempt := func(ctx context.Context) error {
		h := cluster.leaderHost(shardID)
		if h == nil {
			return quicraft.ErrNotLeader
		}
		return call(ctx, h)
	}
	read := func(ctx context.Context) (*quicraft.Membership, error) {
		h := cluster.leaderHost(shardID)
		if h == nil {
			return nil, quicraft.ErrNotLeader
		}
		return h.SyncGetShardMembership(ctx, shardID)
	}
	return attempt, read
}

// syncAddNode adds a voting replica through whichever node is CURRENTLY the
// leader, retrying transient leadership errors, and does not mistake a retry
// of an already-committed add for a failure.
//
// It lived in helpers_test.go until #112. The membership calls there were once
// issued against a leader index sampled ONCE and treated ErrNotLeader as
// fatal, which failed the test for a re-election that refused nothing; that
// was fixed by resolving the leader at the point of use, and this keeps it.
// What that fix did not address is the second unpinned value — see the file
// header.
func syncAddNode(t *testing.T, cluster *testCluster, shardID, replicaID uint64, addr string, timeout time.Duration) {
	t.Helper()

	attempt, read := clusterMembershipAttempt(cluster, shardID,
		func(ctx context.Context, h *quicraft.Host) error {
			//quicraft:direct-config-change the single attempt driveMembershipChange is built from
			return h.SyncRequestAddNode(ctx, shardID, replicaID, addr, 0)
		})

	desc := fmt.Sprintf("SyncRequestAddNode(replica=%d, addr=%s)", replicaID, addr)
	if err := driveMembershipChange(desc, timeout, attempt, read, addNodeLanded(replicaID, addr)); err != nil {
		t.Fatal(err)
	}
}

// syncDeleteNode removes a replica through whichever node is CURRENTLY the
// leader, with the same idempotence as syncAddNode.
func syncDeleteNode(t *testing.T, cluster *testCluster, shardID, replicaID uint64, timeout time.Duration) {
	t.Helper()

	attempt, read := clusterMembershipAttempt(cluster, shardID,
		func(ctx context.Context, h *quicraft.Host) error {
			//quicraft:direct-config-change the single attempt driveMembershipChange is built from
			return h.SyncRequestDeleteNode(ctx, shardID, replicaID, 0)
		})

	desc := fmt.Sprintf("SyncRequestDeleteNode(replica=%d)", replicaID)
	if err := driveMembershipChange(desc, timeout, attempt, read, deleteNodeLanded(replicaID)); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Mutation tests for the retry decision
//
// These drive driveMembershipChange with scripted attempts and a scripted
// membership, so every branch that #112 turns on is pinned WITHOUT needing a
// cluster or a loaded box. The mutation the issue asks for is
// TestMembershipRetry_AlreadyExistsAtDifferentAddressStillFails: a fix that
// swallowed "already exists" unconditionally passes every other test in this
// file and fails that one.
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
// refusals #112 is about.
//
// They are constructed by text rather than imported: the sentinels live in
// pkg/internal/raft, which is unreachable from a test outside the module's
// internal tree, and they reach a caller as opaque errors for the same reason.
// That is not a shortcut around the assertion — it is the point. The driver
// deliberately does NOT branch on these identities; it decides from the
// submission bit and a linearizable membership read, so any non-retryable
// refusal exercises the same path these do.
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

// TestMembershipRetry_AlreadyExistsAfterPostDeliveryLossIsSuccess is #112's
// benign reading: the first add was delivered, leadership moved, the retry was
// correctly rejected, and the membership shows exactly what was asked for.
func TestMembershipRetry_AlreadyExistsAfterPostDeliveryLossIsSuccess(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:59436"}}

	err := driveMembershipChange("add", 2*time.Second,
		scriptedAttempt(&calls, postDeliveryNotLeader(), quicraftAlreadyExists()),
		fixedMembership(m),
		addNodeLanded(11, "127.0.0.1:59436"))
	if err != nil {
		t.Fatalf("a retry of an add that already committed must succeed, got: %v", err)
	}
}

// TestMembershipRetry_AlreadyExistsAtDifferentAddressStillFails is THE
// mutation guard. Peer.validateAddNode answers ErrNodeAlreadyExists from the
// replica ID alone, so "already exists" is also what a genuine replica-ID
// collision looks like. Confirming the ID without the address would make this
// pass, which is precisely the blanket swallow #112 warns against.
func TestMembershipRetry_AlreadyExistsAtDifferentAddressStillFails(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:1234"}}

	err := driveMembershipChange("add", 2*time.Second,
		scriptedAttempt(&calls, postDeliveryNotLeader(), quicraftAlreadyExists()),
		fixedMembership(m),
		addNodeLanded(11, "127.0.0.1:59436"))
	if err == nil {
		t.Fatal("an 'already exists' for a DIFFERENT address is a membership conflict and must fail")
	}
	if !strings.Contains(err.Error(), "127.0.0.1:1234") {
		t.Errorf("the failure must name the address actually holding the replica ID, got: %v", err)
	}
}

// TestMembershipRetry_AlreadyExistsWithoutSubmissionStillFails covers the
// other half of the gate: nothing this loop sent ever reached raft, so an
// "already exists" is somebody else's member, not our landed retry — even
// when the address matches.
func TestMembershipRetry_AlreadyExistsWithoutSubmissionStillFails(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:59436"}}

	err := driveMembershipChange("add", 2*time.Second,
		scriptedAttempt(&calls, preflightNotLeader(), quicraftAlreadyExists()),
		fixedMembership(m),
		addNodeLanded(11, "127.0.0.1:59436"))
	if err == nil {
		t.Fatal("an 'already exists' with no prior in-flight submission must fail")
	}
}

// TestMembershipRetry_AddThatNeverLandsStillFails is the issue's own
// verification clause: make the add genuinely fail, and the loop must still go
// red. The attempt keeps reporting a post-delivery loss and the membership
// never shows the replica.
func TestMembershipRetry_AddThatNeverLandsStillFails(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{1: "127.0.0.1:1"}}

	err := driveMembershipChange("add", 400*time.Millisecond,
		scriptedAttempt(&calls, postDeliveryNotLeader()),
		fixedMembership(m),
		addNodeLanded(11, "127.0.0.1:59436"))
	if err == nil {
		t.Fatal("an add that never appears in the membership must fail, not be swallowed")
	}
	if !strings.Contains(err.Error(), "never succeeded") {
		t.Errorf("expected the budget-exhausted failure, got: %v", err)
	}
	if calls < 2 {
		t.Errorf("expected the loop to retry, it attempted %d time(s)", calls)
	}
}

// TestMembershipRetry_WrongRoleIsAConflict pins the remaining shapes of
// "present, but not what I asked for".
func TestMembershipRetry_WrongRoleIsAConflict(t *testing.T) {
	addr := "127.0.0.1:59436"
	cases := map[string]*quicraft.Membership{
		"observer": {Observers: map[uint64]string{11: addr}},
		"witness":  {Witnesses: map[uint64]string{11: addr}},
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			landed, conflict := addNodeLanded(11, addr)(m)
			if landed {
				t.Fatalf("a %s is not the full voting member the add requested", name)
			}
			if conflict == nil {
				t.Fatalf("a %s occupying the replica ID must be reported as a conflict", name)
			}
		})
	}
}

// TestMembershipRetry_DeleteNotFoundAfterPostDeliveryLossIsSuccess is the
// remove path's mirror of #112, observed 5 times in the same 240 runs as
// "raft: target node not found in membership".
func TestMembershipRetry_DeleteNotFoundAfterPostDeliveryLossIsSuccess(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{2: "127.0.0.1:2"}}

	err := driveMembershipChange("delete", 2*time.Second,
		scriptedAttempt(&calls, postDeliveryNotLeader(), quicraftNodeNotFound()),
		fixedMembership(m),
		deleteNodeLanded(3))
	if err != nil {
		t.Fatalf("a retry of a remove that already committed must succeed, got: %v", err)
	}
}

// TestMembershipRetry_DeleteWithoutSubmissionStillFails: a remove that was
// never submitted, against a replica that was never a member, must not be
// read as success just because the replica is absent.
func TestMembershipRetry_DeleteWithoutSubmissionStillFails(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{2: "127.0.0.1:2"}}

	err := driveMembershipChange("delete", 2*time.Second,
		scriptedAttempt(&calls, quicraftNodeNotFound()),
		fixedMembership(m),
		deleteNodeLanded(3))
	if err == nil {
		t.Fatal("a remove that was never submitted must not be confirmed by mere absence")
	}
}

// TestMembershipRetry_DeleteStillPresentFails: the confirm must not accept a
// remove while the replica is still a member.
func TestMembershipRetry_DeleteStillPresentFails(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{3: "127.0.0.1:3"}}

	err := driveMembershipChange("delete", 400*time.Millisecond,
		scriptedAttempt(&calls, postDeliveryNotLeader()),
		fixedMembership(m),
		deleteNodeLanded(3))
	if err == nil {
		t.Fatal("a remove whose target is still a voting member must fail")
	}
}

// TestMembershipRetry_PendingConfigChangeIsRetried covers the second shape the
// reproduction turned up: the add arriving while the preceding remove is still
// committing, which the engine documents as retryable.
func TestMembershipRetry_PendingConfigChangeIsRetried(t *testing.T) {
	calls := 0
	m := &quicraft.Membership{Nodes: map[uint64]string{11: "127.0.0.1:59436"}}

	err := driveMembershipChange("add", 2*time.Second,
		scriptedAttempt(&calls, quicraft.ErrRejected, nil),
		fixedMembership(m),
		addNodeLanded(11, "127.0.0.1:59436"))
	if err != nil {
		t.Fatalf("ErrRejected on a membership change is retryable, got: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected the rejection to be retried once, attempts = %d", calls)
	}
}

// TestMembershipChangeWasSubmitted pins the classifier the whole gate rests
// on. A pre-flight refusal and a post-delivery loss print identically; only
// the MaybeAppliedError wrapper separates them.
func TestMembershipChangeWasSubmitted(t *testing.T) {
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := membershipChangeWasSubmitted(tc.err); got != tc.want {
				t.Errorf("membershipChangeWasSubmitted(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
