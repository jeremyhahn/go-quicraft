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

// Package membership applies a test's membership change through whichever
// replica leads the shard AT THE MOMENT OF EACH ATTEMPT, and does not mistake
// the retry of a change that already committed for a failure.
//
// # Why this exists
//
// A config change has TWO values a test is tempted to pin and must not.
//
// The first is leadership. A test that writes
//
//	leaderIdx := cluster.waitForClusterLeader(shardID, timeout)
//	...
//	cluster.hosts[leaderIdx].SyncRequestDeleteNode(ctx, shardID, id, 0)
//
// has read a value that was true when it looked and treated it as fixed.
// Package test/proposer records that class for proposals (#84, #88, #90, #91,
// #103, #107); a membership change addressed the same way is refused the same
// way.
//
// The second is WHETHER THE CHANGE COMMITTED. A config change is submitted,
// leadership moves before the response, and the caller sees a
// *quicraft.MaybeAppliedError wrapping a *quicraft.NotLeaderError — or an
// ErrTimeout, which Sync{Add,Delete}Node can only return after the entry was
// handed to the engine. A loop that retries either has read "no answer" as
// "did not happen". The entry is in the log and can still commit under the
// new leader (Leader Completeness guarantees it if it reached a quorum), so
// the retry is then refused CORRECTLY with "already exists" or "not found"
// while the test reports a failure the cluster never had (#112, #132).
//
// # What the loop does, and deliberately does not do
//
// "already exists in membership" and "target node not found" are never
// swallowed on their own. Drive accepts them only when BOTH hold:
//
//  1. an earlier attempt in the SAME loop was actually submitted to Raft:
//     quicraft.MaybeApplied(err), or an ErrTimeout that can only be produced
//     after delivery. A pre-flight refusal (not leader, shard not ready, rate
//     limited, pending config change) submits nothing and never sets this bit;
//     and
//  2. a LINEARIZABLE read of the membership shows the exact change asked for:
//     for an add, the replica present as a full voter at the address this call
//     requested; for a remove, the replica holding no role at all.
//
// An "already exists" for a different address, for a replica in the wrong
// role, or with no prior in-flight submission still fails. So does a change
// that never lands: (2) is a positive observation of the requested state, not
// the absence of an error.
//
// The address half of AddNodeLanded is load-bearing. Peer.validateAddNode
// answers ErrNodeAlreadyExists from the replica ID alone, so "already exists"
// is also what a genuine replica-ID collision looks like; confirming the ID
// without the address would turn that membership bug into a green test.
//
// # Why it is one package and not one copy per suite
//
// The decision core was written for the e2e suite in #122
// (test/integration/e2e/membership_idempotence_test.go), in package e2e_test,
// which no other suite can import. The host and linearizability suites needed
// the same core (#135), and CLAUDE.md forbids a second copy. test/proposer is
// the precedent: the suites share nothing but a way to name the current
// leader, so that is all this package asks for — the same proposer.Resolver.
// The e2e copy is retired once its files are free; that is #145.
package membership

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// AttemptTimeout bounds ONE membership RPC.
//
// It is deliberately shorter than any budget a suite hands Drive, which is the
// budget for the whole loop. When the two were the same value an attempt that
// ran to its deadline consumed the entire budget and the "retry" loop got
// exactly one attempt. A config change commits in milliseconds when it commits
// at all; what the remainder of the budget buys is the retries.
const AttemptTimeout = 10 * time.Second

// RetryInterval is how long Drive waits before re-attempting.
const RetryInterval = 50 * time.Millisecond

// leaderTransferInProgress is the text of the engine's internal
// raft.ErrTransferInProgress, which handleLeaderPropose returns for EVERY
// proposal — config changes included — while a leadership transfer is in
// flight, and which Host.translateProposalError does not map to an exported
// sentinel. It reaches a caller as an opaque error, so it can only be matched
// by text here (#146 asks for the mapping). It is produced before the entry
// enters the log, so it is retryable and never counts as a submission.
const leaderTransferInProgress = "transfer in progress"

// Confirm answers "is the change I asked for present in m?" against a
// membership snapshot.
//
// It returns three distinguishable outcomes, and the third is the point of the
// type: landed with a nil conflict (the requested state is there), not landed
// with a nil conflict (not there yet — keep trying), and a non-nil conflict,
// meaning the membership holds something INCOMPATIBLE with the request. A
// conflict is a hard failure and must never be retried away: it is what
// separates "my change landed" from "somebody else's change occupies the
// replica ID I asked for".
type Confirm func(m *quicraft.Membership) (landed bool, conflict error)

// ConflictError reports a replica ID that is present in the membership in a
// state INCOMPATIBLE with the add that requested it: a voter at a different
// address, or a non-voting role. It names the address actually holding the ID
// so a replica-ID collision is diagnosable from the failure alone.
type ConflictError struct {
	ReplicaID uint64
	// Role is the role the replica ID currently holds: "voting member",
	// "observer" or "witness".
	Role string
	// Addr is the address the membership records for the replica ID.
	Addr string
	// Requested is the address the add asked for.
	Requested string
}

func (e *ConflictError) Error() string {
	if e.Role == roleVoter {
		return fmt.Sprintf(
			"membership: replica %d is a voting member at %q, not the %q this call requested: "+
				"the replica ID is held by a different node, which is a membership conflict "+
				"and not a landed retry",
			e.ReplicaID, e.Addr, e.Requested)
	}
	return fmt.Sprintf(
		"membership: replica %d is a %s at %q; this call requested a full voting member at %q",
		e.ReplicaID, e.Role, e.Addr, e.Requested)
}

// FailedError is a membership change that Drive gave up on: a non-retryable
// error from an attempt, or a ConflictError from the membership read.
type FailedError struct {
	Desc string
	Err  error
}

func (e *FailedError) Error() string {
	return fmt.Sprintf("membership: %s failed: %v", e.Desc, e.Err)
}

func (e *FailedError) Unwrap() error { return e.Err }

// ExhaustedError is a membership change that never landed within its budget.
// Last is the error the final attempt produced, or quicraft.ErrTimeout when
// no attempt ever ran.
type ExhaustedError struct {
	Desc   string
	Budget time.Duration
	Last   error
}

func (e *ExhaustedError) Error() string {
	return fmt.Sprintf("membership: %s never succeeded within %v: %v", e.Desc, e.Budget, e.Last)
}

func (e *ExhaustedError) Unwrap() error { return e.Last }

const (
	roleVoter    = "voting member"
	roleObserver = "observer"
	roleWitness  = "witness"
)

// AddNodeLanded confirms an AddNode: replicaID must be a FULL VOTER at exactly
// addr. Any other occupant of the replica ID is a ConflictError.
func AddNodeLanded(replicaID uint64, addr string) Confirm {
	return func(m *quicraft.Membership) (bool, error) {
		if m == nil {
			return false, nil
		}
		if got, ok := m.Nodes[replicaID]; ok {
			if got != addr {
				return false, &ConflictError{ReplicaID: replicaID, Role: roleVoter, Addr: got, Requested: addr}
			}
			return true, nil
		}
		if got, ok := m.Observers[replicaID]; ok {
			return false, &ConflictError{ReplicaID: replicaID, Role: roleObserver, Addr: got, Requested: addr}
		}
		if got, ok := m.Witnesses[replicaID]; ok {
			return false, &ConflictError{ReplicaID: replicaID, Role: roleWitness, Addr: got, Requested: addr}
		}
		return false, nil
	}
}

// DeleteNodeLanded confirms a RemoveNode: replicaID must hold no role at all.
//
// Absence alone is weak evidence — a replica that was never a member is also
// absent — which is exactly why Drive consults a Confirm only after an attempt
// was actually submitted to Raft. Read the two together.
func DeleteNodeLanded(replicaID uint64) Confirm {
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

// WasSubmitted reports whether err's config change had already been delivered
// to Raft when it failed, so the entry may still commit.
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
// that is already done BEFORE submitting anything, and Drive cancels an
// attempt's context only after that attempt has returned, so a cancellation
// seen here submitted nothing.
func WasSubmitted(err error) bool {
	if err == nil {
		return false
	}
	if quicraft.MaybeApplied(err) {
		return true
	}
	return errors.Is(err, quicraft.ErrTimeout)
}

// retryableSentinels are the errors one more attempt can change the answer
// to. Every one of them clears on its own: an election completes, a pipeline
// drains, a shard finishes loading, a deadline that was too short gets a fresh
// one, a pending config change commits.
//
// ErrRejected is here for membership calls specifically. On this path it has
// one producer, raft.ErrPendingConfigChange, which Host.translateProposalError
// documents as retryable in the same breath as it maps it: nothing entered the
// log, so re-proposing once the pending change commits succeeds. A suite that
// issues a remove and an add back to back routinely has the add arrive while
// the remove is still committing. It is NOT retryable on the propose path,
// where ErrRejected is also what an expired client session maps to, which is
// why this set is this package's and not test/proposer's.
var retryableSentinels = [...]error{
	quicraft.ErrNotLeader,
	quicraft.ErrSystemBusy,
	quicraft.ErrTimeout,
	quicraft.ErrCanceled,
	quicraft.ErrShardNotReady,
	quicraft.ErrRejected,
	context.DeadlineExceeded,
	context.Canceled,
}

// IsRetryable reports whether one more attempt at a membership change is
// worth making after err.
//
// The set is narrow on purpose. A refusal the membership validation produces —
// already exists, not found, would remove the last voter, an invalid address —
// is a real answer and must reach the test unless Drive's submission-and-
// confirmation gate has established that the change in question is this
// loop's own, already committed.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	for _, s := range retryableSentinels {
		if errors.Is(err, s) {
			return true
		}
	}
	return strings.Contains(err.Error(), leaderTransferInProgress)
}

// Drive runs attempt until the requested membership change is in effect, the
// budget runs out, or something non-retryable comes back.
//
// It is separated from the cluster it usually drives so the decision logic can
// be tested directly — see membership_test.go, which is where the "already
// exists for a DIFFERENT address must still fail" mutation is pinned.
//
// attempt performs one membership RPC. readMembership performs one
// LINEARIZABLE membership read. Both resolve the leader themselves at the
// point of use, and both may report a transient error, which is treated as
// "ask again".
func Drive(
	desc string,
	budget time.Duration,
	attempt func(context.Context) error,
	readMembership func(context.Context) (*quicraft.Membership, error),
	confirm Confirm,
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
		if WasSubmitted(err) {
			submitted = true
		}

		if submitted {
			landed, conflict := observe(deadline, readMembership, confirm)
			if conflict != nil {
				return &FailedError{Desc: desc, Err: conflict}
			}
			if landed {
				return nil
			}
		}

		if !IsRetryable(err) {
			return &FailedError{Desc: desc, Err: err}
		}
		lastErr = err
		waitBriefly(RetryInterval)
	}

	if lastErr == nil {
		lastErr = quicraft.ErrTimeout
	}
	return &ExhaustedError{Desc: desc, Budget: budget, Last: lastErr}
}

// attemptBudget is how long one RPC gets: the shorter of AttemptTimeout and
// whatever is left of the loop's budget, never less than RetryInterval so a
// nearly-expired budget still makes a bounded call rather than one with an
// already-dead context.
func attemptBudget(deadline time.Time) time.Duration {
	remaining := time.Until(deadline)
	if remaining > AttemptTimeout {
		remaining = AttemptTimeout
	}
	if remaining < RetryInterval {
		remaining = RetryInterval
	}
	return remaining
}

// observe takes one linearizable membership read and asks confirm about it. A
// read that fails is not evidence of anything, so it reports "not landed" and
// lets the caller try again.
func observe(
	deadline time.Time,
	readMembership func(context.Context) (*quicraft.Membership, error),
	confirm Confirm,
) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), attemptBudget(deadline))
	defer cancel()

	m, err := readMembership(ctx)
	if err != nil {
		return false, nil
	}
	return confirm(m)
}

// waitBriefly blocks for d without time.Sleep, which this repository does not
// use.
func waitBriefly(d time.Duration) {
	timer := time.NewTimer(d)
	<-timer.C
	timer.Stop()
}

// resolved returns the two closures Drive needs for a suite's cluster: both
// resolve the leader at the point of use through resolve, so neither pins a
// leadership observation across a call. A moment with no leader is reported
// as ErrNotLeader, which IsRetryable turns into "ask again".
func resolved(
	resolve proposer.Resolver,
	shardID uint64,
	call func(context.Context, *quicraft.Host) error,
) (func(context.Context) error, func(context.Context) (*quicraft.Membership, error)) {
	attempt := func(ctx context.Context) error {
		h := resolve(shardID)
		if h == nil {
			return quicraft.ErrNotLeader
		}
		return call(ctx, h)
	}
	read := func(ctx context.Context) (*quicraft.Membership, error) {
		h := resolve(shardID)
		if h == nil {
			return nil, quicraft.ErrNotLeader
		}
		return h.SyncGetShardMembership(ctx, shardID)
	}
	return attempt, read
}

// TryAddNode adds replicaID as a voting member at addr through whichever
// replica leads shardID at each attempt, and returns nil once a linearizable
// read of the membership shows it there. It is AddNode's error-returning
// form, for callers with resources to release before failing the test.
func TryAddNode(resolve proposer.Resolver, shardID, replicaID uint64, addr string, budget time.Duration) error {
	attempt, read := resolved(resolve, shardID, func(ctx context.Context, h *quicraft.Host) error {
		return h.SyncRequestAddNode(ctx, shardID, replicaID, addr, 0) //quicraft:direct-config-change the single attempt Drive is built from
	})
	desc := fmt.Sprintf("SyncRequestAddNode(replica=%d, addr=%s)", replicaID, addr)
	return Drive(desc, budget, attempt, read, AddNodeLanded(replicaID, addr))
}

// TryDeleteNode removes replicaID through whichever replica leads shardID at
// each attempt, and returns nil once a linearizable read of the membership
// shows it holding no role. It is DeleteNode's error-returning form.
func TryDeleteNode(resolve proposer.Resolver, shardID, replicaID uint64, budget time.Duration) error {
	attempt, read := resolved(resolve, shardID, func(ctx context.Context, h *quicraft.Host) error {
		return h.SyncRequestDeleteNode(ctx, shardID, replicaID, 0) //quicraft:direct-config-change the single attempt Drive is built from
	})
	desc := fmt.Sprintf("SyncRequestDeleteNode(replica=%d)", replicaID)
	return Drive(desc, budget, attempt, read, DeleteNodeLanded(replicaID))
}

// AddNode is TryAddNode for the test goroutine: it fails the test on any
// error.
func AddNode(t *testing.T, resolve proposer.Resolver, shardID, replicaID uint64, addr string, budget time.Duration) {
	t.Helper()
	if err := TryAddNode(resolve, shardID, replicaID, addr, budget); err != nil {
		t.Fatal(err)
	}
}

// DeleteNode is TryDeleteNode for the test goroutine: it fails the test on
// any error.
func DeleteNode(t *testing.T, resolve proposer.Resolver, shardID, replicaID uint64, budget time.Duration) {
	t.Helper()
	if err := TryDeleteNode(resolve, shardID, replicaID, budget); err != nil {
		t.Fatal(err)
	}
}
