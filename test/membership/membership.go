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

// Package membership applies a membership change through whichever replica leads at each attempt, accepting an "already exists" or "not found" only after a submitted attempt and a linearizable read showing the requested state.
package membership

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// AttemptTimeout bounds one membership RPC, shorter than the budget for the whole loop.
const AttemptTimeout = 10 * time.Second

// RetryInterval is how long Drive waits before re-attempting.
const RetryInterval = 50 * time.Millisecond

// Confirm reports whether the requested change is present in m; a non-nil conflict is a hard failure.
type Confirm func(m *quicraft.Membership) (landed bool, conflict error)

// ConflictError reports a replica ID held in a state incompatible with the requested add, naming the address holding it.
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

// AddNodeLanded confirms an AddNode: replicaID must be a full voter at exactly addr; any other occupant is a ConflictError.
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

// AddObserverLanded confirms an AddNonVoting: replicaID must be an observer at exactly addr; any other occupant is a ConflictError.
func AddObserverLanded(replicaID uint64, addr string) Confirm {
	return func(m *quicraft.Membership) (bool, error) {
		if m == nil {
			return false, nil
		}
		if got, ok := m.Observers[replicaID]; ok {
			if got != addr {
				return false, &ConflictError{ReplicaID: replicaID, Role: roleObserver, Addr: got, Requested: addr}
			}
			return true, nil
		}
		if got, ok := m.Nodes[replicaID]; ok {
			return false, &ConflictError{ReplicaID: replicaID, Role: roleVoter, Addr: got, Requested: addr}
		}
		if got, ok := m.Witnesses[replicaID]; ok {
			return false, &ConflictError{ReplicaID: replicaID, Role: roleWitness, Addr: got, Requested: addr}
		}
		return false, nil
	}
}

// PromoteNodeLanded confirms promoting an observer to a full voter at addr; the observer at addr reads as not landed yet.
func PromoteNodeLanded(replicaID uint64, addr string) Confirm {
	voter := AddNodeLanded(replicaID, addr)
	return func(m *quicraft.Membership) (bool, error) {
		if m != nil {
			if got, ok := m.Observers[replicaID]; ok && got == addr {
				return false, nil
			}
		}
		return voter(m)
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

// WasSubmitted reports whether err's config change had reached Raft: quicraft.MaybeApplied or ErrTimeout, never ErrCanceled.
func WasSubmitted(err error) bool {
	if err == nil {
		return false
	}
	if quicraft.MaybeApplied(err) {
		return true
	}
	return errors.Is(err, quicraft.ErrTimeout)
}

// retryableSentinels are the errors that clear on their own; ErrRejected here means a pending config change.
var retryableSentinels = [...]error{
	quicraft.ErrNotLeader,
	quicraft.ErrSystemBusy,
	quicraft.ErrTimeout,
	quicraft.ErrCanceled,
	quicraft.ErrShardNotReady,
	quicraft.ErrRejected,
	// A leadership transfer in flight refuses work before the log; matched by sentinel.
	quicraft.ErrTransferInProgress,
	context.DeadlineExceeded,
	context.Canceled,
}

// IsRetryable reports whether another membership attempt is worth making after err; validation refusals are not.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	for _, s := range retryableSentinels {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}

// Drive runs attempt until the change is in effect, the budget runs out or a non-retryable error returns; readMembership is a linearizable read.
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

		// seen reports whether the read settled anything. After a submission
		// an unread membership leaves a refusal unexplained, not proven: the
		// "already exists" may be the submitted entry itself, so a refusal
		// fails only once a read has shown the change is not in effect.
		seen := false
		if submitted {
			var landed bool
			var conflict error
			seen, landed, conflict = observe(deadline, readMembership, confirm)
			if conflict != nil {
				return &FailedError{Desc: desc, Err: conflict}
			}
			if landed {
				return nil
			}
		}

		if !IsRetryable(err) && (!submitted || seen) {
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

// observe takes one linearizable membership read and asks confirm about it.
// A read that fails is not evidence of anything: it reports seen false, and
// neither landed nor a conflict, so the caller can look again.
func observe(
	deadline time.Time,
	readMembership func(context.Context) (*quicraft.Membership, error),
	confirm Confirm,
) (seen, landed bool, conflict error) {
	ctx, cancel := context.WithTimeout(context.Background(), attemptBudget(deadline))
	defer cancel()

	m, err := readMembership(ctx)
	if err != nil {
		return false, false, nil
	}
	landed, conflict = confirm(m)
	return true, landed, conflict
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

// TryAddNode adds replicaID as a voter at addr with identity through the current leader, returning nil once a linearizable read shows it.
func TryAddNode(resolve proposer.Resolver, shardID, replicaID uint64, addr, identity string, budget time.Duration) error {
	attempt, read := resolved(resolve, shardID, func(ctx context.Context, h *quicraft.Host) error {
		return h.SyncRequestAddNodeWithIdentity(ctx, shardID, replicaID, addr, identity, 0) //quicraft:direct-config-change the single attempt Drive is built from
	})
	desc := fmt.Sprintf("SyncRequestAddNodeWithIdentity(replica=%d, addr=%s)", replicaID, addr)
	return Drive(desc, budget, attempt, read, AddNodeLanded(replicaID, addr))
}

// TryAddObserver adds replicaID as a non-voting observer at addr, committing
// identity as its node identity, through whichever replica leads shardID at
// each attempt, and returns nil once a linearizable read of the membership
// shows it there. It is AddObserver's error-returning form.
func TryAddObserver(resolve proposer.Resolver, shardID, replicaID uint64, addr, identity string, budget time.Duration) error {
	attempt, read := resolved(resolve, shardID, func(ctx context.Context, h *quicraft.Host) error {
		return h.SyncRequestAddObserverWithIdentity(ctx, shardID, replicaID, addr, identity, 0) //quicraft:direct-config-change the single attempt Drive is built from
	})
	desc := fmt.Sprintf("SyncRequestAddObserverWithIdentity(replica=%d, addr=%s)", replicaID, addr)
	return Drive(desc, budget, attempt, read, AddObserverLanded(replicaID, addr))
}

// TryPromoteNode promotes the observer replicaID at addr to a full voting
// member — an AddNode for the same replica ID, address and identity — through
// whichever replica leads shardID at each attempt, and returns nil once a
// linearizable read of the membership shows it as a voter. It is
// PromoteNode's error-returning form.
func TryPromoteNode(resolve proposer.Resolver, shardID, replicaID uint64, addr, identity string, budget time.Duration) error {
	attempt, read := resolved(resolve, shardID, func(ctx context.Context, h *quicraft.Host) error {
		return h.SyncRequestAddNodeWithIdentity(ctx, shardID, replicaID, addr, identity, 0) //quicraft:direct-config-change the single attempt Drive is built from
	})
	desc := fmt.Sprintf("SyncRequestAddNodeWithIdentity(promote replica=%d, addr=%s)", replicaID, addr)
	return Drive(desc, budget, attempt, read, PromoteNodeLanded(replicaID, addr))
}

// TryDeleteNode removes replicaID through whichever replica leads shardID at
// each attempt, and returns nil once a linearizable read of the membership
// shows it holding no role. It is DeleteNode's error-returning form.
func TryDeleteNode(resolve proposer.Resolver, shardID, replicaID uint64, budget time.Duration) error {
	attempt, read := resolved(resolve, shardID, func(ctx context.Context, h *quicraft.Host) error {
		return h.SyncRequestRemoveNode(ctx, shardID, replicaID, 0) //quicraft:direct-config-change the single attempt Drive is built from
	})
	desc := fmt.Sprintf("SyncRequestRemoveNode(replica=%d)", replicaID)
	return Drive(desc, budget, attempt, read, DeleteNodeLanded(replicaID))
}

// AddNode is TryAddNode for the test goroutine: it fails the test on any
// error.
func AddNode(t *testing.T, resolve proposer.Resolver, shardID, replicaID uint64, addr, identity string, budget time.Duration) {
	t.Helper()
	if err := TryAddNode(resolve, shardID, replicaID, addr, identity, budget); err != nil {
		t.Fatal(err)
	}
}

// AddObserver is TryAddObserver for the test goroutine: it fails the test on
// any error.
func AddObserver(t *testing.T, resolve proposer.Resolver, shardID, replicaID uint64, addr, identity string, budget time.Duration) {
	t.Helper()
	if err := TryAddObserver(resolve, shardID, replicaID, addr, identity, budget); err != nil {
		t.Fatal(err)
	}
}

// PromoteNode is TryPromoteNode for the test goroutine: it fails the test on
// any error.
func PromoteNode(t *testing.T, resolve proposer.Resolver, shardID, replicaID uint64, addr, identity string, budget time.Duration) {
	t.Helper()
	if err := TryPromoteNode(resolve, shardID, replicaID, addr, identity, budget); err != nil {
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
