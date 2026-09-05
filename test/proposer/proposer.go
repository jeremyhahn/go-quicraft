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

// Package proposer submits test proposals and linearizable reads to whichever
// replica leads a shard AT THE MOMENT OF THE CALL.
//
// # Why this exists
//
// Observing a leader does not pin it. A test that writes
//
//	leaderIdx := cluster.waitForClusterLeader(shardID, timeout)
//	leaderHost := cluster.hosts[leaderIdx]
//	...
//	_, err := leaderHost.SyncPropose(ctx, shardID, cmd)
//
// has read a value that was true when it looked and treated it as fixed. Raft
// is free to hold an election in the gap, and the proposal is then refused
// CORRECTLY while the test reports a failure the cluster never had.
//
// The refusal is narrow, which is why the pattern survives most runs and fails
// a loaded one. Host.Propose FORWARDS to the leader whenever the local replica
// knows who that is; it fails fast only when leadership has moved and the
// addressed replica does not yet know the successor. That is the
// "not leader for shard N, leader unknown" seen in CI runs 10735 and 10801.
//
// # Why it is one package and not one copy per suite
//
// The e2e, raft, host and linearizability suites each define their own
// testCluster, so the helper used to be copied into each of them. Four copies
// of a retry loop is four places a fix has to land. The suites share nothing
// but a way to name the current leader, so that is all this package asks for:
// a Resolver.
//
// # Reads share the loop, and for the same reason
//
// A linearizable read is bound to the leader exactly as a proposal is. SyncRead
// is a ReadIndex followed by a local lookup, and handleFollowerReadIndex
// forwards a ReadIndex whenever the follower knows the leader and refuses it
// with the same "not leader ... leader unknown" when it does not. A pending
// read on a leader that loses the shard is failed the same way a pending
// proposal is. So the twelve host-suite reads that pinned a leaderHost binding
// (#147) failed on the same election the proposals used to fail on, and they
// are fixed by the same loop: Read / TryRead and ReadIndex / TryReadIndex
// re-resolve before every attempt and retry ONLY the not-leader rejection.
//
// Re-issuing a refused read is safe unconditionally — a read that was refused
// was never answered, so there is no "applied twice" to reason about — but the
// retry set stays as narrow as it is for proposals. A timeout, a busy pipeline
// or ErrReadIndexNotReady comes from the replica that genuinely leads the
// shard; re-resolving changes nothing about it, so it is a real result that
// must reach the test.
//
// # The pinned callers share the same loop
//
// Some suite helpers deliberately do NOT re-resolve, because the test is about
// the replica it addressed. They had a second copy of this retry loop, with a
// blanket retry-on-any-error that reported a permanent rejection as a timeout
// (#97). They now use the same loop through Pinned, differing only in the
// Retryable they pass; TryWith documents why that one difference is real.
package proposer

import (
	"context"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// Resolver returns the Host that leads shardID right now, or nil when no live
// replica reports leadership.
//
// It is a function rather than an interface because that is the entire surface
// the suites have in common, and because it forces the caller to hand over a
// way to ask AGAIN rather than an answer taken once.
type Resolver func(shardID uint64) *quicraft.Host

// Pinned is the Resolver for a caller that has already chosen its target and
// means THAT replica: it returns h for every shard, every time.
//
// It exists so the pinned callers share one retry loop with the re-resolving
// ones instead of keeping a second copy of it. Re-resolution and retry are
// separate remedies; Pinned gives up the first and keeps the second, and
// TryWith explains which errors each remedy can actually fix.
func Pinned(h *quicraft.Host) Resolver {
	return func(uint64) *quicraft.Host { return h }
}

// Retryable reports whether an error from one attempt is worth another. It is
// the ONE axis on which this package's retry loops differ; see TryWith.
//
// quicraft.Temporary satisfies it directly, and so does IsNotLeaderRejection.
type Retryable func(error) bool

// retryInterval is how long Try waits before re-resolving after a not-leader
// rejection. An election completes in roughly one election timeout, so polling
// faster than this only burns CPU.
const retryInterval = 20 * time.Millisecond

// minAttempt is the smallest budget Try will start an attempt with. Below it
// the attempt would time out on arrival and overwrite the real cause with
// ErrTimeout, reporting leadership churn as a slow shard.
const minAttempt = 200 * time.Millisecond

// ToLeader proposes cmd to whichever replica leads shardID at the moment of the
// call, waits for the entry to commit, and fails the test on any error.
//
// Callers that mean "the current leader" use this. Callers that mean one
// SPECIFIC replica — the deposed incumbent on the minority side of a partition,
// the follower in a proposal-forwarding test — must NOT: for them the addressed
// node is the subject of the assertion, and re-resolving would test something
// else. Those sites keep their own direct call and say so with a
// quicraft:direct-propose marker (see test/buildgates).
func ToLeader(
	t *testing.T,
	resolve Resolver,
	shardID uint64,
	cmd []byte,
	timeout time.Duration,
) quicraft.RequestResult {
	t.Helper()
	result, err := Try(resolve, shardID, cmd, timeout)
	if err != nil {
		t.Fatalf("propose to the leader of shard %d failed within %v: %v",
			shardID, timeout, err)
	}
	return result
}

// Try is ToLeader's error-returning form, for callers that layer their own
// retry on top and for goroutines, which must not touch a *testing.T.
//
// ONLY the not-leader rejection is retried, and it is retried by RE-RESOLVING
// the leader rather than by re-sending to the same replica. Every other error —
// a timeout, a busy pipeline, a rejected entry, a state machine failure — is
// returned unchanged on its first occurrence. A blanket retry-on-any-error
// would mask exactly the defects these suites exist to catch.
//
// Re-proposal is safe for the commands these suites write. A not-leader error
// can arrive after the entry already reached the log
// (quicraft.MaybeApplied), so a retry may apply the command twice; every
// command here is a last-write-wins KV put, which is idempotent under
// re-proposal.
//
// A caller that addresses ONE replica on purpose wants TryWith with Pinned and
// quicraft.Temporary instead; TryWith says why the two retry sets differ.
func Try(
	resolve Resolver,
	shardID uint64,
	cmd []byte,
	timeout time.Duration,
) (quicraft.RequestResult, error) {
	return TryWith(resolve, shardID, cmd, timeout, IsNotLeaderRejection)
}

// TryWith is Try's general form: it re-resolves the target before every
// attempt and retries exactly the errors retryable reports true for.
//
// # Why there are two policies and not one
//
// The suites need two retry sets, and the difference is NOT that one loop is
// stricter for its own sake. A loop should retry an error when, and only when,
// the thing it does between attempts can change that error's answer. The two
// loops do different things between attempts, so they retry different sets:
//
//   - Try re-resolves the leader. Its remedy is "ask a DIFFERENT replica", and
//     the only error that remedy fixes is the not-leader rejection. A timeout,
//     a busy pipeline or a rejected entry came from the replica that genuinely
//     leads the shard; asking it again through a fresh resolve changes nothing,
//     so those are real results and must reach the test.
//
//   - A pinned caller (Pinned) has already chosen its replica because the test
//     is ABOUT that replica. Its remedy is "wait, and ask the SAME replica
//     again", which fixes the conditions that clear on their own: a shard still
//     coming up, a momentarily full pipeline, an election in flight. That is
//     exactly the set quicraft.Temporary names, and a pinned caller passes it.
//
// ErrNotLeader is in quicraft.Temporary, so the pinned set contains the
// re-resolving one. That is not an inconsistency, it is the reason a pinned
// caller keeps working: Host.Propose FORWARDS to the leader whenever the local
// replica knows who that is, and ErrNotLeader is only the window before it
// learns — a window that closes without any help from the caller.
//
// What neither policy is any longer is a blanket retry on every error. That is
// the policy both of these replaced (#97): it reports ErrDiskFull,
// ErrInvalidOperation or a state machine failure as "timed out", and burns the
// whole budget to say so.
//
// retryable must not be nil.
func TryWith(
	resolve Resolver,
	shardID uint64,
	cmd []byte,
	timeout time.Duration,
	retryable Retryable,
) (quicraft.RequestResult, error) {
	return drive(resolve, shardID, timeout, retryable,
		func(h *quicraft.Host, budget time.Duration) (quicraft.RequestResult, error) {
			return Once(h, shardID, cmd, budget)
		})
}

// drive is the ONE retry loop in this package. Every exported form — proposal
// or read, fatal or error-returning, re-resolving or pinned — is drive with a
// different attempt and a different retryable, so the resolve-before-every-
// attempt rule, the minimum attempt budget and the deadline arithmetic cannot
// drift between them.
//
// attempt is handed the Host resolved for THIS attempt and the budget left for
// it. drive never calls attempt with a nil Host and never starts an attempt
// with less than minAttempt.
func drive[T any](
	resolve Resolver,
	shardID uint64,
	timeout time.Duration,
	retryable Retryable,
	attempt func(h *quicraft.Host, budget time.Duration) (T, error),
) (T, error) {
	var zero T
	deadline := time.Now().Add(timeout)
	retry := time.NewTicker(retryInterval)
	defer retry.Stop()
	expiry := time.NewTimer(timeout)
	defer expiry.Stop()

	// Reported if the deadline passes without a target ever being resolved,
	// which is itself a not-leader condition. A Pinned resolver never returns
	// nil, so a pinned caller can only ever see an error an attempt produced.
	lastErr := quicraft.ErrNotLeader

	for {
		if h := resolve(shardID); h != nil {
			remaining := time.Until(deadline)
			if remaining < minAttempt {
				return zero, lastErr
			}
			result, err := attempt(h, remaining)
			if err == nil {
				return result, nil
			}
			if !retryable(err) {
				return zero, err
			}
			lastErr = err
		}
		select {
		case <-expiry.C:
			return zero, lastErr
		case <-retry.C:
		}
	}
}

// Once submits cmd to h and waits for the entry to commit, returning the first
// error from either half of that round trip. It does NOT resolve the leader and
// does NOT retry: it is the single attempt Try is built from, and the way a
// test addresses one specific replica on purpose.
func Once(
	h *quicraft.Host,
	shardID uint64,
	cmd []byte,
	timeout time.Duration,
) (quicraft.RequestResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rs, err := h.Propose(ctx, shardID, cmd) //quicraft:direct-propose the single attempt Try is built from
	if err != nil {
		return quicraft.RequestResult{}, err
	}
	result, err := rs.Result()
	rs.Release()
	if err != nil {
		return quicraft.RequestResult{}, err
	}
	return result, nil
}

// Read issues a linearizable read of query against whichever replica leads
// shardID at the moment of the call and fails the test on any error.
//
// The same rule as ToLeader decides who may use it: a caller that means "the
// current leader" does; a caller that means one SPECIFIC replica — a deposed
// incumbent whose refusal is the assertion, a follower whose forwarding is the
// subject, the target of a leadership transfer whose own lease is under test
// — keeps its direct call and says so with a quicraft:direct-read marker.
func Read(
	t *testing.T,
	resolve Resolver,
	shardID uint64,
	query interface{},
	timeout time.Duration,
) interface{} {
	t.Helper()
	result, err := TryRead(resolve, shardID, query, timeout)
	if err != nil {
		t.Fatalf("linearizable read of %v on shard %d failed within %v: %v",
			query, shardID, timeout, err)
	}
	return result
}

// TryRead is Read's error-returning form, for callers that layer their own
// retry on top and for goroutines, which must not touch a *testing.T.
//
// ONLY the not-leader rejection is retried, by RE-RESOLVING the leader. A read
// that was refused was never answered, so re-issuing it cannot violate
// linearizability; a read that was answered returns on the spot and is never
// re-issued.
func TryRead(
	resolve Resolver,
	shardID uint64,
	query interface{},
	timeout time.Duration,
) (interface{}, error) {
	return drive(resolve, shardID, timeout, IsNotLeaderRejection,
		func(h *quicraft.Host, budget time.Duration) (interface{}, error) {
			return readOnce(h, shardID, query, budget)
		})
}

// readOnce is the single attempt TryRead is built from: one SyncRead against h
// with its own deadline, no resolve, no retry.
func readOnce(
	h *quicraft.Host,
	shardID uint64,
	query interface{},
	timeout time.Duration,
) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return h.SyncRead(ctx, shardID, query) //quicraft:direct-read the single attempt TryRead is built from
}

// ReadIndex obtains a confirmed read index from whichever replica leads
// shardID at the moment of the call, waits for it to resolve, and fails the
// test on any error. It returns the confirmed index.
//
// It exists for the tests whose subject is the ReadIndex API itself. Those
// used to submit a ReadIndex to a pinned host and release the RequestState
// without reading it, which asserted only that a request was accepted; a
// not-leader outcome would have surfaced in the result they never read.
func ReadIndex(
	t *testing.T,
	resolve Resolver,
	shardID uint64,
	timeout time.Duration,
) uint64 {
	t.Helper()
	index, err := TryReadIndex(resolve, shardID, timeout)
	if err != nil {
		t.Fatalf("read index on shard %d failed within %v: %v", shardID, timeout, err)
	}
	return index
}

// TryReadIndex is ReadIndex's error-returning form. It retries exactly what
// TryRead retries, for exactly the same reason.
func TryReadIndex(
	resolve Resolver,
	shardID uint64,
	timeout time.Duration,
) (uint64, error) {
	return drive(resolve, shardID, timeout, IsNotLeaderRejection,
		func(h *quicraft.Host, budget time.Duration) (uint64, error) {
			return readIndexOnce(h, shardID, budget)
		})
}

// readIndexOnce is the single attempt TryReadIndex is built from: one
// ReadIndex against h, resolved and released, no resolve, no retry.
func readIndexOnce(
	h *quicraft.Host,
	shardID uint64,
	timeout time.Duration,
) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rs, err := h.ReadIndex(ctx, shardID) //quicraft:direct-read the single attempt TryReadIndex is built from
	if err != nil {
		return 0, err
	}
	result, err := rs.Result()
	rs.Release()
	if err != nil {
		return 0, err
	}
	return result.Value, nil
}

// IsNotLeaderRejection reports whether err is the not-leader rejection and
// nothing else. It is the ONLY error Try, TryRead and TryReadIndex retry.
//
// quicraft.NotLeader matches both the bare ErrNotLeader sentinel and a
// *quicraft.NotLeaderError, wrapped or not — including one wrapped in
// *quicraft.MaybeAppliedError, which is the form Result returns when leadership
// moves out from under an entry that is already in the log.
//
// Deliberately NARROW: a timeout, a busy pipeline or a closed shard is a real
// result here and must fail the test.
func IsNotLeaderRejection(err error) bool {
	_, ok := quicraft.NotLeader(err)
	return ok
}
