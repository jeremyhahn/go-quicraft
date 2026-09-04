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

// Package proposer submits test proposals to whichever replica leads a shard
// AT THE MOMENT OF THE CALL.
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
func Try(
	resolve Resolver,
	shardID uint64,
	cmd []byte,
	timeout time.Duration,
) (quicraft.RequestResult, error) {
	deadline := time.Now().Add(timeout)
	retry := time.NewTicker(retryInterval)
	defer retry.Stop()
	expiry := time.NewTimer(timeout)
	defer expiry.Stop()

	// Reported if the deadline passes without a leader ever being observed,
	// which is itself a not-leader condition.
	lastErr := quicraft.ErrNotLeader

	for {
		if h := resolve(shardID); h != nil {
			remaining := time.Until(deadline)
			if remaining < minAttempt {
				return quicraft.RequestResult{}, lastErr
			}
			result, err := Once(h, shardID, cmd, remaining)
			if err == nil {
				return result, nil
			}
			if !IsNotLeaderRejection(err) {
				return quicraft.RequestResult{}, err
			}
			lastErr = err
		}
		select {
		case <-expiry.C:
			return quicraft.RequestResult{}, lastErr
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

// IsNotLeaderRejection reports whether err is the not-leader rejection and
// nothing else. It is the ONLY error Try retries.
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
