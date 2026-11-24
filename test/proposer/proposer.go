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

// Package proposer submits test proposals and linearizable reads to whichever replica leads a shard at the moment of the call, re-resolving before every attempt through a Resolver, since an observed leader can change; reads also retry ErrReadIndexNotReady (thesis 6.4), and pinned callers share the same loop through Pinned.
package proposer

import (
	"context"
	"errors"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// Resolver returns the Host that leads shardID now, or nil when no live replica leads; a function so callers can ask again.
type Resolver func(shardID uint64) *quicraft.Host

// Pinned is the Resolver for a caller that means one replica: it always returns h, so pinned callers share the retry loop (see TryWith).
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

// ToLeader proposes cmd to shardID's current leader, waits for the commit and fails the test on error; a site that means one specific replica keeps a direct call with a quicraft:direct-propose marker (see test/buildgates).
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

// Try is ToLeader's error-returning form: it retries only the not-leader rejection, re-resolving the leader, and returns every other error at once. Re-proposal may apply twice, which is safe for these idempotent puts; pinned callers use TryWith with Pinned and quicraft.Temporary.
func Try(
	resolve Resolver,
	shardID uint64,
	cmd []byte,
	timeout time.Duration,
) (quicraft.RequestResult, error) {
	return TryWith(resolve, shardID, cmd, timeout, IsNotLeaderRejection)
}

// TryWith re-resolves the target before every attempt and retries exactly the errors retryable accepts: re-resolving callers retry not-leader only, pinned callers retry quicraft.Temporary. retryable must not be nil.
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

// drive is the package's one retry loop: attempt receives the Host resolved for this attempt (never nil) and its remaining budget (at least minAttempt).
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

// driveRead is drive with the re-resolving read loops' retry set, readRefusal.
// TryRead and TryReadIndex both take it from here, so the two cannot disagree
// on what a read retries, and the unit tests can script the attempt.
func driveRead[T any](
	resolve Resolver,
	shardID uint64,
	timeout time.Duration,
	attempt func(h *quicraft.Host, budget time.Duration) (T, error),
) (T, error) {
	return drive(resolve, shardID, timeout, readRefusal, attempt)
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

// Read reads query linearizably from shardID's current leader and fails the test on error; a site that means one specific replica keeps a direct call with a quicraft:direct-read marker.
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

// TryRead is Read's error-returning form, retrying readRefusal errors with a re-resolve before each attempt; a refused read was never answered, so re-issuing it is safe.
func TryRead(
	resolve Resolver,
	shardID uint64,
	query interface{},
	timeout time.Duration,
) (interface{}, error) {
	return driveRead(resolve, shardID, timeout,
		func(h *quicraft.Host, budget time.Duration) (interface{}, error) {
			return readOnce(h, shardID, query, budget)
		})
}

// TryReadWith is TryRead's general form: it resolves the target before every attempt and retries exactly what retryable accepts, for pinned reads (Pinned(h), quicraft.Temporary). retryable must not be nil.
func TryReadWith(
	resolve Resolver,
	shardID uint64,
	query interface{},
	timeout time.Duration,
	retryable Retryable,
) (interface{}, error) {
	return drive(resolve, shardID, timeout, retryable,
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

// ReadIndex obtains a confirmed read index from the current leader of shardID, waits for it, fails the test on error and returns the index.
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
	return driveRead(resolve, shardID, timeout,
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

// IsNotLeaderRejection reports whether err is the not-leader rejection (quicraft.NotLeader, wrapped or in *quicraft.MaybeAppliedError) and nothing else.
func IsNotLeaderRejection(err error) bool {
	_, ok := quicraft.NotLeader(err)
	return ok
}

// readRefusal is the Retryable of TryRead and TryReadIndex: the not-leader rejection or ErrReadIndexNotReady, narrower than quicraft.Temporary.
func readRefusal(err error) bool {
	return IsNotLeaderRejection(err) || errors.Is(err, quicraft.ErrReadIndexNotReady)
}
