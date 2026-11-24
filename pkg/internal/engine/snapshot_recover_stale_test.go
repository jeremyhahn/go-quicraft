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

package engine

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
)

// failingRecoverSnapshotter refuses RecoverFromSnapshot, so a test can observe
// what the pool does with a restore that actually went wrong -- as opposed to
// one that was declined before the state machine was touched.
type failingRecoverSnapshotter struct {
	*snapshotTestSnapshotter
	err error
}

func (s *failingRecoverSnapshotter) RecoverFromSnapshot(context.Context, io.Reader, <-chan struct{}) error {
	return s.err
}

// TestSnapshotPool_StaleRecovery_DeclinesWithoutFailingTheShard verifies a stale recovery is refused and the shard stays alive.
func TestSnapshotPool_StaleRecovery_DeclinesWithoutFailingTheShard(t *testing.T) {
	// Register wg.Wait first so it runs after close(stopC).
	var wg sync.WaitGroup
	defer wg.Wait()
	stopC := make(chan struct{})
	defer close(stopC)
	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.start(&wg)

	// The follower's own durable snapshot sits at 500 while it has applied
	// through 900: the shape a stale InstallSnapshot produces.
	seedRecoverableSnapshot(t, ldb, 1, 1, 500, 4, 500)

	ss := newSnapshotTestSnapshotter(900)
	node, peer := newRecoveryTestNode(t, 1, 1, ss)
	node.SeedLastApplied(900, 6)
	installSnapshotIntoLog(t, node, peer, 900, 6)

	if err := p.requestSnapshot(snapshotRequest{shardID: 1, replicaID: 1, node: node}); err != nil {
		t.Fatalf("requestSnapshot: %v", err)
	}

	res := awaitSnapshotResult(t, p, "recovery")
	var stale *SnapshotRecoveryStaleError
	if !errors.As(res.err, &stale) {
		t.Fatalf("recovery error = %T (%v), want *SnapshotRecoveryStaleError", res.err, res.err)
	}
	if stale.SnapshotIndex != 500 || stale.LastApplied != 900 {
		t.Errorf("stale error = %+v, want SnapshotIndex 500 / LastApplied 900", stale)
	}

	// The whole point: a declined recovery is not a dead shard.
	if node.IsFailed() {
		t.Fatal("a stale recovery failed the shard; the state machine was never " +
			"touched and is already past the snapshot, so there is nothing to fail for")
	}
	// And the shard is left usable, not wedged behind the recovering gate.
	if node.IsRecovering() {
		t.Error("the recovering gate is still raised after a declined recovery")
	}
	if got := ss.state.Load(); got != 900 {
		t.Errorf("state machine state = %d, want 900 (untouched)", got)
	}
	if got := node.LastApplied(); got != 900 {
		t.Errorf("node.LastApplied = %d, want 900 (untouched)", got)
	}
}

// TestSnapshotPool_FailedRecovery_StillFailsTheShard verifies an error from RecoverFromSnapshot fails the shard.
func TestSnapshotPool_FailedRecovery_StillFailsTheShard(t *testing.T) {
	// Register wg.Wait first so it runs after close(stopC).
	var wg sync.WaitGroup
	defer wg.Wait()
	stopC := make(chan struct{})
	defer close(stopC)
	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.start(&wg)

	// A snapshot the log holds and the state machine is legitimately behind,
	// so the bounds are satisfied and the restore is actually attempted.
	seedRecoverableSnapshot(t, ldb, 1, 1, 100, 7, 100)

	ss := &failingRecoverSnapshotter{
		snapshotTestSnapshotter: newSnapshotTestSnapshotter(0),
		err:                     errors.New("user state machine refused the snapshot"),
	}
	node, peer := newRecoveryTestNode(t, 1, 1, ss)
	installSnapshotIntoLog(t, node, peer, 100, 7)

	if err := p.requestSnapshot(snapshotRequest{shardID: 1, replicaID: 1, node: node}); err != nil {
		t.Fatalf("requestSnapshot: %v", err)
	}

	res := awaitSnapshotResult(t, p, "recovery")
	if res.err == nil {
		t.Fatal("a failed RecoverFromSnapshot completed without an error")
	}

	if !node.IsFailed() {
		t.Fatal("a failed RecoverFromSnapshot left the shard alive; applies would " +
			"resume on a possibly half-restored state machine")
	}
}
