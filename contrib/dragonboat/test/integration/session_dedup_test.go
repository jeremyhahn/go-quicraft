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

package integration_test

import (
	"context"
	"errors"
	"testing"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
)

// TestIntegration_DB_TrackedSession_DuplicateRejection verifies at-most-once
// enforcement: re-proposing with the same series ID (without calling
// ProposalCompleted) is either rejected or returns the cached result. After
// ProposalCompleted is called and the series ID advances, the next proposal
// with the incremented series succeeds normally.
func TestIntegration_DB_TrackedSession_DuplicateRejection(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Resolved AFTER the warmup above, not from the index read before it: the
	// tracked session below is registered on this replica and every proposal in
	// this test is deliberately addressed to it, so the one resolve that does
	// happen must be as late as possible.
	leaderHost := cluster.waitForLeaderHost(1, leaderElectionTimeout)

	// Register a tracked session.
	getCtx, getCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer getCancel()
	session, err := leaderHost.SyncGetSession(getCtx, 1)
	if err != nil {
		t.Fatalf("SyncGetSession failed: %v", err)
	}
	if session.IsNoOPSession() {
		t.Fatal("expected tracked session, got no-op session")
	}
	seriesBefore := session.SeriesID()
	t.Logf("tracked session registered: ClientID=%d, SeriesID=%d", session.ClientID(), seriesBefore)

	// First proposal with series ID N -- must succeed.
	prop1Ctx, prop1Cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer prop1Cancel()
	cmd1 := encodeKVPut("dup-reject-key", "dup-reject-val")
	//quicraft:direct-propose the tracked session's SERIES ID is the subject here.
	// tryProposeToLeader retries a not-leader rejection by re-proposing with the
	// series unchanged, which is indistinguishable from the duplicate this suite
	// exists to detect -- it would silently produce the cached-result outcome the
	// test is trying to attribute to the RSM. The session was registered on this
	// replica and every assertion here is about what it does next.
	result1, prop1Err := leaderHost.SyncPropose(prop1Ctx, session, cmd1)
	if prop1Err != nil {
		t.Fatalf("first SyncPropose failed: %v", prop1Err)
	}
	if result1.Value != uint64(len("dup-reject-val")) {
		t.Fatalf("first result.Value = %d, want %d", result1.Value, len("dup-reject-val"))
	}
	t.Logf("first proposal succeeded with SeriesID=%d", seriesBefore)

	// Do NOT call ProposalCompleted. Re-propose with the same session
	// (same series ID). The RSM should either reject the duplicate or
	// return the cached result from the first proposal.
	dup1Ctx, dup1Cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer dup1Cancel()
	cmd2 := encodeKVPut("dup-reject-key2", "dup-reject-val2")
	//quicraft:direct-propose the tracked session's SERIES ID is the subject here.
	// tryProposeToLeader retries a not-leader rejection by re-proposing with the
	// series unchanged, which is indistinguishable from the duplicate this suite
	// exists to detect -- it would silently produce the cached-result outcome the
	// test is trying to attribute to the RSM. The session was registered on this
	// replica and every assertion here is about what it does next.
	dupResult, dupErr := leaderHost.SyncPropose(dup1Ctx, session, cmd2)

	if dupErr != nil {
		// Duplicate proposal was rejected with an error. This is the
		// expected behavior for at-most-once semantics: the session
		// manager rejects the duplicate because the series ID has not
		// advanced.
		t.Logf("duplicate proposal correctly rejected: %v", dupErr)
		if !errors.Is(dupErr, dragonboat.ErrRejected) {
			t.Logf("note: rejection error type is %T (may vary by implementation)", dupErr)
		}
	} else {
		// The duplicate was accepted and returned a cached result. This
		// is also valid at-most-once behavior: the RSM returns the same
		// result as the original proposal without re-applying it.
		t.Logf("duplicate proposal returned cached result: Value=%d", dupResult.Value)
		if dupResult.Value != result1.Value {
			t.Fatalf("cached result mismatch: got %d, want %d (same as first)", dupResult.Value, result1.Value)
		}
	}

	// Verify the second key was NOT written (the duplicate should not
	// have applied the different command). The proposals above stay pinned
	// because the SERIES ID is the subject; this read is not, so it resolves
	// the leader.
	readResult := cluster.readFromLeader(t, 1, encodeKVGet("dup-reject-key2"), defaultTimeout)
	val, ok := readResult.(string)
	if ok && val == "dup-reject-val2" {
		// If the duplicate wrote a different value, that is a violation
		// of at-most-once semantics.
		t.Fatal("duplicate proposal should NOT have applied a different command")
	}
	t.Logf("confirmed: duplicate command was not applied to state machine")

	// Now advance the series ID via ProposalCompleted.
	session.ProposalCompleted()
	seriesAfter := session.SeriesID()
	if seriesAfter <= seriesBefore {
		t.Fatalf("SeriesID did not advance: before=%d, after=%d", seriesBefore, seriesAfter)
	}
	t.Logf("SeriesID advanced: %d -> %d", seriesBefore, seriesAfter)

	// Propose with the advanced series ID -- must succeed.
	prop3Ctx, prop3Cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer prop3Cancel()
	cmd3 := encodeKVPut("dup-post-key", "dup-post-val")
	//quicraft:direct-propose the tracked session's SERIES ID is the subject here.
	// tryProposeToLeader retries a not-leader rejection by re-proposing with the
	// series unchanged, which is indistinguishable from the duplicate this suite
	// exists to detect -- it would silently produce the cached-result outcome the
	// test is trying to attribute to the RSM. The session was registered on this
	// replica and every assertion here is about what it does next.
	result3, prop3Err := leaderHost.SyncPropose(prop3Ctx, session, cmd3)
	if prop3Err != nil {
		t.Fatalf("post-advance SyncPropose failed: %v", prop3Err)
	}
	if result3.Value != uint64(len("dup-post-val")) {
		t.Fatalf("post-advance result.Value = %d, want %d", result3.Value, len("dup-post-val"))
	}
	t.Logf("post-advance proposal succeeded with SeriesID=%d", seriesAfter)

	// Verify the post-advance entry was written.
	readResult3 := cluster.readFromLeader(t, 1, encodeKVGet("dup-post-key"), defaultTimeout)
	val3, ok3 := readResult3.(string)
	if !ok3 || val3 != "dup-post-val" {
		t.Fatalf("post-advance SyncRead = %q, want %q", val3, "dup-post-val")
	}

	// Verify replication to all nodes.
	waitForReplication(t, cluster, trackers, 1, "dup-reject-key", "dup-reject-val", defaultTimeout)
	waitForReplication(t, cluster, trackers, 1, "dup-post-key", "dup-post-val", defaultTimeout)

	// Close the tracked session.
	closeCtx, closeCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer closeCancel()
	if closeErr := leaderHost.SyncCloseSession(closeCtx, session); closeErr != nil {
		t.Fatalf("SyncCloseSession failed: %v", closeErr)
	}
	t.Logf("duplicate rejection test passed: at-most-once semantics verified")
}
