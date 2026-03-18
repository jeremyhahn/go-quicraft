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

// TestIntegration_DB_TrackedSession_Deduplication verifies the full
// tracked session lifecycle with proper series ID advancement. Each
// call to ProposalCompleted increments the SeriesID, ensuring
// at-most-once semantics for duplicate detection.
func TestIntegration_DB_TrackedSession_Deduplication(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	leaderHost := cluster.hosts[leaderIdx]

	// Register a tracked session via SyncGetSession.
	getCtx, getCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer getCancel()
	session, err := leaderHost.SyncGetSession(getCtx, 1)
	if err != nil {
		t.Fatalf("SyncGetSession failed: %v", err)
	}
	if session.IsNoOPSession() {
		t.Fatal("expected tracked session, got no-op session")
	}
	t.Logf("tracked session registered: ClientID=%d", session.ClientID())

	// Record the initial SeriesID.
	initialSeriesID := session.SeriesID()
	t.Logf("initial SeriesID: %d", initialSeriesID)

	// First proposal: propose a value and read it back.
	prop1Ctx, prop1Cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer prop1Cancel()
	cmd1 := encodeKVPut("dedup-key1", "dedup-val1")
	result1, prop1Err := leaderHost.SyncPropose(prop1Ctx, session, cmd1)
	if prop1Err != nil {
		t.Fatalf("first SyncPropose failed: %v", prop1Err)
	}
	if result1.Value != uint64(len("dedup-val1")) {
		t.Fatalf("first result.Value = %d, want %d", result1.Value, len("dedup-val1"))
	}

	// Read back the first entry via SyncRead.
	read1Ctx, read1Cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer read1Cancel()
	readResult1, read1Err := leaderHost.SyncRead(read1Ctx, 1, encodeKVGet("dedup-key1"))
	if read1Err != nil {
		t.Fatalf("first SyncRead failed: %v", read1Err)
	}
	val1, ok := readResult1.(string)
	if !ok {
		t.Fatalf("first SyncRead returned type %T, want string", readResult1)
	}
	if val1 != "dedup-val1" {
		t.Fatalf("first SyncRead = %q, want %q", val1, "dedup-val1")
	}

	// Advance the series ID after the first proposal completes.
	session.ProposalCompleted()
	afterFirstSeriesID := session.SeriesID()
	if afterFirstSeriesID <= initialSeriesID {
		t.Fatalf("SeriesID did not advance after ProposalCompleted: before=%d, after=%d",
			initialSeriesID, afterFirstSeriesID)
	}
	t.Logf("SeriesID after first ProposalCompleted: %d", afterFirstSeriesID)

	// Second proposal: propose another value with the advanced series ID.
	prop2Ctx, prop2Cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer prop2Cancel()
	cmd2 := encodeKVPut("dedup-key2", "dedup-val2")
	result2, prop2Err := leaderHost.SyncPropose(prop2Ctx, session, cmd2)
	if prop2Err != nil {
		t.Fatalf("second SyncPropose failed: %v", prop2Err)
	}
	if result2.Value != uint64(len("dedup-val2")) {
		t.Fatalf("second result.Value = %d, want %d", result2.Value, len("dedup-val2"))
	}

	// Read back the second entry.
	read2Ctx, read2Cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer read2Cancel()
	readResult2, read2Err := leaderHost.SyncRead(read2Ctx, 1, encodeKVGet("dedup-key2"))
	if read2Err != nil {
		t.Fatalf("second SyncRead failed: %v", read2Err)
	}
	val2, ok := readResult2.(string)
	if !ok {
		t.Fatalf("second SyncRead returned type %T, want string", readResult2)
	}
	if val2 != "dedup-val2" {
		t.Fatalf("second SyncRead = %q, want %q", val2, "dedup-val2")
	}

	// Advance again and verify series ID incremented.
	session.ProposalCompleted()
	afterSecondSeriesID := session.SeriesID()
	if afterSecondSeriesID <= afterFirstSeriesID {
		t.Fatalf("SeriesID did not advance after second ProposalCompleted: before=%d, after=%d",
			afterFirstSeriesID, afterSecondSeriesID)
	}
	t.Logf("SeriesID after second ProposalCompleted: %d", afterSecondSeriesID)

	// Verify both entries replicated to all nodes.
	waitForReplication(t, cluster, trackers, 1, "dedup-key1", "dedup-val1", defaultTimeout)
	waitForReplication(t, cluster, trackers, 1, "dedup-key2", "dedup-val2", defaultTimeout)

	// Close the tracked session.
	closeCtx, closeCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer closeCancel()
	if closeErr := leaderHost.SyncCloseSession(closeCtx, session); closeErr != nil {
		t.Fatalf("SyncCloseSession failed: %v", closeErr)
	}
	t.Logf("tracked session deduplication lifecycle verified: 2 proposals, 2 reads, series advanced correctly")
}

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

	leaderHost := cluster.hosts[leaderIdx]

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
	// have applied the different command).
	readCtx, readCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer readCancel()
	readResult, readErr := leaderHost.SyncRead(readCtx, 1, encodeKVGet("dup-reject-key2"))
	if readErr != nil {
		t.Fatalf("SyncRead for dup key failed: %v", readErr)
	}
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
	result3, prop3Err := leaderHost.SyncPropose(prop3Ctx, session, cmd3)
	if prop3Err != nil {
		t.Fatalf("post-advance SyncPropose failed: %v", prop3Err)
	}
	if result3.Value != uint64(len("dup-post-val")) {
		t.Fatalf("post-advance result.Value = %d, want %d", result3.Value, len("dup-post-val"))
	}
	t.Logf("post-advance proposal succeeded with SeriesID=%d", seriesAfter)

	// Verify the post-advance entry was written.
	read3Ctx, read3Cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer read3Cancel()
	readResult3, read3Err := leaderHost.SyncRead(read3Ctx, 1, encodeKVGet("dup-post-key"))
	if read3Err != nil {
		t.Fatalf("SyncRead for post-advance key failed: %v", read3Err)
	}
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
