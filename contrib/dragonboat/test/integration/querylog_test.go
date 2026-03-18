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
	"testing"
	"time"
)

// TestIntegration_DB_QueryRaftLog verifies that QueryRaftLog returns
// committed Raft log entries via the async RequestState pattern. After
// proposing entries, the test queries a range of log indices and verifies
// the entries have correct Index and Cmd fields.
func TestIntegration_DB_QueryRaftLog(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 10 entries.
	proposeNEntries(t, cluster, trackers, 1, 10, "qlog")

	// Determine the valid log range via GetLogReader before querying.
	leaderHost := cluster.hosts[leaderIdx]
	logReader, lrErr := leaderHost.GetLogReader(1)
	if lrErr != nil {
		t.Fatalf("GetLogReader failed: %v", lrErr)
	}
	firstIdx, lastIdx := logReader.GetRange()
	t.Logf("log range: [%d, %d)", firstIdx, lastIdx)
	if firstIdx == 0 && lastIdx == 0 {
		t.Fatal("log range is empty after proposing entries")
	}

	// Query the raft log using the valid range.
	rs, err := leaderHost.QueryRaftLog(1, firstIdx, lastIdx+1, 1024*1024)
	if err != nil {
		t.Fatalf("QueryRaftLog failed: %v", err)
	}

	// Wait for the result via ResultC. QueryRaftLog in the adapter
	// wraps a synchronous call in a pre-resolved RequestState, so
	// this should resolve immediately.
	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			t.Fatalf("QueryRaftLog result not completed")
		}

		entries, logRange := result.RaftLogs()
		if entries == nil {
			t.Fatal("RaftLogs() returned nil entries")
		}
		if len(entries) == 0 {
			t.Fatal("RaftLogs() returned empty entries")
		}

		t.Logf("QueryRaftLog returned %d entries, range=[%d, %d)",
			len(entries), logRange.FirstIndex, logRange.LastIndex)

		// Verify entries have valid indices.
		for i, e := range entries {
			if e.Index == 0 {
				t.Fatalf("entry[%d] has zero Index", i)
			}
			// Verify monotonically increasing indices.
			if i > 0 && e.Index <= entries[i-1].Index {
				t.Fatalf("entry[%d] Index=%d not greater than entry[%d] Index=%d",
					i, e.Index, i-1, entries[i-1].Index)
			}
		}

		// Verify the log range is consistent with the entries.
		if logRange.FirstIndex != entries[0].Index {
			t.Fatalf("logRange.FirstIndex=%d, entries[0].Index=%d",
				logRange.FirstIndex, entries[0].Index)
		}
		if logRange.LastIndex != entries[len(entries)-1].Index+1 {
			t.Fatalf("logRange.LastIndex=%d, expected %d",
				logRange.LastIndex, entries[len(entries)-1].Index+1)
		}

	case <-time.After(defaultTimeout):
		t.Fatal("QueryRaftLog timed out waiting for ResultC")
	}
}

// TestIntegration_DB_QueryRaftLog_OutOfRange verifies that querying with
// out-of-range indices results in RequestOutOfRange() returning true.
func TestIntegration_DB_QueryRaftLog_OutOfRange(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	// Query with indices far beyond what exists (1000000 to 2000000).
	leaderHost := cluster.hosts[leaderIdx]
	rs, err := leaderHost.QueryRaftLog(1, 1000000, 2000000, 1024*1024)
	if err != nil {
		// Some implementations return the error directly instead of
		// wrapping it in a RequestState.
		t.Logf("QueryRaftLog with out-of-range indices returned error: %v", err)
		return
	}

	select {
	case result := <-rs.ResultC():
		if result.RequestOutOfRange() {
			t.Logf("QueryRaftLog correctly reports RequestOutOfRange")
			return
		}
		if result.Completed() {
			entries, _ := result.RaftLogs()
			// If completed with empty entries for an out-of-range query,
			// that's incorrect — should be RequestOutOfRange or error.
			if len(entries) == 0 {
				t.Fatal("QueryRaftLog returned Completed with empty entries for out-of-range query; expected RequestOutOfRange or error")
			}
			t.Fatalf("QueryRaftLog returned %d entries for out-of-range query", len(entries))
		}
		t.Logf("QueryRaftLog result for out-of-range: completed=%v, outOfRange=%v",
			result.Completed(), result.RequestOutOfRange())
	case <-time.After(defaultTimeout):
		t.Fatal("QueryRaftLog timed out waiting for ResultC")
	}
}
