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
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// raftMuProbeCallback records, for each callback it receives, whether
// node.raftMu was free at the moment of the call. A sync.Mutex TryLock on the
// goroutine that already holds it returns false, so this is an exact test of
// "was this callback invoked under the node lock", not a timing heuristic.
type raftMuProbeCallback struct {
	testCallback
	node *Node

	readIndexBatchedFree *bool
	readIndexFailedFree  *bool
	proposalFailedFree   *bool
}

func (c *raftMuProbeCallback) probe() bool {
	if c.node.raftMu.TryLock() {
		c.node.raftMu.Unlock()
		return true
	}
	return false
}

func (c *raftMuProbeCallback) OnReadIndexBatched(shardID, batchKey uint64, aliasKeys []uint64) {
	free := c.probe()
	c.readIndexBatchedFree = &free
	c.testCallback.OnReadIndexBatched(shardID, batchKey, aliasKeys)
}

func (c *raftMuProbeCallback) OnReadIndexFailed(shardID uint64, keys []uint64, err error) {
	free := c.probe()
	c.readIndexFailedFree = &free
	c.testCallback.OnReadIndexFailed(shardID, keys, err)
}

func (c *raftMuProbeCallback) OnProposalRejected(shardID uint64, entries []proto.Entry, err error) {
	free := c.probe()
	c.proposalFailedFree = &free
	c.testCallback.OnProposalRejected(shardID, entries, err)
}

// TestProcessCommitPendingInbox_CallbacksRunOutsideRaftMu pins that engine callbacks are not invoked under node.raftMu.
func TestProcessCommitPendingInbox_CallbacksRunOutsideRaftMu(t *testing.T) {
	const shardID, replicaID = 1, 1

	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	peer, lr := newTestVoterPeer(t, shardID, replicaID, 3)
	nodeCfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	nodeCfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, nodeCfg, nil, "", nil)

	cb := &raftMuProbeCallback{node: node}
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)
	sw.loadNode(node)

	// The commit pipeline owns this shard: this is the path under test.
	node.commitPending.Store(true)

	// Two ReadIndex keys so the batch registers an alias, and one proposal.
	// Both are rejected by a leaderless follower.
	if !node.DeliverReadIndex(11) || !node.DeliverReadIndex(12) {
		t.Fatal("readIndexQ rejected the test keys")
	}
	if !node.DeliverProposal([]proto.Entry{{Index: 0, Term: 0, Cmd: []byte("x")}}) {
		t.Fatal("proposalQ rejected the test entry")
	}

	sw.processCommitPendingInbox(shardID, node)

	if cb.readIndexBatchedFree == nil {
		t.Fatal("OnReadIndexBatched never fired; the test did not exercise the path")
	}
	if !*cb.readIndexBatchedFree {
		t.Error("OnReadIndexBatched ran while node.raftMu was held: it blocks the " +
			"commit worker's Commit for every shard that worker owns")
	}

	if cb.readIndexFailedFree == nil {
		t.Fatal("OnReadIndexFailed never fired; a leaderless follower must reject " +
			"the batched ReadIndex")
	}
	if !*cb.readIndexFailedFree {
		t.Error("OnReadIndexFailed ran while node.raftMu was held")
	}

	if cb.proposalFailedFree == nil {
		t.Fatal("OnProposalRejected never fired; a leaderless follower must reject " +
			"the batched Propose")
	}
	if !*cb.proposalFailedFree {
		t.Error("OnProposalRejected ran while node.raftMu was held: this is the hot " +
			"path, driven by NotLeaderError on every leaderless follower")
	}

	// The alias registration still precedes the raft submission, and every stranded read is completed with the submit error.
	if got := cb.batchCount.Load(); got != 1 {
		t.Fatalf("expected exactly one ReadIndex batch registration, got %d", got)
	}
	if got := cb.readIndexFailedCount.Load(); got != 1 {
		t.Fatalf("expected exactly one ReadIndex failure completion, got %d", got)
	}
	if got := cb.proposalRejectedCount.Load(); got != 1 {
		t.Fatalf("expected exactly one proposal rejection completion, got %d", got)
	}
}

var _ Callback = (*raftMuProbeCallback)(nil)

// TestNotePiggybackFailure_CountsAndRateLimits pins that a GetPiggybackUpdate error is counted and its log rate-limited.
func TestNotePiggybackFailure_CountsAndRateLimits(t *testing.T) {
	// captureLogOutput filters at ERROR; the degradation record is a WARN, so
	// this test installs its own handler.
	logBuf := &logCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	// Claim the report slot so the interval is running, and start from a known
	// counter value.
	lastPiggybackReport.Store(time.Now().UnixNano())
	before := PiggybackFailures()

	const n = 5
	for range n {
		notePiggybackFailure(1, 1, errors.New("piggyback read failed"))
	}

	if got := PiggybackFailures() - before; got != n {
		t.Fatalf("counted %d failures, want %d: the counter is what makes a "+
			"permanently degraded fast path alertable", got, n)
	}
	if c := strings.Count(logBuf.String(), "commit fast path degraded"); c != 0 {
		t.Fatalf("emitted %d log records inside the rate-limit interval, want 0", c)
	}

	// Expire the interval: the next failure must report, carrying the total.
	lastPiggybackReport.Store(time.Now().Add(-2 * piggybackReportInterval).UnixNano())
	notePiggybackFailure(1, 1, errors.New("piggyback read failed"))
	if c := strings.Count(logBuf.String(), "commit fast path degraded"); c != 1 {
		t.Fatalf("emitted %d log records after the interval expired, want 1", c)
	}
	if !strings.Contains(logBuf.String(), "total_failures") {
		t.Fatal("the report must carry the running total, not just the latest error")
	}
}
