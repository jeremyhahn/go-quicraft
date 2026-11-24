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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestEngine_RunStepWorkerOnce_PanicLeavesQueuedItemsToCommitWorker is the
// regression test for the step-worker recovery stealing in-flight items from the
// LIVE commit worker.
//
// Were the recovery to call drainCommitC(sw.commitC) — the SAME channel
// commitWorkers[i] is concurrently consuming — then for every item it won the
// race for it would mark that item's node permanently failed, fire
// OnShardFailed and fail its proposals. With the default CommitCBufferSize of 64, one panic
// on one shard could permanently fail up to 64 unrelated healthy shards, and
// which ones was a coin flip against the live consumer. There is no automatic
// reset: recovery needs an operator StopShard/StartShard.
//
// The stated justification — that leaving an item queued lets the restarted
// worker reuse the shard's commitItem slot and enqueue the same pointer twice —
// is unreachable. getCommitSlot has exactly one caller (enqueueForCommit),
// reachable only when node.commitPending is false; commitPending is set BEFORE
// the commitC send and cleared only by a path that has already RECEIVED the item
// from the channel. While shard X's item sits in commitC, its slot is provably
// unreachable.
//
// So the contract is: a step-worker panic touches NOTHING in commitC. The one
// node that genuinely needs unwedging — the one mid-enqueueForCommit, whose item
// never reached the channel — is handled by clearStepCommitPending, and this
// test pins that half too so the contract cannot be read as "do less".
func TestEngine_RunStepWorkerOnce_PanicLeavesQueuedItemsToCommitWorker(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	sw := e.StepWorker(0)

	// Three unrelated healthy shards, each with an update already handed to the
	// commit pipeline: the step worker filled the shard's pre-allocated slot,
	// enqueued it, and set commitPending. The commit worker owns them now.
	queued := make([]*Node, 3)
	for i := range queued {
		shardID := uint64(i + 1)
		node := NewNode(nil, nil, nil, config.Config{ShardID: shardID, ReplicaID: 1},
			nil, "", nil)

		e.LoadNode(node)
		node.commitPending.Store(true)
		item := sw.getCommitSlot(shardID)
		item.node = node
		item.update = proto.Update{ShardID: shardID, ReplicaID: 1}
		sw.commitC <- item
		queued[i] = node
	}

	// A fourth shard caught mid-enqueueForCommit: commitPending set, item never
	// sent. Nothing else will ever clear it.
	midShard := uint64(4)
	midNode := NewNode(nil, nil, nil, config.Config{ShardID: midShard, ReplicaID: 1},
		nil, "", nil)

	e.LoadNode(midNode)
	midNode.commitPending.Store(true)
	sw.enqueuingNode = midNode

	// Nil the workSignal to force a panic on WaitC inside run().
	sw.workSignal = nil

	if panicked := e.runStepWorkerOnce(sw); !panicked {
		t.Fatal("expected runStepWorkerOnce to report panic from nil workSignal")
	}

	// Every queued item must still be in the channel for the commit worker.
	if got := len(sw.commitC); got != len(queued) {
		t.Fatalf("step-worker panic recovery consumed %d of %d queued commit items; "+
			"commitC is the live commit worker's channel and those items belong to it",
			len(queued)-got, len(queued))
	}
	for i, n := range queued {
		if n.IsFailed() {
			t.Errorf("shard %d was permanently FAILED by an unrelated shard's step-worker "+
				"panic; recovery requires an operator StopShard/StartShard", i+1)
		}
		if !n.commitPending.Load() {
			t.Errorf("shard %d: commitPending cleared while its item is still queued — "+
				"the restarted worker could now extract a second update for it", i+1)
		}
	}

	// The mid-enqueue node is the one the recovery DOES own, and it must be
	// unwedged: its item never reached commitC, so nothing else can clear it.
	if midNode.commitPending.Load() {
		t.Fatal("the node caught mid-enqueueForCommit stayed wedged with commitPending set")
	}
	if midNode.IsFailed() {
		t.Fatal("the mid-enqueue node must be unwedged, not failed: nothing was extracted " +
			"past stable storage for it")
	}
	if sw.enqueuingNode != nil {
		t.Fatal("enqueuingNode should be cleared after recovery")
	}
}
