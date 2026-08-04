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

// TestEngine_RunStepWorkerOnce_PanicDrainsCommitC is the H6 regression test:
// step-worker panic recovery must drain sw.commitC before the worker
// restarts. The step worker's per-shard commitItem slots (getCommitSlot) are
// reused with commitPending as the ownership flag — clearing the flag while
// an item is still queued lets the restarted worker overwrite that slot and
// enqueue the SAME pointer again, so the commit worker processes it twice
// (double Peer.Commit, double apply, double ReleaseCommittedEntries on the
// same pooled slice).
func TestEngine_RunStepWorkerOnce_PanicDrainsCommitC(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	sw := e.StepWorker(0)

	nodeCfg := config.Config{ShardID: 0, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Simulate an update already in flight: the step worker filled the
	// shard's pre-allocated commit slot, enqueued it, and set commitPending —
	// then panicked before the commit worker (not running here) consumed it.
	node.commitPending.Store(true)
	item := sw.getCommitSlot(0)
	item.node = node
	item.update = proto.Update{ShardID: 0, ReplicaID: 1}
	sw.commitC <- item

	// Nil the workSignal to force a panic on WaitC.
	sw.workSignal = nil

	if panicked := e.runStepWorkerOnce(sw); !panicked {
		t.Fatal("expected runStepWorkerOnce to report panic from nil workSignal")
	}

	// The stale queued item must have been drained: a restarted worker
	// reusing the slot would otherwise double-send the same pointer.
	if got := len(sw.commitC); got != 0 {
		t.Fatalf("commitC holds %d stale item(s) after step-worker panic recovery; "+
			"the restarted worker would overwrite the queued slot and the commit "+
			"worker would process the same *commitItem twice", got)
	}
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared after panic recovery")
	}
}
