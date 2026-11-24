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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// TestStepWorker_PreVoteLeavesNodeWhileCommitPending checks a pre-vote is sent while commitPending is set (it is free-order).
func TestStepWorker_PreVoteLeavesNodeWhileCommitPending(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestVoterPeer(t, 1, 1, 3)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// The commit/apply pipeline owns the node, exactly as it does under
	// sustained write load.
	node.commitPending.Store(true)

	// Only tick() runs: this is the sole path a campaign can reach the wire
	// from while commitPending is set.
	for range 400 {
		sw.tick()
		if countMessages(sender, proto.RequestPreVote) > 0 {
			break
		}
	}

	if got := countMessages(sender, proto.RequestPreVote); got == 0 {
		t.Fatalf("no RequestPreVote left the node while commitPending was set "+
			"(peer term=%d, %d messages sent in total). Every campaign this node "+
			"launches under write load is buffered instead of transmitted, so "+
			"the shard pre-campaigns forever and never elects a leader",
			peer.Term(), len(sender.sentMessages()))
	}
}

// countMessages returns how many messages of the given type the test sender
// has been handed.
func countMessages(s *testSender, msgType uint64) int {
	n := 0
	for _, m := range s.sentMessages() {
		if m.Type == msgType {
			n++
		}
	}
	return n
}

// gatedApplier is an Applier that can be held inside Apply, which is what
// keeps a node's commitPending flag set: the flag is raised when an update
// enters the commit pipeline and cleared only once the state machine has
// applied it.
type gatedApplier struct {
	mu      sync.Mutex
	blocked bool
	cond    *sync.Cond
}

func newGatedApplier() *gatedApplier {
	g := &gatedApplier{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *gatedApplier) Apply(entries []proto.Entry, results []sm.Result) error {
	g.mu.Lock()
	for g.blocked {
		g.cond.Wait()
	}
	g.mu.Unlock()
	for i := range results {
		if i < len(entries) {
			results[i] = sm.Result{Value: entries[i].Index}
		}
	}
	return nil
}

func (g *gatedApplier) block() {
	g.mu.Lock()
	g.blocked = true
	g.mu.Unlock()
}

func (g *gatedApplier) release() {
	g.mu.Lock()
	g.blocked = false
	g.mu.Unlock()
	g.cond.Broadcast()
}

var _ Applier = (*gatedApplier)(nil)

// TestPreVote_RoundCompletesUnderSustainedApplyBacklog checks a five-replica cluster elects a new leader while every replica's apply is blocked.
func TestPreVote_RoundCompletesUnderSustainedApplyBacklog(t *testing.T) {
	gate := newGatedApplier()
	c := newTestCluster(t, 1, 5, withApplier(func(uint64) Applier { return gate }))
	t.Cleanup(gate.release)

	leader := c.waitForLeader(t, 15*time.Second)
	if leader == 0 {
		t.Fatal("no leader elected in the initial cluster")
	}

	// Stall every state machine, then drive writes through the leader so each
	// replica has committed entries waiting on the stalled apply.
	gate.block()
	for i := range 8 {
		lr := c.replicas[leader]
		lr.node.DeliverProposal([]proto.Entry{{Cmd: []byte{byte(i)}}})
		lr.engine.NotifyWork(c.shardID)
	}

	// The whole cluster must be in the commitPending state, otherwise the test
	// would prove nothing about which messages may leave it.
	waitFor(t, 15*time.Second, 5*time.Millisecond,
		"not every replica reached commitPending; the test cannot observe the free-order restriction",
		func() bool {
			for _, r := range c.replicas {
				if !r.node.commitPending.Load() {
					return false
				}
			}
			return true
		})

	termAtKill := c.replicas[leader].node.Peer().Term()
	c.router.kill(leader)

	waitFor(t, 30*time.Second, 5*time.Millisecond,
		fmt.Sprintf("no surviving replica completed a pre-vote round after the leader "+
			"was killed (term at kill=%d). With every replica's apply "+
			"stalled — sustained write load — pre-vote traffic never leaves the "+
			"nodes, so the shard pre-campaigns indefinitely and stays leaderless "+
			"with quorum fully available", termAtKill),
		func() bool {
			for id, r := range c.replicas {
				if id != leader && r.node.Peer().Term() > termAtKill {
					return true
				}
			}
			return false
		})
}
