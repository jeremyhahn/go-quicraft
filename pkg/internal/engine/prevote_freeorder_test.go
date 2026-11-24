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

// TestStepWorker_PreVoteLeavesNodeWhileCommitPending is the narrow, fully
// deterministic half of the pre-vote free-order contract.
//
// While commitPending is set, the step worker transmits ONLY the messages
// proto.IsFreeOrderMessage admits: processReady defers the whole shard to
// processCommitPendingInbox and tick()'s inline flush is a
// DrainFreeOrderMessages call. A campaign launched by tick() therefore reaches
// the wire only if RequestPreVote is classified free-order. It legitimately is
// — becomePreCandidate changes neither term nor vote, so a pre-vote has no
// durable state to wait behind — and classifying it as ordered starved
// elections outright: on a busy shard commitPending is set almost
// continuously, so pre-vote traffic sat in the outbound buffer while replicas
// pre-campaigned dozens of times without a single round completing.
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

// TestPreVote_RoundCompletesUnderSustainedApplyBacklog is the cluster-level
// half of the same contract, on a real five-replica in-process cluster.
//
// The state machine is held inside Apply on every replica, which is exactly
// what sustained write load does: commitPending stays set, so no replica ever
// reaches the normal processReady path and the ONLY messages that leave any of
// them are the free-order ones. The leader is then killed.
//
// A completed pre-vote round is directly observable: becomeCandidate is the
// only thing that raises a follower's term, and PreVote gates it on a quorum
// of grants. So a surviving replica reaching a higher term proves both
// directions travelled under commitPending — the RequestPreVote out of the
// campaigner and the RequestPreVoteResp back from a quorum of responders.
//
// This is the failure the reclassification fixed: measured on a five-node
// cluster under chaos, all three surviving replicas pre-campaigned ~28 times
// each without a single round completing, leaving the shard leaderless for
// over 20 seconds with quorum fully available.
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
