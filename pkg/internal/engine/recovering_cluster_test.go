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
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// In-process multi-replica cluster harness
//
// The harness runs a real Engine per replica (step + commit + apply +
// snapshot workers, WAL-mode with an in-memory LogDB) and routes Raft
// messages between them through a shared router. It exists to exercise
// consensus-level liveness properties that a single-node peer test cannot
// reach: elections require RequestVote/RequestVoteResp exchange across
// replicas, and vote responses are ORDERED messages that only leave a
// replica after the commit worker has persisted the vote.
// ---------------------------------------------------------------------------

// clusterRouter delivers Raft messages between the engines of an
// in-process cluster. Replicas marked down neither send nor receive,
// which simulates a crashed node without tearing its engine down.
type clusterRouter struct {
	shardID uint64
	mu      sync.RWMutex
	engines map[uint64]*Engine
	down    map[uint64]bool
}

func newClusterRouter(shardID uint64) *clusterRouter {
	return &clusterRouter{
		shardID: shardID,
		engines: make(map[uint64]*Engine),
		down:    make(map[uint64]bool),
	}
}

func (r *clusterRouter) register(replicaID uint64, e *Engine) {
	r.mu.Lock()
	r.engines[replicaID] = e
	r.mu.Unlock()
}

// kill isolates a replica: every message to or from it is dropped.
func (r *clusterRouter) kill(replicaID uint64) {
	r.mu.Lock()
	r.down[replicaID] = true
	r.mu.Unlock()
}

func (r *clusterRouter) Send(msgs []proto.Message) {
	for i := range msgs {
		r.deliver(msgs[i])
	}
}

func (r *clusterRouter) SendSnapshot(msg proto.Message) {
	r.deliver(msg)
}

func (r *clusterRouter) deliver(msg proto.Message) {
	r.mu.RLock()
	if r.down[msg.From] || r.down[msg.To] {
		r.mu.RUnlock()
		return
	}
	e := r.engines[msg.To]
	r.mu.RUnlock()
	if e == nil {
		return
	}
	e.DeliverMessage(r.shardID, msg)
}

var _ MessageSender = (*clusterRouter)(nil)

// clusterReplica is one engine-backed replica of the test cluster.
type clusterReplica struct {
	replicaID uint64
	engine    *Engine
	node      *Node
	ldb       *memdb.MemDB
}

// testCluster is a set of replicas of a single shard wired through one
// router.
type testCluster struct {
	shardID  uint64
	router   *clusterRouter
	replicas map[uint64]*clusterReplica
}

// clusterOption customizes a test cluster before its replicas are built.
type clusterOption func(*clusterOptions)

// clusterOptions holds the customizable parts of newTestCluster.
type clusterOptions struct {
	// applierFor supplies the Applier for a replica. When nil each replica
	// gets a plain testApplier over its own testSM.
	applierFor func(replicaID uint64) Applier
}

// withApplier makes every replica use the Applier returned by fn. It exists so
// a test can control when apply completes, which is what holds commitPending.
func withApplier(fn func(replicaID uint64) Applier) clusterOption {
	return func(o *clusterOptions) { o.applierFor = fn }
}

// newTestCluster builds and starts an n-replica cluster for shardID. Each
// replica gets its own in-memory LogDB and a real Engine.
func newTestCluster(t *testing.T, shardID uint64, n int, opts ...clusterOption) *testCluster {
	t.Helper()

	var options clusterOptions
	for _, opt := range opts {
		opt(&options)
	}

	members := make(map[uint64]string, n)
	for i := 1; i <= n; i++ {
		members[uint64(i)] = fmt.Sprintf("replica-%d", i)
	}

	c := &testCluster{
		shardID:  shardID,
		router:   newClusterRouter(shardID),
		replicas: make(map[uint64]*clusterReplica, n),
	}

	for i := 1; i <= n; i++ {
		replicaID := uint64(i)
		ldb := memdb.New()
		lr := logdb.NewLogReader(shardID, replicaID, ldb)
		if err := lr.ApplySnapshot(proto.Snapshot{
			Index:      0,
			Term:       0,
			Membership: proto.Membership{Addresses: copyAddresses(members)},
		}); err != nil {
			t.Fatalf("replica %d: ApplySnapshot failed: %v", replicaID, err)
		}

		cfg := config.Config{
			ShardID:      shardID,
			ReplicaID:    replicaID,
			ElectionRTT:  10,
			HeartbeatRTT: 1,
		}
		cfg.SetDefaults()
		if err := cfg.Validate(); err != nil {
			t.Fatalf("replica %d: config invalid: %v", replicaID, err)
		}

		peer, err := raft.NewPeer(cfg, lr, nil)
		if err != nil {
			t.Fatalf("replica %d: NewPeer failed: %v", replicaID, err)
		}

		hostCfg := newTestHostConfig()
		hostCfg.NumWorkers = 1
		hostCfg.MaxApplyWorkers = 1
		hostCfg.RTTMillisecond = 10

		applier := Applier(newTestApplier(newTestSM()))
		if options.applierFor != nil {
			applier = options.applierFor(replicaID)
		}

		e := NewEngine(hostCfg, ldb, c.router, nil, nil, nil)
		node := NewNode(peer, applier, lr, cfg, nil, "", nil)
		e.LoadNode(node)
		c.router.register(replicaID, e)
		e.Start()

		c.replicas[replicaID] = &clusterReplica{
			replicaID: replicaID,
			engine:    e,
			node:      node,
			ldb:       ldb,
		}
	}

	t.Cleanup(func() {
		for _, r := range c.replicas {
			r.engine.Stop()
			if err := r.ldb.Close(); err != nil {
				t.Errorf("replica %d: logdb close failed: %v", r.replicaID, err)
			}
		}
	})

	return c
}

// copyAddresses returns an independent copy of a membership address map so
// each replica's LogReader owns its own maps.
func copyAddresses(members map[uint64]string) map[uint64]string {
	out := make(map[uint64]string, len(members))
	for k, v := range members {
		out[k] = v
	}
	return out
}

// waitForLeader polls until exactly one live replica reports itself leader
// and returns its replica ID. skip lists replicas that must be ignored
// (e.g. a killed leader that still believes it leads its old term).
func (c *testCluster) waitForLeader(t *testing.T, timeout time.Duration, skip ...uint64) uint64 {
	t.Helper()
	skipped := make(map[uint64]bool, len(skip))
	for _, s := range skip {
		skipped[s] = true
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, r := range c.replicas {
			if skipped[id] {
				continue
			}
			if r.node.Peer().IsLeader() {
				return id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return 0
}

// TestRecovering_ClusterElectsLeaderWhileReplicasRecover is the C1
// regression test. Two of five replicas are held in snapshot recovery (the
// recovering gate raised exactly as snapshotPool.handleRequest raises it
// around RecoverFromSnapshot) and the leader is then killed, together with
// one healthy follower — two failures in a five-replica shard, which is
// precisely its tolerable failure budget, so a new leader MUST be electable.
//
// Quorum is three and only three replicas remain live: one healthy and the
// two recovering. Every possible winner therefore needs a vote from a
// RECOVERING replica, which it can only get if that replica still steps
// RequestVote and emits its RequestVoteResp (an ORDERED message, so it
// leaves only after the commit worker has persisted the vote). A recovering
// replica that is inert to Raft protocol traffic dissolves quorum with every
// node still "up", and the whole shard stays leaderless for the unbounded
// duration of user RecoverFromSnapshot code.
func TestRecovering_ClusterElectsLeaderWhileReplicasRecover(t *testing.T) {
	c := newTestCluster(t, 1, 5)

	leader := c.waitForLeader(t, 15*time.Second)
	if leader == 0 {
		t.Fatal("no leader elected in the initial cluster")
	}

	// Split the four followers: two are held in snapshot recovery, one
	// stays healthy, and the last is killed alongside the leader.
	var recovering []uint64
	var healthy uint64
	var alsoKilled uint64
	for id := range c.replicas {
		if id == leader {
			continue
		}
		switch {
		case len(recovering) < 2:
			recovering = append(recovering, id)
		case healthy == 0:
			healthy = id
		default:
			alsoKilled = id
		}
	}
	for _, id := range recovering {
		c.replicas[id].node.SetRecovering(true)
	}
	t.Cleanup(func() {
		for _, id := range recovering {
			c.replicas[id].node.SetRecovering(false)
		}
	})

	c.router.kill(leader)
	c.router.kill(alsoKilled)

	newLeader := c.waitForLeader(t, 20*time.Second, leader, alsoKilled)
	if newLeader == 0 {
		t.Fatalf("no new leader elected after the leader was killed while replicas %v were recovering "+
			"(live: healthy %d, recovering %v)", recovering, healthy, recovering)
	}
}
