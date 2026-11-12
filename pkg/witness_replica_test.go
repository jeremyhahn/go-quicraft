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

package quicraft

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// ---------------------------------------------------------------------------
// A witness replica on real hosts (#820).
//
// Two full replicas and one witness, each on its own host over QUIC. The
// witness votes and acks, so with one full replica stopped the other still
// commits; it holds no state machine data, so its state machine applies no
// user entry; and the replicated witness policy refuses the add that would
// leave the full replicas unable to form a quorum on their own.
// ---------------------------------------------------------------------------

const witnessReplicaShard uint64 = 1

// witnessCountingSM counts the user entries (non-empty commands) it applies.
type witnessCountingSM struct {
	mockStateMachine
	applied *atomic.Int64
}

func (s *witnessCountingSM) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		if len(entries[i].Cmd) > 0 {
			s.applied.Add(1)
		}
	}
	return s.mockStateMachine.Update(ctx, entries, results)
}

type witnessReplicaNode struct {
	host    *Host
	applied atomic.Int64
}

func (n *witnessReplicaNode) create(_, _ uint64) interface{} {
	return &witnessCountingSM{applied: &n.applied}
}

func TestWitnessReplica_KeepsQuorumWithoutApplyingAndTheWitnessLimitHolds(t *testing.T) {
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	nodes := make([]*witnessReplicaNode, 3)
	ids := make(map[uint64]string, 3)
	for i := range nodes {
		h, err := NewHost(config.HostConfig{
			WALDir:          t.TempDir(),
			NodeHostDir:     t.TempDir(),
			RaftAddress:     "127.0.0.1:0",
			ListenAddress:   "127.0.0.1:0",
			RTTMillisecond:  5,
			DeploymentID:    1,
			NoSyncWAL:       true,
			TransportConfig: config.TransportConfig{MTLSConfig: leaves[i]},
		}, WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		nodes[i] = &witnessReplicaNode{host: h}
		ids[uint64(i+1)] = h.NodeIdentity()
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			if n.host != nil {
				n.host.Close()
			}
		}
	})

	// Replicas 1 and 2 bootstrap the shard as full voters.
	full := map[uint64]string{1: nodes[0].host.RaftAddress(), 2: nodes[1].host.RaftAddress()}
	fullIDs := map[uint64]string{1: ids[1], 2: ids[2]}
	for i := range 2 {
		scfg := testShardConfig(witnessReplicaShard, uint64(i+1))
		scfg.MemberIdentities = fullIDs
		if err := nodes[i].host.StartShard(full, false, nodes[i].create, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}

	// leader is the index of the full replica leading the shard.
	leader := func() int {
		t.Helper()
		found := -1
		if !waitUntil(20*time.Second, func() bool {
			for i, n := range nodes[:2] {
				if n.host == nil {
					continue
				}
				if info, err := n.host.ShardInfo(witnessReplicaShard); err == nil && info.IsLeader {
					found = i
					return true
				}
			}
			return false
		}) {
			t.Fatal("no full replica became leader")
		}
		return found
	}
	propose := func(tag string) {
		t.Helper()
		var lastErr error
		if waitUntil(20*time.Second, func() bool {
			n := nodes[leader()]
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, lastErr = n.host.SyncPropose(ctx, witnessReplicaShard, []byte(tag))
			return lastErr == nil
		}) {
			return
		}
		t.Fatalf("proposal %q never committed: %v", tag, lastErr)
	}
	addWitness := func(replicaID uint64, addr, identity string) error {
		t.Helper()
		h := nodes[leader()].host
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		m, err := h.SyncGetShardMembership(ctx, witnessReplicaShard)
		if err != nil {
			return err
		}
		// A transport host refuses an add that commits no identity, so the
		// identity-carrying form of SyncRequestAddWitness is the one to use.
		return h.SyncRequestAddWitnessWithIdentity(ctx, witnessReplicaShard,
			replicaID, addr, identity, m.ConfigChangeID)
	}

	propose("before-witness")

	// Host 3 joins as the witness, anchored on the two full replicas.
	wcfg := testShardConfig(witnessReplicaShard, 3)
	wcfg.IsWitness = true
	wcfg.MemberIdentities = map[uint64]string{1: ids[1], 2: ids[2], 3: ids[3]}
	if err := nodes[2].host.StartShard(nil, true, nodes[2].create, wcfg); err != nil {
		t.Fatalf("StartShard (witness): %v", err)
	}
	if err := addWitness(3, nodes[2].host.RaftAddress(), ids[3]); err != nil {
		t.Fatalf("adding the witness: %v", err)
	}

	for i := range 5 {
		propose(fmt.Sprintf("with-witness-%d", i))
	}

	// Stop the leading full replica: the survivor needs the witness's vote to
	// become leader and its ack to commit.
	stopped := leader()
	survivor := 1 - stopped
	nodes[stopped].host.Close()
	nodes[stopped].host = nil
	for i := range 5 {
		propose(fmt.Sprintf("one-full-down-%d", i))
	}
	if got := leader(); got != survivor {
		t.Fatalf("leader is replica %d, want the surviving full replica %d", got+1, survivor+1)
	}
	if got := nodes[survivor].applied.Load(); got < 11 {
		t.Fatalf("the surviving full replica applied %d user entries, want at least 11", got)
	}

	info, err := nodes[2].host.ShardInfo(witnessReplicaShard)
	if err != nil {
		t.Fatalf("ShardInfo (witness): %v", err)
	}
	if !info.IsWitness {
		t.Fatalf("replica 3 does not report itself a witness: %+v", info)
	}
	if got := nodes[2].applied.Load(); got != 0 {
		t.Fatalf("the witness's state machine applied %d user entries, want 0", got)
	}

	// Add witnesses until the replicated policy refuses one.
	var addErr error
	for id := uint64(4); id < 10 && addErr == nil; id++ {
		addErr = addWitness(id, fmt.Sprintf("127.0.0.1:%d", 7000+id), testIdentity(id))
	}
	if !errors.Is(addErr, ErrTooManyWitnesses) && !errors.Is(addErr, ErrBreaksFullReplicaMajority) {
		t.Fatalf("adding witnesses ended with %v, want ErrTooManyWitnesses or ErrBreaksFullReplicaMajority", addErr)
	}
}
