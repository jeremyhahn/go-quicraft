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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// ---------------------------------------------------------------------------
// Remove, then re-add the same replica ID (#810).
//
// Three real hosts on the QUIC transport run one shard. A follower is removed:
// the linearizable membership must stop listing it, its Host must apply no
// entry past the removal, and an add of the same replica ID must fail with
// ErrNodeRemoved, because replica IDs are never recycled. The shard keeps
// committing proposals before, between and after both changes.
// ---------------------------------------------------------------------------

const removeReaddShard uint64 = 1

type removeReaddCluster struct {
	t     *testing.T
	hosts []*Host
	addrs map[uint64]string
	ids   map[uint64]string
}

func newRemoveReaddCluster(t *testing.T) *removeReaddCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	c := &removeReaddCluster{t: t, addrs: make(map[uint64]string, 3), ids: make(map[uint64]string, 3)}
	t.Cleanup(func() {
		for _, h := range c.hosts {
			h.Close()
		}
	})
	for i := range 3 {
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
		c.hosts = append(c.hosts, h)
		c.addrs[uint64(i+1)] = h.RaftAddress()
		c.ids[uint64(i+1)] = h.NodeIdentity()
	}
	for i, h := range c.hosts {
		scfg := testShardConfig(removeReaddShard, uint64(i+1))
		scfg.MemberIdentities = c.ids
		if err := h.StartShard(c.addrs, false, testCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	return c
}

// leader returns the replica ID and Host of the current leader, waiting
// across an election.
func (c *removeReaddCluster) leader() (uint64, *Host) {
	c.t.Helper()
	var id uint64
	var lh *Host
	if !waitUntil(20*time.Second, func() bool {
		for i, h := range c.hosts {
			info, err := h.ShardInfo(removeReaddShard)
			if err == nil && info.IsLeader {
				id, lh = uint64(i+1), h
				return true
			}
		}
		return false
	}) {
		c.t.Fatal("the shard never elected a leader")
	}
	return id, lh
}

// propose commits one entry through whichever replica leads, retrying across
// an election.
func (c *removeReaddCluster) propose(tag string) {
	c.t.Helper()
	var lastErr error
	if waitUntil(20*time.Second, func() bool {
		_, h := c.leader()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, lastErr = h.SyncPropose(ctx, removeReaddShard, []byte(tag))
		return lastErr == nil
	}) {
		return
	}
	c.t.Fatalf("proposal %q never committed: %v", tag, lastErr)
}

func (c *removeReaddCluster) lastApplied(replicaID uint64) uint64 {
	info, err := c.hosts[replicaID-1].ShardInfo(removeReaddShard)
	if err != nil {
		return 0
	}
	return info.LastApplied
}

func TestRemoveReadd_RemovedReplicaStopsServingAndItsIDCannotBeAddedBack(t *testing.T) {
	c := newRemoveReaddCluster(t)
	for i := range 3 {
		c.propose(fmt.Sprintf("before-%d", i))
	}

	leaderID, lh := c.leader()
	removed := leaderID%3 + 1 // a follower

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := lh.SyncRequestRemoveNode(ctx, removeReaddShard, removed, 0); err != nil {
		t.Fatalf("SyncRequestRemoveNode(%d): %v", removed, err)
	}

	m, err := lh.SyncGetShardMembership(ctx, removeReaddShard)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}
	if _, ok := m.Nodes[removed]; ok {
		t.Fatalf("membership still lists removed replica %d as a voter: %v", removed, m.Nodes)
	}
	if _, ok := m.Removed[removed]; !ok {
		t.Fatalf("membership does not record replica %d as removed: %v", removed, m.Removed)
	}
	if len(m.Nodes) != 2 {
		t.Fatalf("membership has %d voters after the removal, want 2: %v", len(m.Nodes), m.Nodes)
	}
	// The linearizable read applied everything up to the removal on the
	// leader; the removed replica may apply at most that far.
	removalApplied := c.lastApplied(leaderID)

	for i := range 3 {
		c.propose(fmt.Sprintf("between-%d", i))
	}
	survivors := make([]uint64, 0, 2)
	for id := uint64(1); id <= 3; id++ {
		if id != removed {
			survivors = append(survivors, id)
		}
	}
	if !waitUntil(10*time.Second, func() bool {
		for _, id := range survivors {
			if c.lastApplied(id) <= removalApplied {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("the remaining replicas did not apply past the removal at %d", removalApplied)
	}
	if got := c.lastApplied(removed); got > removalApplied {
		t.Fatalf("removed replica %d applied index %d, past the removal at %d", removed, got, removalApplied)
	}

	_, lh = c.leader()
	err = lh.SyncRequestAddNode(ctx, removeReaddShard, removed, c.addrs[removed], 0)
	if !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("SyncRequestAddNode(%d) after its removal = %v, want ErrNodeRemoved", removed, err)
	}

	m, err = lh.SyncGetShardMembership(ctx, removeReaddShard)
	if err != nil {
		t.Fatalf("SyncGetShardMembership after the refused add: %v", err)
	}
	if _, ok := m.Nodes[removed]; ok {
		t.Fatalf("refused add still put replica %d back in the membership: %v", removed, m.Nodes)
	}
	for i := range 3 {
		c.propose(fmt.Sprintf("after-%d", i))
	}
	if got := c.lastApplied(removed); got > removalApplied {
		t.Fatalf("removed replica %d applied index %d after the refused add, past the removal at %d",
			removed, got, removalApplied)
	}
}
