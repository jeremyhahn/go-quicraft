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
	"encoding/binary"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// ---------------------------------------------------------------------------
// SyncRead across a leader transfer (#786).
//
// A write the leader acknowledged is committed; a linearizable read issued
// after the acknowledgement must observe it, on whichever replica it is
// issued and whoever leads by then. A leader transfer is the case the
// failover suites in test/integration/linearizability do not drive: the old
// leader steps down voluntarily, the target campaigns at once, and a read
// can arrive at the old leader, the new leader or a follower while the
// handover is still settling.
// ---------------------------------------------------------------------------

const srltShard uint64 = 786

// srltCounter is a state machine whose state is the number of entries it has
// applied. Update answers each entry with the count after it, so the
// proposer's acknowledgement carries the value every later read must reach.
type srltCounter struct {
	n atomic.Uint64
}

func (c *srltCounter) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		results[i] = sm.Result{Value: c.n.Add(1)}
	}
	return nil
}

func (c *srltCounter) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	return c.n.Load(), nil
}

func (c *srltCounter) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], c.n.Load())
	_, err := w.Write(b[:])
	return err
}

func (c *srltCounter) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	c.n.Store(binary.BigEndian.Uint64(b[:]))
	return nil
}

func (c *srltCounter) Close(_ context.Context) error { return nil }

func srltCreate(_, _ uint64) interface{} { return &srltCounter{} }

// srltCluster is three hosts on the real transport, each with its own
// certificate and so its own committed node identity, running one shard.
type srltCluster struct {
	t     *testing.T
	hosts []*Host
}

func newSRLTCluster(t *testing.T) *srltCluster {
	t.Helper()
	leaves := testpki.NewCA(t).Leaves(t, 3)
	c := &srltCluster{t: t}
	members := make(map[uint64]string, 3)
	ids := make(map[uint64]string, 3)
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
		members[uint64(i+1)] = h.RaftAddress()
		ids[uint64(i+1)] = h.NodeIdentity()
	}
	t.Cleanup(func() {
		for _, h := range c.hosts {
			h.Close()
		}
	})
	for i, h := range c.hosts {
		scfg := testShardConfig(srltShard, uint64(i+1))
		scfg.MemberIdentities = ids
		if err := h.StartShard(members, false, srltCreate, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	return c
}

// host returns the host running replicaID.
func (c *srltCluster) host(replicaID uint64) *Host { return c.hosts[replicaID-1] }

// leader waits for a replica that reports itself leader and returns its ID.
func (c *srltCluster) leader(timeout time.Duration) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, h := range c.hosts {
			if info, err := h.ShardInfo(srltShard); err == nil && info.IsLeader {
				return uint64(i + 1)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no leader within %v", timeout)
	return 0
}

// increment proposes one entry on the current leader, retrying across a
// leadership change, and returns the acknowledged counter value.
func (c *srltCluster) increment() uint64 {
	c.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		id := c.leader(10 * time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err := c.host(id).SyncPropose(ctx, srltShard, []byte("inc"))
		cancel()
		if err == nil {
			return res.Value
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("increment never acknowledged: %v", lastErr)
	return 0
}

// transfer asks leader to hand leadership to target and waits until target
// reports itself leader. A request can be dropped while the shard is busy
// or campaigning, so it is repeated until the target leads.
func (c *srltCluster) transfer(leader, target uint64) {
	c.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if info, err := c.host(target).ShardInfo(srltShard); err == nil && info.IsLeader {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		lastErr = c.host(leader).RequestLeaderTransfer(ctx, srltShard, target)
		cancel()
		wait := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(wait) {
			if info, err := c.host(target).ShardInfo(srltShard); err == nil && info.IsLeader {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		// Leadership may have moved elsewhere: ask whoever leads now.
		leader = c.leader(10 * time.Second)
		if leader == target {
			return
		}
	}
	c.t.Fatalf("leadership never moved to replica %d (last request: %v)", target, lastErr)
}

// readAtLeast issues SyncRead on replicaID, each attempt under its own
// deadline, until one succeeds, and fails the test if the value it returns
// is older than acked. An error (a timeout, no leader yet) is retried: it is
// not a stale read. A returned value below acked is.
func (c *srltCluster) readAtLeast(role string, replicaID, acked uint64) {
	c.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		v, err := c.host(replicaID).SyncRead(ctx, srltShard, nil)
		cancel()
		if err != nil {
			lastErr = err
			time.Sleep(10 * time.Millisecond)
			continue
		}
		got, ok := v.(uint64)
		if !ok {
			c.t.Fatalf("SyncRead on %s replica %d returned %T, want uint64", role, replicaID, v)
		}
		if got < acked {
			c.t.Fatalf("stale read: SyncRead on %s replica %d returned %d, below the acknowledged %d",
				role, replicaID, got, acked)
		}
		return
	}
	c.t.Fatalf("SyncRead on %s replica %d never succeeded: %v", role, replicaID, lastErr)
}

func TestSyncReadLeaderTransfer_ReadsObserveAcknowledgedWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("three-host transport test")
	}
	c := newSRLTCluster(t)
	for round := range 20 {
		acked := c.increment()
		oldLeader := c.leader(10 * time.Second)
		newLeader := oldLeader%3 + 1
		follower := newLeader%3 + 1
		c.transfer(oldLeader, newLeader)
		c.readAtLeast("old leader", oldLeader, acked)
		c.readAtLeast("new leader", newLeader, acked)
		c.readAtLeast("follower", follower, acked)
		if t.Failed() {
			t.Fatalf("round %d failed", round)
		}
	}
}
