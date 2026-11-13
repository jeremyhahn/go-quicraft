// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quicraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// These tests cover docs/membership-changes/engineering.md "Leader
// self-removal": a remove of the leader's own replica commits like any other,
// the leader steps down, and the remaining voters elect a new leader that
// serves every write the old leader acknowledged.

const removeLeaderShard uint64 = 1

const removeLeaderRTTMillis = 5

type removeLeaderCluster struct {
	t     *testing.T
	hosts []*Host
	scfg  config.Config
}

func newRemoveLeaderCluster(t *testing.T) *removeLeaderCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	c := &removeLeaderCluster{t: t}
	addrs := make(map[uint64]string, 3)
	ids := make(map[uint64]string, 3)
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
			RTTMillisecond:  removeLeaderRTTMillis,
			DeploymentID:    1,
			NoSyncWAL:       true,
			TransportConfig: config.TransportConfig{MTLSConfig: leaves[i]},
		}, WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		c.hosts = append(c.hosts, h)
		addrs[uint64(i+1)] = h.RaftAddress()
		ids[uint64(i+1)] = h.NodeIdentity()
	}
	for i, h := range c.hosts {
		scfg := testShardConfig(removeLeaderShard, uint64(i+1))
		scfg.MemberIdentities = ids
		c.scfg = scfg
		if err := h.StartShard(addrs, false, kv.NewMemoryCreateFunc(), scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	return c
}

// electionTimeout is the shard's configured election timeout.
func (c *removeLeaderCluster) electionTimeout() time.Duration {
	return time.Duration(c.scfg.ElectionRTT) * removeLeaderRTTMillis * time.Millisecond
}

// leaderAmong returns the replica ID and Host of a replica in ids that
// reports itself leader, waiting up to bound.
func (c *removeLeaderCluster) leaderAmong(ids []uint64, bound time.Duration) (uint64, *Host, bool) {
	var id uint64
	var lh *Host
	ok := waitUntil(bound, func() bool {
		for _, rid := range ids {
			h := c.hosts[rid-1]
			info, err := h.ShardInfo(removeLeaderShard)
			if err == nil && info.IsLeader {
				id, lh = rid, h
				return true
			}
		}
		return false
	})
	return id, lh, ok
}

// put commits key=value through h and fails the test on any error.
func (c *removeLeaderCluster) put(h *Host, key, value string) {
	c.t.Helper()
	cmd, err := kv.EncodePut([]byte(key), []byte(value))
	if err != nil {
		c.t.Fatalf("EncodePut(%q): %v", key, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := h.SyncPropose(ctx, removeLeaderShard, cmd); err != nil {
		c.t.Fatalf("SyncPropose(%q): %v", key, err)
	}
}

// putOnLeader proposes a put through whichever of ids currently leads,
// retrying on ErrNotLeader until ctx ends. A freshly elected leader can lose
// quorum while the transport settles; a retried put of the same value is
// idempotent, so an attempt that committed despite the error is harmless.
func (c *removeLeaderCluster) putOnLeader(ctx context.Context, ids []uint64, key, value string) {
	c.t.Helper()
	cmd, err := kv.EncodePut([]byte(key), []byte(value))
	if err != nil {
		c.t.Fatalf("EncodePut(%q): %v", key, err)
	}
	for {
		_, h, ok := c.leaderAmong(ids, time.Until(deadlineOf(ctx)))
		if !ok {
			c.t.Fatalf("SyncPropose(%q): no leader among %v before the deadline", key, ids)
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := h.SyncPropose(pctx, removeLeaderShard, cmd)
		cancel()
		switch {
		case err == nil:
			return
		case errors.Is(err, ErrNotLeader) && ctx.Err() == nil:
			continue
		default:
			c.t.Fatalf("SyncPropose(%q): %v", key, err)
		}
	}
}

// deadlineOf returns ctx's deadline, or a point 20 s away when it has none.
func deadlineOf(ctx context.Context) time.Time {
	if d, ok := ctx.Deadline(); ok {
		return d
	}
	return time.Now().Add(20 * time.Second)
}

// checkRead reads key through a linearizable SyncRead on h and fails the
// test unless it returns value.
func (c *removeLeaderCluster) checkRead(h *Host, key, value string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := h.SyncRead(ctx, removeLeaderShard, []byte(key))
	if err != nil {
		c.t.Fatalf("SyncRead(%q): %v", key, err)
	}
	b, ok := got.([]byte)
	if !ok || !bytes.Equal(b, []byte(value)) {
		c.t.Fatalf("SyncRead(%q) = %v (%T), want %q", key, got, got, value)
	}
}

func TestRemoveLeader_HandsOverAndLosesNoAcknowledgedWrite(t *testing.T) {
	c := newRemoveLeaderCluster(t)
	all := []uint64{1, 2, 3}
	// 30 acknowledged puts through the current leader. Leadership may move
	// while the transport settles; the leader to remove is chosen after.
	const before = 30
	pctx0, pcancel0 := context.WithTimeout(context.Background(), 20*time.Second)
	for i := range before {
		c.putOnLeader(pctx0, all, fmt.Sprintf("key-%02d", i), fmt.Sprintf("before-%02d", i))
	}
	pcancel0()
	oldID, oldHost, ok := c.leaderAmong(all, 20*time.Second)
	if !ok {
		t.Fatal("the shard has no leader after the initial puts")
	}

	// (a) The leader removes its own replica.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := oldHost.SyncRequestRemoveNode(ctx, removeLeaderShard, oldID, 0)
	if err != nil && !MaybeApplied(err) {
		t.Fatalf("SyncRequestRemoveNode(own replica %d) on the leader = %v, want nil or a MaybeApplied error", oldID, err)
	}
	removedAt := time.Now()

	survivors := make([]uint64, 0, 2)
	for _, id := range all {
		if id != oldID {
			survivors = append(survivors, id)
		}
	}
	// A randomized election timeout lies in [T, 2T); the bound allows the
	// step-down, one split vote and scheduling under -race on top of that.
	bound := 20 * c.electionTimeout()

	// Neither a nil return nor a MaybeApplied timeout from
	// SyncRequestRemoveNode synchronizes with the removal's commit, so
	// confirm it: a survivor's committed membership records the old leader
	// as removed. Survivors may learn the commit index only from the next
	// leader, so the bound covers an election.
	if !waitUntil(bound, func() bool {
		for _, rid := range survivors {
			m, err := c.hosts[rid-1].ShardMembership(removeLeaderShard)
			if err != nil {
				continue
			}
			if _, removed := m.Removed[oldID]; removed {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("no survivor of %v recorded leader %d as removed within %v", survivors, oldID, bound)
	}

	// Peer.Commit steps the leader down, but the Host's cached IsLeader flag
	// is updated later by the step worker's listener dispatch. Wait, with a
	// bound, for the old host to publish that initial step-down before
	// watching for renewed leadership.
	const stepDownBound = 5 * time.Second
	if !waitUntil(stepDownBound, func() bool {
		info, err := oldHost.ShardInfo(removeLeaderShard)
		return err != nil || !info.IsLeader
	}) {
		t.Fatalf("removed leader %d still reported itself leader %v after its removal committed", oldID, stepDownBound)
	}

	// (d) From here on, the removed host must never report itself leader again.
	var sawLeader atomic.Bool
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				if info, err := oldHost.ShardInfo(removeLeaderShard); err == nil && info.IsLeader {
					sawLeader.Store(true)
				}
			}
		}
	}()
	stopWatch := func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		wg.Wait()
	}
	defer stopWatch()

	// (b) The survivors elect a new leader within the election-timeout bound.
	newID, newHost, ok := c.leaderAmong(survivors, bound)
	if !ok {
		t.Fatalf("the survivors %v elected no leader within %v of the removal", survivors, bound)
	}
	t.Logf("replica %d removed itself; replica %d leads %v later (election timeout %v)",
		oldID, newID, time.Since(removedAt), c.electionTimeout())
	// A new leader serves linearizable reads once it has committed an entry
	// of its term (ShardReadReady); before that ReadIndex answers
	// ErrReadIndexNotReady.
	if !waitUntil(bound, func() bool {
		lid, _, ready, err := newHost.ShardReadReady(removeLeaderShard)
		return err == nil && ready && lid == newID
	}) {
		t.Fatalf("new leader %d was not read-ready within %v", newID, bound)
	}

	// The removal's ctx may already have expired (a MaybeApplied timeout is
	// an accepted outcome), so the membership read gets its own timeout.
	mctx, mcancel := context.WithTimeout(context.Background(), 5*time.Second)
	m, err := newHost.SyncGetShardMembership(mctx, removeLeaderShard)
	mcancel()
	if err != nil {
		t.Fatalf("SyncGetShardMembership on survivor %d: %v", newID, err)
	}
	if _, ok := m.Nodes[oldID]; ok {
		t.Fatalf("membership still lists removed leader %d as a voter: %v", oldID, m.Nodes)
	}
	if _, ok := m.Removed[oldID]; !ok {
		t.Fatalf("membership does not record removed leader %d as removed: %v", oldID, m.Removed)
	}
	if len(m.Nodes) != 2 {
		t.Fatalf("membership has %d voters after the removal, want 2: %v", len(m.Nodes), m.Nodes)
	}

	for i := range before {
		c.checkRead(newHost, fmt.Sprintf("key-%02d", i), fmt.Sprintf("before-%02d", i))
	}

	// (c) Further puts through the new leader succeed and read back.
	const after = 10
	for i := range after {
		c.put(newHost, fmt.Sprintf("after-%02d", i), fmt.Sprintf("after-%02d", i))
	}
	for i := range after {
		c.checkRead(newHost, fmt.Sprintf("after-%02d", i), fmt.Sprintf("after-%02d", i))
	}

	// (d) A proposal on the removed host fails with a documented error, and
	// it never reported itself leader after the removal.
	cmd, err := kv.EncodePut([]byte("on-removed"), []byte("x"))
	if err != nil {
		t.Fatalf("EncodePut: %v", err)
	}
	pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pcancel()
	_, err = oldHost.SyncPropose(pctx, removeLeaderShard, cmd)
	switch {
	case err == nil:
		t.Fatal("SyncPropose on the removed host succeeded")
	case errors.Is(err, ErrNotLeader), errors.Is(err, ErrNodeRemoved),
		errors.Is(err, ErrShardNotFound), errors.Is(err, ErrShardNotReady):
		t.Logf("SyncPropose on the removed host: %v", err)
	default:
		t.Fatalf("SyncPropose on the removed host = %v (%T), want ErrNotLeader, ErrNodeRemoved, ErrShardNotFound or ErrShardNotReady", err, err)
	}
	time.Sleep(4 * c.electionTimeout())
	stopWatch()
	if sawLeader.Load() {
		t.Fatalf("removed replica %d reported itself leader after its removal", oldID)
	}
	if _, lh, ok := c.leaderAmong(survivors, bound); !ok || lh == oldHost {
		t.Fatalf("no survivor leads after the removed host's proposal")
	}
}
