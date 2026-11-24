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
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

const (
	membershipEventsShard    = 954
	membershipEventsObserver = 4
	membershipEventsTimeout  = 30 * time.Second
	// membershipEventsQuiet is how long a Host's event count must hold
	// still before the test treats it as settled.
	membershipEventsQuiet = 300 * time.Millisecond
)

// membershipEventRecorder counts the OnMembershipChanged callbacks one Host
// fires and hands each one to the test for the post-event membership check.
type membershipEventRecorder struct {
	mu     sync.Mutex
	events []config.EventShardInfo
	fired  chan config.EventShardInfo
}

func newMembershipEventRecorder() *membershipEventRecorder {
	return &membershipEventRecorder{fired: make(chan config.EventShardInfo, 64)}
}

func (r *membershipEventRecorder) onMembershipChanged(info config.EventShardInfo) {
	r.mu.Lock()
	r.events = append(r.events, info)
	r.mu.Unlock()
	select {
	case r.fired <- info:
	default:
	}
}

func (r *membershipEventRecorder) snapshot() []config.EventShardInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]config.EventShardInfo(nil), r.events...)
}

func (r *membershipEventRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// drain discards every queued notification, so the next receive is for a
// callback fired after this call.
func (r *membershipEventRecorder) drain() {
	for {
		select {
		case <-r.fired:
		default:
			return
		}
	}
}

// membershipEventsCluster is a three-Host mTLS cluster, built as
// newTransferCluster builds one, with a counting EventListener per Host.
type membershipEventsCluster struct {
	hosts     []*Host
	recorders []*membershipEventRecorder
}

func newMembershipEventsCluster(t *testing.T) *membershipEventsCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	c := &membershipEventsCluster{}
	t.Cleanup(func() {
		for _, h := range c.hosts {
			h.Close()
		}
	})
	members := make(map[uint64]string, 3)
	ids := make(map[uint64]string, 3)
	for i := range 3 {
		rec := newMembershipEventRecorder()
		h, err := NewHost(config.HostConfig{
			WALDir:          t.TempDir(),
			NodeHostDir:     t.TempDir(),
			RaftAddress:     "127.0.0.1:0",
			ListenAddress:   "127.0.0.1:0",
			RTTMillisecond:  transferRTTMillis,
			DeploymentID:    1,
			NoSyncWAL:       true,
			TransportConfig: config.TransportConfig{MTLSConfig: leaves[i]},
			EventListener: &config.EventListener{
				OnMembershipChanged: rec.onMembershipChanged,
			},
		}, WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		c.hosts = append(c.hosts, h)
		c.recorders = append(c.recorders, rec)
		members[uint64(i+1)] = h.RaftAddress()
		ids[uint64(i+1)] = h.NodeIdentity()
	}
	for i, h := range c.hosts {
		scfg := testShardConfig(membershipEventsShard, uint64(i+1))
		scfg.MemberIdentities = ids
		if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(), scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	return c
}

// leaderIndex waits until every Host names the same leader at the same term
// and returns the index of the Host that holds it.
func (c *membershipEventsCluster) leaderIndex(ctx context.Context, t *testing.T) int {
	t.Helper()
	for ctx.Err() == nil {
		id, term, ready, err := c.hosts[0].LeaderIDAndTerm(membershipEventsShard)
		agreed := err == nil && ready && id >= 1 && id <= uint64(len(c.hosts))
		for i := 1; agreed && i < len(c.hosts); i++ {
			oid, oterm, oready, oerr := c.hosts[i].LeaderIDAndTerm(membershipEventsShard)
			agreed = oerr == nil && oready && oid == id && oterm == term
		}
		if agreed {
			return int(id - 1)
		}
		waitFor(ctx, 10*time.Millisecond)
	}
	t.Fatalf("replicas never agreed on a leader: %v", ctx.Err())
	return -1
}

// waitFor blocks for d or until ctx ends, whichever is first.
func waitFor(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// membership reads host i's membership through SyncGetShardMembership, the
// linearizable (ReadIndex-fenced) read, so it reflects every entry the shard
// had committed when the read began.
func (c *membershipEventsCluster) membership(ctx context.Context, t *testing.T, i int) *Membership {
	t.Helper()
	var lastErr error
	for ctx.Err() == nil {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		m, err := c.hosts[i].SyncGetShardMembership(rctx, membershipEventsShard)
		cancel()
		if err == nil {
			return m
		}
		lastErr = err
		if !isTransferProposalError(err) && !errors.Is(err, ErrReadIndexNotReady) {
			break
		}
		waitFor(ctx, 20*time.Millisecond)
	}
	t.Fatalf("SyncGetShardMembership on host %d: %v (ctx %v)", i, lastErr, ctx.Err())
	return nil
}

// settle waits until no Host's event count has moved for
// membershipEventsQuiet, after fencing every Host's apply with a
// linearizable membership read, and returns the settled counts.
func (c *membershipEventsCluster) settle(ctx context.Context, t *testing.T) []int {
	t.Helper()
	for i := range c.hosts {
		c.membership(ctx, t, i)
	}
	counts := c.counts()
	stableSince := time.Now()
	for ctx.Err() == nil {
		waitFor(ctx, 20*time.Millisecond)
		now := c.counts()
		if fmt.Sprint(now) != fmt.Sprint(counts) {
			counts, stableSince = now, time.Now()
			continue
		}
		if time.Since(stableSince) >= membershipEventsQuiet {
			return counts
		}
	}
	t.Fatalf("event counts never settled: %v", ctx.Err())
	return nil
}

func (c *membershipEventsCluster) counts() []int {
	out := make([]int, len(c.recorders))
	for i, r := range c.recorders {
		out[i] = r.count()
	}
	return out
}

// awaitEvents waits until host i has fired want callbacks in total, then
// checks, through a linearizable read on that same Host made after the
// callback, that the applied membership already satisfies check.
func (c *membershipEventsCluster) awaitEvents(ctx context.Context, t *testing.T, i, want int, check func(*Membership) error) {
	t.Helper()
	rec := c.recorders[i]
	for rec.count() < want {
		select {
		case <-ctx.Done():
			t.Fatalf("host %d: %d OnMembershipChanged callbacks, want %d: %v", i, rec.count(), want, ctx.Err())
		case <-rec.fired:
		}
	}
	m := c.membership(ctx, t, i)
	if err := check(m); err != nil {
		t.Fatalf("host %d: membership read after OnMembershipChanged does not reflect the change: %v", i, err)
	}
}

// propose applies cmd through the current leader, retrying the errors a
// proposal may fail with while leadership settles.
func (c *membershipEventsCluster) propose(ctx context.Context, t *testing.T, cmd []byte) {
	t.Helper()
	for ctx.Err() == nil {
		leader := c.leaderIndex(ctx, t)
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := c.hosts[leader].SyncPropose(pctx, membershipEventsShard, cmd)
		cancel()
		if err == nil {
			return
		}
		if !isTransferProposalError(err) {
			t.Fatalf("SyncPropose: %v", err)
		}
	}
	t.Fatalf("SyncPropose never succeeded: %v", ctx.Err())
}

func hasObserver(m *Membership) error {
	if _, ok := m.Observers[membershipEventsObserver]; !ok {
		return fmt.Errorf("observer %d absent: observers %v", membershipEventsObserver, m.Observers)
	}
	return nil
}

func observerRemoved(m *Membership) error {
	if _, ok := m.Observers[membershipEventsObserver]; ok {
		return fmt.Errorf("observer %d still present", membershipEventsObserver)
	}
	if _, ok := m.Removed[membershipEventsObserver]; !ok {
		return fmt.Errorf("observer %d not in the removed set %v", membershipEventsObserver, m.Removed)
	}
	return nil
}

// TestMembershipChangedEvents_FiresOncePerReplicaPerChange pins the
// documented contract of config.EventListener.OnMembershipChanged on a
// three-Host cluster: ordinary proposals fire nothing; an applied observer
// add and its removal each fire exactly one callback on every Host, carrying
// the shard ID and that Host's own replica ID; and the callback never runs
// ahead of the membership the Host serves.
func TestMembershipChangedEvents_FiresOncePerReplicaPerChange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*membershipEventsTimeout)
	defer cancel()
	c := newMembershipEventsCluster(t)

	// Bootstrap may itself report the initial membership; measure every
	// assertion against the settled post-bootstrap counts.
	c.leaderIndex(ctx, t)
	base := c.settle(ctx, t)
	t.Logf("post-bootstrap OnMembershipChanged counts per Host: %v", base)

	// (a) Ordinary proposals fire no OnMembershipChanged on any Host.
	for i := range 20 {
		cmd, err := kv.EncodePut([]byte(fmt.Sprintf("k%02d", i)), []byte("v"))
		if err != nil {
			t.Fatalf("EncodePut: %v", err)
		}
		c.propose(ctx, t, cmd)
	}
	if got := c.settle(ctx, t); fmt.Sprint(got) != fmt.Sprint(base) {
		t.Fatalf("(a) ordinary proposals fired OnMembershipChanged: counts %v, want %v", got, base)
	}

	// (b) An applied observer add fires exactly once on each Host.
	for _, r := range c.recorders {
		r.drain()
	}
	leader := c.leaderIndex(ctx, t)
	m := c.membership(ctx, t, leader)
	actx, acancel := context.WithTimeout(ctx, membershipEventsTimeout)
	err := c.hosts[leader].SyncRequestAddObserverWithIdentity(actx, membershipEventsShard,
		membershipEventsObserver, "127.0.0.1:1", testIdentity(membershipEventsObserver), m.ConfigChangeID)
	acancel()
	if err != nil {
		t.Fatalf("SyncRequestAddObserverWithIdentity: %v", err)
	}
	for i := range c.hosts {
		// (d) The callback never runs ahead of the applied membership.
		c.awaitEvents(ctx, t, i, base[i]+1, hasObserver)
	}
	if got, want := c.settle(ctx, t), addEach(base, 1); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("(b) observer add: counts %v, want %v", got, want)
	}
	c.checkInfos(t, base, 1)

	// (c) Removing that observer fires exactly one more on each Host.
	for _, r := range c.recorders {
		r.drain()
	}
	leader = c.leaderIndex(ctx, t)
	m = c.membership(ctx, t, leader)
	rctx, rcancel := context.WithTimeout(ctx, membershipEventsTimeout)
	err = c.hosts[leader].SyncRequestRemoveNode(rctx, membershipEventsShard, membershipEventsObserver, m.ConfigChangeID)
	rcancel()
	if err != nil {
		t.Fatalf("SyncRequestRemoveNode: %v", err)
	}
	for i := range c.hosts {
		// (d) again, for the removal.
		c.awaitEvents(ctx, t, i, base[i]+2, observerRemoved)
	}
	if got, want := c.settle(ctx, t), addEach(base, 2); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("(c) observer removal: counts %v, want %v", got, want)
	}
	c.checkInfos(t, base, 2)
}

// checkInfos asserts that every callback each Host fired after its baseline
// carried the shard ID and that Host's own replica ID.
func (c *membershipEventsCluster) checkInfos(t *testing.T, base []int, want int) {
	t.Helper()
	for i, r := range c.recorders {
		events := r.snapshot()
		if len(events) != base[i]+want {
			t.Fatalf("host %d: %d callbacks, want %d", i, len(events), base[i]+want)
		}
		for _, info := range events[base[i]:] {
			if info.ShardID != membershipEventsShard || info.ReplicaID != uint64(i+1) {
				t.Fatalf("host %d: callback carried shard %d replica %d, want shard %d replica %d",
					i, info.ShardID, info.ReplicaID, membershipEventsShard, i+1)
			}
		}
	}
}

func addEach(base []int, n int) []int {
	out := make([]int, len(base))
	for i, b := range base {
		out[i] = b + n
	}
	return out
}
