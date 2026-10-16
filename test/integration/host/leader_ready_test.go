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

//go:build integration

package host_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// readyShard is the one shard every test in this file starts.
const readyShard uint64 = 1

// readyEvent is one delivered OnLeaderUpdated (ready false) or OnLeaderReady.
type readyEvent struct {
	ready    bool
	leaderID uint64
	term     uint64
}

func (e readyEvent) String() string {
	if e.ready {
		return fmt.Sprintf("ready(%d,%d)", e.leaderID, e.term)
	}
	return fmt.Sprintf("updated(%d,%d)", e.leaderID, e.term)
}

// readyLog records one replica's leadership events in delivery order and wakes
// a waiter on every event, so a test waits for readiness the way a consumer
// does: on the event, never by polling the query or sleeping.
type readyLog struct {
	mu     sync.Mutex
	events []readyEvent
	wake   chan struct{}
}

func newReadyLog() *readyLog {
	return &readyLog{wake: make(chan struct{}, 1)}
}

func (l *readyLog) record(e readyEvent) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// listener is the EventListener feeding the log. Both callbacks only append
// and signal, so they never block the dispatcher.
func (l *readyLog) listener() *config.EventListener {
	return &config.EventListener{
		OnLeaderUpdated: func(i config.LeaderInfo) {
			l.record(readyEvent{leaderID: i.LeaderID, term: i.Term})
		},
		OnLeaderReady: func(i config.LeaderInfo) {
			l.record(readyEvent{ready: true, leaderID: i.LeaderID, term: i.Term})
		},
	}
}

func (l *readyLog) snapshot() []readyEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]readyEvent(nil), l.events...)
}

// await blocks until pred holds for the recorded sequence. It re-evaluates only
// when an event is delivered: the wake channel holds one pending signal, so an
// event recorded between the check and the wait is never missed.
func (l *readyLog) await(t *testing.T, timeout time.Duration, desc string, pred func([]readyEvent) bool) []readyEvent {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if ev := l.snapshot(); pred(ev) {
			return ev
		}
		select {
		case <-l.wake:
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for %s; events: %v", timeout, desc, l.snapshot())
		}
	}
}

// current returns the readiness a replica holds now: the last event, when it is
// an OnLeaderReady. Any later OnLeaderUpdated has ended it.
func current(events []readyEvent) (readyEvent, bool) {
	if len(events) == 0 || !events[len(events)-1].ready {
		return readyEvent{}, false
	}
	return events[len(events)-1], true
}

// assertReadyOrdering checks the delivery contract over a replica's whole
// history: every OnLeaderReady follows the OnLeaderUpdated of its own pair with
// no other OnLeaderUpdated in between, at most once per announcement, and
// never names an unknown leader.
func assertReadyOrdering(t *testing.T, replica int, events []readyEvent) {
	t.Helper()
	var announced readyEvent
	known, readied := false, false
	for i, e := range events {
		if !e.ready {
			announced, known, readied = e, true, false
			continue
		}
		if e.leaderID == 0 || !known || readied ||
			announced.leaderID != e.leaderID || announced.term != e.term {
			t.Fatalf("replica %d: %v at position %d does not follow its own "+
				"OnLeaderUpdated exactly once; events: %v", replica, e, i, events)
		}
		readied = true
	}
}

// readyCluster is a set of hosts on real QUIC, each with its own readyLog
// installed as its EventListener. The log outlives a restart of its host, so a
// test can follow one replica across it.
type readyCluster struct {
	t      *testing.T
	ca     *testpki.CA
	leaves []*config.MTLSConfig
	addrs  []string
	dirs   []nodeConfig
	ids    map[uint64]string
	hosts  []*quicraft.Host
	logs   []*readyLog
}

func newReadyCluster(t *testing.T, n int) *readyCluster {
	t.Helper()
	assertApplyOrder(t)
	c := &readyCluster{
		t:     t,
		ca:    testpki.NewCA(t),
		addrs: make([]string, n),
		dirs:  make([]nodeConfig, n),
		ids:   make(map[uint64]string, n),
		hosts: make([]*quicraft.Host, n),
		logs:  make([]*readyLog, n),
	}
	c.leaves = c.ca.Leaves(t, n)
	for i := range n {
		c.addrs[i] = kernelAddr
		c.dirs[i] = nodeConfig{walDir: t.TempDir(), nodeHostDir: t.TempDir()}
		c.logs[i] = newReadyLog()
	}
	t.Cleanup(func() {
		for _, h := range c.hosts {
			if h != nil {
				h.Close()
			}
		}
	})
	// Each host binds a kernel-assigned port and keeps it: restart reopens
	// the host on the address recorded here.
	for i := range n {
		c.hosts[i] = c.newHost(i)
		c.addrs[i] = c.hosts[i].RaftAddress()
		c.dirs[i].addr = c.addrs[i]
		c.ids[uint64(i+1)] = c.hosts[i].NodeIdentity()
	}
	for i := range n {
		c.startShard(i)
	}
	return c
}

func (c *readyCluster) newHost(i int) *quicraft.Host {
	c.t.Helper()
	cfg := hostConfigAt(c.dirs[i].walDir, c.dirs[i].nodeHostDir, c.addrs[i], c.leaves[i])
	cfg.EventListener = c.logs[i].listener()
	h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption())
	if err != nil {
		c.t.Fatalf("NewHost[%d]: %v", i, err)
	}
	return h
}

func (c *readyCluster) startShard(i int) {
	c.t.Helper()
	members := make(map[uint64]string, len(c.addrs))
	for j, addr := range c.addrs {
		members[uint64(j+1)] = addr
	}
	createFn, _ := kvCreateFunc()
	scfg := testShardConfig(readyShard, uint64(i+1))
	scfg.MemberIdentities = c.ids
	if err := c.hosts[i].StartShard(members, false, createFn, scfg); err != nil {
		c.t.Fatalf("StartShard[%d]: %v", i, err)
	}
}

func (c *readyCluster) stop(i int) {
	c.hosts[i].Close()
	c.hosts[i] = nil
}

// restart reopens replica i from its own data directories, under the same
// identity and the same readyLog.
func (c *readyCluster) restart(i int) {
	c.t.Helper()
	c.hosts[i] = c.newHost(i)
	c.startShard(i)
}

// awaitAgreedReady waits until every replica in idxs currently holds readiness
// for one and the same (leader, term) pair with a term above after, and
// returns that pair. A replica that is ready for an older term than another is
// waited on again; the term only rises, so the wait settles.
func (c *readyCluster) awaitAgreedReady(idxs []int, after uint64) readyEvent {
	c.t.Helper()
	var want readyEvent
	for {
		for _, i := range idxs {
			ev := c.logs[i].await(c.t, failoverTimeout,
				fmt.Sprintf("replica %d ready above term %d", i+1, max(after, want.term)),
				func(ev []readyEvent) bool {
					cur, ok := current(ev)
					return ok && cur.term > after && cur.term >= want.term
				})
			if cur, _ := current(ev); cur.term > want.term {
				want = cur
			}
		}
		agreed := true
		for _, i := range idxs {
			if cur, ok := current(c.logs[i].snapshot()); !ok || cur != want {
				agreed = false
			}
		}
		if agreed {
			return want
		}
	}
}

// assertServes checks, on each replica in idxs, that the synchronous query
// agrees with the delivered readiness and that a linearizable read issued on
// that replica is served.
func (c *readyCluster) assertServes(idxs []int, want readyEvent) {
	c.t.Helper()
	for _, i := range idxs {
		leaderID, term, ready, err := c.hosts[i].ShardReadReady(readyShard)
		if err != nil || !ready || leaderID != want.leaderID || term != want.term {
			c.t.Fatalf("replica %d: ShardReadReady = (%d, %d, %v, %v), want (%d, %d, true, nil)",
				i+1, leaderID, term, ready, err, want.leaderID, want.term)
		}
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		//quicraft:direct-read every ready replica IS the subject: OnLeaderReady promised its own ReadIndex is served
		_, err = c.hosts[i].SyncRead(ctx, readyShard, "k")
		cancel()
		if err != nil {
			c.t.Fatalf("replica %d: SyncRead after OnLeaderReady(%d,%d): %v",
				i+1, want.leaderID, want.term, err)
		}
	}
}

func (c *readyCluster) assertOrdering() {
	c.t.Helper()
	for i, l := range c.logs {
		assertReadyOrdering(c.t, i+1, l.snapshot())
	}
}

func others(n, exclude int) []int {
	idxs := make([]int, 0, n-1)
	for i := range n {
		if i != exclude {
			idxs = append(idxs, i)
		}
	}
	return idxs
}

// Every replica — the leader and both followers — is told its shard is
// read-ready for the same (leader, term), and from then on a read issued on
// any of them is served.
func TestLeaderReady_ReachesEveryReplica(t *testing.T) {
	c := newReadyCluster(t, 3)
	all := []int{0, 1, 2}
	p := c.awaitAgreedReady(all, 0)
	c.assertServes(all, p)
	c.assertOrdering()
}

// Losing the leader ends its readiness on every survivor with an
// OnLeaderUpdated, and the new leader's readiness is announced once its own
// first commit lands, at a higher term.
func TestLeaderReady_Failover(t *testing.T) {
	c := newReadyCluster(t, 3)
	p1 := c.awaitAgreedReady([]int{0, 1, 2}, 0)
	leader := int(p1.leaderID - 1)

	c.stop(leader)
	survivors := others(3, leader)
	p2 := c.awaitAgreedReady(survivors, p1.term)
	if p2.leaderID == p1.leaderID {
		t.Fatalf("readiness announced for the stopped leader %d at term %d", p2.leaderID, p2.term)
	}
	c.assertServes(survivors, p2)
	c.assertOrdering()
}

// A replica reopened from its data directory announces the current leader and
// its readiness again, even in the same term: the restarted host knows nothing
// until its own commit index is brought into the leader's term.
func TestLeaderReady_ReannouncedAfterRestart(t *testing.T) {
	c := newReadyCluster(t, 3)
	p1 := c.awaitAgreedReady([]int{0, 1, 2}, 0)
	follower := others(3, int(p1.leaderID-1))[0]

	c.stop(follower)
	mark := len(c.logs[follower].snapshot())
	c.restart(follower)
	c.logs[follower].await(t, failoverTimeout,
		fmt.Sprintf("restarted replica %d ready", follower+1),
		func(ev []readyEvent) bool {
			_, ok := current(ev[mark:])
			return ok
		})
	p2 := c.awaitAgreedReady([]int{0, 1, 2}, p1.term-1)
	c.assertServes([]int{0, 1, 2}, p2)
	c.assertOrdering()
}
