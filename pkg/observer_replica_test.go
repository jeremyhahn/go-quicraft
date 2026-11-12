// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// observerShard is the shard the observer test runs.
const observerShard uint64 = 1

// observerID is the replica ID of the non-voting observer.
const observerID uint64 = 4

// observerNode is one Host of the observer test, on its own directories,
// address and leaf certificate.
type observerNode struct {
	walDir, nodeHostDir, addr string
	leaf                      *config.MTLSConfig
	host                      *Host
}

func observerHostConfig(n *observerNode) config.HostConfig {
	return config.HostConfig{
		WALDir:         n.walDir,
		NodeHostDir:    n.nodeHostDir,
		RaftAddress:    n.addr,
		ListenAddress:  n.addr,
		RTTMillisecond: 5,
		DeploymentID:   1,
		NoSyncWAL:      true,
		TransportConfig: config.TransportConfig{
			MTLSConfig: n.leaf,
		},
	}
}

// observerCluster is three voters and one observer, each on its own Host,
// connected over the real mTLS transport inside this process.
type observerCluster struct {
	t     *testing.T
	nodes []*observerNode // index i holds replica i+1
	ids   map[uint64]string
}

func newObserverCluster(t *testing.T) *observerCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 4)
	c := &observerCluster{t: t, ids: make(map[uint64]string, 4)}
	for i := range 4 {
		n := &observerNode{walDir: t.TempDir(), nodeHostDir: t.TempDir(), addr: "127.0.0.1:0", leaf: leaves[i]}
		h, err := NewHost(observerHostConfig(n), WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		n.addr, n.host = h.RaftAddress(), h
		c.ids[uint64(i+1)] = h.NodeIdentity()
		c.nodes = append(c.nodes, n)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			if n.host != nil {
				n.host.Close()
			}
		}
	})
	return c
}

// host is the Host running replica id.
func (c *observerCluster) host(id uint64) *Host { return c.nodes[id-1].host }

// voterIdentities is the committed identity of each founding voter.
func (c *observerCluster) voterIdentities() map[uint64]string {
	m := make(map[uint64]string, 3)
	for id := uint64(1); id <= 3; id++ {
		m[id] = c.ids[id]
	}
	return m
}

// startVoters bootstraps replicas 1-3 as the shard's voting set.
func (c *observerCluster) startVoters() {
	c.t.Helper()
	members := make(map[uint64]string, 3)
	for id := uint64(1); id <= 3; id++ {
		members[id] = c.nodes[id-1].addr
	}
	for id := uint64(1); id <= 3; id++ {
		scfg := testShardConfig(observerShard, id)
		// The negative phase needs a leader that cannot step down.
		scfg.CheckQuorum = config.BoolPtr(false)
		scfg.MemberIdentities = c.voterIdentities()
		if err := c.host(id).StartShard(members, false, kv.NewMemoryCreateFunc(), scfg); err != nil {
			c.t.Fatalf("StartShard[%d]: %v", id, err)
		}
	}
}

// leader is the voter that reports itself leader, or 0.
func (c *observerCluster) leader() uint64 {
	for id := uint64(1); id <= 3; id++ {
		h := c.host(id)
		if h == nil {
			continue
		}
		if info, err := h.ShardInfo(observerShard); err == nil && info.IsLeader {
			return id
		}
	}
	return 0
}

// put commits key=value through whichever voter leads, retrying across an
// election.
func (c *observerCluster) put(key, value string) {
	c.t.Helper()
	cmd, err := kv.EncodePut([]byte(key), []byte(value))
	if err != nil {
		c.t.Fatalf("EncodePut: %v", err)
	}
	var lastErr error
	if waitUntil(20*time.Second, func() bool {
		id := c.leader()
		if id == 0 {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, lastErr = c.host(id).SyncPropose(ctx, observerShard, cmd)
		return lastErr == nil
	}) {
		return
	}
	c.t.Fatalf("put %q never committed: %v", key, lastErr)
}

// assertObserverNeverLeads fails if the observer reports itself leader at
// any point during d.
func (c *observerCluster) assertObserverNeverLeads(d time.Duration) {
	c.t.Helper()
	obs := c.host(observerID)
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		leaderID, term, ok, err := obs.LeaderIDAndTerm(observerShard)
		if err != nil {
			c.t.Fatalf("observer LeaderIDAndTerm: %v", err)
		}
		if ok && leaderID == observerID {
			c.t.Fatalf("observer reports itself leader at term %d", term)
		}
		if info, err := obs.ShardInfo(observerShard); err == nil && info.IsLeader {
			c.t.Fatalf("observer ShardInfo reports IsLeader: %+v", info)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// admitObserver admits replica 4 through a committed config change, then
// starts it as a joiner anchored on the voters' committed identities.
func (c *observerCluster) admitObserver(t *testing.T) {
	t.Helper()
	obs := c.host(observerID)
	if !waitUntil(20*time.Second, func() bool {
		id := c.leader()
		if id == 0 {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return c.host(id).SyncRequestAddObserverWithIdentity(ctx, observerShard, observerID,
			obs.RaftAddress(), c.ids[observerID], 0) == nil
	}) {
		t.Fatal("SyncRequestAddObserverWithIdentity never committed")
	}
	scfg := testShardConfig(observerShard, observerID)
	scfg.IsObserver = true
	// The negative phase needs a leader that cannot step down.
	scfg.CheckQuorum = config.BoolPtr(false)
	scfg.MemberIdentities = c.voterIdentities()
	if err := obs.StartShard(nil, true, kv.NewMemoryCreateFunc(), scfg); err != nil {
		t.Fatalf("observer StartShard(join=true): %v", err)
	}
}

func TestObserverReplica_ReceivesEveryEntryAndNeverCountsTowardQuorum(t *testing.T) {
	c := newObserverCluster(t)
	c.startVoters()
	c.put("before", "the-observer")

	c.admitObserver(t)
	obs := c.host(observerID)

	const entries = 20
	for i := range entries {
		c.put(fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
	}

	// The observer applies every entry: its state machine hash reaches the
	// leader's at the same applied index.
	var lastObs, lastLeader string
	if !waitUntil(20*time.Second, func() bool {
		id := c.leader()
		if id == 0 {
			return false
		}
		lh, li, lok, lerr := c.host(id).StateMachineHash(observerShard)
		oh, oi, ook, oerr := obs.StateMachineHash(observerShard)
		lastLeader = fmt.Sprintf("hash=%x index=%d ok=%v err=%v", lh, li, lok, lerr)
		lastObs = fmt.Sprintf("hash=%x index=%d ok=%v err=%v", oh, oi, ook, oerr)
		return lerr == nil && oerr == nil && lok && ook && li == oi && lh == oh
	}) {
		t.Fatalf("observer state machine never matched the leader's: leader %s, observer %s", lastLeader, lastObs)
	}

	// A stale read on the observer serves the applied values, the first one
	// included (it predates the observer's admission).
	for _, kvp := range [][2]string{{"before", "the-observer"}, {"k00", "v00"}, {"k19", "v19"}} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		got, err := obs.StaleRead(ctx, observerShard, []byte(kvp[0]))
		cancel()
		if err != nil {
			t.Fatalf("observer StaleRead(%q): %v", kvp[0], err)
		}
		b, ok := got.([]byte)
		if !ok || !bytes.Equal(b, []byte(kvp[1])) {
			t.Fatalf("observer StaleRead(%q) = %#v, want %q", kvp[0], got, kvp[1])
		}
	}

	// The committed membership lists replica 4 as an observer, not a voter.
	leaderID := c.leader()
	if leaderID == 0 {
		t.Fatal("no leader before the membership read")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	m, err := c.host(leaderID).SyncGetShardMembership(ctx, observerShard)
	cancel()
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}
	if _, ok := m.Observers[observerID]; !ok {
		t.Fatalf("replica %d is not in Membership.Observers: %+v", observerID, m)
	}
	if _, ok := m.Nodes[observerID]; ok {
		t.Fatalf("replica %d is in Membership.Nodes, the voting set: %+v", observerID, m)
	}
	if len(m.Nodes) != 3 {
		t.Fatalf("Membership.Nodes = %v, want the three voters", m.Nodes)
	}
	c.assertObserverNeverLeads(200 * time.Millisecond)

	// Stop two voters. One voter and one observer are no quorum: no proposal
	// commits, through the surviving voter or the observer, and the observer
	// never takes leadership.
	// The survivor is the current leader, so the lone voter keeps its role and
	// only the commit rule can stop a proposal: if the observer's
	// acknowledgements counted, leader plus observer would commit.
	var survivor uint64
	if !waitUntil(20*time.Second, func() bool {
		survivor = c.leader()
		return survivor != 0
	}) {
		t.Fatal("no leader before stopping two voters")
	}
	before, err := c.host(survivor).ShardInfo(observerShard)
	if err != nil {
		t.Fatalf("survivor ShardInfo before the stop: %v", err)
	}
	leaderTerm := before.Term
	for id := uint64(1); id <= 3; id++ {
		if id == survivor {
			continue
		}
		if err := c.nodes[id-1].host.Close(); err != nil {
			t.Fatalf("Close[%d]: %v", id, err)
		}
		c.nodes[id-1].host = nil
	}
	cmd, err := kv.EncodePut([]byte("after"), []byte("quorum-lost"))
	if err != nil {
		t.Fatalf("EncodePut: %v", err)
	}
	for _, id := range []uint64{survivor, observerID} {
		for attempt := range 3 {
			pctx, pcancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, perr := c.host(id).SyncPropose(pctx, observerShard, cmd)
			pcancel()
			if perr == nil {
				t.Fatalf("proposal through replica %d (attempt %d) committed with two of three voters stopped", id, attempt)
			}
			if id == survivor && errors.Is(perr, ErrNotLeader) {
				t.Fatalf("proposal through the survivor %d (attempt %d) failed with ErrNotLeader: it stepped down", id, attempt)
			}
		}
	}
	c.assertObserverNeverLeads(time.Second)

	// The survivor still leads in the same term, and the observer still
	// follows it: only the commit rule stopped the proposals.
	after, err := c.host(survivor).ShardInfo(observerShard)
	if err != nil {
		t.Fatalf("survivor ShardInfo after the proposals: %v", err)
	}
	if !after.IsLeader || after.Term != leaderTerm {
		t.Fatalf("survivor ShardInfo = %+v, want IsLeader at term %d", after, leaderTerm)
	}
	oinfo, err := obs.ShardInfo(observerShard)
	if err != nil {
		t.Fatalf("observer ShardInfo after the proposals: %v", err)
	}
	if oinfo.LeaderID != survivor || oinfo.Term != leaderTerm {
		t.Fatalf("observer ShardInfo = %+v, want LeaderID %d at term %d", oinfo, survivor, leaderTerm)
	}

	// Nothing the failed proposals carried reached the observer's state.
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	got, err := obs.StaleRead(rctx, observerShard, []byte("after"))
	rcancel()
	if err == nil {
		t.Fatalf("observer StaleRead(\"after\") = %#v, want key not found", got)
	}
	var nf *kv.KeyNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("observer StaleRead(\"after\") error = %v, want *kv.KeyNotFoundError", err)
	}
}

// TestObserverReplica_TwoVotersCommitWithObserverDown stops the observer and
// one non-leader voter. The two remaining voters are a quorum of three, so a
// proposal through the leader commits; were the observer counted as a voter,
// quorum would be 3 of 4 and the proposal could not commit.
func TestObserverReplica_TwoVotersCommitWithObserverDown(t *testing.T) {
	c := newObserverCluster(t)
	c.startVoters()
	c.admitObserver(t)
	c.put("k", "v")

	// The observer catches up: its state machine hash reaches the leader's at
	// the same applied index.
	obs := c.host(observerID)
	if !waitUntil(20*time.Second, func() bool {
		id := c.leader()
		if id == 0 {
			return false
		}
		lh, li, lok, lerr := c.host(id).StateMachineHash(observerShard)
		oh, oi, ook, oerr := obs.StateMachineHash(observerShard)
		return lerr == nil && oerr == nil && lok && ook && li == oi && lh == oh
	}) {
		t.Fatal("observer never caught up with the leader")
	}

	var leaderID uint64
	if !waitUntil(20*time.Second, func() bool {
		leaderID = c.leader()
		return leaderID != 0
	}) {
		t.Fatal("no leader before the stop")
	}
	var stopped, other uint64
	for id := uint64(1); id <= 3; id++ {
		if id == leaderID {
			continue
		}
		if stopped == 0 {
			stopped = id
		} else {
			other = id
		}
	}
	for _, id := range []uint64{observerID, stopped} {
		if err := c.nodes[id-1].host.Close(); err != nil {
			t.Fatalf("Close[%d]: %v", id, err)
		}
		c.nodes[id-1].host = nil
	}

	cmd, err := kv.EncodePut([]byte("after"), []byte("two-voters"))
	if err != nil {
		t.Fatalf("EncodePut: %v", err)
	}
	var lastErr error
	if !waitUntil(20*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, lastErr = c.host(leaderID).SyncPropose(ctx, observerShard, cmd)
		return lastErr == nil
	}) {
		t.Fatalf("proposal through leader %d with voters %d and %d up never committed: %v",
			leaderID, leaderID, other, lastErr)
	}

	// The other surviving voter applies the committed value.
	var got any
	if !waitUntil(20*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		got, lastErr = c.host(other).StaleRead(ctx, observerShard, []byte("after"))
		b, ok := got.([]byte)
		return lastErr == nil && ok && bytes.Equal(b, []byte("two-voters"))
	}) {
		t.Fatalf("voter %d StaleRead(\"after\") = %#v, %v; want \"two-voters\"", other, got, lastErr)
	}
}
