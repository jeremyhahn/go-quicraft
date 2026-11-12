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
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// retryCountingSM counts every entry the state machine is handed and answers
// each with retryResultBase plus that count, so a result names the apply that
// produced it and a second apply of one command would show in both the count
// and the result. last holds the log index of the latest entry it applied.
type retryCountingSM struct {
	mockStateMachine
	applied *atomic.Uint64
	last    *atomic.Uint64
}

const retryResultBase = 1000

func (s *retryCountingSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		results[i] = sm.Result{Value: retryResultBase + s.applied.Add(1)}
		s.last.Store(entries[i].Index)
	}
	return nil
}

const retryShard uint64 = 1

type retryNode struct {
	host *Host
}

// sessionRetryCluster is a three-replica shard on three in-process hosts
// over mTLS on loopback, each replica with its own apply counter.
type sessionRetryCluster struct {
	t       *testing.T
	nodes   []*retryNode
	applied [3]atomic.Uint64
	last    [3]atomic.Uint64
	// base is the number of entries every replica had applied once the
	// stableLeader barrier settled; the tests count from it.
	base uint64
}

func newSessionRetryCluster(t *testing.T) *sessionRetryCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	c := &sessionRetryCluster{t: t}
	ids := make(map[uint64]string, 3)
	members := make(map[uint64]string, 3)
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.host.Close()
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
		c.nodes = append(c.nodes, &retryNode{host: h})
		ids[uint64(i+1)] = h.NodeIdentity()
		members[uint64(i+1)] = h.RaftAddress()
	}
	for i, n := range c.nodes {
		counter := &c.applied[i]
		last := &c.last[i]
		create := func(_, _ uint64) interface{} {
			return &retryCountingSM{applied: counter, last: last}
		}
		scfg := testShardConfig(retryShard, uint64(i+1))
		scfg.MemberIdentities = ids
		if err := n.host.StartShard(members, false, create, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	return c
}

// leader waits for a leader every host agrees on and returns its index.
func (c *sessionRetryCluster) leader() int {
	c.t.Helper()
	idx := -1
	if !waitUntil(20*time.Second, func() bool {
		idx = -1
		for i, n := range c.nodes {
			info, err := n.host.ShardInfo(retryShard)
			if err != nil {
				return false
			}
			if info.IsLeader {
				if idx >= 0 {
					return false
				}
				idx = i
			}
		}
		return idx >= 0
	}) {
		c.t.Fatal("no single leader elected")
	}
	return idx
}

// stableLeader returns the index of a leader that has committed a plain
// barrier entry in its own term, so a session created on it next does not
// race a leader that steps down right after its election. A barrier refused
// with a not-leader error (or one that times out) is retried against a
// re-sampled leader until ctx expires. Once the barrier commits, every
// replica is waited on to apply through it, so any earlier refused barrier
// that was "already submitted to raft" has either applied before it or been
// truncated for good; the applied count at that point is recorded in c.base
// and every later count is checked relative to it.
func (c *sessionRetryCluster) stableLeader(ctx context.Context) int {
	c.t.Helper()
	for {
		idx := c.leader()
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := c.nodes[idx].host.SyncPropose(attempt, retryShard, []byte("barrier"))
		cancel()
		if err == nil {
			c.settle(idx)
			return idx
		}
		if !errors.Is(err, ErrNotLeader) && !errors.Is(err, context.DeadlineExceeded) {
			c.t.Fatalf("barrier on replica %d: %v", idx+1, err)
		}
		if ctx.Err() != nil {
			c.t.Fatalf("no leader committed a barrier before the deadline: last error %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// settle waits until every replica has applied through the leader's last
// applied index and has applied the same number of entries, then records
// that number as the baseline in c.base.
func (c *sessionRetryCluster) settle(leader int) {
	c.t.Helper()
	final := c.last[leader].Load()
	if !waitUntil(10*time.Second, func() bool {
		for _, n := range c.nodes {
			info, err := n.host.ShardInfo(retryShard)
			if err != nil || info.LastApplied < final {
				return false
			}
		}
		base := c.applied[0].Load()
		for i := range c.applied {
			if c.applied[i].Load() != base {
				return false
			}
		}
		return true
	}) {
		c.t.Fatalf("replicas did not settle through barrier index %d", final)
	}
	c.base = c.applied[0].Load()
}

// transfer moves leadership from the replica at index from to another one and
// returns the new leader's index.
func (c *sessionRetryCluster) transfer(from int) int {
	c.t.Helper()
	to := (from + 1) % len(c.nodes)
	if !waitUntil(20*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.nodes[from].host.RequestLeaderTransfer(ctx, retryShard, uint64(to+1))
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if info, err := c.nodes[to].host.ShardInfo(retryShard); err == nil && info.IsLeader {
				return true
			}
			time.Sleep(10 * time.Millisecond)
		}
		return false
	}) {
		c.t.Fatalf("leadership never moved from replica %d to replica %d", from+1, to+1)
	}
	if got := c.leader(); got != to {
		c.t.Fatalf("leader after transfer = replica %d, want replica %d", got+1, to+1)
	}
	return to
}

// awaitApplied takes the log index of the latest entry the state machine on
// replica leader applied (the final proposal, which leader answered), waits
// until every replica's applied index reaches it, and only then checks every
// replica applied exactly want entries. Waiting on the index rather than the
// count keeps a duplicate apply on a follower from passing as the final one.
func (c *sessionRetryCluster) awaitApplied(leader int, want uint64) {
	c.t.Helper()
	final := c.last[leader].Load()
	if final == 0 {
		c.t.Fatalf("replica %d has applied no entry", leader+1)
	}
	if !waitUntil(10*time.Second, func() bool {
		for _, n := range c.nodes {
			info, err := n.host.ShardInfo(retryShard)
			if err != nil || info.LastApplied < final {
				return false
			}
		}
		return true
	}) {
		c.t.Fatalf("not every replica applied through index %d", final)
	}
	for i := range c.applied {
		if got := c.applied[i].Load() - c.base; got != want {
			c.t.Fatalf("replica %d applied %d entries after the barrier, want %d", i+1, got, want)
		}
	}
}

// proposeOnce proposes cmd on h with session and returns its result without
// advancing the session, as a client does whose reply it has not yet acted
// on: the next call on the session reuses the same series.
func proposeOnce(t *testing.T, h *Host, session *Session, cmd []byte) RequestResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rs, err := h.ProposeWithSession(ctx, session, cmd)
	if err != nil {
		t.Fatalf("ProposeWithSession: %v", err)
	}
	defer rs.Release()
	select {
	case r := <-rs.ResultC():
		return r
	case <-ctx.Done():
		t.Fatalf("ProposeWithSession: no result: %v", ctx.Err())
		return RequestResult{}
	}
}

func newRetrySession(t *testing.T, h *Host) *Session {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := h.NewSession(ctx, retryShard)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	return session
}

// A proposal answered by the old leader and retried with the same session and
// series against the new leader is applied once on every replica, and the
// retry returns the first result.
func TestSessionRetry_LeaderChangeAppliesOnce(t *testing.T) {
	c := newSessionRetryCluster(t)
	bctx, bcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer bcancel()
	old := c.stableLeader(bctx)
	resultBase := retryResultBase + c.base
	session := newRetrySession(t, c.nodes[old].host)
	series := session.SeriesID()

	first := proposeOnce(t, c.nodes[old].host, session, []byte("once"))
	if first.Err != nil {
		t.Fatalf("first attempt: %v", first.Err)
	}
	if first.Value != resultBase+1 {
		t.Fatalf("first result = %d, want %d", first.Value, resultBase+1)
	}
	c.awaitApplied(old, 1)

	next := c.transfer(old)
	if session.SeriesID() != series {
		t.Fatalf("series moved from %d to %d without ProposalCompleted", series, session.SeriesID())
	}
	retry := proposeOnce(t, c.nodes[next].host, session, []byte("once"))
	if retry.Err != nil {
		t.Fatalf("retry on the new leader: %v", retry.Err)
	}
	if retry.Value != first.Value {
		t.Fatalf("retry result = %d, want the first result %d", retry.Value, first.Value)
	}
	session.ProposalCompleted()

	// A later series on the new leader applies, which also shows every
	// replica has applied past the retry without applying it again.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.nodes[next].host.SyncProposeWithSession(ctx, session, []byte("after"))
	if err != nil {
		t.Fatalf("next series: %v", err)
	}
	if r.Value != resultBase+2 {
		t.Fatalf("next series result = %d, want %d", r.Value, resultBase+2)
	}
	c.awaitApplied(next, 2)
}

// A retry whose series is strictly below the session's replicated responded-to watermark,
// replayed against the new leader, is refused with *SessionExpiredError (the
// cached response was discarded, PhD Figure 6.1 step 3) and applied nowhere.
func TestSessionRetry_BelowRespondedToIsSessionExpired(t *testing.T) {
	c := newSessionRetryCluster(t)
	bctx, bcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer bcancel()
	old := c.stableLeader(bctx)
	resultBase := retryResultBase + c.base
	session := newRetrySession(t, c.nodes[old].host)
	h := c.nodes[old].host

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := h.SyncProposeWithSession(ctx, session, []byte("one")); err != nil {
		t.Fatalf("series 1: %v", err)
	}
	// Series 2 carries RespondedTo=1, so every replica evicts series 1.
	if _, err := h.SyncProposeWithSession(ctx, session, []byte("two")); err != nil {
		t.Fatalf("series 2: %v", err)
	}
	// Series 3 carries RespondedTo=2, so the replicated watermark is 2 and
	// the series-1 retry below falls strictly below it, not on it.
	if _, err := h.SyncProposeWithSession(ctx, session, []byte("three")); err != nil {
		t.Fatalf("series 3: %v", err)
	}
	if session.RespondedTo() < SeriesIDFirstProposal+2 {
		t.Fatalf("RespondedTo = %d, want at least %d", session.RespondedTo(), SeriesIDFirstProposal+2)
	}
	c.awaitApplied(old, 3)

	next := c.transfer(old)

	// A stale handle on the same client still at series 1, as a delayed
	// retransmission of the first request is.
	stale := NewSession(retryShard, session.ClientID())
	stale.PrepareForPropose()
	r := proposeOnce(t, c.nodes[next].host, stale, []byte("one"))
	var expired *SessionExpiredError
	if !errors.As(r.Err, &expired) {
		t.Fatalf("below-watermark retry: err = %v (value %d), want *SessionExpiredError", r.Err, r.Value)
	}
	if expired.ClientID != session.ClientID() {
		t.Fatalf("SessionExpiredError.ClientID = %d, want %d", expired.ClientID, session.ClientID())
	}

	// The live session is unaffected: its next series applies once, after
	// the refused entry on every replica.
	r2, err := c.nodes[next].host.SyncProposeWithSession(ctx, session, []byte("four"))
	if err != nil {
		t.Fatalf("series 4: %v", err)
	}
	if r2.Value != resultBase+4 {
		t.Fatalf("series 4 result = %d, want %d", r2.Value, resultBase+4)
	}
	c.awaitApplied(next, 4)
}
