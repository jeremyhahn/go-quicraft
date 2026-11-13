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
	"encoding/gob"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

const concurrentProposeShard = uint64(797)

// recordingStateMachine counts how many times each command was applied, so a
// test can tell an entry applied twice from one applied once. Its hash is a
// function of the counts alone, never of the order entries were applied in.
type recordingStateMachine struct {
	mu      sync.Mutex
	applied map[string]int

	// replicaID and gate, when gate is set, let a test hold this replica's
	// apply of one key (see applyGate).
	replicaID uint64
	gate      *applyGate
}

func newRecordingStateMachine() *recordingStateMachine {
	return &recordingStateMachine{applied: make(map[string]int)}
}

func (r *recordingStateMachine) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		if r.gate != nil {
			r.gate.wait(ctx, r.replicaID, string(entries[i].Cmd))
		}
		r.mu.Lock()
		r.applied[string(entries[i].Cmd)]++
		r.mu.Unlock()
		results[i] = sm.Result{Value: entries[i].Index}
	}
	return nil
}

// concurrentProposeGateKey is the command an armed applyGate holds.
const concurrentProposeGateKey = "gate"

// applyGate blocks one replica's apply of concurrentProposeGateKey until it
// is opened. Held at the leader, it keeps every later proposal on that
// leader committed but not completed, which is the window a leadership
// change turns into MaybeApplied failures.
type applyGate struct {
	replica     atomic.Uint64
	reached     chan struct{}
	release     chan struct{}
	reachedOnce sync.Once
	releaseOnce sync.Once
}

func newApplyGate() *applyGate {
	return &applyGate{reached: make(chan struct{}), release: make(chan struct{})}
}

// arm makes replicaID's apply of the gate key block until open.
func (g *applyGate) arm(replicaID uint64) { g.replica.Store(replicaID) }

// open releases a held apply and disarms the gate.
func (g *applyGate) open() {
	g.replica.Store(0)
	g.releaseOnce.Do(func() { close(g.release) })
}

func (g *applyGate) wait(ctx context.Context, replicaID uint64, key string) {
	if key != concurrentProposeGateKey || g.replica.Load() != replicaID {
		return
	}
	g.reachedOnce.Do(func() { close(g.reached) })
	select {
	case <-g.release:
	case <-ctx.Done():
	}
}

func (r *recordingStateMachine) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	return query, nil
}

// SaveSnapshot writes the counts, so a replica brought up to date by a
// snapshot rather than by the log keeps the record the assertions read.
func (r *recordingStateMachine) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return gob.NewEncoder(w).Encode(r.applied)
}

func (r *recordingStateMachine) RecoverFromSnapshot(_ context.Context, rd io.Reader, _ <-chan struct{}) error {
	applied := make(map[string]int)
	if err := gob.NewDecoder(rd).Decode(&applied); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = applied
	return nil
}

func (r *recordingStateMachine) Close(context.Context) error { return nil }

func (r *recordingStateMachine) GetHash() (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.applied))
	for k := range r.applied {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	h := fnv.New64a()
	for _, k := range keys {
		fmt.Fprintf(h, "%s=%d;", k, r.applied[k])
	}
	return h.Sum64(), nil
}

// snapshot returns a copy of the counts.
func (r *recordingStateMachine) snapshot() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(r.applied))
	for k, v := range r.applied {
		out[k] = v
	}
	return out
}

// concurrentProposeCluster is three hosts on loopback QUIC, with leaves from
// one CA, running one shard of recordingStateMachines. Its hosts can be
// closed and restarted from their own logs at the same addresses.
type concurrentProposeCluster struct {
	cfgs    []config.HostConfig
	hosts   []*Host
	members map[uint64]string
	ids     map[uint64]string
	gate    *applyGate

	mu       sync.Mutex
	machines map[uint64]*recordingStateMachine
	// preRestart is each replica's counts as the workload left them,
	// saved by restart after every host closed and before log replay
	// rebuilds the machines.
	preRestart map[uint64]map[string]int
	// down marks hosts closed by closeHost; restart saves it in
	// preRestartDown and clears it.
	down           map[int]bool
	preRestartDown map[uint64]bool
}

// closeHost closes h before the drain restart and marks it down: its
// replica's counts stay where the close left them.
func (c *concurrentProposeCluster) closeHost(t *testing.T, h *Host) {
	t.Helper()
	for i, x := range c.hosts {
		if x != h {
			continue
		}
		if err := h.Close(); err != nil {
			t.Fatalf("Close host %d: %v", i, err)
		}
		c.mu.Lock()
		c.down[i] = true
		c.mu.Unlock()
		return
	}
	t.Fatal("closeHost: host is not in the cluster")
}

// ackedFenceLocked returns "" once every live replica's own state machine
// has applied every acknowledged key, and otherwise names one replica and
// key still missing. An acknowledged entry is committed, so every replica
// that stays up applies it; one that never does skipped a committed entry.
// c.mu must be held.
func (c *concurrentProposeCluster) ackedFenceLocked(acked map[string]struct{}) string {
	for id, m := range c.machines {
		if c.down[int(id-1)] {
			continue
		}
		applied := m.snapshot()
		for key := range acked {
			if applied[key] == 0 {
				return fmt.Sprintf("replica %d has not applied acknowledged key %s", id, key)
			}
		}
	}
	return ""
}

// newConcurrentProposeCluster creates the three hosts; no shard is started.
func newConcurrentProposeCluster(t *testing.T) *concurrentProposeCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	c := &concurrentProposeCluster{
		members:  make(map[uint64]string, 3),
		ids:      make(map[uint64]string, 3),
		gate:     newApplyGate(),
		machines: make(map[uint64]*recordingStateMachine, 3),
		down:     make(map[int]bool, 3),
	}
	t.Cleanup(func() {
		for _, h := range c.hosts {
			h.Close()
		}
	})
	// Registered after the hosts' cleanup, so it runs first: a held apply
	// must not block Close.
	t.Cleanup(c.gate.open)
	for i := range 3 {
		cfg := config.HostConfig{
			WALDir:          t.TempDir(),
			NodeHostDir:     t.TempDir(),
			RaftAddress:     "127.0.0.1:0",
			ListenAddress:   "127.0.0.1:0",
			RTTMillisecond:  10,
			DeploymentID:    1,
			NoSyncWAL:       true,
			TransportConfig: config.TransportConfig{MTLSConfig: leaves[i]},
		}
		h, err := NewHost(cfg, WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		// A restart binds the address the committed membership names.
		cfg.RaftAddress, cfg.ListenAddress = h.RaftAddress(), h.RaftAddress()
		c.cfgs = append(c.cfgs, cfg)
		c.hosts = append(c.hosts, h)
		c.members[uint64(i+1)] = h.RaftAddress()
		c.ids[uint64(i+1)] = h.NodeIdentity()
	}
	return c
}

func (c *concurrentProposeCluster) create(_, replicaID uint64) interface{} {
	r := newRecordingStateMachine()
	r.replicaID, r.gate = replicaID, c.gate
	c.mu.Lock()
	c.machines[replicaID] = r
	c.mu.Unlock()
	return r
}

// startShard starts the shard on host i (replica i+1).
func (c *concurrentProposeCluster) startShard(t *testing.T, i int) {
	t.Helper()
	scfg := testShardConfig(concurrentProposeShard, uint64(i+1))
	scfg.MemberIdentities = c.ids
	if err := c.hosts[i].StartShard(c.members, false, c.create, scfg); err != nil {
		t.Fatalf("StartShard[%d]: %v", i, err)
	}
}

// machine returns replica replicaID's current state machine.
func (c *concurrentProposeCluster) machine(replicaID uint64) *recordingStateMachine {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.machines[replicaID]
}

// waitLeader waits for a leader and returns its host.
func (c *concurrentProposeCluster) waitLeader(t *testing.T) *Host {
	t.Helper()
	var l *Host
	if !waitUntil(20*time.Second, func() bool {
		l = concurrentProposeLeader(c.hosts)
		return l != nil
	}) {
		t.Fatal("no leader elected")
	}
	return l
}

// restart closes every host, then brings each back from its own WALDir and
// NodeHostDir at the same address with a fresh state machine, which replays
// what the replica's snapshot and log hold. No caller may be proposing.
func (c *concurrentProposeCluster) restart(t *testing.T, acked map[string]struct{}) {
	t.Helper()
	c.gate.open()
	// Fence the live replicas at the acknowledged prefix before any host
	// closes: each workload state machine must itself have applied every
	// acknowledged key, so concurrentProposeCheckPreRestart checks the
	// pipeline's own applies and not what replay later repairs. Proposals
	// still in flight (MaybeApplied) may apply on some replicas and not
	// others while the hosts close one at a time; they are not fenced.
	var missing string
	if !waitUntil(30*time.Second, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		missing = c.ackedFenceLocked(acked)
		return missing == ""
	}) {
		t.Fatalf("live replicas never reached the acknowledged prefix before the restart: %s", missing)
	}
	for i, h := range c.hosts {
		if err := h.Close(); err != nil {
			t.Fatalf("Close[%d]: %v", i, err)
		}
	}
	// Every host is closed, so no apply can still run: these are the
	// counts the workload itself produced, before replay replaces them.
	c.mu.Lock()
	c.preRestart = make(map[uint64]map[string]int, len(c.machines))
	c.preRestartDown = make(map[uint64]bool, len(c.machines))
	for id, m := range c.machines {
		c.preRestart[id] = m.snapshot()
		c.preRestartDown[id] = c.down[int(id-1)]
	}
	clear(c.machines)
	clear(c.down)
	c.mu.Unlock()
	for i, cfg := range c.cfgs {
		h, err := NewHost(cfg, WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d] (restart): %v", i, err)
		}
		c.hosts[i] = h
		c.startShard(t, i)
	}
}

// concurrentProposeLeader returns the host that reports itself leader of the
// shard, or nil when none does.
func concurrentProposeLeader(hosts []*Host) *Host {
	for _, n := range hosts {
		if info, err := n.ShardInfo(concurrentProposeShard); err == nil && info.IsLeader {
			return n
		}
	}
	return nil
}

// proposeOutcomes classifies SyncPropose outcomes. Each class bounds how many
// times a replica may apply the key:
//   - acked: acknowledged, applied exactly once;
//   - maybe: already submitted to raft when it failed (MaybeApplied),
//     applied at most once;
//   - refused: a definite refusal before submission (a not-leader refusal,
//     or ErrTransferInProgress from a leader mid-transfer), applied zero
//     times;
//   - failed: any other error, which fails the test.
type proposeOutcomes struct {
	mu      sync.Mutex
	acked   map[string]struct{}
	maybe   map[string]error
	refused map[string]error
	failed  map[string]error
}

func newProposeOutcomes() *proposeOutcomes {
	return &proposeOutcomes{
		acked:   make(map[string]struct{}),
		maybe:   make(map[string]error),
		refused: make(map[string]error),
		failed:  make(map[string]error),
	}
}

// record files key's outcome under its class and reports whether it was a
// refusal, after which the proposer re-resolves the leader.
func (o *proposeOutcomes) record(key string, err error) (moved bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch {
	case err == nil:
		o.acked[key] = struct{}{}
	case MaybeApplied(err):
		o.maybe[key] = err
	case errors.Is(err, ErrNotLeader), errors.Is(err, ErrTransferInProgress):
		o.refused[key] = err
		moved = true
	default:
		o.failed[key] = err
	}
	return moved
}

// TestConcurrentPropose_EveryAcknowledgedEntryAppliedExactlyOnce drives 64
// goroutines of 50 SyncPropose calls each, with unique keys, at the leader of
// a three-replica shard on loopback QUIC. Every acknowledged key must be
// applied exactly once on every replica, no other key may be applied, and the
// replicas' state hashes must agree.
//
// TestMultipleConcurrentProposals_Complete (host_test.go) covers only ten
// concurrent proposals on one replica and checks that each returns.
func TestConcurrentPropose_EveryAcknowledgedEntryAppliedExactlyOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("three-host cluster over loopback QUIC")
	}
	const (
		goroutines = 64
		perG       = 50
	)

	c := newConcurrentProposeCluster(t)
	for i := range c.hosts {
		c.startShard(t, i)
	}
	var leader atomic.Pointer[Host]
	leader.Store(c.waitLeader(t))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A leader change during the run is legitimate; a refused proposer
	// re-resolves the leader and carries on with its next key.
	out := newProposeOutcomes()
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perG {
				key := fmt.Sprintf("g%02d-k%02d", g, i)
				_, err := leader.Load().SyncPropose(ctx, concurrentProposeShard, []byte(key))
				if out.record(key, err) {
					waitUntil(5*time.Second, func() bool {
						if l := concurrentProposeLeader(c.hosts); l != nil {
							leader.Store(l)
							return true
						}
						return false
					})
				}
			}
		}()
	}
	wg.Wait()

	for key, err := range out.failed {
		t.Errorf("SyncPropose(%s): %v", key, err)
	}
	if len(out.acked) == 0 {
		t.Fatal("no proposal was acknowledged")
	}
	if len(out.maybe)+len(out.refused) > 0 {
		t.Logf("leadership moved during the run: %d proposals may still commit, %d refused before submission",
			len(out.maybe), len(out.refused))
	}

	barrierKey, barriers, want := concurrentProposeDrain(ctx, t, c, out)
	concurrentProposeCheckPreRestart(t, c, out)
	at, hashes := concurrentProposeConverge(ctx, t, c.hosts, want)
	concurrentProposeCheck(t, c, out, barrierKey, barriers)
	if hashes[0] != hashes[1] || hashes[0] != hashes[2] {
		t.Fatalf("replica state hashes differ at index %d: %v", at, hashes)
	}
}

// concurrentProposeDrain settles every outstanding workload proposal before
// the counts are read, and returns what concurrentProposeBarrier returns.
//
// wg.Wait() only means the callers returned. A MaybeApplied proposal can
// still commit, and a proposal forwarded to a former leader is out of reach
// of any barrier through the current one: a follower forwards a Propose to
// whatever leader it knows at that moment (raft.go stepFollower, msg.To =
// r.leaderID), on its stream to THAT target, and a stale-term Propose is
// accepted and re-forwarded by its receiver once it is a follower, so a
// message delayed on the stream to the former leader can commit after every
// fence through the current leader. Closing every host drops every message
// still in flight with its transport. Each host then restarts from its own
// log with no caller proposing, so the only workload entries that can ever
// commit are those already in a replica's log, and a barrier through the
// restarted leader commits after every one of them that will.
func concurrentProposeDrain(ctx context.Context, t *testing.T, c *concurrentProposeCluster, out *proposeOutcomes) (string, map[string]struct{}, uint64) {
	t.Helper()
	c.restart(t, out.acked)
	return concurrentProposeBarrier(ctx, t, c.hosts)
}

// concurrentProposeCheck asserts each replica applied every key within its
// class's bound (see proposeOutcomes), the acknowledged barrier exactly once,
// every other barrier attempt at most once, and nothing else, and that
// every replica holds the same count for every key.
// concurrentProposeCheckPreRestart asserts on the counts the workload's own
// state machines held when restart closed the hosts: every acknowledged key
// applied exactly once on each live replica (on a replica closed
// mid-workload, at most once), no key more than once, and no refused or
// unsubmitted key at all. Replay after the restart applies each persisted
// entry once and would repair a skipped apply, so only these counts can show
// a lost or duplicate apply during the concurrent workload. It does not compare the
// replicas with each other: restart closes the hosts one at a time, and a
// MaybeApplied proposal still in flight can commit and apply on the
// remaining quorum after the first host has stopped, so the saved prefixes
// may legitimately differ. Whole-state equality is asserted by
// concurrentProposeCheck, after the restart fence and the final barrier.
func concurrentProposeCheckPreRestart(t *testing.T, c *concurrentProposeCluster, out *proposeOutcomes) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.preRestart) != 3 {
		t.Fatalf("%d replicas' counts were saved before the restart, want 3", len(c.preRestart))
	}
	for id, applied := range c.preRestart {
		down := c.preRestartDown[id]
		for key := range out.acked {
			// restart fenced every live replica at the acknowledged
			// prefix; a replica closed mid-workload may stop short of the
			// entries acknowledged after it.
			if n := applied[key]; n != 1 && (!down || n > 1) {
				t.Errorf("before restart replica %d (down=%v) applied acknowledged key %s %d times, want 1", id, down, key, n)
			}
		}
		for key, n := range applied {
			if n > 1 {
				t.Errorf("before restart replica %d (down=%v) applied key %s %d times, want at most 1", id, down, key, n)
			}
			_, acked := out.acked[key]
			_, maybe := out.maybe[key]
			if _, ok := out.refused[key]; ok {
				t.Errorf("before restart replica %d applied key %s refused before submission (%d times), want 0", id, key, n)
			} else if !acked && !maybe {
				t.Errorf("before restart replica %d applied unacknowledged key %s (%d times)", id, key, n)
			}
		}
	}
}

func concurrentProposeCheck(t *testing.T, c *concurrentProposeCluster, out *proposeOutcomes, barrierKey string, barriers map[string]struct{}) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.machines) != 3 {
		t.Fatalf("%d state machines were created, want 3", len(c.machines))
	}
	for id, m := range c.machines {
		applied := m.snapshot()
		for key := range out.acked {
			if n := applied[key]; n != 1 {
				t.Errorf("replica %d applied acknowledged key %s %d times, want 1", id, key, n)
			}
		}
		if n := applied[barrierKey]; n != 1 {
			t.Errorf("replica %d applied barrier key %s %d times, want 1", id, barrierKey, n)
		}
		for key, n := range applied {
			if _, ok := out.acked[key]; ok {
				continue
			}
			if _, ok := barriers[key]; ok {
				if n > 1 {
					t.Errorf("replica %d applied barrier attempt %s %d times, want at most 1", id, key, n)
				}
				continue
			}
			if _, ok := out.refused[key]; ok {
				t.Errorf("replica %d applied key %s refused before submission (%d times), want 0", id, key, n)
				continue
			}
			if _, ok := out.maybe[key]; ok {
				if n > 1 {
					t.Errorf("replica %d applied may-still-commit key %s %d times, want at most 1", id, key, n)
				}
				continue
			}
			t.Errorf("replica %d applied unacknowledged key %s (%d times)", id, key, n)
		}
	}
	// The final barrier is the common applied prefix: after the restart
	// fence no workload proposal is in flight, and concurrentProposeConverge
	// has every replica settled at one applied index, so each must hold the
	// same count for every key.
	const refID uint64 = 1
	ref := c.machines[refID].snapshot()
	for id, m := range c.machines {
		if id == refID {
			continue
		}
		applied := m.snapshot()
		for key, n := range applied {
			if ref[key] != n {
				t.Errorf("replica %d applied key %s %d times, replica %d %d times", id, key, n, refID, ref[key])
			}
		}
		for key, n := range ref {
			if _, ok := applied[key]; !ok {
				t.Errorf("replica %d applied key %s 0 times, replica %d %d times", id, key, refID, n)
			}
		}
	}
}

// concurrentProposeBarrier commits a barrier entry through the current
// leader, retrying with a fresh key on a not-leader refusal or a
// MaybeApplied failure, and returns the key that was acknowledged, every
// barrier key attempted (each applied at most once), and the barrier's
// applied index.
func concurrentProposeBarrier(ctx context.Context, t *testing.T, hosts []*Host) (string, map[string]struct{}, uint64) {
	t.Helper()
	attempted := make(map[string]struct{})
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			t.Fatalf("barrier never committed: %v", ctx.Err())
		}
		var current *Host
		if !waitUntil(20*time.Second, func() bool {
			current = concurrentProposeLeader(hosts)
			return current != nil
		}) {
			t.Fatal("no leader after the run")
		}
		key := fmt.Sprintf("barrier-%02d", attempt)
		attempted[key] = struct{}{}
		res, err := current.SyncPropose(ctx, concurrentProposeShard, []byte(key))
		switch {
		case err == nil:
			return key, attempted, res.Value
		case MaybeApplied(err), errors.Is(err, ErrNotLeader):
			t.Logf("barrier %s: %v; retrying", key, err)
		default:
			t.Fatalf("barrier SyncPropose(%s): %v", key, err)
		}
	}
}

// concurrentProposeConverge waits until every replica reports one common
// applied index at or past want with equal state hashes in the same sample,
// and returns that index and the hashes.
func concurrentProposeConverge(ctx context.Context, t *testing.T, hosts []*Host, want uint64) (uint64, []uint64) {
	t.Helper()
	var (
		at      uint64
		hashes  []uint64
		indexes []uint64
		lastErr error
	)
	ok := waitUntil(30*time.Second, func() bool {
		if ctx.Err() != nil {
			lastErr = ctx.Err()
			return false
		}
		hashes, indexes = hashes[:0], indexes[:0]
		for i, n := range hosts {
			hash, idx, ok, err := n.StateMachineHash(concurrentProposeShard)
			if err != nil || !ok {
				lastErr = fmt.Errorf("replica %d StateMachineHash: ok=%v err=%v", i+1, ok, err)
				return false
			}
			hashes = append(hashes, hash)
			indexes = append(indexes, idx)
		}
		at = indexes[0]
		for i := range indexes {
			if indexes[i] < want || indexes[i] != at || hashes[i] != hashes[0] {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("replicas never settled at one index >= %d with equal hashes: indexes=%v hashes=%v last error=%v",
			want, indexes, hashes, lastErr)
	}
	return at, append([]uint64(nil), hashes...)
}
