// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

const noQuorumShard uint64 = 1

// noQuorumElectionRTT is the election timeout, in 5 ms ticks, of every
// replica in TestStaleReadNoQuorum: long enough (2 s) that the isolated
// leader cannot step down before phase 1 has submitted its SyncRead.
const noQuorumElectionRTT uint64 = 400

// noQuorumSM counts the entries it has applied; Lookup answers the count, so
// a read shows exactly the replica's applied state.
type noQuorumSM struct{ applied atomic.Uint64 }

func (s *noQuorumSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		results[i] = sm.Result{Value: s.applied.Add(1)}
	}
	return nil
}

func (s *noQuorumSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	return s.applied.Load(), nil
}

func (s *noQuorumSM) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}

func (s *noQuorumSM) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (s *noQuorumSM) Close(_ context.Context) error { return nil }

// TestStaleReadNoQuorum pins the guarantee split between the two reads: a
// replica that has lost its quorum still answers StaleRead from its applied
// state, while SyncRead, which must confirm a quorum, fails within its
// deadline with the documented timeout error. Closing the hosts afterwards
// leaves no goroutine behind.
func TestStaleReadNoQuorum(t *testing.T) {
	baseline := runtime.NumGoroutine()

	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	hosts := make([]*Host, 3)
	ids := make(map[uint64]string, 3)
	members := make(map[uint64]string, 3)
	closeAll := func() {
		for i, h := range hosts {
			if h != nil {
				h.Close()
				hosts[i] = nil
			}
		}
	}
	t.Cleanup(closeAll)
	for i := range hosts {
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
		hosts[i] = h
		ids[uint64(i+1)] = h.NodeIdentity()
		members[uint64(i+1)] = h.RaftAddress()
	}
	sms := []*noQuorumSM{{}, {}, {}}
	for i, h := range hosts {
		scfg := testShardConfig(noQuorumShard, uint64(i+1))
		scfg.MemberIdentities = ids
		// 400 ticks of 5 ms: CheckQuorum needs at least 2 s to step an
		// isolated leader down, so phase 1 reads while it still leads.
		scfg.ElectionRTT = noQuorumElectionRTT
		create := func(_, _ uint64) interface{} { return sms[i] }
		if err := h.StartShard(members, false, create, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}

	const entries = 5
	for n := range entries {
		proposeOnLeader(t, hosts, fmt.Sprintf("e%d", n))
	}
	if !waitUntil(10*time.Second, func() bool {
		for _, s := range sms {
			if s == nil || s.applied.Load() != entries {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("replicas never all applied %d entries", entries)
	}

	// Keep the leader: a leader that loses its quorum is the replica most
	// tempted to answer a linearizable read from its own state.
	survivor := -1
	for i, h := range hosts {
		if info, err := h.ShardInfo(noQuorumShard); err == nil && info.IsLeader {
			survivor = i
		}
	}
	if survivor < 0 {
		t.Fatal("no leader after the entries committed")
	}
	for i, h := range hosts {
		if i != survivor {
			h.Close()
			hosts[i] = nil
		}
	}
	h := hosts[survivor]

	// Phase 1: the leader has not yet noticed it lost its quorum (it must
	// still lead before and after the SyncRead). Phase 2: it has stepped
	// down and knows no leader. Neither may serve SyncRead.
	assertNoQuorumReads(t, h, "leader without quorum", entries, false)
	if !waitUntil(20*time.Second, func() bool {
		info, err := h.ShardInfo(noQuorumShard)
		return err == nil && !info.IsLeader
	}) {
		t.Fatal("the leader never stepped down after losing its quorum")
	}
	assertNoQuorumReads(t, h, "stepped-down replica", entries, true)

	closeAll()
	if !waitUntil(10*time.Second, func() bool { return runtime.NumGoroutine() <= baseline }) {
		buf := make([]byte, 1<<16)
		t.Fatalf("goroutines after Close: %d, baseline %d\n%s",
			runtime.NumGoroutine(), baseline, buf[:runtime.Stack(buf, true)])
	}
}

// proposeOnLeader commits one proposal through whichever host leads.
// assertNoQuorumReads checks, on a replica cut off from its quorum, that
// StaleRead answers the applied count within 1s and that SyncRead with a
// 500ms deadline refuses: with ErrTimeout or ErrCanceled when the wait for a
// quorum runs out, or with ErrNotLeader when the replica steps down during
// the read or already knows no leader. It never answers a value.
func assertNoQuorumReads(t *testing.T, h *Host, phase string, want uint64, allowNotLeader bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	start := time.Now()
	v, err := h.StaleRead(ctx, noQuorumShard, nil)
	cancel()
	if err != nil {
		t.Fatalf("%s: StaleRead: %v", phase, err)
	}
	if got, ok := v.(uint64); !ok || got != want {
		t.Fatalf("%s: StaleRead = %v, want the applied count %d", phase, v, want)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("%s: StaleRead took %v, want under 1s", phase, d)
	}

	// While the replica must still lead, the SyncRead below reaches the
	// leader's read path: a refusal by ErrNotLeader would prove nothing.
	if !allowNotLeader {
		assertStillLeads(t, h, phase+": before SyncRead")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
	start = time.Now()
	v, err = h.SyncRead(ctx, noQuorumShard, nil)
	cancel()
	if !allowNotLeader {
		assertStillLeads(t, h, phase+": after SyncRead")
	}
	if err == nil {
		t.Fatalf("%s: SyncRead answered %v, want an error", phase, v)
	}
	switch {
	case errors.Is(err, ErrTimeout), errors.Is(err, ErrCanceled):
	case allowNotLeader && errors.Is(err, ErrNotLeader):
	case allowNotLeader:
		t.Fatalf("%s: SyncRead: %v, want ErrTimeout, ErrCanceled or ErrNotLeader", phase, err)
	default:
		t.Fatalf("%s: SyncRead: %v, want ErrTimeout or ErrCanceled from the leader", phase, err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("%s: SyncRead took %v past its 500ms deadline", phase, d)
	}
}

// assertStillLeads fails the test unless h reports itself the leader of
// noQuorumShard.
func assertStillLeads(t *testing.T, h *Host, what string) {
	t.Helper()
	info, err := h.ShardInfo(noQuorumShard)
	if err != nil {
		t.Fatalf("%s: ShardInfo: %v", what, err)
	}
	if !info.IsLeader {
		t.Fatalf("%s: the replica no longer leads", what)
	}
}

func proposeOnLeader(t *testing.T, hosts []*Host, tag string) {
	t.Helper()
	var lastErr error
	if waitUntil(20*time.Second, func() bool {
		for _, h := range hosts {
			info, err := h.ShardInfo(noQuorumShard)
			if err != nil || !info.IsLeader {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, lastErr = h.SyncPropose(ctx, noQuorumShard, []byte(tag))
			cancel()
			if lastErr == nil {
				return true
			}
		}
		return false
	}) {
		return
	}
	t.Fatalf("proposal %q never committed: %v", tag, lastErr)
}
