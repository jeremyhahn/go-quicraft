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
	"fmt"
	"hash/fnv"
	"io"
	"slices"
	"sync"
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
}

func newRecordingStateMachine() *recordingStateMachine {
	return &recordingStateMachine{applied: make(map[string]int)}
}

func (r *recordingStateMachine) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range entries {
		r.applied[string(entries[i].Cmd)]++
		results[i] = sm.Result{Value: entries[i].Index}
	}
	return nil
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

// TestConcurrentPropose_EveryAcknowledgedEntryAppliedExactlyOnce drives 64
// goroutines of 50 SyncPropose calls each, with unique keys, at the leader of
// a three-replica shard on loopback QUIC. Every acknowledged key must be
// applied exactly once on every replica, no other key may be applied, and the
// replicas' state hashes must agree.
//
// TestMultipleConcurrentProposals_Complete (host_test.go) covers only ten
// concurrent proposals on one replica and checks that each returns.
// newConcurrentProposeHosts starts three hosts on loopback QUIC with leaves
// from one CA and returns them with the shard's members and identities.
func newConcurrentProposeHosts(t *testing.T) ([]*Host, map[uint64]string, map[uint64]string) {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	hosts := make([]*Host, 0, 3)
	members := make(map[uint64]string, 3)
	ids := make(map[uint64]string, 3)
	t.Cleanup(func() {
		for _, h := range hosts {
			h.Close()
		}
	})
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
		hosts = append(hosts, h)
		members[uint64(i+1)] = h.RaftAddress()
		ids[uint64(i+1)] = h.NodeIdentity()
	}
	return hosts, members, ids
}

func TestConcurrentPropose_EveryAcknowledgedEntryAppliedExactlyOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("three-host cluster over loopback QUIC")
	}
	const (
		goroutines = 64
		perG       = 50
	)

	hosts, members, ids := newConcurrentProposeHosts(t)
	var smMu sync.Mutex
	machines := make(map[uint64]*recordingStateMachine, 3)
	create := func(_, replicaID uint64) interface{} {
		r := newRecordingStateMachine()
		smMu.Lock()
		machines[replicaID] = r
		smMu.Unlock()
		return r
	}
	for i, h := range hosts {
		scfg := testShardConfig(concurrentProposeShard, uint64(i+1))
		scfg.MemberIdentities = ids
		if err := h.StartShard(members, false, create, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}

	var leader *Host
	if !waitUntil(20*time.Second, func() bool {
		for _, n := range hosts {
			if info, err := n.ShardInfo(concurrentProposeShard); err == nil && info.IsLeader {
				leader = n
				return true
			}
		}
		return false
	}) {
		t.Fatal("no leader elected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var (
		ackMu  sync.Mutex
		acked  = make(map[string]struct{}, goroutines*perG)
		failed = make(map[string]error)
		wg     sync.WaitGroup
	)
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perG {
				key := fmt.Sprintf("g%02d-k%02d", g, i)
				_, err := leader.SyncPropose(ctx, concurrentProposeShard, []byte(key))
				ackMu.Lock()
				if err != nil {
					failed[key] = err
				} else {
					acked[key] = struct{}{}
				}
				ackMu.Unlock()
			}
		}()
	}
	wg.Wait()

	for key, err := range failed {
		t.Errorf("SyncPropose(%s): %v", key, err)
	}
	if len(acked) == 0 {
		t.Fatal("no proposal was acknowledged")
	}

	// Every replica must reach the leader's applied index before its record
	// is read; a follower may trail the acknowledgements.
	_, want, _, err := leader.StateMachineHash(concurrentProposeShard)
	if err != nil {
		t.Fatalf("leader StateMachineHash: %v", err)
	}
	if !waitUntil(30*time.Second, func() bool {
		for _, n := range hosts {
			_, idx, _, err := n.StateMachineHash(concurrentProposeShard)
			if err != nil || idx < want {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("replicas never reached applied index %d", want)
	}

	smMu.Lock()
	defer smMu.Unlock()
	if len(machines) != 3 {
		t.Fatalf("%d state machines were created, want 3", len(machines))
	}
	for id, m := range machines {
		applied := m.snapshot()
		for key := range acked {
			if n := applied[key]; n != 1 {
				t.Errorf("replica %d applied acknowledged key %s %d times, want 1", id, key, n)
			}
		}
		for key, n := range applied {
			if _, ok := acked[key]; !ok {
				t.Errorf("replica %d applied unacknowledged key %s (%d times)", id, key, n)
			}
		}
	}

	var hashes []uint64
	for i, n := range hosts {
		hash, idx, ok, err := n.StateMachineHash(concurrentProposeShard)
		if err != nil || !ok {
			t.Fatalf("replica %d StateMachineHash: ok=%v err=%v", i+1, ok, err)
		}
		if idx != want {
			t.Fatalf("replica %d at applied index %d, want %d", i+1, idx, want)
		}
		hashes = append(hashes, hash)
	}
	if hashes[0] != hashes[1] || hashes[0] != hashes[2] {
		t.Fatalf("replica state hashes differ at index %d: %v", want, hashes)
	}
}
