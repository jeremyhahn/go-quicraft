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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// sealCountingBarrier is a mockBarrier that counts Seal calls and can be told
// to fail them, so the Close tests can assert that every barrier the host was
// given is sealed exactly once and that a seal failure is reported.
type sealCountingBarrier struct {
	*mockBarrier
	seals   atomic.Int64
	sealErr error
}

func newSealCountingBarrier() *sealCountingBarrier {
	return &sealCountingBarrier{mockBarrier: &mockBarrier{}}
}

func (b *sealCountingBarrier) Seal() error {
	b.seals.Add(1)
	if b.sealErr != nil {
		return b.sealErr
	}
	return b.mockBarrier.Seal()
}

// startShardBarrierTestShards starts one single-replica shard per ID.
func startShardBarrierTestShards(t *testing.T, h *Host, shardIDs ...uint64) {
	t.Helper()
	members := map[uint64]string{1: "127.0.0.1:5000"}
	for _, shardID := range shardIDs {
		if err := h.StartShard(members, false, testCreateFunc, testShardConfig(shardID, 1)); err != nil {
			t.Fatalf("StartShard(%d) failed: %v", shardID, err)
		}
	}
}

// TestClose_SealsPerShardBarriers verifies Host.Close seals every barrier the
// WithShardBarriers resolver handed the host, not only the WithBarrier one.
// Leaving them unsealed kept every tenant's key material live after the host
// that used it had shut down.
func TestClose_SealsPerShardBarriers(t *testing.T) {
	cases := []struct {
		name        string
		hostBarrier bool
	}{
		{name: "with host-wide barrier", hostBarrier: true},
		{name: "per-shard only", hostBarrier: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shardOne := newSealCountingBarrier()
			shardTwo := newSealCountingBarrier()
			opts := []HostOption{
				WithLogDB(newMockLogDB()),
				WithoutTransport(),
				WithShardBarriers(func(shardID uint64) Barrier {
					switch shardID {
					case 1:
						return shardOne
					case 2:
						return shardTwo
					}
					return nil
				}),
			}
			var host *sealCountingBarrier
			if tc.hostBarrier {
				host = newSealCountingBarrier()
				opts = append(opts, WithBarrier(host))
			}
			h, err := NewHost(testHostConfig(t), opts...)
			if err != nil {
				t.Fatalf("NewHost failed: %v", err)
			}
			startShardBarrierTestShards(t, h, 1, 2)

			if err := h.Close(); err != nil {
				t.Fatalf("Close failed: %v", err)
			}

			for name, b := range map[string]*sealCountingBarrier{"shard 1": shardOne, "shard 2": shardTwo} {
				if !b.IsSealed() {
					t.Errorf("%s barrier is not sealed after Host.Close()", name)
				}
				if got := b.seals.Load(); got != 1 {
					t.Errorf("%s barrier sealed %d times, want 1", name, got)
				}
			}
			if host != nil && host.seals.Load() != 1 {
				t.Errorf("host-wide barrier sealed %d times, want 1", host.seals.Load())
			}
		})
	}
}

// TestClose_SealsSharedBarrierOnce verifies barriers are deduplicated by
// identity: one barrier shared by two shards, and a resolver that returns the
// host-wide barrier itself for a third shard, are each sealed exactly once.
func TestClose_SealsSharedBarrierOnce(t *testing.T) {
	host := newSealCountingBarrier()
	shared := newSealCountingBarrier()
	h, err := NewHost(testHostConfig(t),
		WithLogDB(newMockLogDB()),
		WithoutTransport(),
		WithBarrier(host),
		WithShardBarriers(func(shardID uint64) Barrier {
			if shardID == 3 {
				return host
			}
			return shared
		}))
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	startShardBarrierTestShards(t, h, 1, 2, 3)

	if err := h.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if got := shared.seals.Load(); got != 1 {
		t.Errorf("barrier shared by shards 1 and 2 sealed %d times, want 1", got)
	}
	if got := host.seals.Load(); got != 1 {
		t.Errorf("host-wide barrier also resolved for shard 3 sealed %d times, want 1", got)
	}
}

// TestClose_SealsBarrierOfShardStoppedBeforeClose verifies the host seals a
// per-shard barrier it was given even when that shard was stopped before
// Close: the barrier was still handed to this host, and nothing else on the
// host's shutdown path would ever seal it.
func TestClose_SealsBarrierOfShardStoppedBeforeClose(t *testing.T) {
	shardBarrier := newSealCountingBarrier()
	h, err := NewHost(testHostConfig(t),
		WithLogDB(newMockLogDB()),
		WithoutTransport(),
		WithShardBarriers(func(uint64) Barrier { return shardBarrier }))
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	startShardBarrierTestShards(t, h, 1)
	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard failed: %v", err)
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if got := shardBarrier.seals.Load(); got != 1 {
		t.Errorf("barrier of a shard stopped before Close sealed %d times, want 1", got)
	}
}

// TestClose_PerShardBarrierSealError verifies a per-shard barrier's seal
// failure is joined into Close's error exactly as the host-wide barrier's is,
// while an already-sealed per-shard barrier is not an error.
func TestClose_PerShardBarrierSealError(t *testing.T) {
	sealErr := errors.New("per-shard seal failure")
	failing := newSealCountingBarrier()
	failing.sealErr = sealErr
	alreadySealed := newSealCountingBarrier()
	alreadySealed.sealed = true
	h, err := NewHost(testHostConfig(t),
		WithLogDB(newMockLogDB()),
		WithoutTransport(),
		WithShardBarriers(func(shardID uint64) Barrier {
			if shardID == 1 {
				return failing
			}
			return alreadySealed
		}))
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	startShardBarrierTestShards(t, h, 1)
	// A resolution is all it takes for the host to have been given a barrier;
	// no shard needs to run under the already-sealed one.
	if _, err := h.barrierForShard(2); err != nil {
		t.Fatalf("barrierForShard(2) failed: %v", err)
	}

	err = h.Close()
	if !errors.Is(err, sealErr) {
		t.Fatalf("Close error = %v, want it to wrap the per-shard seal failure", err)
	}
	if errors.Is(err, ErrBarrierSealed) {
		t.Errorf("Close error = %v; an already-sealed barrier must not be reported", err)
	}
	if got := alreadySealed.seals.Load(); got != 1 {
		t.Errorf("already-sealed barrier Seal called %d times, want 1", got)
	}
}

// valueBarrier is a valid Barrier whose dynamic value is NOT comparable: a
// struct holding a slice. Its methods are promoted from the embedded pointer,
// so every copy seals the same underlying barrier. Using it as a map key or
// comparing two of them with == panics at run time, which is what the host
// must never do with a Barrier the API does not require to be comparable.
type valueBarrier struct {
	*sealCountingBarrier
	labels []string
}

func newValueBarrier(label string) valueBarrier {
	return valueBarrier{sealCountingBarrier: newSealCountingBarrier(), labels: []string{label}}
}

// TestClose_SealsNonComparableValueBarriers verifies StartShard and Close
// accept a per-shard Barrier whose dynamic value is not comparable, and seal
// it. A value copy shared by two shards seals the one underlying barrier, and
// the second Seal's ErrBarrierSealed is not a Close error.
func TestClose_SealsNonComparableValueBarriers(t *testing.T) {
	shared := newValueBarrier("shared")
	own := newValueBarrier("own")
	h, err := NewHost(testHostConfig(t),
		WithLogDB(newMockLogDB()),
		WithoutTransport(),
		WithShardBarriers(func(shardID uint64) Barrier {
			if shardID == 3 {
				return own
			}
			return shared
		}))
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	startShardBarrierTestShards(t, h, 1, 2, 3)

	if err := h.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	for name, b := range map[string]valueBarrier{"shared": shared, "own": own} {
		if !b.IsSealed() {
			t.Errorf("%s value barrier is not sealed after Host.Close()", name)
		}
	}
}

// TestClose_SealsNonComparableHostWideBarrier verifies a host-wide Barrier
// whose dynamic value is not comparable, also returned by the per-shard
// resolver, is sealed by Close without comparing the two interface values.
func TestClose_SealsNonComparableHostWideBarrier(t *testing.T) {
	hostWide := newValueBarrier("host")
	h, err := NewHost(testHostConfig(t),
		WithLogDB(newMockLogDB()),
		WithoutTransport(),
		WithBarrier(hostWide),
		WithShardBarriers(func(uint64) Barrier { return hostWide }))
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	startShardBarrierTestShards(t, h, 1)

	if err := h.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !hostWide.IsSealed() {
		t.Error("host-wide value barrier is not sealed after Host.Close()")
	}
}

// identityKeyedLogDB is an encrypted per-barrier LogDB that keys its answer on
// the barrier handle itself, as a careless implementation would. Handed a
// barrier whose dynamic value is not comparable, its map lookup panics.
type identityKeyedLogDB struct {
	*mockLogDB
	owned   map[any]struct{}
	queried atomic.Int64
}

func (db *identityKeyedLogDB) AtRestProtection() logdb.AtRestProtection {
	return logdb.AtRestBarrierEncrypted
}

func (db *identityKeyedLogDB) MinLiveEpochForBarrier(barrier any) (uint64, bool, error) {
	db.queried.Add(1)
	_, known := db.owned[barrier]
	return 0, known, nil
}

// TestStartShard_NonPointerShardBarrierRefusedBeforeLogDBQuery verifies that a
// per-shard barrier without pointer identity, with no host-wide barrier, is
// refused with ErrShardBarrierLogDBMismatch before the LogDB is asked about
// it: no LogDB can prove it stores the shard under a barrier that has no
// identity, and handing the handle to one that compares it would panic.
func TestStartShard_NonPointerShardBarrierRefusedBeforeLogDBQuery(t *testing.T) {
	shardBarrier := newValueBarrier("tenant")
	db := &identityKeyedLogDB{mockLogDB: newMockLogDB(), owned: map[any]struct{}{}}
	h, err := NewHost(testHostConfig(t),
		WithLogDB(db),
		WithoutTransport(),
		WithShardBarriers(func(uint64) Barrier { return shardBarrier }))
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{1: "127.0.0.1:5000"}
	err = h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1))
	if !errors.Is(err, ErrShardBarrierLogDBMismatch) {
		t.Fatalf("StartShard error = %v, want ErrShardBarrierLogDBMismatch", err)
	}
	if got := db.queried.Load(); got != 0 {
		t.Fatalf("LogDB queried %d times for a barrier without identity, want 0", got)
	}
}

// TestGivenBarriers_RecordsEveryDistinctBarrierOnce verifies the record keeps
// each pointer barrier once however often and for however many shards it is
// resolved, and keeps a barrier a shard resolved to before it re-resolved to
// another: the per-shard shortcut for a repeated resolution must not drop it.
func TestGivenBarriers_RecordsEveryDistinctBarrierOnce(t *testing.T) {
	a, b := newSealCountingBarrier(), newSealCountingBarrier()
	var g givenBarriers
	g.record(1, a)
	g.record(1, a)
	g.record(2, a)
	g.record(1, b)
	g.record(1, a)
	g.record(2, b)

	seen := map[*sealCountingBarrier]int{}
	g.each(func(r Barrier) { seen[r.(*sealCountingBarrier)]++ })
	if len(seen) != 2 || seen[a] != 1 || seen[b] != 1 {
		t.Fatalf("recorded a=%d b=%d (distinct %d), want each once", seen[a], seen[b], len(seen))
	}
}
