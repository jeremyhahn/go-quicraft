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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

const proposeMarkersShard uint64 = 1104

// indexRecordingStore records the highest entry index its Update applied.
type indexRecordingStore struct {
	*kv.ConcurrentStore
	applied *atomic.Uint64
}

func (s *indexRecordingStore) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	if err := s.ConcurrentStore.Update(ctx, entries, results); err != nil {
		return err
	}
	if n := len(entries); n > 0 {
		s.applied.Store(entries[n-1].Index)
	}
	return nil
}

// markerProbe runs on the apply worker right after a proposal's caller has
// been completed (ObserveProposalLatency follows rs.complete). At that
// moment the caller can already act on the acknowledgement, so the host's
// published commit index and applied index must cover the entry.
type markerProbe struct {
	NoOpMetricsCollector
	host    atomic.Pointer[Host]
	applied *atomic.Uint64

	mu       sync.Mutex
	observed int
	stale    []string
}

func (p *markerProbe) ObserveProposalLatency(shardID uint64, _ time.Duration) {
	h := p.host.Load()
	if h == nil || shardID != proposeMarkersShard {
		return
	}
	entry := p.applied.Load()
	var applied, committed uint64
	if ns, err := h.getNode(shardID); err == nil {
		applied = ns.lastApplied.Load()
	}
	if n := h.engine.GetNode(shardID); n != nil {
		committed = n.CommittedIndex()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observed++
	if applied < entry || committed < entry {
		p.stale = append(p.stale, fmt.Sprintf("entry %d acknowledged with applied=%d committed=%d", entry, applied, committed))
	}
}

// TestSyncPropose_AcknowledgedEntryIsCoveredByLeaderMarkers proves that a
// proposal is acknowledged only once the leader's applied index and its
// published commit index cover the entry (#1104). A FollowerRead or
// ReadIndex taken right after the acknowledgement relies on both.
func TestSyncPropose_AcknowledgedEntryIsCoveredByLeaderMarkers(t *testing.T) {
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	var hosts []*Host
	var probes []*markerProbe
	t.Cleanup(func() {
		for _, h := range hosts {
			h.Close()
		}
	})
	addrs := make(map[uint64]string, 3)
	ids := make(map[uint64]string, 3)
	for i := range 3 {
		probe := &markerProbe{applied: &atomic.Uint64{}}
		h, err := NewHost(config.HostConfig{
			WALDir:          t.TempDir(),
			NodeHostDir:     t.TempDir(),
			RaftAddress:     "127.0.0.1:0",
			ListenAddress:   "127.0.0.1:0",
			RTTMillisecond:  5,
			DeploymentID:    1,
			NoSyncWAL:       true,
			TransportConfig: config.TransportConfig{MTLSConfig: leaves[i]},
		}, WithoutEncryption(), WithMetrics(probe))
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		probe.host.Store(h)
		hosts = append(hosts, h)
		probes = append(probes, probe)
		addrs[uint64(i+1)] = h.RaftAddress()
		ids[uint64(i+1)] = h.NodeIdentity()
	}
	for i, h := range hosts {
		scfg := testShardConfig(proposeMarkersShard, uint64(i+1))
		scfg.MemberIdentities = ids
		applied := probes[i].applied
		create := sm.NewConcurrentCreateFunc(func(_, _ uint64) sm.ConcurrentStateMachine {
			return &indexRecordingStore{ConcurrentStore: kv.NewConcurrentStore(), applied: applied}
		})
		if err := h.StartShard(addrs, false, create, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}

	const writes = 50
	acked := 0
	deadline := time.Now().Add(60 * time.Second)
	for acked < writes && time.Now().Before(deadline) {
		li := -1
		for i, h := range hosts {
			if info, err := h.ShardInfo(proposeMarkersShard); err == nil && info.IsLeader {
				li = i
				break
			}
		}
		if li < 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		cmd, err := kv.EncodePut([]byte(fmt.Sprintf("k%02d", acked)), []byte("v"))
		if err != nil {
			t.Fatalf("EncodePut: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err = hosts[li].SyncPropose(ctx, proposeMarkersShard, cmd)
		cancel()
		if err == nil {
			acked++
		}
	}
	if acked < writes {
		t.Fatalf("only %d of %d writes acknowledged in 60s", acked, writes)
	}

	observed := 0
	for i, p := range probes {
		p.mu.Lock()
		observed += p.observed
		for _, s := range p.stale {
			t.Errorf("replica %d: %s", i+1, s)
		}
		p.mu.Unlock()
	}
	if observed < writes {
		t.Fatalf("probe observed %d acknowledgements, want at least %d", observed, writes)
	}
}
