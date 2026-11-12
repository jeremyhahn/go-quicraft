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
	transferShard      = 811
	transferRTTMillis  = 10
	transferProposers  = 8
	transferProposeTTL = 2 * time.Second
)

// transferCluster is three in-process hosts, each running one replica of a
// KV shard over mTLS QUIC on loopback. hosts[i] runs replica i+1.
type transferCluster struct {
	t     *testing.T
	hosts []*Host
}

func newTransferCluster(t *testing.T) *transferCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	c := &transferCluster{t: t}
	t.Cleanup(func() {
		for _, h := range c.hosts {
			h.Close()
		}
	})
	members := make(map[uint64]string, 3)
	ids := make(map[uint64]string, 3)
	for i := range 3 {
		h, err := NewHost(config.HostConfig{
			WALDir:          t.TempDir(),
			NodeHostDir:     t.TempDir(),
			RaftAddress:     "127.0.0.1:0",
			ListenAddress:   "127.0.0.1:0",
			RTTMillisecond:  transferRTTMillis,
			DeploymentID:    1,
			NoSyncWAL:       true,
			TransportConfig: config.TransportConfig{MTLSConfig: leaves[i]},
		}, WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		c.hosts = append(c.hosts, h)
		members[uint64(i+1)] = h.RaftAddress()
		ids[uint64(i+1)] = h.NodeIdentity()
	}
	for i, h := range c.hosts {
		scfg := testShardConfig(transferShard, uint64(i+1))
		scfg.MemberIdentities = ids
		if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(), scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	return c
}

// electionTimeout is the shard's election timeout in wall-clock time.
func (c *transferCluster) electionTimeout() time.Duration {
	scfg := testShardConfig(transferShard, 1)
	return time.Duration(scfg.ElectionRTT) * transferRTTMillis * time.Millisecond
}

// leader is the leader and term host i reports, or 0 while it knows none.
func (c *transferCluster) leader(i int) (uint64, uint64) {
	id, term, ready, err := c.hosts[i].LeaderIDAndTerm(transferShard)
	if err != nil || !ready {
		return 0, 0
	}
	return id, term
}

// agreedLeader waits until every host names the same leader at the same
// term, and returns them.
func (c *transferCluster) agreedLeader(timeout time.Duration) (uint64, uint64) {
	c.t.Helper()
	var leaderID, term uint64
	if !waitUntil(timeout, func() bool {
		leaderID, term = c.leader(0)
		if leaderID == 0 {
			return false
		}
		for i := 1; i < len(c.hosts); i++ {
			if id, tm := c.leader(i); id != leaderID || tm != term {
				return false
			}
		}
		return true
	}) {
		c.t.Fatalf("replicas never agreed on a leader")
	}
	return leaderID, term
}

// transferProposalErrors are the errors a proposal may fail with while
// leadership moves. Anything else is a defect.
var transferProposalErrors = []error{ErrTransferInProgress, ErrNotLeader, ErrDropped, ErrTimeout}

func isTransferProposalError(err error) bool {
	for _, want := range transferProposalErrors {
		if errors.Is(err, want) {
			return true
		}
	}
	return false
}

// proposeUntil runs one proposer: distinct Puts through whichever replica it
// believes leads, until stop closes. It returns the acknowledged keys and
// every error outside transferProposalErrors.
func (c *transferCluster) proposeUntil(g int, stop <-chan struct{}) (acked []string, bad []error) {
	target := 0
	for n := 0; ; n++ {
		select {
		case <-stop:
			return acked, bad
		default:
		}
		key := fmt.Sprintf("g%d-%d", g, n)
		cmd, err := kv.EncodePut([]byte(key), []byte(key))
		if err != nil {
			bad = append(bad, err)
			return acked, bad
		}
		ctx, cancel := context.WithTimeout(context.Background(), transferProposeTTL)
		_, err = c.hosts[target].SyncPropose(ctx, transferShard, cmd)
		cancel()
		if err == nil {
			acked = append(acked, key)
			continue
		}
		if !isTransferProposalError(err) {
			bad = append(bad, err)
		}
		if id, _ := c.leader(target); id != 0 {
			target = int(id - 1)
		}
		time.Sleep(time.Millisecond)
	}
}

// transferTo moves leadership to target and waits, at most ten election
// timeouts, until target reports itself leader at a term above fromTerm.
func (c *transferCluster) transferTo(target, fromTerm uint64) uint64 {
	c.t.Helper()
	deadline := 10 * c.electionTimeout()
	start := time.Now()
	requested := false
	var lastErr error
	for time.Since(start) < deadline {
		if !requested {
			if leaderID, _ := c.leader(int(target - 1)); leaderID != 0 && leaderID != target {
				ctx, cancel := context.WithTimeout(context.Background(), deadline)
				lastErr = c.hosts[leaderID-1].RequestLeaderTransfer(ctx, transferShard, target)
				cancel()
				requested = lastErr == nil
			}
		}
		if id, term := c.leader(int(target - 1)); id == target && term > fromTerm {
			return term
		}
		time.Sleep(time.Millisecond)
	}
	id, term := c.leader(int(target - 1))
	c.t.Fatalf("transfer to %d: after %v replica %d reports leader %d term %d (from term %d, requested %v, last error %v)",
		target, deadline, target, id, term, fromTerm, requested, lastErr)
	return 0
}

// converged waits until every replica has applied the same index with the
// same state machine hash.
func (c *transferCluster) converged(timeout time.Duration) {
	c.t.Helper()
	var detail string
	if waitUntil(timeout, func() bool {
		var hash0, index0 uint64
		for i, h := range c.hosts {
			hash, index, ok, err := h.StateMachineHash(transferShard)
			if err != nil || !ok {
				detail = fmt.Sprintf("replica %d: ok %v err %v", i+1, ok, err)
				return false
			}
			if i == 0 {
				hash0, index0 = hash, index
				continue
			}
			if index != index0 || hash != hash0 {
				detail = fmt.Sprintf("replica 1 at %d hash %x, replica %d at %d hash %x", index0, hash0, i+1, index, hash)
				return false
			}
		}
		return true
	}) {
		return
	}
	c.t.Fatalf("replicas never converged: %s", detail)
}

// TestLeaderTransfer_MovesLeadershipAndKeepsEveryAcknowledgedWrite moves
// leadership to each follower in turn while eight goroutines propose. Every
// transfer must land on its target at a higher term within ten election
// timeouts, every failed proposal must fail with a documented error, and
// every acknowledged proposal must be in the final state on every replica.
func TestLeaderTransfer_MovesLeadershipAndKeepsEveryAcknowledgedWrite(t *testing.T) {
	c := newTransferCluster(t)
	first, term := c.agreedLeader(20 * time.Second)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	acked := make([][]string, transferProposers)
	bad := make([][]error, transferProposers)
	for g := range transferProposers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			acked[g], bad[g] = c.proposeUntil(g, stop)
		}()
	}
	stopped := false
	stopLoad := func() {
		if !stopped {
			stopped = true
			close(stop)
			wg.Wait()
		}
	}
	defer stopLoad()

	// Each follower of the first leader in turn, then the first leader
	// again: every target is a follower when its transfer is requested.
	var targets []uint64
	for id := uint64(1); id <= 3; id++ {
		if id != first {
			targets = append(targets, id)
		}
	}
	targets = append(targets, first)
	for _, target := range targets {
		time.Sleep(5 * c.electionTimeout())
		term = c.transferTo(target, term)
	}
	time.Sleep(5 * c.electionTimeout())
	stopLoad()

	total := 0
	for g := range transferProposers {
		for _, err := range bad[g] {
			t.Errorf("proposer %d: undocumented proposal error %T: %v", g, err, err)
		}
		total += len(acked[g])
	}
	if total == 0 {
		t.Fatal("no proposal was acknowledged")
	}
	t.Logf("%d transfers, %d acknowledged proposals", len(targets), total)

	c.converged(20 * time.Second)
	for i, h := range c.hosts {
		missing := 0
		for g := range transferProposers {
			for _, key := range acked[g] {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				v, err := h.StaleRead(ctx, transferShard, []byte(key))
				cancel()
				if b, ok := v.([]byte); err != nil || !ok || string(b) != key {
					if missing < 5 {
						t.Errorf("replica %d: acknowledged key %q: value %v, error %v", i+1, key, v, err)
					}
					missing++
				}
			}
		}
		if missing > 0 {
			t.Errorf("replica %d: %d of %d acknowledged keys missing", i+1, missing, total)
		}
	}
}
