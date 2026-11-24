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
	"fmt"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

const (
	witnessPolicyShard = uint64(837)
	witnessPolicyRTTMs = uint64(20)
)

// witnessPolicyCluster is three real Hosts running one shard over mTLS, plus
// spare Hosts whose identities and addresses witness replicas are admitted
// under. A witness is admitted by membership only: it never runs, and the
// three full replicas hold the quorum by themselves for every membership
// this test builds (at most two witnesses, so quorum 3 of 5).
//
// newExecCluster (leaderexec_test.go) never starts its engines, so no config
// change can commit through it; this cluster is built the way
// newFailoverCluster builds its own.
type witnessPolicyCluster struct {
	t       *testing.T
	hosts   []*Host
	spares  []*Host
	cfgs    []config.HostConfig
	ids     map[uint64]string
	members map[uint64]string
}

func newWitnessPolicyCluster(t *testing.T, spares int) *witnessPolicyCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3+spares)
	c := &witnessPolicyCluster{
		t:       t,
		ids:     make(map[uint64]string, 3),
		members: make(map[uint64]string, 3),
	}
	for i := range 3 + spares {
		hcfg := config.HostConfig{
			WALDir:          t.TempDir(),
			NodeHostDir:     t.TempDir(),
			RaftAddress:     "127.0.0.1:0",
			ListenAddress:   "127.0.0.1:0",
			RTTMillisecond:  witnessPolicyRTTMs,
			DeploymentID:    1,
			NoSyncWAL:       true,
			TransportConfig: config.TransportConfig{MTLSConfig: leaves[i]},
		}
		h, err := NewHost(hcfg, WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		if i < 3 {
			c.hosts = append(c.hosts, h)
			c.cfgs = append(c.cfgs, hcfg)
			c.ids[uint64(i+1)] = h.NodeIdentity()
			c.members[uint64(i+1)] = h.RaftAddress()
		} else {
			c.spares = append(c.spares, h)
		}
	}
	t.Cleanup(func() {
		for _, h := range append(c.hosts, c.spares...) {
			if h != nil {
				_ = h.Close()
			}
		}
	})
	for i := range c.hosts {
		c.startShard(i)
	}
	return c
}

func (c *witnessPolicyCluster) startShard(i int) {
	c.t.Helper()
	scfg := testShardConfig(witnessPolicyShard, uint64(i+1))
	scfg.MemberIdentities = c.ids
	if err := c.hosts[i].StartShard(c.members, false, testCreateFunc, scfg); err != nil {
		c.t.Fatalf("StartShard[%d]: %v", i, err)
	}
}

// restart closes replica i's Host and opens a new one on the same
// directories, certificate and address, then restarts the shard on it.
func (c *witnessPolicyCluster) restart(i int) {
	c.t.Helper()
	addr := c.hosts[i].RaftAddress()
	if err := c.hosts[i].Close(); err != nil {
		c.t.Fatalf("closing replica %d: %v", i+1, err)
	}
	c.hosts[i] = nil
	hcfg := c.cfgs[i]
	hcfg.RaftAddress = addr
	hcfg.ListenAddress = addr
	h, err := NewHost(hcfg, WithoutEncryption())
	if err != nil {
		c.t.Fatalf("reopening replica %d: %v", i+1, err)
	}
	c.hosts[i] = h
	c.startShard(i)
}

// leader waits for a replica that reports itself as the shard's leader.
func (c *witnessPolicyCluster) leader() *Host {
	c.t.Helper()
	var leader *Host
	if !waitUntil(30*time.Second, func() bool {
		for i, h := range c.hosts {
			id, ok, err := h.LeaderID(witnessPolicyShard)
			if err == nil && ok && id == uint64(i+1) {
				leader = h
				return true
			}
		}
		return false
	}) {
		c.t.Fatal("the shard never elected a leader")
	}
	return leader
}

// change runs a config change on the leader, retrying while leadership
// moves, and returns the first answer that is not a leadership refusal.
func (c *witnessPolicyCluster) change(fn func(ctx context.Context, h *Host, ccid uint64) error) error {
	c.t.Helper()
	var err error
	for range 10 {
		h := c.leader()
		m, merr := h.ShardMembership(witnessPolicyShard)
		if merr != nil {
			c.t.Fatalf("ShardMembership on the leader: %v", merr)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		err = fn(ctx, h, m.ConfigChangeID)
		cancel()
		var nl *NotLeaderError
		if !errors.As(err, &nl) {
			return err
		}
	}
	return err
}

// policy is one replica's committed witness policy as ShardMembership reports it.
type witnessPolicyView struct {
	max     uint64
	rfrm    bool
	witness int
}

func (v witnessPolicyView) String() string {
	return fmt.Sprintf("MaxWitnesses=%d RequireFullReplicaMajority=%v witnesses=%d", v.max, v.rfrm, v.witness)
}

func (c *witnessPolicyCluster) view(i int) (witnessPolicyView, error) {
	m, err := c.hosts[i].ShardMembership(witnessPolicyShard)
	if err != nil {
		return witnessPolicyView{}, err
	}
	return witnessPolicyView{max: m.MaxWitnesses, rfrm: m.RequireFullReplicaMajority, witness: len(m.Witnesses)}, nil
}

// expectEverywhere waits until every replica reports want.
func (c *witnessPolicyCluster) expectEverywhere(step string, want witnessPolicyView) {
	c.t.Helper()
	if waitUntil(15*time.Second, func() bool {
		for i := range c.hosts {
			if v, err := c.view(i); err != nil || v != want {
				return false
			}
		}
		return true
	}) {
		return
	}
	for i := range c.hosts {
		v, err := c.view(i)
		c.t.Errorf("%s: replica %d reports %v (err %v), want %v", step, i+1, v, err, want)
	}
	c.t.FailNow()
}

func (c *witnessPolicyCluster) addWitness(spare int, replicaID uint64) error {
	s := c.spares[spare]
	return c.change(func(ctx context.Context, h *Host, ccid uint64) error {
		return h.SyncRequestAddWitnessWithIdentity(ctx, witnessPolicyShard, replicaID,
			s.RaftAddress(), s.NodeIdentity(), ccid)
	})
}

func (c *witnessPolicyCluster) updatePolicy(max int, rfrm bool) error {
	return c.change(func(ctx context.Context, h *Host, ccid uint64) error {
		return h.SyncRequestUpdateWitnessPolicy(ctx, witnessPolicyShard, config.WitnessConfig{
			MaxWitnesses:               max,
			RequireFullReplicaMajority: config.BoolPtr(rfrm),
		}, ccid)
	})
}

// TestWitnessPolicy_UpdateReplicatesSurvivesRestartAndRefusesAViolatedPolicy
// checks docs/membership-changes (Change the witness policy): the policy is
// replicated state every replica reports through ShardMembership, a witness
// add the policy forbids and a tightening the membership already violates are
// refused with ErrTooManyWitnesses, a stale configChangeIndex is refused, and
// a restarted replica recovers the policy.
func TestWitnessPolicy_UpdateReplicatesSurvivesRestartAndRefusesAViolatedPolicy(t *testing.T) {
	c := newWitnessPolicyCluster(t, 2)

	// (a) The update is adopted by every replica.
	if err := c.updatePolicy(1, false); err != nil {
		t.Fatalf("(a) SyncRequestUpdateWitnessPolicy(MaxWitnesses=1): %v", err)
	}
	c.expectEverywhere("(a) after the update", witnessPolicyView{max: 1})

	// (b) One witness fits the cap; a second is refused.
	if err := c.addWitness(0, 4); err != nil {
		t.Fatalf("(b) adding the first witness under MaxWitnesses=1: %v", err)
	}
	c.expectEverywhere("(b) after the first witness", witnessPolicyView{max: 1, witness: 1})
	if err := c.addWitness(1, 5); !errors.Is(err, ErrTooManyWitnesses) {
		t.Fatalf("(b) adding a second witness under MaxWitnesses=1: got %v, want ErrTooManyWitnesses", err)
	}
	c.expectEverywhere("(b) after the refused witness", witnessPolicyView{max: 1, witness: 1})

	// (c) Raise the cap, admit the second witness, then tighten below the
	// current witness count: refused, and the policy stays as it was.
	if err := c.updatePolicy(2, false); err != nil {
		t.Fatalf("(c) SyncRequestUpdateWitnessPolicy(MaxWitnesses=2): %v", err)
	}
	if err := c.addWitness(1, 5); err != nil {
		t.Fatalf("(c) adding the second witness under MaxWitnesses=2: %v", err)
	}
	c.expectEverywhere("(c) with two witnesses", witnessPolicyView{max: 2, witness: 2})
	if err := c.updatePolicy(1, false); !errors.Is(err, ErrTooManyWitnesses) {
		t.Fatalf("(c) tightening to MaxWitnesses=1 with two witnesses: got %v, want ErrTooManyWitnesses", err)
	}
	c.expectEverywhere("(c) after the refused tightening", witnessPolicyView{max: 2, witness: 2})

	// (d) A configChangeIndex read before the last change is refused.
	stale := uint64(0)
	err := c.change(func(ctx context.Context, h *Host, ccid uint64) error {
		if stale == 0 {
			stale = ccid
			if err := h.SyncRequestUpdateWitnessPolicy(ctx, witnessPolicyShard,
				config.WitnessConfig{MaxWitnesses: 3, RequireFullReplicaMajority: config.BoolPtr(false)}, ccid); err != nil {
				return fmt.Errorf("the update that moves the index: %w", err)
			}
		}
		return h.SyncRequestUpdateWitnessPolicy(ctx, witnessPolicyShard,
			config.WitnessConfig{MaxWitnesses: 2, RequireFullReplicaMajority: config.BoolPtr(true)}, stale)
	})
	var sce *StaleConfigChangeError
	if !errors.As(err, &sce) {
		t.Fatalf("(d) an update with the configChangeIndex %d from before the last change: got %v, want *StaleConfigChangeError", stale, err)
	}
	c.expectEverywhere("(d) after the stale update", witnessPolicyView{max: 3, witness: 2})

	// (e) Restart a replica: it reports the committed policy.
	if err := c.updatePolicy(2, true); err != nil {
		t.Fatalf("(e) SyncRequestUpdateWitnessPolicy(MaxWitnesses=2, RequireFullReplicaMajority): %v", err)
	}
	want := witnessPolicyView{max: 2, rfrm: true, witness: 2}
	c.expectEverywhere("(e) before the restart", want)
	for _, i := range []int{0, 1, 2} {
		c.restart(i)
		c.expectEverywhere(fmt.Sprintf("(e) after restarting replica %d", i+1), want)
	}
}
