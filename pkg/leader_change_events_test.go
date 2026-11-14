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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

const leaderEventsShard = 953

// leaderEventRecorder records every leader-change event one Host's
// EventListener receives for leaderEventsShard.
type leaderEventRecorder struct {
	mu       sync.Mutex
	updated  []config.LeaderInfo
	lost     []config.EventShardInfo
	campaign []config.EventShardInfo
}

func (r *leaderEventRecorder) listener() *config.EventListener {
	return &config.EventListener{
		OnLeaderUpdated: func(info config.LeaderInfo) {
			if info.ShardID != leaderEventsShard {
				return
			}
			r.mu.Lock()
			r.updated = append(r.updated, info)
			r.mu.Unlock()
		},
		OnLeaderLost: func(info config.EventShardInfo) {
			if info.ShardID != leaderEventsShard {
				return
			}
			r.mu.Lock()
			r.lost = append(r.lost, info)
			r.mu.Unlock()
		},
		OnCampaignLaunched: func(info config.EventShardInfo) {
			if info.ShardID != leaderEventsShard {
				return
			}
			r.mu.Lock()
			r.campaign = append(r.campaign, info)
			r.mu.Unlock()
		},
	}
}

// lastUpdate returns the most recent OnLeaderUpdated, ok false when none.
func (r *leaderEventRecorder) lastUpdate() (config.LeaderInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.updated) == 0 {
		return config.LeaderInfo{}, false
	}
	return r.updated[len(r.updated)-1], true
}

// counts returns how many OnLeaderLost and OnCampaignLaunched events have
// been recorded so far.
func (r *leaderEventRecorder) counts() (lost, campaign int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.lost), len(r.campaign)
}

// since returns copies of the OnLeaderLost and OnCampaignLaunched events
// recorded after the first lost and campaign events of each kind.
func (r *leaderEventRecorder) since(lost, campaign int) ([]config.EventShardInfo, []config.EventShardInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]config.EventShardInfo(nil), r.lost[lost:]...),
		append([]config.EventShardInfo(nil), r.campaign[campaign:]...)
}

// leaderEventsCluster is three Hosts over mTLS, Host i running replica i+1
// of leaderEventsShard, each with its own event recorder.
type leaderEventsCluster struct {
	hosts []*Host
	recs  []*leaderEventRecorder
}

func newLeaderEventsCluster(t *testing.T) *leaderEventsCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	c := &leaderEventsCluster{}
	t.Cleanup(func() {
		for _, h := range c.hosts {
			h.Close()
		}
	})
	members := make(map[uint64]string, 3)
	ids := make(map[uint64]string, 3)
	for i := range 3 {
		rec := &leaderEventRecorder{}
		h, err := NewHost(config.HostConfig{
			WALDir:          t.TempDir(),
			NodeHostDir:     t.TempDir(),
			RaftAddress:     "127.0.0.1:0",
			ListenAddress:   "127.0.0.1:0",
			RTTMillisecond:  transferRTTMillis,
			DeploymentID:    1,
			NoSyncWAL:       true,
			EventListener:   rec.listener(),
			TransportConfig: config.TransportConfig{MTLSConfig: leaves[i]},
		}, WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		c.hosts = append(c.hosts, h)
		c.recs = append(c.recs, rec)
		members[uint64(i+1)] = h.RaftAddress()
		ids[uint64(i+1)] = h.NodeIdentity()
	}
	for i, h := range c.hosts {
		scfg := testShardConfig(leaderEventsShard, uint64(i+1))
		scfg.MemberIdentities = ids
		if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(), scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	return c
}

func (c *leaderEventsCluster) electionTimeout() time.Duration {
	scfg := testShardConfig(leaderEventsShard, 1)
	return time.Duration(scfg.ElectionRTT) * transferRTTMillis * time.Millisecond
}

// awaitLeaderEvents polls cond until it holds or ctx ends, failing the test with what
// and the last detail cond reported.
func awaitLeaderEvents(ctx context.Context, t *testing.T, what string, cond func() (bool, string)) {
	t.Helper()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	var detail string
	for {
		var ok bool
		if ok, detail = cond(); ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v: %s", what, ctx.Err(), detail)
		case <-tick.C:
		}
	}
}

// agreedLeader waits until every Host reports the same ready leader and
// term for the shard, accepted by accept, and returns them.
func (c *leaderEventsCluster) agreedLeader(ctx context.Context, t *testing.T, accept func(id, term uint64) bool) (uint64, uint64) {
	t.Helper()
	var leaderID, term uint64
	awaitLeaderEvents(ctx, t, "replicas never agreed on a leader", func() (bool, string) {
		leaderID, term = 0, 0
		for i, h := range c.hosts {
			id, tm, ready, err := h.LeaderIDAndTerm(leaderEventsShard)
			if err != nil || !ready || id == 0 {
				return false, fmt.Sprintf("replica %d: leader %d term %d ready %v err %v", i+1, id, tm, ready, err)
			}
			if i == 0 {
				leaderID, term = id, tm
				continue
			}
			if id != leaderID || tm != term {
				return false, fmt.Sprintf("replica 1 leader %d term %d, replica %d leader %d term %d", leaderID, term, i+1, id, tm)
			}
		}
		if !accept(leaderID, term) {
			return false, fmt.Sprintf("agreed leader %d term %d not accepted", leaderID, term)
		}
		return true, ""
	})
	return leaderID, term
}

// awaitLastUpdate waits until every Host's most recent OnLeaderUpdated
// names leaderID at a term accepted by termOK, and returns that term.
func (c *leaderEventsCluster) awaitLastUpdate(ctx context.Context, t *testing.T, leaderID uint64, termOK func(uint64) bool) uint64 {
	t.Helper()
	var term uint64
	awaitLeaderEvents(ctx, t, fmt.Sprintf("most recent OnLeaderUpdated never named leader %d on every Host", leaderID), func() (bool, string) {
		for i, rec := range c.recs {
			info, ok := rec.lastUpdate()
			if !ok || info.LeaderID != leaderID || !termOK(info.Term) {
				return false, fmt.Sprintf("Host %d: last update %+v (any %v)", i+1, info, ok)
			}
			if info.ReplicaID != uint64(i+1) {
				return false, fmt.Sprintf("Host %d: last update reports replica %d", i+1, info.ReplicaID)
			}
			term = info.Term
		}
		return true, ""
	})
	return term
}

// TestLeaderChangeEvents_TransferFiresDocumentedEvents moves leadership on
// a three-Host cluster with RequestLeaderTransfer and checks the events each
// Host's EventListener receives against their documentation.
func TestLeaderChangeEvents_TransferFiresDocumentedEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c := newLeaderEventsCluster(t)
	et := c.electionTimeout()

	// (a) Once the cluster agrees on L at T, every Host's most recent
	// OnLeaderUpdated names L and T.
	leaderID, fromTerm := c.agreedLeader(ctx, t, func(_, _ uint64) bool { return true })
	c.awaitLastUpdate(ctx, t, leaderID, func(tm uint64) bool { return tm == fromTerm })

	baseLost := make([]int, len(c.recs))
	baseCampaign := make([]int, len(c.recs))
	for i, rec := range c.recs {
		baseLost[i], baseCampaign[i] = rec.counts()
	}

	// (b) Transfer to the follower after L.
	target := leaderID%uint64(len(c.hosts)) + 1
	tctx, tcancel := context.WithTimeout(ctx, 10*et)
	err := c.hosts[leaderID-1].RequestLeaderTransfer(tctx, leaderEventsShard, target)
	tcancel()
	if err != nil {
		t.Fatalf("RequestLeaderTransfer from %d to %d at term %d: %v", leaderID, target, fromTerm, err)
	}
	_, newTerm := c.agreedLeader(ctx, t, func(id, tm uint64) bool { return id == target && tm > fromTerm })
	c.awaitLastUpdate(ctx, t, target, func(tm uint64) bool { return tm > fromTerm })

	oldRec := c.recs[leaderID-1]
	awaitLeaderEvents(ctx, t, fmt.Sprintf("Host %d never fired OnLeaderLost", leaderID), func() (bool, string) {
		lost, _ := oldRec.counts()
		return lost > baseLost[leaderID-1], fmt.Sprintf("%d OnLeaderLost", lost-baseLost[leaderID-1])
	})
	tgtRec := c.recs[target-1]
	awaitLeaderEvents(ctx, t, fmt.Sprintf("Host %d never fired OnCampaignLaunched above term %d", target, fromTerm), func() (bool, string) {
		_, campaigns := tgtRec.since(baseLost[target-1], baseCampaign[target-1])
		for _, e := range campaigns {
			if e.ReplicaID == target && e.Term > fromTerm {
				return true, ""
			}
		}
		return false, fmt.Sprintf("campaigns since transfer %+v", campaigns)
	})

	// (d) A proposal after the transfer commits.
	cmd, err := kv.EncodePut([]byte("after-transfer"), []byte("v"))
	if err != nil {
		t.Fatalf("EncodePut: %v", err)
	}
	pctx, pcancel := context.WithTimeout(ctx, 10*et)
	_, err = c.hosts[target-1].SyncPropose(pctx, leaderEventsShard, cmd)
	pcancel()
	if err != nil {
		t.Fatalf("SyncPropose on new leader %d: %v", target, err)
	}

	// (d) The cluster stays idle for two election timeouts: no further
	// OnLeaderLost or OnCampaignLaunched may fire on any Host.
	midLost := make([]int, len(c.recs))
	midCampaign := make([]int, len(c.recs))
	for i, rec := range c.recs {
		midLost[i], midCampaign[i] = rec.counts()
	}
	idle, icancel := context.WithTimeout(ctx, 2*et)
	tick := time.NewTicker(5 * time.Millisecond)
	for quiet := true; quiet; {
		select {
		case <-idle.Done():
			quiet = false
		case <-tick.C:
			for i, rec := range c.recs {
				if lost, camp := rec.counts(); lost != midLost[i] || camp != midCampaign[i] {
					l, cp := rec.since(midLost[i], midCampaign[i])
					t.Errorf("Host %d while idle: OnLeaderLost %+v, OnCampaignLaunched %+v", i+1, l, cp)
					quiet = false
				}
			}
		}
	}
	tick.Stop()
	icancel()

	// (b) L's Host fired OnLeaderLost exactly once for the change, with its
	// own replica ID; (c) no other Host fired OnLeaderLost.
	for i, rec := range c.recs {
		lost, _ := rec.since(baseLost[i], baseCampaign[i])
		if uint64(i+1) == leaderID {
			if len(lost) != 1 {
				t.Errorf("old leader Host %d: %d OnLeaderLost events %+v, want exactly 1", i+1, len(lost), lost)
				continue
			}
			if lost[0].ReplicaID != leaderID {
				t.Errorf("old leader Host %d: OnLeaderLost reports replica %d, want %d", i+1, lost[0].ReplicaID, leaderID)
			}
			continue
		}
		if len(lost) != 0 {
			t.Errorf("Host %d was not the leader but fired OnLeaderLost %+v", i+1, lost)
		}
	}

	// The leader is still F at the transfer's term on every Host.
	if id, tm := c.agreedLeader(ctx, t, func(_, _ uint64) bool { return true }); id != target || tm != newTerm {
		t.Errorf("after idle: leader %d term %d, want %d term %d", id, tm, target, newTerm)
	}
	t.Logf("leader %d term %d -> leader %d term %d", leaderID, fromTerm, target, newTerm)
}
