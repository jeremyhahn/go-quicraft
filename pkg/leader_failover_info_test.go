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
	"fmt"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

const (
	failoverShard = uint64(1)
	// failoverRTTMillisecond is the tick length. With the default ElectionRTT
	// it makes one election timeout 200ms, which leaves room for the race
	// detector inside the 10-timeout bound the test asserts.
	failoverRTTMillisecond = uint64(20)
)

// failoverView is what one replica reports about the shard's leadership,
// read through both public accessors.
type failoverView struct {
	leaderID, term         uint64
	known                  bool
	infoLeader, infoTerm   uint64
	infoIsLeader, readable bool
}

func (v failoverView) String() string {
	return fmt.Sprintf("LeaderIDAndTerm=(%d,%d,%v) ShardInfo=(%d,%d,leader=%v) readable=%v",
		v.leaderID, v.term, v.known, v.infoLeader, v.infoTerm, v.infoIsLeader, v.readable)
}

// failoverCluster is an in-process three-replica shard over loopback mTLS.
type failoverCluster struct {
	t     *testing.T
	hosts []*Host
}

func newFailoverCluster(t *testing.T) *failoverCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	c := &failoverCluster{t: t}
	ids := make(map[uint64]string, 3)
	members := make(map[uint64]string, 3)
	for i := range 3 {
		h, err := NewHost(config.HostConfig{
			WALDir:          t.TempDir(),
			NodeHostDir:     t.TempDir(),
			RaftAddress:     "127.0.0.1:0",
			ListenAddress:   "127.0.0.1:0",
			RTTMillisecond:  failoverRTTMillisecond,
			DeploymentID:    1,
			NoSyncWAL:       true,
			TransportConfig: config.TransportConfig{MTLSConfig: leaves[i]},
		}, WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		c.hosts = append(c.hosts, h)
		ids[uint64(i+1)] = h.NodeIdentity()
		members[uint64(i+1)] = h.RaftAddress()
	}
	t.Cleanup(func() {
		for _, h := range c.hosts {
			if h != nil {
				_ = h.Close()
			}
		}
	})
	for i, h := range c.hosts {
		scfg := testShardConfig(failoverShard, uint64(i+1))
		scfg.MemberIdentities = ids
		if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	return c
}

// view reads replica i's leadership through LeaderIDAndTerm and ShardInfo.
func (c *failoverCluster) view(i int) failoverView {
	var v failoverView
	leaderID, term, known, err := c.hosts[i].LeaderIDAndTerm(failoverShard)
	if err != nil {
		return v
	}
	info, err := c.hosts[i].ShardInfo(failoverShard)
	if err != nil {
		return v
	}
	return failoverView{
		leaderID: leaderID, term: term, known: known,
		infoLeader: info.LeaderID, infoTerm: info.Term, infoIsLeader: info.IsLeader,
		readable: true,
	}
}

// agreed reports the leader and term every replica in idx reports through
// both accessors, or ok=false while they disagree or know no leader.
func (c *failoverCluster) agreed(idx []int) (leaderID, term uint64, ok bool) {
	for n, i := range idx {
		v := c.view(i)
		if !v.readable || !v.known || v.leaderID == 0 ||
			v.infoLeader != v.leaderID || v.infoTerm != v.term {
			return 0, 0, false
		}
		// ShardInfo.IsLeader must hold exactly on the leader's own host.
		if v.infoIsLeader != (v.leaderID == uint64(i+1)) {
			return 0, 0, false
		}
		if n == 0 {
			leaderID, term = v.leaderID, v.term
		} else if v.leaderID != leaderID || v.term != term {
			return 0, 0, false
		}
	}
	return leaderID, term, true
}

func (c *failoverCluster) views(idx []int) string {
	s := ""
	for _, i := range idx {
		s += fmt.Sprintf("\n  replica %d: %s", i+1, c.view(i))
	}
	return s
}

// TestLeaderFailover_LeaderIDAndShardInfoReportTheNewLeader stops the
// leader's Host and requires every survivor's LeaderIDAndTerm and ShardInfo
// to agree on one new leader at a higher term within 10 election timeouts,
// with no survivor ever reporting the stopped replica at a term above the
// one it led in.
func TestLeaderFailover_LeaderIDAndShardInfoReportTheNewLeader(t *testing.T) {
	c := newFailoverCluster(t)
	all := []int{0, 1, 2}

	var oldLeader, oldTerm uint64
	if !waitUntil(30*time.Second, func() bool {
		var ok bool
		oldLeader, oldTerm, ok = c.agreed(all)
		return ok
	}) {
		t.Fatalf("the three replicas never agreed on a first leader:%s", c.views(all))
	}

	stopped := int(oldLeader - 1)
	if err := c.hosts[stopped].Close(); err != nil {
		t.Fatalf("closing the leader's Host (replica %d): %v", oldLeader, err)
	}
	c.hosts[stopped] = nil
	survivors := make([]int, 0, 2)
	for _, i := range all {
		if i != stopped {
			survivors = append(survivors, i)
		}
	}

	scfg := testShardConfig(failoverShard, 1)
	electionTimeout := time.Duration(scfg.ElectionRTT*failoverRTTMillisecond) * time.Millisecond
	bound := 10 * electionTimeout

	// The stopped replica may still be reported at oldTerm (the survivors have
	// not yet noticed it is gone), never at a later term: no later term was
	// ever led by it.
	checkNoStaleLeader := func() {
		t.Helper()
		for _, i := range survivors {
			v := c.view(i)
			if !v.readable {
				continue
			}
			if v.known && v.leaderID == oldLeader && v.term > oldTerm {
				t.Fatalf("replica %d reports the stopped replica %d as leader at term %d (it led at %d): %s",
					i+1, oldLeader, v.term, oldTerm, v)
			}
			if v.infoLeader == oldLeader && v.infoTerm > oldTerm {
				t.Fatalf("replica %d ShardInfo reports the stopped replica %d as leader at term %d (it led at %d): %s",
					i+1, oldLeader, v.infoTerm, oldTerm, v)
			}
		}
	}

	start := time.Now()
	var newLeader, newTerm uint64
	if !waitUntil(bound, func() bool {
		checkNoStaleLeader()
		var ok bool
		newLeader, newTerm, ok = c.agreed(survivors)
		return ok && newLeader != oldLeader && newTerm > oldTerm
	}) {
		t.Fatalf("within %v (10 election timeouts) of stopping leader %d (term %d) the survivors "+
			"did not agree on a new leader at a higher term:%s", bound, oldLeader, oldTerm, c.views(survivors))
	}
	t.Logf("leader %d (term %d) stopped; survivors agree on leader %d at term %d after %v",
		oldLeader, oldTerm, newLeader, newTerm, time.Since(start))

	checkNoStaleLeader()
	if l, term, ok := c.agreed(survivors); !ok || l != newLeader || term != newTerm {
		t.Fatalf("the survivors' agreement on leader %d at term %d did not hold:%s",
			newLeader, newTerm, c.views(survivors))
	}
}
