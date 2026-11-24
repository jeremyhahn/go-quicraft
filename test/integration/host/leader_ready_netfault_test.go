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

//go:build integration && quicraft_netfault

package host_test

import (
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-quicraft/test/netfault"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// A replica re-elected at a higher term announces readiness afresh after the new term's first commit; the re-election is built by isolating C, then B, so only A can win.
func TestLeaderReady_SameLeaderReelectedAtHigherTerm(t *testing.T) {
	c := newReadyCluster(t, 3)
	// Registered after the cluster's cleanup so it runs before it: the
	// injector is process-global.
	t.Cleanup(netfault.Heal)

	p1 := c.awaitAgreedReady([]int{0, 1, 2}, 0)
	a := int(p1.leaderID - 1)
	rest := others(3, a)
	b, cc := rest[0], rest[1]

	netfault.Isolate(uint64(cc + 1))
	if got := netfault.BlockedCount(); got < 1 {
		t.Fatalf("fault injection is not in effect: BlockedCount()=%d", got)
	}
	if _, err := proposer.Once(c.hosts[a], readyShard, encodeKVPut("k", "v"), defaultTimeout); err != nil {
		t.Fatalf("propose on leader %d with %d isolated: %v", a+1, cc+1, err)
	}
	netfault.Isolate(uint64(b + 1))
	c.logs[a].await(t, failoverTimeout, fmt.Sprintf("leader %d to step down", a+1),
		func(ev []readyEvent) bool {
			last := ev[len(ev)-1]
			return !last.ready && last.leaderID != p1.leaderID
		})
	if _, term, ready, err := c.hosts[a].ShardReadReady(readyShard); err != nil || ready {
		t.Fatalf("deposed leader %d reports read-ready at term %d (err=%v)", a+1, term, err)
	}

	netfault.Rejoin(uint64(cc + 1))
	p2 := c.awaitAgreedReady([]int{a, cc}, p1.term)
	if p2.leaderID != p1.leaderID {
		t.Fatalf("leader %d at term %d, want %d re-elected; A: %v C: %v",
			p2.leaderID, p2.term, p1.leaderID, c.logs[a].snapshot(), c.logs[cc].snapshot())
	}
	var readyTerms []uint64
	for _, e := range c.logs[a].snapshot() {
		if e.ready {
			readyTerms = append(readyTerms, e.term)
		}
	}
	if len(readyTerms) != 2 || readyTerms[0] != p1.term || readyTerms[1] != p2.term {
		t.Fatalf("leader %d announced readiness for terms %v, want [%d %d]: %v",
			a+1, readyTerms, p1.term, p2.term, c.logs[a].snapshot())
	}
	c.assertServes([]int{a, cc}, p2)
	c.assertOrdering()
}
