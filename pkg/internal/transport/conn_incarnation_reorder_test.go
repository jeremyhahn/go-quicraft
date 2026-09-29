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

package transport

import (
	"testing"
	"time"
)

// currentPeerEpoch returns the epoch t currently attributes to target, and
// whether any record exists.
func currentPeerEpoch(t *QUICTransport, target string) (uint64, bool) {
	t.peerEpochMu.Lock()
	defer t.peerEpochMu.Unlock()
	rec, ok := t.peerEpoch[target]
	if !ok {
		return 0, false
	}
	return rec.epoch, true
}

// TestEvictStaleOutboundOnRecv_LateFrameFromDeadIncarnationDoesNotEvict is the
// regression test for an eviction/re-dial cascade that fired during the exact
// event restart detection exists to handle: a peer restart.
//
// Boot epochs are UnixNano ^ crypto/rand, so two of them cannot be ordered by
// comparison. The record of a peer's current incarnation was therefore plain
// last-writer-wins: ANY authorized batch whose SourceEpoch differed from the
// stored one replaced it, with no test that the new value was actually newer.
//
// A restart does not stop the old incarnation's already-written frames from
// arriving. B restarts E1->E2; A sees E2, evicts, and re-dials stamped E2. A
// frame written by the DEAD E1 incarnation then drains out of the old inbound:
// the record is dragged back to E1, the healthy E2 outbound no longer matches
// the "current" epoch and is evicted, and the re-dial is stamped E1 -- which
// the next genuine E2 batch evicts in turn. The pair ping-pongs, each round
// costing a dial, a stream re-open and every in-flight replication on the
// connection.
//
// The invariant asserted here is that a peer's incarnation history is one-way:
// an epoch this node has already watched be replaced can never again be
// treated as current, nor evict anything.
func TestEvictStaleOutboundOnRecv_LateFrameFromDeadIncarnationDoesNotEvict(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	inbound, err := t2.getConnection(t1.Addr().String(), "")
	if err != nil {
		t.Fatalf("t2.getConnection failed: %v", err)
	}

	oldEpoch := t2.bootEpoch
	newEpoch := oldEpoch + 7 // a restart; the value carries no ordering

	// Steady state under the peer's first incarnation: the epoch is known and
	// the outbound is dialed (and therefore stamped) under it.
	t1.evictStaleOutboundOnRecv(target, oldEpoch, inbound)
	if _, err := t1.getConnection(target, ""); err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// The peer restarts. The outbound to the dead incarnation is black-holed
	// and must go.
	t1.evictStaleOutboundOnRecv(target, newEpoch, inbound)
	if t1.ConnectionCount() != 0 {
		t.Fatalf("outbound survived a peer restart (%d connections)",
			t1.ConnectionCount())
	}
	if _, err := t1.getConnection(target, ""); err != nil {
		t.Fatalf("re-dial failed: %v", err)
	}
	if _, err := t1.getStream(target, 1, false, ""); err != nil {
		t.Fatalf("getStream failed: %v", err)
	}

	// A frame written by the DEAD incarnation now drains out of the old
	// inbound and arrives after the re-dial.
	t1.evictStaleOutboundOnRecv(target, oldEpoch, inbound)

	if t1.ConnectionCount() != 1 {
		t.Fatalf("a late frame from the peer's DEAD incarnation evicted the "+
			"healthy connection dialed under its live one (%d connections): "+
			"the re-dial is then stamped with the dead epoch and the next "+
			"genuine batch evicts it again -- an eviction/re-dial cascade "+
			"during a peer restart", t1.ConnectionCount())
	}
	// Eviction takes every pooled stream for the target with it, so an empty
	// pool is the observable. The exact count is not asserted: the dial path
	// pre-opens the heartbeat stream asynchronously.
	if t1.StreamCount() == 0 {
		t.Fatal("pooled streams were evicted by a late frame from a dead " +
			"incarnation")
	}
	if epoch, ok := currentPeerEpoch(t1, target); !ok || epoch != newEpoch {
		t.Fatalf("the recorded incarnation regressed to a dead epoch "+
			"(recorded=%d live=%d present=%v): every subsequent re-dial is "+
			"stamped from this value", epoch, newEpoch, ok)
	}

	// The live incarnation keeps talking. Nothing further may be evicted.
	for i := range 5 {
		t1.evictStaleOutboundOnRecv(target, newEpoch, inbound)
		if t1.ConnectionCount() != 1 {
			t.Fatalf("batch %d from the LIVE incarnation evicted the connection "+
				"dialed under it (%d connections): the cascade is self-sustaining",
				i, t1.ConnectionCount())
		}
	}
}

// TestReapPeerIncarnations_ForgetsDepartedPeersOnly covers the lifetime of the
// incarnation map.
//
// The record must survive a teardown, because the re-dial that follows is
// stamped from it: dropping it there stamps the fresh outbound "unknown" and
// the next batch evicts it again. That constraint removed the map's only two
// reapers, leaving a per-process leak — a replica removed from membership kept
// its entry for the lifetime of the host, and repeated membership churn grew
// the map without bound.
//
// Reaping on an idle window satisfies both: an address that is neither
// connected nor talking is genuinely gone, while anything connected or recently
// heard from keeps the epoch its next dial needs.
func TestReapPeerIncarnations_ForgetsDepartedPeersOnly(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	connected := t2.Addr().String()
	inbound, err := t2.getConnection(t1.Addr().String(), "")
	if err != nil {
		t.Fatalf("t2.getConnection failed: %v", err)
	}

	// A peer this node holds an outbound to.
	t1.evictStaleOutboundOnRecv(connected, t2.bootEpoch, inbound)
	if _, err := t1.getConnection(connected, ""); err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Two peers with no cached outbound: one heard from recently, one long
	// departed (a replica removed from membership).
	const recent = "127.0.0.1:59001"
	const departed = "127.0.0.1:59002"
	now := time.Now()
	t1.peerEpochMu.Lock()
	t1.peerEpoch[recent] = &peerIncarnation{epoch: 11, lastSeen: now}
	t1.peerEpoch[departed] = &peerIncarnation{
		epoch:    12,
		lastSeen: now.Add(-peerIncarnationRetention - time.Minute),
	}
	t1.peerEpochMu.Unlock()

	// Driven through the periodic sweep rather than the reaper directly: the
	// leak was caused by removing the only two call sites, so the sweep
	// actually invoking a reaper is half of what must be asserted.
	t1.cleanStaleConnections()

	if _, ok := currentPeerEpoch(t1, departed); ok {
		t.Fatal("a peer with no outbound and no traffic for longer than the " +
			"retention window kept its incarnation record: membership churn " +
			"grows this map for the lifetime of the process")
	}
	if _, ok := currentPeerEpoch(t1, recent); !ok {
		t.Fatal("a recently-seen peer lost its incarnation record: its next " +
			"dial is stamped \"unknown\" and the following batch evicts it")
	}
	if _, ok := currentPeerEpoch(t1, connected); !ok {
		t.Fatal("a peer with a live cached outbound lost its incarnation " +
			"record: the epoch that outbound was stamped from is what proves " +
			"it belongs to the incarnation still talking to us")
	}
}

// TestKnownPeerEpoch_DialRefreshesRetention guards the reaper against forgetting
// a peer this node dials but never receives from. Dialing is contact: a target
// being connected to is not a departed replica.
func TestKnownPeerEpoch_DialRefreshesRetention(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	const target = "127.0.0.1:59003"
	stale := time.Now().Add(-peerIncarnationRetention - time.Minute)
	t1.peerEpochMu.Lock()
	t1.peerEpoch[target] = &peerIncarnation{epoch: 99, lastSeen: stale}
	t1.peerEpochMu.Unlock()
	_ = t2

	if got := t1.knownPeerEpoch(target); got != 99 {
		t.Fatalf("knownPeerEpoch = %d, want 99", got)
	}

	t1.reapPeerIncarnations(time.Now())

	if _, ok := currentPeerEpoch(t1, target); !ok {
		t.Fatal("a peer stamped onto an outbound moments ago was reaped: the " +
			"connection just dialed can no longer be attributed to it")
	}
}
