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

import "testing"

// TestEvictStaleOutboundOnRecv_UnattributableOutboundEvicted is the regression
// test for a 20-30 second one-way network black hole that stalled leader
// election on a cluster with quorum intact.
//
// The scenario the soak reproduced: a peer restarts on the same address, and
// the surviving nodes hold outbound connections dialed to the DEAD incarnation.
// Writing into such a connection SUCCEEDS locally -- QUIC buffers into the send
// window and reports no error -- so the sender believes every RequestPreVoteResp
// was delivered while the restarted peer receives nothing. With 2 of 5 nodes
// down every surviving voter's grant is required, so one silently broken
// direction blocks EVERY election until the QUIC MaxIdleTimeout (30s) finally
// kills the connection.
//
// Restart detection is therefore the only thing standing between a peer restart
// and a multi-second outage, and it must not have holes. It used to fire only
// when a PREVIOUS epoch had been recorded for the peer, which is a strictly
// smaller set than "we hold an outbound of unproven provenance": we dial peers
// before their first batch reaches us, and connection teardown used to drop the
// epoch record while a later re-dial repopulated the connection cache. In both
// states a restart is indistinguishable from first contact and the black-holed
// outbound survived.
//
// The invariant asserted here is the complete one: an outbound that cannot be
// attributed to the incarnation whose batch just arrived is not trustworthy and
// must be evicted, whether or not this node happens to remember an earlier
// epoch for that address.
func TestEvictStaleOutboundOnRecv_UnattributableOutboundEvicted(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// t1 dials t2 without ever having received a batch from it, which is the
	// ordinary case: the dial path has no way to learn the peer's epoch. The
	// resulting outbound is therefore of unproven provenance.
	outbound, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}
	if _, err := t1.getStream(target, 1, false, ""); err != nil {
		t.Fatalf("getStream failed: %v", err)
	}
	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 cached outbound, got %d", t1.ConnectionCount())
	}

	// A batch now arrives from t2 announcing its boot epoch, on a connection
	// that is NOT the cached outbound (each peer dials independently). The
	// cached outbound is alive, so the dead-connection branch does not apply:
	// only incarnation attribution can decide whether it is usable.
	inbound, err := t2.getConnection(t1.Addr().String(), "")
	if err != nil {
		t.Fatalf("t2.getConnection failed: %v", err)
	}
	if outbound == inbound {
		t.Fatal("outbound and inbound must be distinct connections")
	}

	t1.evictStaleOutboundOnRecv(target, t2.bootEpoch, inbound)

	if t1.ConnectionCount() != 0 {
		t.Fatalf("outbound dialed under an unknown peer incarnation was kept "+
			"(%d connections remain): a black-holed connection to a restarted "+
			"peer survives here until MaxIdleTimeout, silently dropping every "+
			"vote response for ~30s", t1.ConnectionCount())
	}
	if t1.StreamCount() != 0 {
		t.Fatalf("expected pooled streams to be evicted with the connection, got %d",
			t1.StreamCount())
	}
}

// TestEvictStaleOutboundOnRecv_AttributedOutboundSurvivesRepeatedBatches
// guards the opposite failure: eviction must converge, not cascade.
//
// An earlier identity-based heuristic evicted on every re-dial, so A evicting
// made B see a new inbound and evict in turn, forever. Attribution has the same
// hazard if the epoch a fresh outbound is stamped with can be forgotten
// underneath it -- every subsequent batch would then look unattributable and
// tear down a perfectly good connection. Once a connection has been dialed
// while the peer's incarnation was known, repeated batches from that same
// incarnation must be a no-op.
func TestEvictStaleOutboundOnRecv_AttributedOutboundSurvivesRepeatedBatches(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	inbound, err := t2.getConnection(t1.Addr().String(), "")
	if err != nil {
		t.Fatalf("t2.getConnection failed: %v", err)
	}

	// First batch: t1 holds no outbound, so this only records the epoch.
	t1.evictStaleOutboundOnRecv(target, t2.bootEpoch, inbound)

	// The dial now happens with t2's incarnation known, so the outbound is
	// attributable to it.
	if _, err := t1.getConnection(target, ""); err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}
	if _, err := t1.getStream(target, 1, false, ""); err != nil {
		t.Fatalf("getStream failed: %v", err)
	}

	for i := range 5 {
		t1.evictStaleOutboundOnRecv(target, t2.bootEpoch, inbound)
		if t1.ConnectionCount() != 1 {
			t.Fatalf("batch %d from the SAME peer incarnation evicted an "+
				"attributable outbound (%d connections): repeated eviction of "+
				"live connections is the cascade that starves election traffic",
				i, t1.ConnectionCount())
		}
	}
}

// TestEvictStaleOutboundOnRecv_RestartAfterTeardownEvicts covers the exact
// sequence the soak hit: the outbound to a peer is torn down and re-dialed
// (a write failure, a dead-connection sweep) and the peer then restarts.
//
// The teardown must not erase the peer's known incarnation, because the
// re-dialed connection is stamped from it. If it did, the re-dial would be
// stamped "unknown", every later batch would look unattributable, and eviction
// would forget the epoch again -- an endless evict/re-dial loop. Equally, the
// re-dialed connection must still be recognised as stale once the peer's epoch
// actually changes.
func TestEvictStaleOutboundOnRecv_RestartAfterTeardownEvicts(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	inbound, err := t2.getConnection(t1.Addr().String(), "")
	if err != nil {
		t.Fatalf("t2.getConnection failed: %v", err)
	}

	t1.evictStaleOutboundOnRecv(target, t2.bootEpoch, inbound)
	if _, err := t1.getConnection(target, ""); err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Tear the outbound down and re-dial, as a write failure would.
	t1.removeAndCloseConnection(target, "test teardown")
	t1.evictStreamsForTarget(target)
	if _, err := t1.getConnection(target, ""); err != nil {
		t.Fatalf("re-dial failed: %v", err)
	}

	// Same incarnation: the re-dialed connection must survive, proving the
	// teardown did not erase the epoch it was stamped from.
	t1.evictStaleOutboundOnRecv(target, t2.bootEpoch, inbound)
	if t1.ConnectionCount() != 1 {
		t.Fatalf("re-dialed outbound was evicted by a batch from the same peer "+
			"incarnation (%d connections): the teardown erased the epoch the "+
			"re-dial was stamped from", t1.ConnectionCount())
	}

	// The peer now restarts: a different epoch must evict.
	t1.evictStaleOutboundOnRecv(target, t2.bootEpoch+1, inbound)
	if t1.ConnectionCount() != 0 {
		t.Fatalf("outbound survived a peer restart (%d connections)",
			t1.ConnectionCount())
	}
}
