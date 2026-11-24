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

// TestEvictStaleOutboundOnRecv_UnattributableOutboundEvicted: an outbound not attributable to the incarnation whose batch arrived is evicted.
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

// TestEvictStaleOutboundOnRecv_AttributedOutboundSurvivesRepeatedBatches: eviction converges and leaves an attributed outbound alone.
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

// TestEvictStaleOutboundOnRecv_RestartAfterTeardownEvicts: a teardown keeps the peer's incarnation, and a later restart still evicts.
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
