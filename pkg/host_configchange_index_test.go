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
	"testing"
	"time"
)

// configChangeIndex is advanced by OnApplied and by reconcileMembershipFromPeer; the latter must never regress it.

// TestReconcileMembershipFromPeer_NeverRegressesConfigChangeIndex asserts a lower ConfigChangeID does not move the guard.
func TestReconcileMembershipFromPeer_NeverRegressesConfigChangeIndex(t *testing.T) {
	h := startSingleVoterShard(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A real committed config change, so the peer's membership carries a real
	// (and deliberately LOW) ConfigChangeID rather than zero.
	if err := h.SyncRequestAddObserver(ctx, 1, 10, "127.0.0.1:6000", 0); err != nil {
		t.Fatalf("SyncRequestAddObserver: %v", err)
	}
	engNode := h.engine.GetNode(1)
	if engNode == nil || engNode.Peer() == nil {
		t.Fatal("shard 1 has no engine node")
	}
	peerCCID := engNode.Peer().GetMembership().ConfigChangeID
	if peerCCID == 0 {
		t.Fatal("precondition: the committed config change left ConfigChangeID at 0")
	}

	ns := testLoadNode(h, 1)
	// A later incremental apply already seen by this replica.
	const ahead = 1_000
	newer := peerCCID + ahead
	ns.configChangeIndex.Store(newer)

	cb := &hostEngineCallback{h: h}
	cb.reconcileMembershipFromPeer(1, ns)

	if got := ns.configChangeIndex.Load(); got != newer {
		t.Fatalf("configChangeIndex regressed from %d to %d after reconciling a "+
			"membership carrying the OLDER ConfigChangeID %d: the guard now "+
			"rejects the very value SyncGetShardMembership reports, so every "+
			"subsequent RequestAddNode/RequestRemoveNode on this shard fails with "+
			"StaleConfigChangeError", newer, got, peerCCID)
	}

	// Positive control: reconciliation still advances a guard that is behind.
	ns.configChangeIndex.Store(0)
	cb.reconcileMembershipFromPeer(1, ns)
	if got := ns.configChangeIndex.Load(); got != peerCCID {
		t.Fatalf("configChangeIndex = %d after reconciling from a membership at "+
			"ConfigChangeID %d, want it seeded to %d: a guard left behind the "+
			"committed membership rejects every RequestAddNode/RequestRemoveNode "+
			"with StaleConfigChangeError{Expected: 0}", got, peerCCID, peerCCID)
	}
}
