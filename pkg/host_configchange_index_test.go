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

// nodeState.configChangeIndex is the staleness guard every membership change is
// compare-and-swapped against: RequestAddNode/RemoveNode reject a caller whose
// ConfigChangeID does not match, which is what stops two operators racing
// contradictory changes.
//
// Two writers advance it, and they run on different goroutines with no ordering
// between them:
//
//	OnApplied (incremental)          Store(entry.Index) as each config change applies
//	reconcileMembershipFromPeer      seeds it from a snapshot-adopted membership,
//	                                 whose config-change entries were compacted
//	                                 out of the log and so never re-fire OnApplied
//
// The second is a CATCH-UP, not an authority: the snapshot it reconciles from
// can be older than an incremental apply that already landed. Storing its value
// unconditionally would REGRESS the guard, and a regressed guard rejects the
// very ConfigChangeID that SyncGetShardMembership just handed the caller —
// wedging membership changes on that shard until something else advances it.
// Hence the never-regress compare-and-swap loop, which had no test at all:
// replacing the whole loop with an unconditional Store left ./pkg/ fully green.

// TestReconcileMembershipFromPeer_NeverRegressesConfigChangeIndex drives the
// membership-restored path with a LOWER ConfigChangeID than the guard already
// holds and asserts the stored value does not move.
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
	// A LATER incremental apply that this replica has already seen. In
	// production it arrives from OnApplied on the apply worker while a snapshot
	// install reconciles concurrently; the value is what SyncGetShardMembership
	// would have just reported to the operator.
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

	// Positive control: the reconciliation must still ADVANCE a guard that is
	// BEHIND the membership it is restoring — that is the whole reason the
	// seeding exists (a snapshot compacts the config-change entries away, so
	// OnApplied never fires for them and a fresh nodeState would sit at 0 and
	// reject every membership change forever). Without this half, a mutation
	// that simply deleted the write would pass the assertion above.
	ns.configChangeIndex.Store(0)
	cb.reconcileMembershipFromPeer(1, ns)
	if got := ns.configChangeIndex.Load(); got != peerCCID {
		t.Fatalf("configChangeIndex = %d after reconciling from a membership at "+
			"ConfigChangeID %d, want it seeded to %d: a guard left behind the "+
			"committed membership rejects every RequestAddNode/RequestRemoveNode "+
			"with StaleConfigChangeError{Expected: 0}", got, peerCCID, peerCCID)
	}
}
