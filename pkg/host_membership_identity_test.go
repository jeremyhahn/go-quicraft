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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Committed node identities must survive the snapshot-metadata round-trip; each test asserts a promiscuous certificate is still refused after recovery.

// identityProtoMembership is a committed configuration carrying identities
// across more than one role.
func identityProtoMembership() proto.Membership {
	return proto.Membership{
		ConfigChangeID:             12,
		MaxWitnesses:               1,
		RequireFullReplicaMajority: true,
		Addresses:                  map[uint64]string{1: "10.0.0.1:5001", 2: "10.0.0.2:5001"},
		Observers:                  map[uint64]string{3: "10.0.0.3:5001"},
		Witnesses:                  map[uint64]string{4: "10.0.0.4:5001"},
		Removed:                    map[uint64]bool{9: true},
		Identities: map[uint64]string{
			1: "spki:1111111111111111111111111111111111111111111111111111111111111111",
			2: "spki:2222222222222222222222222222222222222222222222222222222222222222",
			3: "spki:3333333333333333333333333333333333333333333333333333333333333333",
		},
		// RemovedIdentities is the removal deny-list and must survive the round-trip.
		RemovedIdentities: map[uint64]string{
			9: "spki:9999999999999999999999999999999999999999999999999999999999999999",
		},
		// The replicated session policy travels the same path and was equally
		// unguarded here: a snapshot that loses it comes back unset, the
		// replica falls back to its own Config, and it expires sessions at log
		// indexes its peers do not.
		SessionExpiryEntries:   7000,
		MaxTotalSessions:       8192,
		MaxResponsesPerSession: 32,
		MaxSessionsPerOwner:    11,
	}
}

// TestMembershipConvertersRoundTripIdentities pins both directions of the
// proto <-> logdb conversion. The save path (protoMembershipToLogDB) and the
// recovery path (logdbMembershipToProto) are separate functions, so a field
// carried by one and dropped by the other still loses it.
func TestMembershipConvertersRoundTripIdentities(t *testing.T) {
	want := identityProtoMembership()

	stored := protoMembershipToLogDB(want)
	if len(stored.Identities) != len(want.Identities) {
		t.Fatalf("protoMembershipToLogDB dropped identities: got %v, want %v",
			stored.Identities, want.Identities)
	}

	got := logdbMembershipToProto(stored)
	for id, identity := range want.Identities {
		if got.Identities[id] != identity {
			t.Fatalf("round-trip Identities[%d] = %q, want %q",
				id, got.Identities[id], identity)
		}
	}
	if len(got.Identities) != len(want.Identities) {
		t.Fatalf("round-trip Identities = %v, want %v", got.Identities, want.Identities)
	}

	// RemovedIdentities is asserted separately and loudly: it is the deny-list
	// that stops an ejected replica's keypair being readmitted, and until this
	// fixture carried it the read-back direction could drop the whole map with
	// nothing failing. Losing it "revokes a revocation" — a replica restarts
	// having forgotten which keypairs it ejected.
	for id, identity := range want.RemovedIdentities {
		if got.RemovedIdentities[id] != identity {
			t.Fatalf("round-trip RemovedIdentities[%d] = %q, want %q: the "+
				"removal deny-list did not survive, so a decommissioned "+
				"replica still holding this certificate would be readmitted "+
				"on the next restart", id, got.RemovedIdentities[id], identity)
		}
	}
	if len(got.RemovedIdentities) != len(want.RemovedIdentities) {
		t.Fatalf("round-trip RemovedIdentities = %v, want %v",
			got.RemovedIdentities, want.RemovedIdentities)
	}

	// The neighbouring fields must be unharmed, including the replicated
	// session policy — a snapshot that loses it makes the replica expire
	// sessions at log indexes its peers do not.
	if got.ConfigChangeID != want.ConfigChangeID ||
		got.MaxWitnesses != want.MaxWitnesses ||
		!got.RequireFullReplicaMajority ||
		got.Addresses[2] != want.Addresses[2] ||
		got.Observers[3] != want.Observers[3] ||
		got.Witnesses[4] != want.Witnesses[4] ||
		!got.Removed[9] {
		t.Fatalf("round-trip lost a neighbouring field: %+v", got)
	}
	if got.SessionExpiryEntries != want.SessionExpiryEntries ||
		got.MaxTotalSessions != want.MaxTotalSessions ||
		got.MaxResponsesPerSession != want.MaxResponsesPerSession ||
		got.MaxSessionsPerOwner != want.MaxSessionsPerOwner {
		t.Fatalf("round-trip lost the replicated session policy: "+
			"expiry=%d total=%d perClient=%d perOwner=%d, want %d/%d/%d/%d",
			got.SessionExpiryEntries, got.MaxTotalSessions, got.MaxResponsesPerSession,
			got.MaxSessionsPerOwner,
			want.SessionExpiryEntries, want.MaxTotalSessions, want.MaxResponsesPerSession,
			want.MaxSessionsPerOwner)
	}
}

// TestMembershipConvertersCopyIdentityMap pins that neither converter aliases the caller's map.
func TestMembershipConvertersCopyIdentityMap(t *testing.T) {
	source := identityProtoMembership()

	stored := protoMembershipToLogDB(source)
	source.Identities[2] = "spki:tampered"
	if stored.Identities[2] == "spki:tampered" {
		t.Fatal("protoMembershipToLogDB aliased the caller's Identities map: a " +
			"later mutation by the owning layer rewrote committed authorization state")
	}

	back := logdbMembershipToProto(stored)
	stored.Identities[1] = "spki:tampered"
	if back.Identities[1] == "spki:tampered" {
		t.Fatal("logdbMembershipToProto aliased the LogDB Identities map: storage " +
			"and the in-memory raft configuration must not share it")
	}
}

// TestMembershipConvertersPreserveAbsentIdentities pins the compatibility
// state: a membership with no identities converts to one with no identities,
// in both directions. An identity is only ever committed, never invented.
func TestMembershipConvertersPreserveAbsentIdentities(t *testing.T) {
	legacy := proto.Membership{
		ConfigChangeID: 4,
		Addresses:      map[uint64]string{1: "10.0.0.1:5001"},
	}

	stored := protoMembershipToLogDB(legacy)
	if len(stored.Identities) != 0 {
		t.Fatalf("protoMembershipToLogDB invented identities: %v", stored.Identities)
	}
	got := logdbMembershipToProto(stored)
	if len(got.Identities) != 0 {
		t.Fatalf("logdbMembershipToProto invented identities: %v", got.Identities)
	}
	if got.Addresses[1] != "10.0.0.1:5001" {
		t.Fatalf("legacy addresses lost: %v", got.Addresses)
	}
}

// recoveredMembership round-trips ms through protoMembershipToLogDB, the backend and logdbMembershipToProto.
func recoveredMembership(t *testing.T, ldb logdb.LogDB, reopen func() logdb.LogDB,
	ms proto.Membership) proto.Membership {
	t.Helper()

	if err := ldb.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 5, Vote: 1, Commit: 10},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := ldb.SaveSnapshot(1, 1, logdb.Snapshot{
		Index: 10, Term: 5, Filepath: "snap-10.dat",
		Membership: protoMembershipToLogDB(ms),
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	restarted := reopen()
	snap, err := restarted.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot after restart: %v", err)
	}
	return logdbMembershipToProto(snap.Membership)
}

// TestSnapshotRecoveryKeepsTheQuorumGateBoundToIdentities checks that after snapshot recovery a wildcard-certificate member forging replica 2 is refused, not delivered.
func TestSnapshotRecoveryKeepsTheQuorumGateBoundToIdentities(t *testing.T) {
	const (
		victimAddr    = "10.0.0.2:5001"
		attackerIDStr = "spki:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	)

	for _, backend := range logDBBackendsForRecovery() {
		t.Run(backend.name, func(t *testing.T) {
			ldb, reopen := backend.open(t)
			recovered := recoveredMembership(t, ldb, reopen, identityProtoMembership())

			eng, handler, metrics := identityRig(t, recovered)

			// The attacker's valid certificate covers the victim's committed address.
			attacker := certCoveringAs(attackerIDStr, victimAddr)
			if !attacker.Authorizes(victimAddr) {
				t.Fatal("fixture: the attacker's certificate must cover the victim's " +
					"committed address, or the address fallback would refuse it anyway")
			}

			batch := proto.MessageBatch{
				SourceAddress: victimAddr,
				Requests: []proto.Message{
					{ShardID: 1, From: 2, To: 1, Type: proto.ReplicateResp, Term: 5},
				},
			}
			if err := handler.HandleMessage(batch, attacker); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatal("a forged ReplicateResp from a wildcard-certificate holder was " +
					"COUNTED after snapshot recovery: the committed identities did not " +
					"survive the snapshot-metadata round-trip, so the gate fell back to " +
					"the address check the identity binding exists to replace")
			}
			if reason := metrics.lastReason(); reason != dropIdentityMismatch {
				t.Fatalf("drop reason = %q, want %q; any other reason means the gate "+
					"refused it for a reason other than the committed identity",
					reason, dropIdentityMismatch)
			}
		})
	}
}

// TestSnapshotRecoveryKeepsTheLegitimateHolderAuthorized is the liveness half.
// A gate that refused everything would pass the forgery test above while
// bricking the shard, so the member that genuinely holds the committed keypair
// must still be delivered after the same recovery.
func TestSnapshotRecoveryKeepsTheLegitimateHolderAuthorized(t *testing.T) {
	const victimAddr = "10.0.0.2:5001"
	victimID := identityProtoMembership().Identities[2]

	for _, backend := range logDBBackendsForRecovery() {
		t.Run(backend.name, func(t *testing.T) {
			ldb, reopen := backend.open(t)
			recovered := recoveredMembership(t, ldb, reopen, identityProtoMembership())

			eng, handler, metrics := identityRig(t, recovered)

			batch := proto.MessageBatch{
				SourceAddress: victimAddr,
				Requests: []proto.Message{
					{ShardID: 1, From: 2, To: 1, Type: proto.ReplicateResp, Term: 5},
				},
			}
			if err := handler.HandleMessage(batch, certCoveringAs(victimID, victimAddr)); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 1 {
				t.Fatalf("the replica that holds the committed keypair was refused after "+
					"snapshot recovery (reason %q)", metrics.lastReason())
			}
		})
	}
}

// recoveryBackend is one LogDB implementation and the strongest restart it supports.
type recoveryBackend struct {
	name string
	open func(t *testing.T) (logdb.LogDB, func() logdb.LogDB)
}

func logDBBackendsForRecovery() []recoveryBackend {
	return []recoveryBackend{
		{
			name: "memdb",
			open: func(t *testing.T) (logdb.LogDB, func() logdb.LogDB) {
				t.Helper()
				db := memdb.New()
				t.Cleanup(func() { _ = db.Close() })
				// memdb has no on-disk form; its round-trip is the copy path.
				return db, func() logdb.LogDB { return db }
			},
		},
		{
			name: "waldb",
			open: func(t *testing.T) (logdb.LogDB, func() logdb.LogDB) {
				t.Helper()
				dir := t.TempDir()
				open := func() logdb.LogDB {
					db, err := waldb.Open(dir, waldb.WithNoSync(true))
					if err != nil {
						t.Fatalf("waldb.Open: %v", err)
					}
					return db
				}
				db := open()
				current := &db
				t.Cleanup(func() { _ = (*current).Close() })
				// A genuine restart: close the database and replay the
				// segments, which is the only thing that proves the field
				// reached the record encoding rather than an in-memory map.
				return db, func() logdb.LogDB {
					if err := (*current).Close(); err != nil {
						t.Fatalf("Close before reopen: %v", err)
					}
					*current = open()
					return *current
				}
			},
		},
	}
}
