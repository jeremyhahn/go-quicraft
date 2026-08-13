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

// ---------------------------------------------------------------------------
// Committed node identities must survive the snapshot-metadata round-trip
//
// Identities reach membership through the LOG: the EntryInitialMembership at
// index 1 and every EntryConfigChange. A replica that rebuilds its
// configuration from a SNAPSHOT whose log has been compacted past those entries
// has the logdb snapshot record and nothing else to relearn them from.
//
// Dropping the field anywhere on that path is silent. Nothing errors, the
// recovered configuration still names every member, and the receive-side gate
// simply falls back to the committed ADDRESS — which is the check a wildcard or
// shared-SAN certificate defeats, and the reason the identity binding exists.
// So every test here asserts the DISTINGUISHING observable: that a promiscuous
// certificate is still refused after recovery. A test that asserted only "the
// membership came back" would pass against the dropped field.
// ---------------------------------------------------------------------------

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

	// The neighbouring fields must be unharmed.
	if got.ConfigChangeID != want.ConfigChangeID ||
		got.MaxWitnesses != want.MaxWitnesses ||
		!got.RequireFullReplicaMajority ||
		got.Addresses[2] != want.Addresses[2] ||
		got.Observers[3] != want.Observers[3] ||
		got.Witnesses[4] != want.Witnesses[4] ||
		!got.Removed[9] {
		t.Fatalf("round-trip lost a neighbouring field: %+v", got)
	}
}

// TestMembershipConvertersCopyIdentityMap pins that neither converter ALIASES
// the caller's map. Several bugs in this codebase came from sharing a map
// across a boundary, and this one is read by the authorization gate on every
// quorum-bearing message while the source is owned by another layer.
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

// recoveredMembership stores ms as a replica's own snapshot metadata, puts the
// backend through the strongest round-trip it supports, and returns the
// membership a restarting replica would rebuild its configuration from.
//
// It walks the real path in both directions: protoMembershipToLogDB is what
// engine/snapshot_pool uses when a replica snapshots itself, and
// logdbMembershipToProto is what host.recoverLogReader uses on the way back.
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

// TestSnapshotRecoveryKeepsTheQuorumGateBoundToIdentities is the end-to-end
// regression, and the reason the field has to be carried at all.
//
// A replica restarts and rebuilds its configuration from snapshot metadata,
// its log having been compacted past the entries that carried the identities.
// Then a GENUINE, authenticated member whose certificate is a cluster wildcard
// forges a quorum-bearing ReplicateResp as replica 2.
//
// If the identities survived, the gate compares the attacker's own keypair
// against the one committed for replica 2 and refuses with identity_mismatch.
// If they were dropped anywhere on the path, the gate falls back to the
// address check, the wildcard certificate covers replica 2's committed address,
// and the forgery is DELIVERED and counted toward commit quorum. Delivery is
// therefore the observable that distinguishes the fixed path from the broken
// one — asserting the membership merely "came back" would not.
func TestSnapshotRecoveryKeepsTheQuorumGateBoundToIdentities(t *testing.T) {
	const (
		victimAddr    = "10.0.0.2:5001"
		attackerIDStr = "spki:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	)

	for _, backend := range logDBBackendsForRecovery() {
		t.Run(backend.name, func(t *testing.T) {
			ldb, reopen := backend.open(t)
			recovered := recoveredMembership(t, ldb, reopen, identityProtoMembership())

			eng, handler, metrics := identityRig(t, recovered, false)

			// The attacker holds a valid cluster certificate that COVERS the
			// victim's committed address. Withholding that would make the test
			// pass against the address binding too, proving nothing.
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

			eng, handler, metrics := identityRig(t, recovered, false)

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

// TestSnapshotRecoveryUnderStrictModeKeepsMembersAuthorized pins the posture
// the whole gap blocked. Under RequireMemberIdentity a member whose identity
// did not survive recovery has its quorum-bearing traffic dropped outright, so
// this is the assertion that says strict mode is safe to enable across a
// restart.
func TestSnapshotRecoveryUnderStrictModeKeepsMembersAuthorized(t *testing.T) {
	const victimAddr = "10.0.0.2:5001"
	victimID := identityProtoMembership().Identities[2]

	for _, backend := range logDBBackendsForRecovery() {
		t.Run(backend.name, func(t *testing.T) {
			ldb, reopen := backend.open(t)
			recovered := recoveredMembership(t, ldb, reopen, identityProtoMembership())

			eng, handler, metrics := identityRig(t, recovered, true)

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
				t.Fatalf("under RequireMemberIdentity a fully migrated member was dropped "+
					"after snapshot recovery (reason %q): the shard loses quorum on every "+
					"restart until the identities survive the snapshot round-trip",
					metrics.lastReason())
			}
		})
	}
}

// recoveryBackend is one LogDB implementation plus the strongest restart it
// can be put through. Both are exercised through the SAME assertions: a field
// preserved by the backend tests use while the backend production uses drops
// it is the failure mode this pairing exists to prevent.
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
