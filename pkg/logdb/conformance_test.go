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

// Conformance suite pinning the LogDB interface contract against every
// implementation (memdb and waldb). It guards the contract items that
// previously diverged: IterateEntries error semantics and maxSize budget
// accounting, EntryRange span semantics, snapshot persistence through
// SaveState, and Cmd deep-copy on SaveState.

package logdb_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
)

// backends enumerates every LogDB implementation under conformance test.
func backends(t *testing.T) map[string]func(t *testing.T) logdb.LogDB {
	t.Helper()
	return map[string]func(t *testing.T) logdb.LogDB{
		"memdb": func(t *testing.T) logdb.LogDB {
			db := memdb.New()
			t.Cleanup(func() { _ = db.Close() })
			return db
		},
		"waldb": func(t *testing.T) logdb.LogDB {
			db, err := waldb.Open(t.TempDir(), waldb.WithNoSync(true))
			if err != nil {
				t.Fatalf("waldb.Open: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })
			return db
		},
	}
}

// forEachBackend runs fn as a subtest against every implementation.
func forEachBackend(t *testing.T, fn func(t *testing.T, db logdb.LogDB)) {
	for name, open := range backends(t) {
		t.Run(name, func(t *testing.T) {
			fn(t, open(t))
		})
	}
}

// reopener owns one backend instance and can put it through the strongest
// round-trip that implementation supports.
//
// The reopen step is the entire value of this harness. waldb answers
// GetSnapshot and ReadState out of an in-memory map that SaveSnapshot fills
// from the CALLER'S value, so a save/read pair inside one process never
// reaches the record encoding at all — it would pass identically against a
// backend that serialized nothing. Only closing the database and replaying the
// segments proves a field reached the disk.
//
// memdb has no on-disk form, so Reopen is the identity there. That is not a
// weaker assertion smuggled in: the copy path it does exercise is memdb's
// whole round-trip, and running both backends through ONE set of assertions is
// what stops a field being preserved by the implementation tests use while the
// implementation production uses drops it.
type reopener struct {
	db     logdb.LogDB
	reopen func(t *testing.T, db logdb.LogDB) logdb.LogDB
}

// DB returns the live instance, which Reopen replaces.
func (r *reopener) DB() logdb.LogDB { return r.db }

// Reopen closes the current instance and returns a freshly recovered one.
func (r *reopener) Reopen(t *testing.T) logdb.LogDB {
	t.Helper()
	r.db = r.reopen(t, r.db)
	return r.db
}

// forEachReopenableBackend runs fn as a subtest against every implementation,
// handing it a reopener rather than a bare LogDB.
func forEachReopenableBackend(t *testing.T, fn func(t *testing.T, r *reopener)) {
	t.Run("memdb", func(t *testing.T) {
		r := &reopener{
			db: memdb.New(),
			reopen: func(_ *testing.T, db logdb.LogDB) logdb.LogDB {
				return db
			},
		}
		t.Cleanup(func() { _ = r.db.Close() })
		fn(t, r)
	})

	t.Run("waldb", func(t *testing.T) {
		dir := t.TempDir()
		open := func(t *testing.T) logdb.LogDB {
			t.Helper()
			db, err := waldb.Open(dir, waldb.WithNoSync(true))
			if err != nil {
				t.Fatalf("waldb.Open: %v", err)
			}
			return db
		}
		r := &reopener{
			db: open(t),
			reopen: func(t *testing.T, db logdb.LogDB) logdb.LogDB {
				t.Helper()
				if err := db.Close(); err != nil {
					t.Fatalf("Close before reopen: %v", err)
				}
				return open(t)
			},
		}
		t.Cleanup(func() { _ = r.db.Close() })
		fn(t, r)
	})
}

// identityMembership is a committed configuration of the shape the
// receive-side sender gate authorizes against: two voters, one observer, one
// removal tombstone, and node identities spanning more than one role. It also
// carries a committed session policy, whose three values are deliberately
// distinct from each other and from every default so a backend that dropped one
// and defaulted it is caught.
func identityMembership() logdb.Membership {
	return logdb.Membership{
		ConfigChangeID:             11,
		MaxWitnesses:               1,
		RequireFullReplicaMajority: true,
		SessionExpiryEntries:       12345,
		MaxTotalSessions:           4321,
		MaxSessionsPerClient:       77,
		MaxSessionsPerOwner:        33,
		Addresses:                  map[uint64]string{1: "10.0.0.1:5001", 2: "10.0.0.2:5001"},
		Observers:                  map[uint64]string{3: "10.0.0.3:5001"},
		Witnesses:                  map[uint64]string{4: "10.0.0.4:5001"},
		Removed:                    map[uint64]bool{9: true},
		Identities: map[uint64]string{
			1: "spki:1111111111111111111111111111111111111111111111111111111111111111",
			2: "spki:2222222222222222222222222222222222222222222222222222222222222222",
			3: "spki:3333333333333333333333333333333333333333333333333333333333333333",
		},
		// Replica 9 is the tombstone in Removed above; this is the identity the
		// configuration held for it when the removal committed. The two are
		// deliberately consistent — RemovedIdentities' key set is a strict
		// subset of Removed's, so a fixture where they disagree would not be a
		// state the apply path can produce.
		RemovedIdentities: map[uint64]string{
			9: "spki:9999999999999999999999999999999999999999999999999999999999999999",
		},
	}
}

// assertIdentities fails unless got holds exactly want.
func assertIdentities(t *testing.T, where string, got, want map[uint64]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: Identities = %v (%d entries), want %v (%d entries).\n"+
			"The committed node identity is the receive-side gate's ONLY "+
			"authorization input where it is present, so a backend that drops it "+
			"silently downgrades every member to the address check — and under "+
			"RequireMemberIdentity it costs the shard its quorum instead.\n"+
			"If this fails for waldb only, the field did not reach the record "+
			"encoding: its snapshot record carries its OWN membership section "+
			"(pkg/logdb/waldb/db.go, membershipSectionVersion), independent of "+
			"proto.Membership, and it must marshal the map in "+
			"walShard.writeSnapshot, count it in membershipSize, and read it back "+
			"in walShard.replaySnapshot ahead of that function's trailing-bytes "+
			"check.",
			where, got, len(got), want, len(want))
	}
	for id, identity := range want {
		if got[id] != identity {
			t.Fatalf("%s: Identities[%d] = %q, want %q", where, id, got[id], identity)
		}
	}
}

// TestConformanceSnapshotMembershipIdentitiesSurviveReopen pins the field the
// whole identity binding depends on surviving the snapshot-metadata round-trip.
//
// Identities reach membership through the LOG — the EntryInitialMembership at
// index 1 and every EntryConfigChange — so a replica whose log has been
// compacted past those entries has this record and nothing else to relearn them
// from. A backend that drops the field does not fail: the replica recovers a
// configuration that still names every member, and quietly authorizes all of
// them by address, which is the forgery the identity binding exists to close.
//
// Asserting only "no error" would pass against exactly that. The assertion is
// the map's CONTENT after the round-trip.
func TestConformanceSnapshotMembershipIdentitiesSurviveReopen(t *testing.T) {
	forEachReopenableBackend(t, func(t *testing.T, r *reopener) {
		want := identityMembership()

		if err := r.DB().SaveState([]logdb.Update{{
			ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 4, Vote: 1, Commit: 10},
		}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}
		if err := r.DB().SaveSnapshot(1, 1, logdb.Snapshot{
			Index: 10, Term: 4, Filepath: "snap-10.dat", Membership: want,
		}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}

		db := r.Reopen(t)

		snap, err := db.GetSnapshot(1, 1)
		if err != nil {
			t.Fatalf("GetSnapshot after reopen: %v", err)
		}
		assertIdentities(t, "GetSnapshot", snap.Membership.Identities, want.Identities)

		// The recovery path reads the membership back through ReadState too
		// (host.recoverLogReader step 2), so both exits must carry the field.
		_, membership, err := db.ReadState(1, 1)
		if err != nil {
			t.Fatalf("ReadState after reopen: %v", err)
		}
		assertIdentities(t, "ReadState", membership.Identities, want.Identities)

		// The neighbouring fields must be unharmed by the added map.
		if membership.ConfigChangeID != want.ConfigChangeID ||
			membership.MaxWitnesses != want.MaxWitnesses ||
			!membership.RequireFullReplicaMajority ||
			membership.Addresses[2] != want.Addresses[2] ||
			membership.Observers[3] != want.Observers[3] ||
			membership.Witnesses[4] != want.Witnesses[4] ||
			!membership.Removed[9] {
			t.Fatalf("membership round-trip lost a neighbouring field: %+v", membership)
		}
	})
}

// TestConformanceSnapshotMembershipRemovedIdentitiesSurviveReopen pins the
// tombstone half of the removal deny-list across a restart.
//
// Removal authorization is evaluated against the AUTHENTICATED connection
// identity, because the claimed msg.From is a value a removed replica can
// simply change — it still holds its private key, so it authenticates normally
// and evades a tombstone keyed on the ID it chooses to name. RemovedIdentities
// is what makes the deny-list answerable about the identity instead.
//
// Dropping it across this round-trip is not an information loss, it REVOKES A
// REVOCATION. The replica comes back holding the tombstone replica IDs (Removed
// survives) but not the identities they were recorded against, so a
// decommissioned node still carrying its certificate is readmitted — silently,
// with no error raised anywhere, and precisely on the restart an operator
// performs after removing it. Certificate revocation is the only control left
// at that point, and it is opt-in and soft-fail by default.
//
// Asserting only "no error" would pass against exactly that. The assertion is
// the map's CONTENT after the reopen.
func TestConformanceSnapshotMembershipRemovedIdentitiesSurviveReopen(t *testing.T) {
	forEachReopenableBackend(t, func(t *testing.T, r *reopener) {
		want := identityMembership()

		if err := r.DB().SaveState([]logdb.Update{{
			ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 4, Vote: 1, Commit: 10},
		}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}
		if err := r.DB().SaveSnapshot(1, 1, logdb.Snapshot{
			Index: 10, Term: 4, Filepath: "snap-10.dat", Membership: want,
		}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}

		db := r.Reopen(t)

		snap, err := db.GetSnapshot(1, 1)
		if err != nil {
			t.Fatalf("GetSnapshot after reopen: %v", err)
		}
		assertRemovedIdentities(t, "GetSnapshot",
			snap.Membership.RemovedIdentities, want.RemovedIdentities)

		// recoverLogReader reads the membership back through ReadState as well,
		// so both exits must carry the field or the gap reappears on the path
		// an actual restart takes.
		_, membership, err := db.ReadState(1, 1)
		if err != nil {
			t.Fatalf("ReadState after reopen: %v", err)
		}
		assertRemovedIdentities(t, "ReadState",
			membership.RemovedIdentities, want.RemovedIdentities)

		// The tombstone ID and its identity must survive TOGETHER. Keeping
		// Removed while losing RemovedIdentities is the specific inconsistent
		// state this test exists to refuse.
		if !membership.Removed[9] {
			t.Fatalf("Removed[9] was lost while RemovedIdentities survived: %+v", membership)
		}
		// The active identity map must not have absorbed the tombstone: an
		// identity in Identities is an ALLOW, and moving a removal into it
		// would readmit exactly the replica the tombstone ejects.
		if _, ok := membership.Identities[9]; ok {
			t.Fatalf("the removed replica's identity reappeared in Identities, "+
				"which is the active binding: %+v", membership.Identities)
		}
	})
}

// assertRemovedIdentities fails unless got holds exactly want.
func assertRemovedIdentities(t *testing.T, where string, got, want map[uint64]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: RemovedIdentities = %v (%d entries), want %v (%d entries).\n"+
			"This map is the deny-list the receive gate evaluates against the "+
			"AUTHENTICATED connection identity. Losing it readmits a removed "+
			"replica that still holds its certificate, on the restart that "+
			"follows the removal.\n"+
			"If this fails for waldb only, the field did not reach the record "+
			"encoding: waldb's snapshot record carries its OWN membership "+
			"section (membershipSectionVersion, currently v5), independent of "+
			"proto.Membership. It must be counted in membershipSize, marshaled "+
			"in walShard.writeSnapshot AFTER the v4 session-policy tail (so v3 "+
			"and v4 sections keep their existing offsets), and read back in "+
			"walShard.replaySnapshot under a sectionVersion >= v5 guard, ahead "+
			"of that function's trailing-bytes check.",
			where, got, len(got), want, len(want))
	}
	for id, identity := range want {
		if got[id] != identity {
			t.Fatalf("%s: RemovedIdentities[%d] = %q, want %q", where, id, got[id], identity)
		}
	}
}

// TestConformanceSnapshotMembershipDoesNotAliasCallerIdentities pins that a
// backend STORES the identity map rather than borrowing it.
//
// This is a separate assertion from the round-trip above, and it has to be:
// an implementation that merely retains the caller's map passes that one
// completely, because the values are still there to read back. What it does
// not survive is the caller reusing or mutating the map afterwards, at which
// point committed authorization state is rewritten by a layer that does not
// own it — and identities are the input the receive-side gate trusts for every
// quorum-bearing message. It is the same rule Entry.Cmd is held to, for the
// same reason.
//
// waldb de-aliases for free because it serializes; memdb has to clone. Running
// both through this assertion is what keeps the in-memory backend honest.
func TestConformanceSnapshotMembershipDoesNotAliasCallerIdentities(t *testing.T) {
	forEachReopenableBackend(t, func(t *testing.T, r *reopener) {
		want := identityMembership()
		// The map handed to the backend, which the caller keeps owning.
		callerMap := map[uint64]string{}
		for id, identity := range want.Identities {
			callerMap[id] = identity
		}
		stored := identityMembership()
		stored.Identities = callerMap

		if err := r.DB().SaveState([]logdb.Update{{
			ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 4, Vote: 1, Commit: 10},
		}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}
		if err := r.DB().SaveSnapshot(1, 1, logdb.Snapshot{
			Index: 10, Term: 4, Membership: stored,
		}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}

		// The caller reuses its map after the call returns.
		callerMap[2] = "spki:tampered"
		callerMap[7] = "spki:injected"
		delete(callerMap, 1)

		db := r.Reopen(t)
		snap, err := db.GetSnapshot(1, 1)
		if err != nil {
			t.Fatalf("GetSnapshot: %v", err)
		}
		assertIdentities(t, "GetSnapshot after caller mutation",
			snap.Membership.Identities, want.Identities)

		_, membership, err := db.ReadState(1, 1)
		if err != nil {
			t.Fatalf("ReadState: %v", err)
		}
		assertIdentities(t, "ReadState after caller mutation",
			membership.Identities, want.Identities)
	})
}

// TestConformanceMembershipWithoutIdentitiesRoundTrips pins the COMPATIBILITY
// half: a record written before identities existed, or by an operator who
// supplied none, must load as a membership with no identities rather than
// failing or manufacturing entries.
//
// An absent identity is a compatibility state that falls back to the committed
// address, never a permission, so the readable observable is an EMPTY map that
// is safe to look up.
func TestConformanceMembershipWithoutIdentitiesRoundTrips(t *testing.T) {
	forEachReopenableBackend(t, func(t *testing.T, r *reopener) {
		legacy := logdb.Membership{
			ConfigChangeID: 3,
			Addresses:      map[uint64]string{1: "10.0.0.1:5001", 2: "10.0.0.2:5001"},
		}

		if err := r.DB().SaveState([]logdb.Update{{
			ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1, Vote: 1, Commit: 2},
		}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}
		if err := r.DB().SaveSnapshot(1, 1, logdb.Snapshot{
			Index: 2, Term: 1, Membership: legacy,
		}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}

		db := r.Reopen(t)

		snap, err := db.GetSnapshot(1, 1)
		if err != nil {
			t.Fatalf("GetSnapshot after reopen: %v", err)
		}
		if len(snap.Membership.Identities) != 0 {
			t.Fatalf("Identities = %v, want none: an identity is never invented, "+
				"only committed", snap.Membership.Identities)
		}
		// A lookup on the recovered map must be usable, not a trap: the gate
		// reads it for every member on every quorum-bearing message.
		if identity := snap.Membership.Identities[1]; identity != "" {
			t.Fatalf("Identities[1] = %q, want the empty string", identity)
		}
		if snap.Membership.Addresses[2] != "10.0.0.2:5001" {
			t.Fatalf("Addresses = %v, want the legacy addresses intact",
				snap.Membership.Addresses)
		}
	})
}

// assertSessionPolicy fails unless got carries exactly the four replicated
// session limits want does.
func assertSessionPolicy(t *testing.T, where string, got, want logdb.Membership) {
	t.Helper()
	if got.SessionExpiryEntries == want.SessionExpiryEntries &&
		got.MaxTotalSessions == want.MaxTotalSessions &&
		got.MaxSessionsPerClient == want.MaxSessionsPerClient &&
		got.MaxSessionsPerOwner == want.MaxSessionsPerOwner {
		return
	}
	t.Fatalf("%s: session policy = {expiry:%d total:%d perClient:%d perOwner:%d}, want "+
		"{expiry:%d total:%d perClient:%d perOwner:%d}.\n"+
		"These four drive the DETERMINISTIC apply path: the expiry threshold is "+
		"the interval of the apply checkpoint walk, and the three limits decide "+
		"which session registrations and cached responses are admitted. A backend "+
		"that drops them does not fail — the replica recovers, falls back to its "+
		"own Config, and expires sessions at log indexes its peers do not, which "+
		"is a state-machine divergence with every Raft invariant intact.\n"+
		"If this fails for waldb only, the fields did not reach the record "+
		"encoding: its snapshot record carries its OWN membership section "+
		"(pkg/logdb/waldb/db.go, membershipSectionVersion), independent of "+
		"proto.Membership, and the policy must be written in walShard.writeSnapshot, "+
		"counted in membershipSize, and read back in walShard.replaySnapshot ahead "+
		"of that function's trailing-bytes check.",
		where, got.SessionExpiryEntries, got.MaxTotalSessions, got.MaxSessionsPerClient,
		got.MaxSessionsPerOwner,
		want.SessionExpiryEntries, want.MaxTotalSessions, want.MaxSessionsPerClient,
		want.MaxSessionsPerOwner)
}

// TestConformanceSnapshotMembershipSessionPolicySurvivesReopen pins the
// replicated session policy against the snapshot-metadata round-trip.
//
// The policy reaches membership through the LOG — the EntryInitialMembership at
// index 1 — so a replica whose log has been compacted past that entry has this
// record and nothing else to relearn it from. That replica is exactly the one
// that must not fall back to local configuration: it is rejoining a running
// shard whose other replicas are already applying the committed policy.
//
// The assertion is the VALUES after the round-trip. Asserting only "no error"
// would pass against a backend that silently zeroed all three, which is the
// shape the failure actually takes.
func TestConformanceSnapshotMembershipSessionPolicySurvivesReopen(t *testing.T) {
	forEachReopenableBackend(t, func(t *testing.T, r *reopener) {
		want := identityMembership()

		if err := r.DB().SaveState([]logdb.Update{{
			ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 4, Vote: 1, Commit: 10},
		}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}
		if err := r.DB().SaveSnapshot(1, 1, logdb.Snapshot{
			Index: 10, Term: 4, Filepath: "snap-10.dat", Membership: want,
		}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}

		db := r.Reopen(t)

		snap, err := db.GetSnapshot(1, 1)
		if err != nil {
			t.Fatalf("GetSnapshot after reopen: %v", err)
		}
		assertSessionPolicy(t, "GetSnapshot", snap.Membership, want)

		// The recovery path reads the membership back through ReadState too
		// (host.recoverLogReader step 2), and that is the exit StartShard hands
		// to the peer, so both must carry the policy.
		_, membership, err := db.ReadState(1, 1)
		if err != nil {
			t.Fatalf("ReadState after reopen: %v", err)
		}
		assertSessionPolicy(t, "ReadState", membership, want)

		// The neighbouring fields must be unharmed by the added ones.
		if membership.ConfigChangeID != want.ConfigChangeID ||
			membership.MaxWitnesses != want.MaxWitnesses ||
			!membership.RequireFullReplicaMajority ||
			membership.Addresses[2] != want.Addresses[2] {
			t.Fatalf("membership round-trip lost a neighbouring field: %+v", membership)
		}
		assertIdentities(t, "ReadState", membership.Identities, want.Identities)
	})
}

// TestConformanceMembershipWithoutSessionPolicyRoundTrips is the COMPATIBILITY
// half: a record written before the policy was replicated must load with all
// four fields ZERO rather than failing or inventing limits.
//
// Zero is not a policy of zero. MaxTotalSessions can never legitimately be zero
// in a committed policy (config.Validate rejects it), which is what makes it
// the "no policy recorded" discriminator — proto.SessionPolicy.IsSet — and what
// lets an expiry threshold of zero keep its own meaning of "expiry explicitly
// disabled cluster-wide". A backend that manufactured defaults here would tell
// the apply path a shard had committed limits it never did.
func TestConformanceMembershipWithoutSessionPolicyRoundTrips(t *testing.T) {
	forEachReopenableBackend(t, func(t *testing.T, r *reopener) {
		legacy := logdb.Membership{
			ConfigChangeID: 3,
			Addresses:      map[uint64]string{1: "10.0.0.1:5001"},
		}

		if err := r.DB().SaveState([]logdb.Update{{
			ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1, Vote: 1, Commit: 2},
		}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}
		if err := r.DB().SaveSnapshot(1, 1, logdb.Snapshot{
			Index: 2, Term: 1, Membership: legacy,
		}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}

		db := r.Reopen(t)

		snap, err := db.GetSnapshot(1, 1)
		if err != nil {
			t.Fatalf("GetSnapshot after reopen: %v", err)
		}
		assertSessionPolicy(t, "GetSnapshot", snap.Membership, legacy)
		if snap.Membership.Addresses[1] != "10.0.0.1:5001" {
			t.Fatalf("Addresses = %v, want the legacy address intact",
				snap.Membership.Addresses)
		}
	})
}

// saveEntries persists sequential entries [start, start+count) with the
// given per-entry Cmd size.
func saveEntries(t *testing.T, db logdb.LogDB, shardID, replicaID, start, count uint64, cmdSize int) {
	t.Helper()
	entries := make([]logdb.Entry, count)
	for i := uint64(0); i < count; i++ {
		entries[i] = logdb.Entry{Index: start + i, Term: 1, Cmd: make([]byte, cmdSize)}
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID, Entries: entries,
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
}

func TestConformanceIterateEntriesRoundTrip(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		saveEntries(t, db, 1, 1, 1, 5, 8)

		got, size, err := db.IterateEntries(nil, 0, 1, 1, 1, 6, 0)
		if err != nil {
			t.Fatalf("IterateEntries: %v", err)
		}
		if len(got) != 5 {
			t.Fatalf("got %d entries, want 5", len(got))
		}
		wantSize := uint64(5 * (logdb.EntryOverheadBytes + 8))
		if size != wantSize {
			t.Fatalf("size = %d, want %d (EntryOverheadBytes budget)", size, wantSize)
		}
		for i, e := range got {
			if e.Index != uint64(i+1) {
				t.Fatalf("entry %d has index %d, want %d", i, e.Index, i+1)
			}
		}
	})
}

func TestConformanceIterateEntriesUnknownNode(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		if _, _, err := db.IterateEntries(nil, 0, 99, 99, 1, 5, 0); !errors.Is(err, logdb.ErrUnavailable) {
			t.Fatalf("IterateEntries unknown node = %v, want ErrUnavailable", err)
		}
	})
}

func TestConformanceIterateEntriesEmptyRangeIsNoop(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		got, size, err := db.IterateEntries(nil, 7, 99, 99, 5, 5, 0)
		if err != nil {
			t.Fatalf("IterateEntries low>=high = %v, want nil", err)
		}
		if len(got) != 0 || size != 7 {
			t.Fatalf("IterateEntries low>=high = (%d entries, size %d), want (0, 7)", len(got), size)
		}
	})
}

func TestConformanceIterateEntriesRangeWithoutEntries(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		saveEntries(t, db, 1, 1, 1, 3, 4)
		// The node exists but [10, 12) holds nothing.
		if _, _, err := db.IterateEntries(nil, 0, 1, 1, 10, 12, 0); !errors.Is(err, logdb.ErrUnavailable) {
			t.Fatalf("IterateEntries beyond last = %v, want ErrUnavailable", err)
		}
	})
}

func TestConformanceCompactionFloorReturnsErrCompacted(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		saveEntries(t, db, 1, 1, 1, 10, 4)
		if err := db.RemoveEntriesTo(1, 1, 4); err != nil {
			t.Fatalf("RemoveEntriesTo: %v", err)
		}

		if _, _, err := db.IterateEntries(nil, 0, 1, 1, 3, 11, 0); !errors.Is(err, logdb.ErrCompacted) {
			t.Fatalf("IterateEntries below floor = %v, want ErrCompacted", err)
		}
		got, _, err := db.IterateEntries(nil, 0, 1, 1, 5, 11, 0)
		if err != nil {
			t.Fatalf("IterateEntries above floor: %v", err)
		}
		if len(got) != 6 || got[0].Index != 5 {
			t.Fatalf("live suffix = %d entries starting %d, want 6 starting 5", len(got), got[0].Index)
		}
	})
}

// TestConformanceCompactionFloorIsMonotonic pins the floor's monotonicity: a
// lower RemoveEntriesTo call must not lower a floor whose entries are already
// gone, or reads between the two floors surface a hard log hole instead of
// ErrCompacted and raft never falls back to InstallSnapshot.
func TestConformanceCompactionFloorIsMonotonic(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		saveEntries(t, db, 1, 1, 1, 20, 4)
		if err := db.RemoveEntriesTo(1, 1, 15); err != nil {
			t.Fatalf("RemoveEntriesTo(15): %v", err)
		}
		if err := db.RemoveEntriesTo(1, 1, 5); err != nil {
			t.Fatalf("RemoveEntriesTo(5): %v", err)
		}

		if _, _, err := db.IterateEntries(nil, 0, 1, 1, 10, 21, 0); !errors.Is(err, logdb.ErrCompacted) {
			t.Fatalf("IterateEntries(low=10) = %v, want ErrCompacted", err)
		}
		got, _, err := db.IterateEntries(nil, 0, 1, 1, 16, 21, 0)
		if err != nil {
			t.Fatalf("IterateEntries above the floor: %v", err)
		}
		if len(got) != 5 || got[0].Index != 16 {
			t.Fatalf("live suffix = %d entries starting %d, want 5 starting 16", len(got), got[0].Index)
		}
	})
}

// TestConformanceHeadGapReturnsErrLogGapDetected pins the distinction between
// a compaction and a missing prefix: with no compaction floor, a low below the
// first stored entry is a GAP.
func TestConformanceHeadGapReturnsErrLogGapDetected(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		saveEntries(t, db, 1, 1, 5, 3, 4)
		if _, _, err := db.IterateEntries(nil, 0, 1, 1, 3, 8, 0); !errors.Is(err, logdb.ErrLogGapDetected) {
			t.Fatalf("IterateEntries(low=3) = %v, want ErrLogGapDetected", err)
		}
	})
}

// TestConformanceSaveSnapshotMembershipVisibleInReadState pins that the
// membership a snapshot carries becomes the node's membership, whichever entry
// point stored the snapshot.
func TestConformanceSaveSnapshotMembershipVisibleInReadState(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		if err := db.SaveState([]logdb.Update{{
			ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 2, Vote: 1, Commit: 5},
		}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}
		if err := db.SaveSnapshot(1, 1, logdb.Snapshot{
			Index: 5, Term: 2,
			Membership: logdb.Membership{
				ConfigChangeID: 9,
				Addresses:      map[uint64]string{1: "a1"},
			},
		}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}

		_, membership, err := db.ReadState(1, 1)
		if err != nil {
			t.Fatalf("ReadState: %v", err)
		}
		if membership.ConfigChangeID != 9 || membership.Addresses[1] != "a1" {
			t.Fatalf("ReadState membership = %+v, want the snapshot's membership", membership)
		}
	})
}

// TestConformanceZeroIndexSnapshotIsAbsent pins the snapshot-presence
// predicate on Index > 0: a zero-index snapshot is no snapshot at all.
func TestConformanceZeroIndexSnapshotIsAbsent(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		if err := db.SaveState([]logdb.Update{{
			ShardID: 1, ReplicaID: 1,
			State:    logdb.State{Term: 5, Vote: 1},
			Snapshot: logdb.Snapshot{Index: 0, Term: 5},
		}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}
		if _, err := db.GetSnapshot(1, 1); !errors.Is(err, logdb.ErrNoSnapshot) {
			t.Fatalf("GetSnapshot = %v, want ErrNoSnapshot", err)
		}
	})
}

func TestConformanceMaxSizeBudget(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		const cmdSize = 100
		perEntry := uint64(logdb.EntryOverheadBytes + cmdSize)
		saveEntries(t, db, 1, 1, 1, 10, cmdSize)

		// Budget for exactly two entries plus slack smaller than a third:
		// iteration stops BEFORE the entry that would exceed the budget.
		got, size, err := db.IterateEntries(nil, 0, 1, 1, 1, 11, 2*perEntry+10)
		if err != nil {
			t.Fatalf("IterateEntries: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d entries under 2-entry budget, want 2", len(got))
		}
		if size != 2*perEntry {
			t.Fatalf("size = %d, want %d", size, 2*perEntry)
		}

		// A budget below a single entry still returns at least one entry.
		got, _, err = db.IterateEntries(nil, 0, 1, 1, 1, 11, 1)
		if err != nil {
			t.Fatalf("IterateEntries tiny budget: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("got %d entries under tiny budget, want 1", len(got))
		}
	})
}

func TestConformanceEntryRangeSpan(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		first, length, err := db.EntryRange(1, 1)
		if err != nil || first != 0 || length != 0 {
			t.Fatalf("EntryRange empty = (%d, %d, %v), want (0, 0, nil)", first, length, err)
		}

		saveEntries(t, db, 1, 1, 3, 5, 4)
		first, length, err = db.EntryRange(1, 1)
		if err != nil {
			t.Fatalf("EntryRange: %v", err)
		}
		if first != 3 || length != 5 {
			t.Fatalf("EntryRange = (%d, %d), want (3, 5)", first, length)
		}
	})
}

func TestConformanceSaveStatePersistsSnapshot(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		snap := logdb.Snapshot{
			Index:    10,
			Term:     3,
			Filepath: "snap-10.dat",
			Membership: logdb.Membership{
				ConfigChangeID: 7,
				Addresses:      map[uint64]string{1: "a1"},
			},
		}
		if err := db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1, Snapshot: snap}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}

		got, err := db.GetSnapshot(1, 1)
		if err != nil {
			t.Fatalf("GetSnapshot after SaveState: %v", err)
		}
		if got.Index != 10 || got.Term != 3 || got.Filepath != "snap-10.dat" {
			t.Fatalf("GetSnapshot = %+v, want the SaveState snapshot", got)
		}

		// A stale snapshot in a later Update must not regress it.
		stale := logdb.Snapshot{Index: 5, Term: 2}
		if err := db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1, Snapshot: stale}}); err != nil {
			t.Fatalf("SaveState stale: %v", err)
		}
		got, err = db.GetSnapshot(1, 1)
		if err != nil {
			t.Fatalf("GetSnapshot: %v", err)
		}
		if got.Index != 10 {
			t.Fatalf("GetSnapshot.Index = %d after stale update, want 10", got.Index)
		}
	})
}

func TestConformanceSaveStateDeepCopiesCmd(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		buf := []byte("payload-owned-by-caller")
		if err := db.SaveState([]logdb.Update{{
			ShardID: 1, ReplicaID: 1,
			Entries: []logdb.Entry{{Index: 1, Term: 1, Cmd: buf}},
		}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}

		// Caller reuses its buffer after SaveState returns.
		copy(buf, "XXXXXXXXXX")

		got, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 2, 0)
		if err != nil {
			t.Fatalf("IterateEntries: %v", err)
		}
		if len(got) != 1 || !bytes.Equal(got[0].Cmd, []byte("payload-owned-by-caller")) {
			t.Fatalf("stored Cmd = %q, want %q (caller buffer aliased)",
				got[0].Cmd, "payload-owned-by-caller")
		}
	})
}

func TestConformanceClosedErrors(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 2, 0); !errors.Is(err, logdb.ErrClosed) {
			t.Fatalf("IterateEntries after Close = %v, want ErrClosed", err)
		}
		if _, _, err := db.EntryRange(1, 1); !errors.Is(err, logdb.ErrClosed) {
			t.Fatalf("EntryRange after Close = %v, want ErrClosed", err)
		}
		if err := db.SaveState(nil); !errors.Is(err, logdb.ErrClosed) {
			t.Fatalf("SaveState after Close = %v, want ErrClosed", err)
		}
	})
}

// TestConformanceIterateEntriesDoesNotAliasStoredPayloads pins the read-side
// half of the no-aliasing rule.
//
// The write-side half has always been asserted: SaveState deep-copies Cmd, so a
// caller reusing its buffers cannot rewrite the stored log. The read side had
// no such rule and no such test. Both backends served the stored Entry by
// value, which copies the slice HEADER and not the bytes, so the caller and the
// store shared one payload array — and what a caller writing through it would
// be corrupting is the live in-memory log this replica serves to its followers
// and replays to itself on restart. Nothing would report it, nothing would
// attribute it, and every replica would corrupt differently.
//
// The test writes through the returned payload and then re-reads. A store that
// aliases hands back the mutated bytes the second time.
func TestConformanceIterateEntriesDoesNotAliasStoredPayloads(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		const shardID, replicaID = uint64(1), uint64(1)

		want := []byte("the-committed-command-bytes")
		entries := []logdb.Entry{
			{Index: 1, Term: 1, Cmd: append([]byte(nil), want...)},
			{Index: 2, Term: 1, Cmd: append([]byte(nil), want...)},
		}
		if err := db.SaveState([]logdb.Update{{
			ShardID: shardID, ReplicaID: replicaID, Entries: entries,
		}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}

		first, _, err := db.IterateEntries(nil, 0, shardID, replicaID, 1, 3, 0)
		if err != nil {
			t.Fatalf("IterateEntries: %v", err)
		}
		if len(first) != 2 {
			t.Fatalf("IterateEntries returned %d entries, want 2", len(first))
		}

		// The caller owns what it was handed, and does what an owner may do.
		for i := range first {
			for j := range first[i].Cmd {
				first[i].Cmd[j] = 'X'
			}
		}

		second, _, err := db.IterateEntries(nil, 0, shardID, replicaID, 1, 3, 0)
		if err != nil {
			t.Fatalf("IterateEntries (re-read): %v", err)
		}
		if len(second) != 2 {
			t.Fatalf("re-read returned %d entries, want 2", len(second))
		}
		for _, e := range second {
			if !bytes.Equal(e.Cmd, want) {
				t.Fatalf("entry %d came back as %q after the CALLER wrote through the "+
					"payload of an earlier read; the store handed out a slice aliasing "+
					"its own log, so a caller mutating what it was given rewrites the "+
					"committed entries this replica serves to followers and replays on "+
					"restart. want %q", e.Index, e.Cmd, want)
			}
		}

		// Two reads of the same entry must not share memory with each other
		// either: a caller holding results from two calls may write through one.
		if len(first[0].Cmd) > 0 && len(second[0].Cmd) > 0 &&
			&first[0].Cmd[0] == &second[0].Cmd[0] {
			t.Fatal("two separate reads of the same entry returned the SAME backing " +
				"array; writing through one result silently rewrites the other")
		}
	})
}

// TestConformanceGetEntriesDoesNotAliasStoredPayloads is the same rule for
// waldb's GetEntries convenience wrapper, which is the call the apply path
// actually uses. It shares IterateEntries' implementation, so this guards
// against the wrapper acquiring its own path later.
func TestConformanceGetEntriesDoesNotAliasStoredPayloads(t *testing.T) {
	dir := t.TempDir()
	db, err := waldb.Open(dir, waldb.WithNoSync(true))
	if err != nil {
		t.Fatalf("waldb.Open: %v", err)
	}
	defer db.Close()

	const shardID, replicaID = uint64(3), uint64(1)
	want := []byte("apply-path-command")
	if err := db.SaveEntries(shardID, replicaID, []logdb.Entry{
		{Index: 1, Term: 1, Cmd: append([]byte(nil), want...)},
	}); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	got, err := db.GetEntries(shardID, replicaID, 1, 2, 0)
	if err != nil || len(got) != 1 {
		t.Fatalf("GetEntries: %d entries, err %v", len(got), err)
	}
	for j := range got[0].Cmd {
		got[0].Cmd[j] = 'Z'
	}

	again, err := db.GetEntries(shardID, replicaID, 1, 2, 0)
	if err != nil || len(again) != 1 {
		t.Fatalf("GetEntries (re-read): %d entries, err %v", len(again), err)
	}
	if !bytes.Equal(again[0].Cmd, want) {
		t.Fatalf("the stored entry came back as %q after a caller wrote through an "+
			"earlier GetEntries result; the apply path aliases the live log", again[0].Cmd)
	}
}

// TestConformanceSaveStateBelowCompactionFloorIsRefused pins the rejection both
// backends now perform, and the state that made it necessary.
//
// The measured behaviour before the fix, identical in memdb and waldb: with
// entries [10..20] stored and the floor at 9, a SaveState starting at index 5
// returned nil, DESTROYED the live suffix [10..20] (truncate-and-append
// replaces everything from the first new index onward), and left the store
// describing itself inconsistently — EntryRange answered (5, 2) while
// IterateEntries(5, ...) refused the very range it had just advertised with
// ErrCompacted. Nothing reported any of it.
//
// A write below the floor is always a caller bug: those indexes are provably
// applied and already deleted, raft sends a lagging follower an InstallSnapshot
// rather than entries beneath its own floor, and a local proposer never
// proposes below its applied index. So it fails loudly, and — this is the part
// that matters — it fails without storing anything.
func TestConformanceSaveStateBelowCompactionFloorIsRefused(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		const shardID, replicaID = uint64(1), uint64(1)

		var live []logdb.Entry
		for i := uint64(10); i <= 20; i++ {
			live = append(live, logdb.Entry{Index: i, Term: 1, Cmd: []byte("live")})
		}
		if err := db.SaveState([]logdb.Update{{
			ShardID: shardID, ReplicaID: replicaID, Entries: live,
		}}); err != nil {
			t.Fatalf("SaveState(live): %v", err)
		}
		if err := db.RemoveEntriesTo(shardID, replicaID, 9); err != nil {
			t.Fatalf("RemoveEntriesTo(9): %v", err)
		}

		err := db.SaveState([]logdb.Update{{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Entries: []logdb.Entry{
				{Index: 5, Term: 2, Cmd: []byte("below-floor")},
				{Index: 6, Term: 2, Cmd: []byte("below-floor")},
			},
		}})
		if !errors.Is(err, logdb.ErrEntriesBelowCompactionFloor) {
			t.Fatalf("SaveState at index 5 with the floor at 9 = %v, want "+
				"ErrEntriesBelowCompactionFloor: index 5 is provably applied and "+
				"already deleted, and storing it replaces the live suffix [10,20] "+
				"with entries the store will then refuse to serve", err)
		}

		// The typed error must say WHICH write was refused, or the caller bug
		// can be observed but not located.
		var detail *logdb.EntriesBelowCompactionFloorError
		if !errors.As(err, &detail) {
			t.Fatalf("error carries no *EntriesBelowCompactionFloorError: %v", err)
		}
		if detail.ShardID != shardID || detail.ReplicaID != replicaID ||
			detail.FirstIndex != 5 || detail.CompactedTo != 9 {
			t.Fatalf("refusal detail = %+v, want shard %d replica %d first 5 floor 9",
				detail, shardID, replicaID)
		}

		// Nothing was stored, and the live suffix is untouched.
		first, span, err := db.EntryRange(shardID, replicaID)
		if err != nil {
			t.Fatalf("EntryRange after the refusal: %v", err)
		}
		if first != 10 || span != 11 {
			t.Fatalf("EntryRange = (%d, %d) after the refused write, want (10, 11): "+
				"the refused batch was partly stored and the live suffix is gone",
				first, span)
		}
		got, _, err := db.IterateEntries(nil, 0, shardID, replicaID, 10, 21, 0)
		if err != nil {
			t.Fatalf("IterateEntries(10,21) after the refusal: %v (the live suffix "+
				"the refused write would have destroyed is no longer readable)", err)
		}
		if len(got) != 11 {
			t.Fatalf("read %d live entries after the refusal, want 11", len(got))
		}
		for _, e := range got {
			if !bytes.Equal(e.Cmd, []byte("live")) {
				t.Fatalf("entry %d was overwritten by the refused batch: %q", e.Index, e.Cmd)
			}
		}

		// A write at the first index ABOVE the floor is still accepted, so the
		// guard is a boundary and not a blanket refusal.
		if err := db.SaveState([]logdb.Update{{
			ShardID: shardID, ReplicaID: replicaID,
			Entries: []logdb.Entry{{Index: 10, Term: 3, Cmd: []byte("legal")}},
		}}); err != nil {
			t.Fatalf("SaveState at index 10 (floor+1) = %v, want success: the guard "+
				"must reject at-or-below the floor, not above it", err)
		}
		// ...and exactly AT the floor is refused.
		if err := db.SaveState([]logdb.Update{{
			ShardID: shardID, ReplicaID: replicaID,
			Entries: []logdb.Entry{{Index: 9, Term: 3, Cmd: []byte("at-floor")}},
		}}); !errors.Is(err, logdb.ErrEntriesBelowCompactionFloor) {
			t.Fatalf("SaveState at index 9 (exactly the floor) = %v, want "+
				"ErrEntriesBelowCompactionFloor; index 9 was removed by the "+
				"compaction that set the floor", err)
		}
	})
}

// TestConformanceSaveStateBelowFloorRejectsTheWholeBatch pins the atomicity of
// the refusal.
//
// SaveState is specified to persist a batch atomically. A floor violation found
// part-way through must therefore reject the batch, not the update: storing the
// updates that came before it and refusing the rest would return an error
// describing a write that had partly happened, which no caller can unwind.
func TestConformanceSaveStateBelowFloorRejectsTheWholeBatch(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		const replicaID = uint64(1)
		const goodShard, badShard = uint64(1), uint64(2)

		// badShard gets a compaction floor; goodShard has none.
		if err := db.SaveState([]logdb.Update{{
			ShardID: badShard, ReplicaID: replicaID,
			Entries: []logdb.Entry{{Index: 10, Term: 1, Cmd: []byte("x")}},
		}}); err != nil {
			t.Fatalf("SaveState(seed): %v", err)
		}
		if err := db.RemoveEntriesTo(badShard, replicaID, 9); err != nil {
			t.Fatalf("RemoveEntriesTo: %v", err)
		}

		// goodShard's update is FIRST, so a per-update implementation stores it
		// before it ever reaches the violation.
		err := db.SaveState([]logdb.Update{
			{ShardID: goodShard, ReplicaID: replicaID,
				Entries: []logdb.Entry{{Index: 1, Term: 1, Cmd: []byte("first")}}},
			{ShardID: badShard, ReplicaID: replicaID,
				Entries: []logdb.Entry{{Index: 5, Term: 1, Cmd: []byte("below")}}},
		})
		if !errors.Is(err, logdb.ErrEntriesBelowCompactionFloor) {
			t.Fatalf("SaveState = %v, want ErrEntriesBelowCompactionFloor", err)
		}

		first, span, rerr := db.EntryRange(goodShard, replicaID)
		if rerr != nil {
			t.Fatalf("EntryRange(goodShard): %v", rerr)
		}
		if first != 0 || span != 0 {
			t.Fatalf("the shard listed BEFORE the offending update holds [%d,%d] "+
				"after a batch that returned an error: the refusal was applied per "+
				"update instead of per batch, so SaveState reported a failure for a "+
				"write that partly happened", first, first+span-1)
		}
	})
}

// TestConformanceCompactPreservesEverythingAboveTheFloor pins what Compact is
// allowed to touch, which nothing asserted in either backend.
//
// Compact reclaims storage the compaction floor has already made unreachable.
// It is NOT a delete: everything above the floor, plus the hard state, the
// snapshot record and the bootstrap record — none of which the floor covers at
// all — must survive it untouched. memdb implements it as a literal no-op and
// waldb as segment garbage collection, so the two can drift silently.
//
// The gap was measurable: rewriting memdb's Compact to drop the node's entries,
// hard state and snapshot left the whole memdb suite and the whole conformance
// suite green, because the only test of Compact asserted its return value.
// Losing the hard state that way is a lost vote, which permits a second vote in
// the same term.
func TestConformanceCompactPreservesEverythingAboveTheFloor(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		const shardID, replicaID = uint64(1), uint64(1)

		var entries []logdb.Entry
		for i := uint64(1); i <= 10; i++ {
			entries = append(entries, logdb.Entry{Index: i, Term: 2, Cmd: []byte("payload")})
		}
		state := logdb.State{Term: 2, Vote: 3, Commit: 10}
		if err := db.SaveState([]logdb.Update{{
			ShardID: shardID, ReplicaID: replicaID, State: state, Entries: entries,
		}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}
		if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
			Index: 4, Term: 2,
			Membership: logdb.Membership{Addresses: map[uint64]string{1: "a", 2: "b"}},
		}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		if err := db.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
			Addresses: map[uint64]string{1: "a"}, Type: 5,
		}); err != nil {
			t.Fatalf("SaveBootstrap: %v", err)
		}

		// Compact with NO floor set must reclaim nothing at all.
		if err := db.Compact(shardID, replicaID); err != nil {
			t.Fatalf("Compact (no floor): %v", err)
		}
		assertIntact := func(phase string, wantFirst uint64) {
			t.Helper()
			first, span, err := db.EntryRange(shardID, replicaID)
			if err != nil {
				t.Fatalf("%s: EntryRange: %v", phase, err)
			}
			if first != wantFirst || first+span-1 != 10 {
				t.Fatalf("%s: entries are [%d,%d] after Compact, want [%d,10]: Compact "+
					"reclaimed entries the compaction floor had not released",
					phase, first, first+span-1, wantFirst)
			}
			got, _, err := db.IterateEntries(nil, 0, shardID, replicaID, wantFirst, 11, 0)
			if err != nil {
				t.Fatalf("%s: IterateEntries: %v", phase, err)
			}
			if uint64(len(got)) != 11-wantFirst {
				t.Fatalf("%s: read %d entries, want %d", phase, len(got), 11-wantFirst)
			}
			for _, e := range got {
				if !bytes.Equal(e.Cmd, []byte("payload")) {
					t.Fatalf("%s: entry %d payload changed to %q", phase, e.Index, e.Cmd)
				}
			}
			st, _, err := db.ReadState(shardID, replicaID)
			if err != nil {
				t.Fatalf("%s: ReadState after Compact: %v (the hard state does not sit "+
					"below any compaction floor and must survive)", phase, err)
			}
			if st != state {
				t.Fatalf("%s: hard state after Compact = %+v, want %+v: losing a vote "+
					"this way permits a second vote in the same term", phase, st, state)
			}
			ss, err := db.GetSnapshot(shardID, replicaID)
			if err != nil || ss.Index != 4 {
				t.Fatalf("%s: snapshot after Compact = %+v, err %v; want index 4",
					phase, ss.Index, err)
			}
			bs, ok, err := db.GetBootstrap(shardID, replicaID)
			if err != nil || !ok || bs.Type != 5 {
				t.Fatalf("%s: bootstrap after Compact = %+v ok=%v err=%v", phase, bs, ok, err)
			}
		}
		assertIntact("no floor", 1)

		// With a floor at 4, Compact may release entries 1..4 and nothing else.
		if err := db.RemoveEntriesTo(shardID, replicaID, 4); err != nil {
			t.Fatalf("RemoveEntriesTo: %v", err)
		}
		if err := db.Compact(shardID, replicaID); err != nil {
			t.Fatalf("Compact (floor 4): %v", err)
		}
		assertIntact("floor 4", 5)
	})
}
