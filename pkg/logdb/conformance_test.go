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

// assertSessionPolicy fails unless got carries exactly the three replicated
// session limits want does.
func assertSessionPolicy(t *testing.T, where string, got, want logdb.Membership) {
	t.Helper()
	if got.SessionExpiryEntries == want.SessionExpiryEntries &&
		got.MaxTotalSessions == want.MaxTotalSessions &&
		got.MaxSessionsPerClient == want.MaxSessionsPerClient {
		return
	}
	t.Fatalf("%s: session policy = {expiry:%d total:%d perClient:%d}, want "+
		"{expiry:%d total:%d perClient:%d}.\n"+
		"These three drive the DETERMINISTIC apply path: the expiry threshold is "+
		"the interval of the apply checkpoint walk, and the two limits decide "+
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
		want.SessionExpiryEntries, want.MaxTotalSessions, want.MaxSessionsPerClient)
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
// three fields ZERO rather than failing or inventing limits.
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
