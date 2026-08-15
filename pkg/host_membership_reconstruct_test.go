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
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Host-side config-in-log reconstruction: what the index-1 entry has to carry
// back, and what happens when it cannot be read.
//
// reconstructShardMembership is the ONLY thing standing between a restart and
// StartShard's fall-back seed at host.go's `nothingRecovered` branch. That seed
// builds a Membership from the caller's members map, which carries VOTER
// ADDRESSES AND NOTHING ELSE — no witnesses, no observers, no Removed
// tombstones, no committed identities, and neither the replicated witness
// policy nor the replicated session policy. Every one of those is state the
// shard agreed on and this replica must not unilaterally revise, so each field
// is asserted individually below rather than through a whole-struct compare
// that would silently accept a new field it does not know about.

// fixedEntryLogDB serves one caller-supplied entry from the log-read path, so a
// test can hand reconstruction an exact index-1 payload — including one that
// cannot be decoded.
type fixedEntryLogDB struct {
	mockLogDB
	entry logdb.Entry
}

func (f *fixedEntryLogDB) IterateEntries(entries []logdb.Entry, size, shardID, replicaID, low, high, maxSize uint64) ([]logdb.Entry, uint64, error) {
	if low > f.entry.Index || high <= f.entry.Index {
		return entries, size, logdb.ErrUnavailable
	}
	return append(entries, f.entry), size, nil
}

// bootstrapLogDBFor returns a LogDB whose index-1 entry carries the given
// membership, encoded exactly as the bootstrap path encodes it.
func bootstrapLogDBFor(t *testing.T, ms proto.Membership) *fixedEntryLogDB {
	t.Helper()
	e, err := raft.BuildInitialMembershipEntry(ms)
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	return &fixedEntryLogDB{entry: logdb.Entry{
		Index: e.Index, Term: e.Term, Type: e.Type, Cmd: e.Cmd,
	}}
}

// readerOver returns a LogReader whose tracked range is [1, last] over ldb.
func readerOver(t *testing.T, ldb logdb.LogDB, last uint64) *logdb.LogReader {
	t.Helper()
	lr := logdb.NewLogReader(1, 1, ldb)
	if err := lr.SetRange(1, last); err != nil {
		t.Fatalf("SetRange: %v", err)
	}
	return lr
}

// TestReconstructShardMembership_PreservesNonVoterRolesAndPolicy is the
// survival test for everything the caller's members map cannot express. A
// restart that loses any of these computes a different quorum, or enforces a
// different policy, than the peers it is rejoining.
func TestReconstructShardMembership_PreservesNonVoterRolesAndPolicy(t *testing.T) {
	want := proto.Membership{
		Addresses:                  map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		Observers:                  map[uint64]string{7: "a7"},
		Witnesses:                  map[uint64]string{8: "a8"},
		Removed:                    map[uint64]bool{4: true},
		Identities:                 map[uint64]string{1: "id1", 8: "id8"},
		MaxWitnesses:               2,
		RequireFullReplicaMajority: true,
	}
	want.SetSessionPolicy(proto.SessionPolicy{
		ExpiryEntries:        9999,
		MaxTotalSessions:     64,
		MaxSessionsPerClient: 4,
	})

	ldb := bootstrapLogDBFor(t, want)
	got, ok, err := (&Host{}).reconstructShardMembership(readerOver(t, ldb, 12), 12)
	if err != nil {
		t.Fatalf("reconstructShardMembership: %v", err)
	}
	if !ok {
		t.Fatal("reconstruction did not find the bootstrap entry")
	}

	if len(got.Addresses) != 3 {
		t.Fatalf("voters = %v, want 3", got.Addresses)
	}
	if len(got.Observers) != 1 || got.Observers[7] != "a7" {
		t.Fatalf("observers = %v, want {7: a7}: an observer dropped by "+
			"reconstruction is re-seeded from the members map as a VOTER, so this "+
			"replica computes quorum over a different denominator than its peers",
			got.Observers)
	}
	if len(got.Witnesses) != 1 || got.Witnesses[8] != "a8" {
		t.Fatalf("witnesses = %v, want {8: a8}: witnesses count toward the "+
			"unified commit and election quorum", got.Witnesses)
	}
	if !got.Removed[4] {
		t.Fatalf("Removed tombstones = %v, want replica 4 tombstoned: the "+
			"tombstone set IS the per-shard DENY-list, and a restart that loses "+
			"it lets a removed replica re-join", got.Removed)
	}
	if got.Identities[1] != "id1" || got.Identities[8] != "id8" {
		t.Fatalf("identities = %v, want the committed identity map: it is what "+
			"the receive-side gate authenticates connections against",
			got.Identities)
	}
	if got.MaxWitnesses != 2 || !got.RequireFullReplicaMajority {
		t.Fatalf("witness policy = {%d, %v}, want {2, true}: the policy is "+
			"REPLICATED state and must not revert to this node's local config",
			got.MaxWitnesses, got.RequireFullReplicaMajority)
	}
	if sp := got.SessionPolicy(); sp.ExpiryEntries != 9999 ||
		sp.MaxTotalSessions != 64 || sp.MaxSessionsPerClient != 4 {
		t.Fatalf("session policy = %+v, want {9999, 64, 4}: it is replicated "+
			"state, so a reverted value expires different sessions at the same "+
			"log index on different replicas", sp)
	}
}

// TestReconstructShardMembership_CorruptBootstrapCmdFailsClosed pins the
// corrupt-payload path. An index-1 entry of the right TYPE whose Cmd cannot be
// decoded must abort StartShard, not decode to the zero Membership: an empty
// reconstruction is indistinguishable from "no bootstrap entry here" and sends
// the caller straight to the re-seed.
func TestReconstructShardMembership_CorruptBootstrapCmdFailsClosed(t *testing.T) {
	valid, err := raft.BuildInitialMembershipEntry(proto.Membership{
		Addresses: map[uint64]string{1: "a1", 2: "a2"},
	})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	cases := map[string][]byte{
		"foreign payload": []byte("this is not a marshaled membership"),
		"torn tail":       valid.Cmd[:len(valid.Cmd)-3],
		"empty cmd":       nil,
	}
	for name, cmd := range cases {
		t.Run(name, func(t *testing.T) {
			ldb := &fixedEntryLogDB{entry: logdb.Entry{
				Index: raft.BootstrapMembershipIndex,
				Term:  1,
				Type:  proto.EntryInitialMembership,
				Cmd:   cmd,
			}}
			ms, ok, err := (&Host{}).reconstructShardMembership(readerOver(t, ldb, 5), 5)
			if err == nil {
				t.Fatalf("reconstruction returned (ok=%v, membership=%+v) with NO "+
					"error over an undecodable bootstrap entry: StartShard continues "+
					"and re-seeds membership from the caller's members map, dropping "+
					"witnesses, observers, Removed tombstones and the replicated "+
					"witness/session policy", ok, ms)
			}
			if ok {
				t.Fatal("reconstruction reported success alongside its error")
			}
		})
	}
}

// TestReconstructShardMembership_NonBootstrapEntryAtIndexOne pins the
// fail-closed behaviour when index 1 holds something OTHER than the founding
// configuration.
//
// This condition used to be treated as benign — reconstruction reported ok=false
// with no error and StartShard re-seeded membership from the caller's members
// map — on the rationale of a shard bootstrapped the legacy out-of-band way.
// That path no longer exists: config-in-log is the only bootstrap this code base
// performs, so on a shard it created an ordinary entry at index 1 means the log
// is DAMAGED, and the re-seed drops witnesses, observers, Removed tombstones,
// identities and the replicated witness/session policies.
//
// The original assertion survives inside the new one: reconstruction must never
// FABRICATE a membership, and now it must not report a benign not-found either.
func TestReconstructShardMembership_NonBootstrapEntryAtIndexOne(t *testing.T) {
	ldb := &fixedEntryLogDB{entry: logdb.Entry{
		Index: raft.BootstrapMembershipIndex,
		Term:  1,
		Cmd:   []byte("an ordinary application command"),
	}}
	ms, ok, err := (&Host{}).reconstructShardMembership(readerOver(t, ldb, 5), 5)
	var missing *BootstrapEntryMissingError
	if !errors.As(err, &missing) {
		t.Fatalf("reconstruction over an ordinary entry at the bootstrap index = "+
			"(ok=%v, err=%v), want *BootstrapEntryMissingError: a benign not-found "+
			"sends StartShard to the re-seed, which rebuilds membership from the "+
			"caller's members map and drops every non-voter role and replicated "+
			"policy the shard agreed on", ok, err)
	}
	if missing.Index != raft.BootstrapMembershipIndex {
		t.Fatalf("error Index = %d, want %d", missing.Index, raft.BootstrapMembershipIndex)
	}
	if ok {
		t.Fatalf("reconstruction reported success alongside its error, yielding "+
			"membership %+v", ms)
	}
	if len(ms.Addresses) != 0 || len(ms.Witnesses) != 0 || len(ms.Observers) != 0 {
		t.Fatalf("reconstruction returned membership %+v with ok=false: a "+
			"not-found result must carry nothing the caller could mistake for a "+
			"recovered configuration", ms)
	}
}

// TestStartShard_MissingBootstrapEntryFailsClosed is the end-to-end counterpart.
//
// A replica restarts on a durable log that has persisted state and entries but
// NO EntryInitialMembership at index 1. This test previously documented the
// re-seed that followed — StartShard succeeded and rebuilt membership from
// whatever map the call was handed, proven by handing it a map the log never
// agreed to — and flagged itself as the test to update, deliberately, if the
// fall-back was ever narrowed. It has been.
//
// StartShard must now FAIL, and the shard must not be left loaded: a silent
// re-seed produces a replica whose voter set, non-voter roles and replicated
// policies differ from every peer's, which is the divergence the fail-closed
// path exists to prevent. Damaged durable state is an operator problem.
func TestStartShard_MissingBootstrapEntryFailsClosed(t *testing.T) {
	const shardID, replicaID uint64 = 74, 1
	db := memdb.New()

	// A durable log with no bootstrap entry: one ordinary committed entry at
	// index 1 plus a hard state, which is what makes StartShard treat this as a
	// RESTART rather than a first start.
	if err := db.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		State:     logdb.State{Term: 1, Vote: 0, Commit: 1},
		Entries: []logdb.Entry{
			{Index: 1, Term: 1, Cmd: []byte("an ordinary application command")},
		},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	create := func(_, _ uint64) interface{} { return &recordingMemSM{} }
	members := map[uint64]string{1: "127.0.0.1:6401", 42: "127.0.0.1:6442"}
	startErr := h.StartShard(members, false, create,
		testShardConfig(shardID, replicaID))
	if startErr == nil {
		_ = h.StopShard(shardID)
		memb, membErr := h.GetShardMembership(shardID)
		t.Fatalf("StartShard on a log whose index 1 is not the bootstrap entry "+
			"succeeded, with membership %+v (err %v): the shard's configuration "+
			"was re-seeded from the caller's members map, dropping the witnesses, "+
			"observers, Removed tombstones, identities and replicated policies the "+
			"shard agreed on", memb, membErr)
	}
	var missing *BootstrapEntryMissingError
	if !errors.As(startErr, &missing) {
		t.Fatalf("StartShard error = %v, want it to wrap *BootstrapEntryMissingError "+
			"so an operator can tell damaged durable state from a transient "+
			"start failure", startErr)
	}

	// A refused start must leave nothing loaded, or the next attempt is met
	// with ErrShardAlreadyExists over a shard that never came up.
	if _, err := h.GetShardMembership(shardID); err == nil {
		t.Fatal("the shard is still loaded after StartShard failed closed")
	}
}
