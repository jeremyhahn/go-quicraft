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

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// bootstrapEntryForTest builds the index-1 EntryInitialMembership carrying the
// given voters, for reconstruction tests.
func bootstrapEntryForTest(t *testing.T, voters map[uint64]string) proto.Entry {
	t.Helper()
	ms := proto.Membership{Addresses: voters}
	buf := make([]byte, ms.Size())
	if _, err := ms.MarshalTo(buf); err != nil {
		t.Fatalf("marshal bootstrap membership: %v", err)
	}
	return proto.Entry{Index: 1, Term: 1, Type: proto.EntryInitialMembership, Cmd: buf}
}

// TestReconstructShardMembership_ClampsToRecoveredLastIndex: a commit index past the recovered entries still yields the index-1 bootstrap entry.
func TestReconstructShardMembership_ClampsToRecoveredLastIndex(t *testing.T) {
	// A cache-only LogReader holding only the bootstrap entry at index 1.
	lr := logdb.NewLogReader(1, 1, nil)
	if err := lr.Append([]proto.Entry{bootstrapEntryForTest(t, map[uint64]string{1: "a", 2: "b", 3: "c"})}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, last := lr.IndexRange(); last != 1 {
		t.Fatalf("fixture: recovered last index = %d, want 1", last)
	}

	// Commit=10 far outruns the log (last=1).
	ms, ok, err := (&Host{}).reconstructShardMembership(lr, 10)
	if err != nil {
		t.Fatalf("reconstruction errored on a torn-tail commit overrun: %v", err)
	}
	if !ok {
		t.Fatal("reconstruction did NOT find the bootstrap entry when Commit outran " +
			"the recovered log: the durable config-in-log membership would be lost and " +
			"the caller would re-seed from the local members map")
	}
	if len(ms.Addresses) != 3 || ms.Addresses[2] != "b" {
		t.Fatalf("reconstructed membership = %v, want the 3 bootstrap voters", ms.Addresses)
	}
}

// countingReadLogDB counts how many entries the log-read path is asked to
// deliver, so a test can pin reconstruction's cost as O(1) rather than O(log).
type countingReadLogDB struct {
	mockLogDB
	entriesRead int
}

func (c *countingReadLogDB) IterateEntries(entries []logdb.Entry, size, shardID, replicaID, low, high, maxSize uint64) ([]logdb.Entry, uint64, error) {
	c.entriesRead += int(high - low)
	return entries, size, logdb.ErrUnavailable
}

// TestReconstructShardMembership_ReadsOnlyTheBootstrapEntry pins that restart reads exactly one entry.
func TestReconstructShardMembership_ReadsOnlyTheBootstrapEntry(t *testing.T) {
	ldb := &countingReadLogDB{}
	lr := logdb.NewLogReader(1, 1, ldb)
	// A large committed log: 500k entries, as a long-running shard without
	// compaction would have.
	if err := lr.SetRange(1, 500_000); err != nil {
		t.Fatalf("SetRange: %v", err)
	}

	if _, _, err := (&Host{}).reconstructShardMembership(lr, 500_000); err != nil {
		t.Fatalf("reconstruction: %v", err)
	}

	if ldb.entriesRead > 16 {
		t.Fatalf("reconstruction asked for %d entries over a 500k-entry log: it is "+
			"scanning the log instead of reading the single bootstrap entry, which "+
			"delays every restart in proportion to log size and breaks catch-up "+
			"under sustained load", ldb.entriesRead)
	}
}

// iterateErrorLogDB injects a fixed error into the log-read path so a test can
// exercise reconstruction's fail-closed behaviour on a genuine read failure.
type iterateErrorLogDB struct {
	mockLogDB
	err error
}

func (e *iterateErrorLogDB) IterateEntries(entries []logdb.Entry, size, shardID, replicaID, low, high, maxSize uint64) ([]logdb.Entry, uint64, error) {
	return entries, size, e.err
}

// TestReconstructShardMembership_FailsClosedOnReadError pins that a genuine log read error aborts StartShard.
func TestReconstructShardMembership_FailsClosedOnReadError(t *testing.T) {
	ldb := &iterateErrorLogDB{err: logdb.ErrLogGapDetected}
	lr := logdb.NewLogReader(1, 1, ldb)
	if err := lr.SetRange(1, 5); err != nil {
		t.Fatalf("SetRange: %v", err)
	}

	_, ok, err := (&Host{}).reconstructShardMembership(lr, 5)
	if ok {
		t.Fatal("reconstruction claimed success over an unreadable log")
	}
	if !errors.Is(err, logdb.ErrLogGapDetected) {
		t.Fatalf("reconstruction err = %v, want ErrLogGapDetected: a genuine read "+
			"error must fail closed and abort StartShard, not silently fall back to "+
			"re-seeding membership from the caller's map (dropping witnesses, "+
			"observers, tombstones and the replicated witness policy)", err)
	}
}

// TestConfigInLog_BootstrapWritesInitialMembershipEntry proves the founding
// member lays its initial configuration down as a committed
// EntryInitialMembership at index 1, so membership is a function of the durable
// log rather than an out-of-band seed.
func TestConfigInLog_BootstrapWritesInitialMembershipEntry(t *testing.T) {
	const shardID, replicaID uint64 = 70, 1
	db := memdb.New()
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{1: "127.0.0.1:6001"}
	create := func(_, _ uint64) interface{} { return &recordingMemSM{} }
	if err := h.StartShard(members, false, create, testShardConfig(shardID, replicaID)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	defer func() { _ = h.StopShard(shardID) }()

	first, count, err := db.EntryRange(shardID, replicaID)
	if err != nil {
		t.Fatalf("EntryRange: %v", err)
	}
	if first != 1 || count < 1 {
		t.Fatalf("entry range = (first %d, count %d), want first 1 and at least one "+
			"entry: the bootstrap entry was not persisted at index 1", first, count)
	}

	entries, _, err := db.IterateEntries(nil, 0, shardID, replicaID, 1, 2, 0)
	if err != nil {
		t.Fatalf("IterateEntries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("read %d entries at index 1, want 1", len(entries))
	}
	if entries[0].Type&proto.EntryTypeMask != proto.EntryInitialMembership {
		t.Fatalf("index 1 entry type = %d, want EntryInitialMembership (%d)",
			entries[0].Type&proto.EntryTypeMask, proto.EntryInitialMembership)
	}
	var ms proto.Membership
	if _, err := ms.UnmarshalFrom(entries[0].Cmd); err != nil {
		t.Fatalf("index 1 entry does not decode as a Membership: %v", err)
	}
	if ms.Addresses[1] != "127.0.0.1:6001" {
		t.Fatalf("bootstrap entry membership = %v, want voter 1 at 127.0.0.1:6001",
			ms.Addresses)
	}
}

// TestConfigInLog_MembershipReconstructsFromLogOnRestart: on restart membership comes from the log, not the members map.
func TestConfigInLog_MembershipReconstructsFromLogOnRestart(t *testing.T) {
	const shardID, replicaID uint64 = 71, 1
	const trueAddr = "127.0.0.1:6101"
	db := memdb.New()

	// First boot: bootstrap with the true single-voter configuration.
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	create := func(_, _ uint64) interface{} { return &recordingMemSM{} }
	if err := h.StartShard(map[uint64]string{1: trueAddr}, false, create,
		testShardConfig(shardID, replicaID)); err != nil {
		t.Fatalf("StartShard 1: %v", err)
	}
	waitForLeader(t, h, shardID)

	// Unload the shard: the in-memory peer, its membership, and the engine node
	// are all torn down, so the restart below must reconstruct membership from
	// the durable log alone.
	if err := h.StopShard(shardID); err != nil {
		t.Fatalf("StopShard: %v", err)
	}

	// Restart on the same log with a wrong members map: a different address for voter 1 and a phantom voter 99.
	wrongMap := map[uint64]string{1: "127.0.0.1:9999", 99: "127.0.0.1:6199"}
	if err := h.StartShard(wrongMap, false, create, testShardConfig(shardID, replicaID)); err != nil {
		t.Fatalf("StartShard 2 (restart): %v", err)
	}
	defer func() { _ = h.StopShard(shardID) }()

	memb, err := h.ShardMembership(shardID)
	if err != nil {
		t.Fatalf("ShardMembership: %v", err)
	}
	if _, ok := memb.Nodes[99]; ok {
		t.Fatal("recovered membership contains phantom voter 99 from the restart " +
			"members map: membership is being re-seeded from local config instead of " +
			"reconstructed from the committed log")
	}
	if got := memb.Nodes[1]; got != trueAddr {
		t.Fatalf("recovered voter 1 address = %q, want %q (the LOG's value): the "+
			"restart members map overrode the durable membership", got, trueAddr)
	}
	if len(memb.Nodes) != 1 {
		t.Fatalf("recovered membership has %d voters, want 1: %v", len(memb.Nodes), memb.Nodes)
	}
}
