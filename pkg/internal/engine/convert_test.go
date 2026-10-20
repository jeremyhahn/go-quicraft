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

package engine

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

func TestProtoEntryToLogDB(t *testing.T) {
	pe := proto.Entry{
		Term:        5,
		Index:       42,
		Type:        proto.EntryNormal,
		Key:         100,
		ClientID:    200,
		SeriesID:    300,
		RespondedTo: 400,
		Cmd:         []byte("hello"),
	}
	le := protoEntryToLogDB(&pe)
	if le.Term != 5 {
		t.Errorf("Term = %d, want 5", le.Term)
	}
	if le.Index != 42 {
		t.Errorf("Index = %d, want 42", le.Index)
	}
	if le.Type != proto.EntryNormal {
		t.Errorf("Type = %d, want %d", le.Type, proto.EntryNormal)
	}
	if string(le.Cmd) != "hello" {
		t.Errorf("Cmd = %q, want %q", le.Cmd, "hello")
	}
}

func TestProtoEntryToLogDB_ZeroValue(t *testing.T) {
	var pe proto.Entry
	le := protoEntryToLogDB(&pe)
	if le.Term != 0 || le.Index != 0 || le.Type != 0 || le.Cmd != nil {
		t.Errorf("expected zero logdb.Entry, got %+v", le)
	}
}

func TestProtoStateToLogDB(t *testing.T) {
	ps := proto.State{Term: 10, Vote: 3, Commit: 99, VoteWindow: 2_000_000_000}
	ls := protoStateToLogDB(&ps)
	if ls.VoteWindow != ps.VoteWindow {
		t.Errorf("VoteWindow = %d, want %d: a restarted replica would not know the window it owes", ls.VoteWindow, ps.VoteWindow)
	}
	if ls.Term != 10 {
		t.Errorf("Term = %d, want 10", ls.Term)
	}
	if ls.Vote != 3 {
		t.Errorf("Vote = %d, want 3", ls.Vote)
	}
	if ls.Commit != 99 {
		t.Errorf("Commit = %d, want 99", ls.Commit)
	}
}

func TestProtoStateToLogDB_ZeroValue(t *testing.T) {
	var ps proto.State
	ls := protoStateToLogDB(&ps)
	if ls.Term != 0 || ls.Vote != 0 || ls.Commit != 0 {
		t.Errorf("expected zero logdb.State, got %+v", ls)
	}
}

func TestProtoMembershipToLogDB(t *testing.T) {
	pm := proto.Membership{
		ConfigChangeID: 7,
		Addresses:      map[uint64]string{1: "addr1", 2: "addr2"},
		Observers:      map[uint64]string{3: "obs1"},
		Witnesses:      map[uint64]string{4: "wit1"},
		Removed:        map[uint64]bool{5: true, 6: false},
	}
	lm := protoMembershipToLogDB(&pm)
	if lm.ConfigChangeID != 7 {
		t.Errorf("ConfigChangeID = %d, want 7", lm.ConfigChangeID)
	}
	if len(lm.Addresses) != 2 {
		t.Errorf("Addresses length = %d, want 2", len(lm.Addresses))
	}
	if lm.Addresses[1] != "addr1" {
		t.Errorf("Addresses[1] = %q, want %q", lm.Addresses[1], "addr1")
	}
	if len(lm.Observers) != 1 {
		t.Errorf("Observers length = %d, want 1", len(lm.Observers))
	}
	if len(lm.Witnesses) != 1 {
		t.Errorf("Witnesses length = %d, want 1", len(lm.Witnesses))
	}
	if len(lm.Removed) != 2 {
		t.Errorf("Removed length = %d, want 2", len(lm.Removed))
	}
	if !lm.Removed[5] {
		t.Error("Removed[5] = false, want true")
	}
}

func TestProtoMembershipToLogDB_EmptyMaps(t *testing.T) {
	var pm proto.Membership
	lm := protoMembershipToLogDB(&pm)
	if lm.ConfigChangeID != 0 {
		t.Errorf("ConfigChangeID = %d, want 0", lm.ConfigChangeID)
	}
	if lm.Addresses != nil {
		t.Errorf("Addresses = %v, want nil", lm.Addresses)
	}
	if lm.Observers != nil {
		t.Errorf("Observers = %v, want nil", lm.Observers)
	}
	if lm.Witnesses != nil {
		t.Errorf("Witnesses = %v, want nil", lm.Witnesses)
	}
	if lm.Removed != nil {
		t.Errorf("Removed = %v, want nil", lm.Removed)
	}
}

func TestProtoSnapshotToLogDB(t *testing.T) {
	ps := proto.Snapshot{
		Index:       100,
		Term:        5,
		Filepath:    "/data/snap",
		FileSize:    4096,
		OnDiskIndex: 95,
		Epoch:       7,
		Membership: proto.Membership{
			ConfigChangeID: 3,
			Addresses:      map[uint64]string{1: "addr"},
		},
	}
	ls := protoSnapshotToLogDB(&ps)
	if ls.Index != 100 {
		t.Errorf("Index = %d, want 100", ls.Index)
	}
	if ls.Term != 5 {
		t.Errorf("Term = %d, want 5", ls.Term)
	}
	if ls.Filepath != "/data/snap" {
		t.Errorf("Filepath = %q, want %q", ls.Filepath, "/data/snap")
	}
	if ls.FileSize != 4096 {
		t.Errorf("FileSize = %d, want 4096", ls.FileSize)
	}
	if ls.OnDiskIndex != 95 {
		t.Errorf("OnDiskIndex = %d, want 95", ls.OnDiskIndex)
	}
	if ls.Membership.ConfigChangeID != 3 {
		t.Errorf("Membership.ConfigChangeID = %d, want 3", ls.Membership.ConfigChangeID)
	}
	if ls.Epoch != 7 {
		t.Errorf("Epoch = %d, want 7", ls.Epoch)
	}
}

func TestProtoSnapshotToLogDB_PreservesEpoch(t *testing.T) {
	// Epoch must be propagated to support AES-GCM decryption on recovery.
	ps := proto.Snapshot{
		Index: 50,
		Term:  3,
		Epoch: 42,
	}
	ls := protoSnapshotToLogDB(&ps)
	if ls.Epoch != 42 {
		t.Errorf("Epoch = %d, want 42; encrypted snapshot recovery requires Epoch", ls.Epoch)
	}
}

func TestProtoSnapshotToLogDB_ZeroValue(t *testing.T) {
	var ps proto.Snapshot
	ls := protoSnapshotToLogDB(&ps)
	if ls.Index != 0 || ls.Term != 0 || ls.Filepath != "" {
		t.Errorf("expected zero logdb.Snapshot, got %+v", ls)
	}
}

func TestProtoUpdatesToLogDBReuse(t *testing.T) {
	src := []proto.Update{
		{
			ShardID: 1, ReplicaID: 1,
			State:         proto.State{Term: 1},
			EntriesToSave: []proto.Entry{{Term: 1, Index: 1, Cmd: []byte("a")}},
		},
		{
			ShardID: 2, ReplicaID: 2,
			State:         proto.State{Term: 2},
			EntriesToSave: []proto.Entry{{Term: 2, Index: 10, Cmd: []byte("b")}},
		},
	}
	var dst []logdb.Update
	var flatBuf []logdb.Entry
	dst, flatBuf = protoUpdatesToLogDBReuse(dst, flatBuf, src)
	if len(dst) != 2 {
		t.Fatalf("expected 2 updates, got %d", len(dst))
	}
	// Flat buffer should contain 2 total entries (1 per update).
	if len(flatBuf) != 2 {
		t.Fatalf("expected flat buffer len 2, got %d", len(flatBuf))
	}
	if dst[0].ShardID != 1 || dst[1].ShardID != 2 {
		t.Errorf("ShardIDs = %d,%d, want 1,2", dst[0].ShardID, dst[1].ShardID)
	}
	if len(dst[0].Entries) != 1 || dst[0].Entries[0].Index != 1 {
		t.Errorf("dst[0].Entries = %+v, want [Index=1]", dst[0].Entries)
	}
	if len(dst[1].Entries) != 1 || dst[1].Entries[0].Index != 10 {
		t.Errorf("dst[1].Entries = %+v, want [Index=10]", dst[1].Entries)
	}
}

func TestProtoUpdatesToLogDBReuse_FlatBufferSubSlicing(t *testing.T) {
	src := []proto.Update{
		{
			ShardID: 1,
			EntriesToSave: []proto.Entry{
				{Term: 1, Index: 1, Cmd: []byte("a")},
				{Term: 1, Index: 2, Cmd: []byte("b")},
			},
		},
		{
			ShardID: 2,
			EntriesToSave: []proto.Entry{
				{Term: 2, Index: 10, Cmd: []byte("c")},
				{Term: 2, Index: 11, Cmd: []byte("d")},
				{Term: 2, Index: 12, Cmd: []byte("e")},
			},
		},
	}
	var dst []logdb.Update
	var flatBuf []logdb.Entry
	dst, flatBuf = protoUpdatesToLogDBReuse(dst, flatBuf, src)

	// Flat buffer should hold all 5 entries contiguously.
	if len(flatBuf) != 5 {
		t.Fatalf("flat buffer len = %d, want 5", len(flatBuf))
	}

	// dst[0] should be a sub-slice of entries [0:2].
	if len(dst[0].Entries) != 2 {
		t.Fatalf("dst[0].Entries len = %d, want 2", len(dst[0].Entries))
	}
	if dst[0].Entries[0].Index != 1 || dst[0].Entries[1].Index != 2 {
		t.Errorf("dst[0].Entries indices = %d,%d, want 1,2",
			dst[0].Entries[0].Index, dst[0].Entries[1].Index)
	}

	// dst[1] should be a sub-slice of entries [2:5].
	if len(dst[1].Entries) != 3 {
		t.Fatalf("dst[1].Entries len = %d, want 3", len(dst[1].Entries))
	}
	if dst[1].Entries[0].Index != 10 || dst[1].Entries[2].Index != 12 {
		t.Errorf("dst[1].Entries indices = %d,%d, want 10,12",
			dst[1].Entries[0].Index, dst[1].Entries[2].Index)
	}
}

func TestProtoUpdatesToLogDBReuse_ReusesBuffers(t *testing.T) {
	src1 := []proto.Update{
		{
			ShardID: 1,
			EntriesToSave: []proto.Entry{
				{Term: 1, Index: 1, Cmd: []byte("a")},
				{Term: 1, Index: 2, Cmd: []byte("b")},
			},
		},
	}
	dst := make([]logdb.Update, 0, 10)
	flatBuf := make([]logdb.Entry, 0, 10)
	dst, flatBuf = protoUpdatesToLogDBReuse(dst, flatBuf, src1)
	flatCap := cap(flatBuf)
	dstCap := cap(dst)

	// Second call with smaller batch should reuse capacity.
	src2 := []proto.Update{
		{
			ShardID:       2,
			EntriesToSave: []proto.Entry{{Term: 2, Index: 10}},
		},
	}
	dst, flatBuf = protoUpdatesToLogDBReuse(dst, flatBuf, src2)
	if cap(dst) != dstCap {
		t.Errorf("dst cap = %d, want %d (reused)", cap(dst), dstCap)
	}
	if cap(flatBuf) != flatCap {
		t.Errorf("flat buffer cap = %d, want %d (reused)", cap(flatBuf), flatCap)
	}
	if dst[0].ShardID != 2 {
		t.Errorf("dst[0].ShardID = %d, want 2", dst[0].ShardID)
	}
}

func TestProtoUpdatesToLogDBReuse_EmptyEntries(t *testing.T) {
	src := []proto.Update{
		{
			ShardID: 1,
			State:   proto.State{Term: 5},
			// No entries - state-only update.
		},
		{
			ShardID:       2,
			EntriesToSave: []proto.Entry{{Term: 1, Index: 1}},
		},
	}
	var dst []logdb.Update
	var flatBuf []logdb.Entry
	dst, flatBuf = protoUpdatesToLogDBReuse(dst, flatBuf, src)

	if len(dst) != 2 {
		t.Fatalf("expected 2 updates, got %d", len(dst))
	}
	// First update should have empty entries sub-slice.
	if len(dst[0].Entries) != 0 {
		t.Errorf("dst[0].Entries len = %d, want 0", len(dst[0].Entries))
	}
	if dst[0].State.Term != 5 {
		t.Errorf("dst[0].State.Term = %d, want 5", dst[0].State.Term)
	}
	// Second update should have 1 entry.
	if len(dst[1].Entries) != 1 {
		t.Fatalf("dst[1].Entries len = %d, want 1", len(dst[1].Entries))
	}
	// Flat buffer should hold only 1 entry total.
	if len(flatBuf) != 1 {
		t.Errorf("flat buffer len = %d, want 1", len(flatBuf))
	}
}

func TestProtoUpdatesToLogDBReuse_GrowsFlatBuffer(t *testing.T) {
	// Start with a tiny flat buffer that cannot hold all entries.
	flatBuf := make([]logdb.Entry, 0, 1)
	src := []proto.Update{
		{
			ShardID: 1,
			EntriesToSave: []proto.Entry{
				{Term: 1, Index: 1},
				{Term: 1, Index: 2},
				{Term: 1, Index: 3},
			},
		},
		{
			ShardID: 2,
			EntriesToSave: []proto.Entry{
				{Term: 2, Index: 10},
				{Term: 2, Index: 11},
			},
		},
	}
	var dst []logdb.Update
	dst, flatBuf = protoUpdatesToLogDBReuse(dst, flatBuf, src)

	if len(flatBuf) != 5 {
		t.Fatalf("flat buffer len = %d, want 5", len(flatBuf))
	}
	if cap(flatBuf) < 5 {
		t.Errorf("flat buffer cap = %d, want >= 5", cap(flatBuf))
	}
	if len(dst[0].Entries) != 3 {
		t.Errorf("dst[0].Entries len = %d, want 3", len(dst[0].Entries))
	}
	if len(dst[1].Entries) != 2 {
		t.Errorf("dst[1].Entries len = %d, want 2", len(dst[1].Entries))
	}
}

func TestProtoUpdatesToLogDBReuse_AllEmpty(t *testing.T) {
	src := []proto.Update{
		{ShardID: 1, State: proto.State{Term: 1}},
		{ShardID: 2, State: proto.State{Term: 2}},
	}
	var dst []logdb.Update
	var flatBuf []logdb.Entry
	dst, flatBuf = protoUpdatesToLogDBReuse(dst, flatBuf, src)

	if len(dst) != 2 {
		t.Fatalf("expected 2 updates, got %d", len(dst))
	}
	if len(flatBuf) != 0 {
		t.Errorf("flat buffer len = %d, want 0 (no entries)", len(flatBuf))
	}
	for i := range dst {
		if len(dst[i].Entries) != 0 {
			t.Errorf("dst[%d].Entries len = %d, want 0", i, len(dst[i].Entries))
		}
	}
}

func TestProtoMembershipToLogDB_DeepCopy(t *testing.T) {
	pm := proto.Membership{
		Addresses: map[uint64]string{1: "addr1"},
	}
	lm := protoMembershipToLogDB(&pm)
	// Modify original - should not affect copy.
	pm.Addresses[1] = "modified"
	pm.Addresses[99] = "new"
	if lm.Addresses[1] != "addr1" {
		t.Errorf("deep copy broken: Addresses[1] = %q, want %q", lm.Addresses[1], "addr1")
	}
	if _, ok := lm.Addresses[99]; ok {
		t.Error("deep copy broken: Addresses[99] should not exist")
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkProtoUpdatesToLogDBReuse_SmallBatch(b *testing.B) {
	src := make([]proto.Update, 4)
	for i := range src {
		src[i] = proto.Update{
			ShardID:   uint64(i + 1),
			ReplicaID: 1,
			State:     proto.State{Term: uint64(i + 1)},
			EntriesToSave: []proto.Entry{
				{Term: uint64(i + 1), Index: uint64(i*10 + 1), Cmd: []byte("cmd")},
				{Term: uint64(i + 1), Index: uint64(i*10 + 2), Cmd: []byte("cmd")},
			},
		}
	}

	dst := make([]logdb.Update, 0, 8)
	flatBuf := make([]logdb.Entry, 0, 16)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		dst, flatBuf = protoUpdatesToLogDBReuse(dst, flatBuf, src)
	}
}

func BenchmarkProtoUpdatesToLogDBReuse_LargeBatch(b *testing.B) {
	const numUpdates = 64
	const entriesPerUpdate = 16
	src := make([]proto.Update, numUpdates)
	for i := range src {
		entries := make([]proto.Entry, entriesPerUpdate)
		for j := range entries {
			entries[j] = proto.Entry{
				Term:  uint64(i + 1),
				Index: uint64(i*entriesPerUpdate + j + 1),
				Cmd:   []byte("benchmark-payload"),
			}
		}
		src[i] = proto.Update{
			ShardID:       uint64(i + 1),
			ReplicaID:     1,
			State:         proto.State{Term: uint64(i + 1)},
			EntriesToSave: entries,
		}
	}

	dst := make([]logdb.Update, 0, numUpdates)
	flatBuf := make([]logdb.Entry, 0, numUpdates*entriesPerUpdate)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		dst, flatBuf = protoUpdatesToLogDBReuse(dst, flatBuf, src)
	}
}

// TestProtoMembershipToLogDB_CarriesEveryField is the guard the older
// TestProtoMembershipToLogDB could not provide.
//
// That test left MaxWitnesses, RequireFullReplicaMajority, Identities,
// RemovedIdentities and all three session-policy fields at ZERO on both
// sides, so deleting any of them from the converter changed nothing it
// observed — every relevant package stayed green. The one round-trip test in
// the repo that DOES assert them
// (TestMembershipConvertersRoundTripIdentities) exercises the identically
// named function in package quicraft, not this one. This is the converter
// snapshotPool.save actually uses, so a dropped field here means a restarted
// replica computes a different witness quorum than its peers, or expires
// sessions at log indexes they do not.
//
// Every field is given a DISTINCT non-zero value so no two can be confused
// and no omission can be masked by a zero default.
func TestProtoMembershipToLogDB_CarriesEveryField(t *testing.T) {
	pm := proto.Membership{
		ConfigChangeID:             11,
		MaxWitnesses:               3,
		RequireFullReplicaMajority: true,
		SessionExpiryEntries:       5000,
		MaxTotalSessions:           4096,
		MaxResponsesPerSession:     64,
		MaxSessionsPerOwner:        16,
		Addresses:                  map[uint64]string{1: "a1", 2: "a2"},
		Observers:                  map[uint64]string{3: "o3"},
		Witnesses:                  map[uint64]string{4: "w4"},
		Removed:                    map[uint64]bool{5: true},
		Identities:                 map[uint64]string{1: "spki:one", 2: "spki:two"},
		RemovedIdentities:          map[uint64]string{5: "spki:five"},
	}

	lm := protoMembershipToLogDB(&pm)

	checks := []struct {
		field string
		got   any
		want  any
	}{
		{"ConfigChangeID", lm.ConfigChangeID, uint64(11)},
		{"MaxWitnesses", lm.MaxWitnesses, uint64(3)},
		{"RequireFullReplicaMajority", lm.RequireFullReplicaMajority, true},
		{"SessionExpiryEntries", lm.SessionExpiryEntries, uint64(5000)},
		{"MaxTotalSessions", lm.MaxTotalSessions, uint64(4096)},
		{"MaxResponsesPerSession", lm.MaxResponsesPerSession, uint64(64)},
		{"MaxSessionsPerOwner", lm.MaxSessionsPerOwner, uint64(16)},
		{"Addresses[1]", lm.Addresses[1], "a1"},
		{"Addresses[2]", lm.Addresses[2], "a2"},
		{"Observers[3]", lm.Observers[3], "o3"},
		{"Witnesses[4]", lm.Witnesses[4], "w4"},
		{"Removed[5]", lm.Removed[5], true},
		{"Identities[1]", lm.Identities[1], "spki:one"},
		{"Identities[2]", lm.Identities[2], "spki:two"},
		{"RemovedIdentities[5]", lm.RemovedIdentities[5], "spki:five"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v — a field dropped here is persisted "+
				"zeroed into every snapshot", c.field, c.got, c.want)
		}
	}
}

// TestProtoMembershipToLogDB_MapsAreDeepCopied pins the aliasing contract.
// The converter must not hand LogDB the caller's maps: the proto membership
// it is given is raft's live copy, and a backend that stores the reference
// would see it mutate underneath a persisted record.
func TestProtoMembershipToLogDB_MapsAreDeepCopied(t *testing.T) {
	pm := proto.Membership{
		Addresses:         map[uint64]string{1: "a1"},
		Identities:        map[uint64]string{1: "spki:one"},
		RemovedIdentities: map[uint64]string{2: "spki:two"},
		Removed:           map[uint64]bool{2: true},
	}
	lm := protoMembershipToLogDB(&pm)

	pm.Addresses[1] = "MUTATED"
	pm.Identities[1] = "MUTATED"
	pm.RemovedIdentities[2] = "MUTATED"
	pm.Removed[2] = false

	if lm.Addresses[1] != "a1" || lm.Identities[1] != "spki:one" ||
		lm.RemovedIdentities[2] != "spki:two" || !lm.Removed[2] {
		t.Fatal("the converted membership aliases the caller's maps: a later " +
			"mutation of raft's live membership would change a record already " +
			"handed to LogDB")
	}
}

// TestLogDBMembershipFieldCount fails when a field is added to
// logdb.Membership, so the addition cannot be made without also deciding
// whether every converter carries it. The converters are hand-written in four
// places; this is what stops a fifth field from silently reaching only some
// of them.
func TestLogDBMembershipFieldCount(t *testing.T) {
	const mapped = 13
	if got := reflect.TypeOf(logdb.Membership{}).NumField(); got != mapped {
		t.Fatalf("logdb.Membership has %d fields, but the converters map %d.\n"+
			"Adding a field means updating every converter — engine/convert.go, "+
			"host.go protoMembershipToLogDB, host.go logdbMembershipToProto, and "+
			"the inline map in messagehandler.go — and their round-trip tests, "+
			"then bumping this count.", got, mapped)
	}
}

// TestEntryFieldCount is the same guard for the entry types, which have the
// same hazard and more copy sites.
//
// proto.Entry and logdb.Entry are converted field by field in five hand-written
// places, none of which fails to compile when a field is added: engine's
// protoEntryToLogDB, both directions in logdb/reader.go, and the WAL record
// encoder and replayer in logdb/waldb/db.go. A field that reaches only some of
// them does not error — it round-trips as zero, which for a session field means
// at-most-once delivery or a replicated apply-path limit silently changing
// behaviour after a restart. Both types are pinned because the converters
// require them to stay in step.
func TestEntryFieldCount(t *testing.T) {
	const mapped = 9
	if got := reflect.TypeOf(proto.Entry{}).NumField(); got != mapped {
		t.Fatalf("proto.Entry has %d fields, but the converters map %d.\n"+
			"Adding a field means updating protoEntryToLogDB, both conversions "+
			"in logdb/reader.go, writeEntryChunk and replayEntries in "+
			"logdb/waldb/db.go, entryHeaderSize and Entry.MarshalTo/UnmarshalFrom/"+
			"Reset in proto, and entryRecordHeaderSize in waldb — then bumping "+
			"this count.", got, mapped)
	}
	if got := reflect.TypeOf(logdb.Entry{}).NumField(); got != mapped {
		t.Fatalf("logdb.Entry has %d fields, proto.Entry has %d. The converters "+
			"are field-by-field, so the two must stay in step.", got, mapped)
	}
}

// TestSnapshotFieldCount is the same guard for the snapshot types, and it is
// the one that was missing when proto.Snapshot turned out to have no
// Compression field at all.
//
// The two types are converted field by field in six hand-written places — this
// package's protoSnapshotToLogDB, host.go's recoverLogReader and its export
// header, messagehandler.go's dedup re-delivery and its post-receive install,
// and snapshot_pool.go's CreateSnapshot — and not one of them fails to compile
// when a field is added on either side. A field that reaches only some of them
// round-trips as ZERO, and for this type zero is a meaningful value in every
// case: Compression zero means "uncompressed", so a snappy artifact is handed
// to the state machine as garbage; Epoch zero means "plaintext", so ciphertext
// is handed to it raw; FileSize zero makes this replica ship a metadata-only
// snapshot that erases the receiving follower's state.
//
// The counts differ on purpose. proto.Snapshot carries the identity pair
// ShardID/ReplicaID, which has no logdb counterpart (LogDB keys records by
// shard and replica instead of storing them), while logdb.Snapshot carries a
// Type this side does not (proto.Snapshot.Type was removed: it had no writer
// anywhere, so it round-tripped as zero on every snapshot the engine ever
// produced). So the guard pins each side separately rather than requiring
// them to match.
func TestSnapshotFieldCount(t *testing.T) {
	const protoMapped = 10
	if got := reflect.TypeOf(proto.Snapshot{}).NumField(); got != protoMapped {
		t.Fatalf("proto.Snapshot has %d fields, but the converters map %d.\n"+
			"Adding or removing one means updating protoSnapshotToLogDB, the "+
			"proto.Snapshot literals in host.go (recoverLogReader), "+
			"messagehandler.go (both install messages) and snapshot_pool.go "+
			"(CreateSnapshot), plus Size/MarshalTo/UnmarshalFrom/"+
			"hasContentBeyondPosition/Reset in proto — then bumping this count.",
			got, protoMapped)
	}

	const logdbMapped = 8
	if got := reflect.TypeOf(logdb.Snapshot{}).NumField(); got != logdbMapped {
		t.Fatalf("logdb.Snapshot has %d fields, but the converters map %d.\n"+
			"Adding one means deciding whether proto.Snapshot must carry it — "+
			"Compression did, and did not, which made every snapshot record "+
			"built by conversion claim NoCompression — then updating every "+
			"converter and bumping this count.", got, logdbMapped)
	}
}

// TestProtoSnapshotToLogDB_CarriesEveryConvertibleField pins the conversion
// itself, field by field, with a DISTINCT non-zero value per field so a
// mis-wired assignment (two fields reading the same source) is visible.
//
// The field-count guard above catches an addition; this catches a field that
// exists on both sides and is simply not carried, which is the shape
// Compression had.
func TestProtoSnapshotToLogDB_CarriesEveryConvertibleField(t *testing.T) {
	ps := proto.Snapshot{
		ShardID:   9,
		ReplicaID: 4,
		Index:     101,
		Term:      7,
		Membership: proto.Membership{
			ConfigChangeID: 55,
			Addresses:      map[uint64]string{1: "n1:5000"},
		},
		Filepath:    "/snapshots/shard-9/snapshot-101",
		FileSize:    4096,
		OnDiskIndex: 88,
		Epoch:       3,
		Compression: uint64(config.Snappy),
	}

	ls := protoSnapshotToLogDB(&ps)

	for _, c := range []struct {
		field     string
		got, want uint64
	}{
		{"Index", ls.Index, ps.Index},
		{"Term", ls.Term, ps.Term},
		{"FileSize", ls.FileSize, ps.FileSize},
		{"OnDiskIndex", ls.OnDiskIndex, ps.OnDiskIndex},
		{"Epoch", ls.Epoch, ps.Epoch},
		{"Compression", uint64(ls.Compression), ps.Compression},
	} {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d: the conversion drops it, so every "+
				"snapshot record built through this path describes the artifact "+
				"wrongly", c.field, c.got, c.want)
		}
	}
	if ls.Filepath != ps.Filepath {
		t.Errorf("Filepath = %q, want %q", ls.Filepath, ps.Filepath)
	}
	if ls.Membership.ConfigChangeID != ps.Membership.ConfigChangeID {
		t.Errorf("Membership.ConfigChangeID = %d, want %d",
			ls.Membership.ConfigChangeID, ps.Membership.ConfigChangeID)
	}

	// config.Snappy must not be the zero value, or the Compression assertion
	// above passes against a conversion that drops the field.
	if config.Snappy == config.NoCompression {
		t.Fatal("config.Snappy equals NoCompression; the Compression assertion " +
			"cannot distinguish a carried field from a dropped one")
	}
}

// TestProtoSnapshotToLogDB_RecordsTheDirectory pins the convention a LogDB
// snapshot record follows: Filepath names the artifact's DIRECTORY, while a
// proto snapshot's names its data file. The two-phase snapshot receive lets
// the record converted from raft's Update be the first one written, so a
// verbatim copy made every reader open <dir>/snapshot.dat/snapshot.dat.
func TestProtoSnapshotToLogDB_RecordsTheDirectory(t *testing.T) {
	dir := filepath.Join("/data", "snapshots", "shard-1", "replica-2", "snapshot-00000000000000000500")
	for _, tc := range []struct {
		name, in, want string
	}{
		{"data file", filepath.Join(dir, snapshotDataFile), dir},
		{"already a directory", dir, dir},
		{"empty", "", ""},
	} {
		ps := proto.Snapshot{Index: 500, Term: 3, Filepath: tc.in}
		if got := protoSnapshotToLogDB(&ps).Filepath; got != tc.want {
			t.Errorf("%s: Filepath = %q, want %q", tc.name, got, tc.want)
		}
	}
}
