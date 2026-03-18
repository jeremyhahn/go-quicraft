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
	"testing"

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
	ps := proto.State{Term: 10, Vote: 3, Commit: 99}
	ls := protoStateToLogDB(&ps)
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
