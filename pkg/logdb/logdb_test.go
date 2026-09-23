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

package logdb

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// Compile-time interface satisfaction check. This declaration verifies that
// any concrete type claiming to implement LogDB will fail to compile if the
// interface changes. The testLogDB type below is a minimal implementation
// used solely for this purpose.
var _ LogDB = (*testLogDB)(nil)

// testLogDB is a minimal LogDB implementation used exclusively for the
// compile-time interface satisfaction check above.
type testLogDB struct{}

func (t *testLogDB) Name() string { return "test" }

func (t *testLogDB) SaveState(_ []Update) error { return nil }

func (t *testLogDB) IterateEntries(entries []Entry, size uint64, _ uint64, _ uint64,
	_ uint64, _ uint64, _ uint64) ([]Entry, uint64, error) {
	return entries, size, nil
}

func (t *testLogDB) ReadState(_ uint64, _ uint64) (State, Membership, error) {
	return State{}, Membership{}, nil
}

func (t *testLogDB) EntryRange(_ uint64, _ uint64) (uint64, uint64, error) { return 0, 0, nil }

func (t *testLogDB) RemoveEntriesTo(_ uint64, _ uint64, _ uint64) error { return nil }

func (t *testLogDB) Compact(_ uint64, _ uint64) error { return nil }

func (t *testLogDB) SaveSnapshot(_ uint64, _ uint64, _ Snapshot) error { return nil }

func (t *testLogDB) GetSnapshot(_ uint64, _ uint64) (Snapshot, error) {
	return Snapshot{}, nil
}

func (t *testLogDB) RemoveNodeData(_ uint64, _ uint64) error { return nil }

func (t *testLogDB) ListNodeInfo() ([]NodeInfo, error) { return nil, nil }

func (t *testLogDB) SaveBootstrap(_ uint64, _ uint64, _ Bootstrap) error { return nil }

func (t *testLogDB) GetBootstrap(_ uint64, _ uint64) (Bootstrap, bool, error) {
	return Bootstrap{}, false, nil
}

func (t *testLogDB) Close() error { return nil }

// --- NodeInfo tests ---

func TestNodeInfoEquality(t *testing.T) {
	tests := []struct {
		name  string
		a     NodeInfo
		b     NodeInfo
		equal bool
	}{
		{
			name:  "identical nodes are equal",
			a:     NodeInfo{ShardID: 1, ReplicaID: 2},
			b:     NodeInfo{ShardID: 1, ReplicaID: 2},
			equal: true,
		},
		{
			name:  "zero values are equal",
			a:     NodeInfo{},
			b:     NodeInfo{},
			equal: true,
		},
		{
			name:  "max uint64 values are equal",
			a:     NodeInfo{ShardID: ^uint64(0), ReplicaID: ^uint64(0)},
			b:     NodeInfo{ShardID: ^uint64(0), ReplicaID: ^uint64(0)},
			equal: true,
		},
		{
			name:  "different shard IDs are not equal",
			a:     NodeInfo{ShardID: 1, ReplicaID: 2},
			b:     NodeInfo{ShardID: 3, ReplicaID: 2},
			equal: false,
		},
		{
			name:  "different replica IDs are not equal",
			a:     NodeInfo{ShardID: 1, ReplicaID: 2},
			b:     NodeInfo{ShardID: 1, ReplicaID: 4},
			equal: false,
		},
		{
			name:  "both fields different are not equal",
			a:     NodeInfo{ShardID: 1, ReplicaID: 2},
			b:     NodeInfo{ShardID: 10, ReplicaID: 20},
			equal: false,
		},
		{
			name:  "swapped fields are not equal",
			a:     NodeInfo{ShardID: 1, ReplicaID: 2},
			b:     NodeInfo{ShardID: 2, ReplicaID: 1},
			equal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a == tt.b
			if got != tt.equal {
				t.Errorf("NodeInfo{%d, %d} == NodeInfo{%d, %d}: got %v, want %v",
					tt.a.ShardID, tt.a.ReplicaID,
					tt.b.ShardID, tt.b.ReplicaID,
					got, tt.equal)
			}
		})
	}
}

func TestNodeInfoAsMapKey(t *testing.T) {
	m := make(map[NodeInfo]string)
	n1 := NodeInfo{ShardID: 1, ReplicaID: 2}
	n2 := NodeInfo{ShardID: 3, ReplicaID: 4}

	m[n1] = "node-1"
	m[n2] = "node-2"

	if got := m[n1]; got != "node-1" {
		t.Errorf("map lookup for n1: got %q, want %q", got, "node-1")
	}
	if got := m[n2]; got != "node-2" {
		t.Errorf("map lookup for n2: got %q, want %q", got, "node-2")
	}
	if got := len(m); got != 2 {
		t.Errorf("map length: got %d, want 2", got)
	}

	// Overwrite with an equal key.
	duplicate := NodeInfo{ShardID: 1, ReplicaID: 2}
	m[duplicate] = "overwritten"
	if got := m[n1]; got != "overwritten" {
		t.Errorf("map lookup after overwrite: got %q, want %q", got, "overwritten")
	}
	if got := len(m); got != 2 {
		t.Errorf("map length after overwrite: got %d, want 2", got)
	}
}

// --- Error tests ---

func TestErrorMessagesAreNonEmpty(t *testing.T) {
	sentinels := []struct {
		name string
		err  error
	}{
		{"ErrNoState", ErrNoState},
		{"ErrNoSnapshot", ErrNoSnapshot},
		{"ErrNoBootstrap", ErrNoBootstrap},
		{"ErrClosed", ErrClosed},
		{"ErrCompacted", ErrCompacted},
		{"ErrUnavailable", ErrUnavailable},
		{"ErrSnapshotOutOfDate", ErrSnapshotOutOfDate},
	}

	for _, tt := range sentinels {
		t.Run(tt.name, func(t *testing.T) {
			msg := tt.err.Error()
			if msg == "" {
				t.Errorf("%s.Error() returned empty string", tt.name)
			}
		})
	}
}

func TestErrorMessagesContainPrefix(t *testing.T) {
	sentinels := []struct {
		name string
		err  error
	}{
		{"ErrNoState", ErrNoState},
		{"ErrNoSnapshot", ErrNoSnapshot},
		{"ErrNoBootstrap", ErrNoBootstrap},
		{"ErrClosed", ErrClosed},
		{"ErrCompacted", ErrCompacted},
		{"ErrUnavailable", ErrUnavailable},
		{"ErrSnapshotOutOfDate", ErrSnapshotOutOfDate},
	}

	for _, tt := range sentinels {
		t.Run(tt.name, func(t *testing.T) {
			msg := tt.err.Error()
			if len(msg) < 6 || msg[:6] != "logdb:" {
				t.Errorf("%s.Error() = %q, want prefix %q", tt.name, msg, "logdb:")
			}
		})
	}
}

func TestSentinelErrorsAreDistinct(t *testing.T) {
	sentinels := []error{
		ErrNoState,
		ErrNoSnapshot,
		ErrNoBootstrap,
		ErrClosed,
		ErrCompacted,
		ErrUnavailable,
		ErrSnapshotOutOfDate,
	}

	for i := 0; i < len(sentinels); i++ {
		for j := i + 1; j < len(sentinels); j++ {
			if errors.Is(sentinels[i], sentinels[j]) {
				t.Errorf("sentinel errors at index %d and %d should not match via errors.Is", i, j)
			}
		}
	}
}

func TestSentinelErrorsMatchViaErrorsIs(t *testing.T) {
	sentinels := []error{
		ErrNoState,
		ErrNoSnapshot,
		ErrNoBootstrap,
		ErrClosed,
		ErrCompacted,
		ErrUnavailable,
		ErrSnapshotOutOfDate,
	}

	for _, sentinel := range sentinels {
		t.Run(sentinel.Error(), func(t *testing.T) {
			if !errors.Is(sentinel, sentinel) {
				t.Errorf("errors.Is(%v, %v) = false, want true", sentinel, sentinel)
			}
		})
	}
}

// --- Type construction tests ---

func TestEntryConstruction(t *testing.T) {
	e := Entry{
		Index: 42,
		Term:  5,
		Type:  1,
		Cmd:   []byte("test-command"),
	}

	if e.Index != 42 {
		t.Errorf("Entry.Index = %d, want 42", e.Index)
	}
	if e.Term != 5 {
		t.Errorf("Entry.Term = %d, want 5", e.Term)
	}
	if e.Type != 1 {
		t.Errorf("Entry.Type = %d, want 1", e.Type)
	}
	if string(e.Cmd) != "test-command" {
		t.Errorf("Entry.Cmd = %q, want %q", e.Cmd, "test-command")
	}
}

func TestEntryZeroValue(t *testing.T) {
	var e Entry
	if e.Index != 0 || e.Term != 0 || e.Type != 0 || e.Cmd != nil {
		t.Errorf("zero Entry should have all zero fields, got %+v", e)
	}
}

func TestStateConstruction(t *testing.T) {
	s := State{
		Term:   10,
		Vote:   3,
		Commit: 99,
	}

	if s.Term != 10 {
		t.Errorf("State.Term = %d, want 10", s.Term)
	}
	if s.Vote != 3 {
		t.Errorf("State.Vote = %d, want 3", s.Vote)
	}
	if s.Commit != 99 {
		t.Errorf("State.Commit = %d, want 99", s.Commit)
	}
}

func TestStateZeroValue(t *testing.T) {
	var s State
	if s.Term != 0 || s.Vote != 0 || s.Commit != 0 {
		t.Errorf("zero State should have all zero fields, got %+v", s)
	}
}

func TestMembershipConstruction(t *testing.T) {
	m := Membership{
		ConfigChangeID: 7,
		Addresses:      map[uint64]string{1: "addr1", 2: "addr2"},
		Observers:      map[uint64]string{3: "obs1"},
		Witnesses:      map[uint64]string{4: "wit1"},
		Removed:        map[uint64]bool{5: true},
	}

	if m.ConfigChangeID != 7 {
		t.Errorf("Membership.ConfigChangeID = %d, want 7", m.ConfigChangeID)
	}
	if len(m.Addresses) != 2 {
		t.Errorf("Membership.Addresses length = %d, want 2", len(m.Addresses))
	}
	if m.Addresses[1] != "addr1" {
		t.Errorf("Membership.Addresses[1] = %q, want %q", m.Addresses[1], "addr1")
	}
	if len(m.Observers) != 1 {
		t.Errorf("Membership.Observers length = %d, want 1", len(m.Observers))
	}
	if len(m.Witnesses) != 1 {
		t.Errorf("Membership.Witnesses length = %d, want 1", len(m.Witnesses))
	}
	if !m.Removed[5] {
		t.Errorf("Membership.Removed[5] = false, want true")
	}
}

func TestMembershipZeroValue(t *testing.T) {
	var m Membership
	if m.ConfigChangeID != 0 {
		t.Errorf("zero Membership.ConfigChangeID = %d, want 0", m.ConfigChangeID)
	}
	if m.Addresses != nil || m.Observers != nil || m.Witnesses != nil || m.Removed != nil {
		t.Errorf("zero Membership map fields should be nil, got %+v", m)
	}
}

func TestSnapshotConstruction(t *testing.T) {
	s := Snapshot{
		Index: 100,
		Term:  5,
		Membership: Membership{
			Addresses: map[uint64]string{1: "addr1"},
		},
		Filepath:    "/data/snapshot-100",
		FileSize:    4096,
		OnDiskIndex: 95,
	}

	if s.Index != 100 {
		t.Errorf("Snapshot.Index = %d, want 100", s.Index)
	}
	if s.Term != 5 {
		t.Errorf("Snapshot.Term = %d, want 5", s.Term)
	}
	if s.Filepath != "/data/snapshot-100" {
		t.Errorf("Snapshot.Filepath = %q, want %q", s.Filepath, "/data/snapshot-100")
	}
	if s.FileSize != 4096 {
		t.Errorf("Snapshot.FileSize = %d, want 4096", s.FileSize)
	}
	if s.OnDiskIndex != 95 {
		t.Errorf("Snapshot.OnDiskIndex = %d, want 95", s.OnDiskIndex)
	}
	if len(s.Membership.Addresses) != 1 {
		t.Errorf("Snapshot.Membership.Addresses length = %d, want 1", len(s.Membership.Addresses))
	}
}

func TestSnapshotZeroValue(t *testing.T) {
	var s Snapshot
	if s.Index != 0 || s.Term != 0 || s.Filepath != "" || s.FileSize != 0 || s.OnDiskIndex != 0 {
		t.Errorf("zero Snapshot should have all zero fields, got %+v", s)
	}
}

func TestBootstrapConstruction(t *testing.T) {
	b := Bootstrap{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		Join:      true,
		Type:      2,
	}

	if len(b.Addresses) != 3 {
		t.Errorf("Bootstrap.Addresses length = %d, want 3", len(b.Addresses))
	}
	if !b.Join {
		t.Errorf("Bootstrap.Join = false, want true")
	}
	if b.Type != 2 {
		t.Errorf("Bootstrap.Type = %d, want 2", b.Type)
	}
}

func TestBootstrapZeroValue(t *testing.T) {
	var b Bootstrap
	if b.Addresses != nil || b.Join || b.Type != 0 {
		t.Errorf("zero Bootstrap should have all zero fields, got %+v", b)
	}
}

func TestUpdateConstruction(t *testing.T) {
	u := Update{
		ShardID:   10,
		ReplicaID: 20,
		State:     State{Term: 5, Vote: 1, Commit: 50},
		Entries: []Entry{
			{Index: 51, Term: 5, Type: 0, Cmd: []byte("cmd1")},
			{Index: 52, Term: 5, Type: 0, Cmd: []byte("cmd2")},
		},
		Snapshot: Snapshot{Index: 50, Term: 4},
	}

	if u.ShardID != 10 {
		t.Errorf("Update.ShardID = %d, want 10", u.ShardID)
	}
	if u.ReplicaID != 20 {
		t.Errorf("Update.ReplicaID = %d, want 20", u.ReplicaID)
	}
	if u.State.Term != 5 {
		t.Errorf("Update.State.Term = %d, want 5", u.State.Term)
	}
	if len(u.Entries) != 2 {
		t.Errorf("Update.Entries length = %d, want 2", len(u.Entries))
	}
	if u.Snapshot.Index != 50 {
		t.Errorf("Update.Snapshot.Index = %d, want 50", u.Snapshot.Index)
	}
}

func TestUpdateZeroValue(t *testing.T) {
	var u Update
	if u.ShardID != 0 || u.ReplicaID != 0 || u.Entries != nil {
		t.Errorf("zero Update should have all zero fields, got ShardID=%d ReplicaID=%d Entries=%v",
			u.ShardID, u.ReplicaID, u.Entries)
	}
}

// --- LogDB interface tests ---

func TestTestLogDBImplementsInterface(t *testing.T) {
	var db LogDB = &testLogDB{}

	if name := db.Name(); name != "test" {
		t.Errorf("Name() = %q, want %q", name, "test")
	}

	if err := db.Close(); err != nil {
		t.Errorf("Close() returned unexpected error: %v", err)
	}
}

func TestTestLogDBSaveStateAcceptsEmptySlice(t *testing.T) {
	var db LogDB = &testLogDB{}
	if err := db.SaveState(nil); err != nil {
		t.Errorf("SaveState(nil) returned unexpected error: %v", err)
	}
	if err := db.SaveState([]Update{}); err != nil {
		t.Errorf("SaveState([]Update{}) returned unexpected error: %v", err)
	}
}

func TestTestLogDBIterateEntriesReturnsInputs(t *testing.T) {
	var db LogDB = &testLogDB{}
	inputEntries := []Entry{{Index: 1, Term: 1}}
	var inputSize uint64 = 42

	entries, size, err := db.IterateEntries(inputEntries, inputSize, 1, 1, 1, 10, 1024)
	if err != nil {
		t.Errorf("IterateEntries() returned unexpected error: %v", err)
	}
	if len(entries) != len(inputEntries) {
		t.Errorf("IterateEntries() entries length = %d, want %d", len(entries), len(inputEntries))
	}
	if size != inputSize {
		t.Errorf("IterateEntries() size = %d, want %d", size, inputSize)
	}
}

func TestTestLogDBGetBootstrapReturnsNotFound(t *testing.T) {
	var db LogDB = &testLogDB{}
	_, found, err := db.GetBootstrap(1, 1)
	if err != nil {
		t.Errorf("GetBootstrap() returned unexpected error: %v", err)
	}
	if found {
		t.Errorf("GetBootstrap() found = true, want false for test implementation")
	}
}

// The tests above this line are, with one exception, statements about Go rather
// than about this package: they assign struct fields and assert the same values
// back, which no production defect can falsify — removing or retyping a field
// is a COMPILE error, not a test failure. The exception is the sentinel group,
// which does pin production values: losing the "logdb: " prefix, or aliasing two
// sentinels so errors.Is cannot tell them apart, both fail there. That
// distinction is load-bearing — raft falls back to InstallSnapshot on
// ErrCompacted and retries on ErrUnavailable — so it is worth having.
//
// What follows are the package's actual behaviours, which had no tests at all.

// TestCopyEntryCmd_DetachesThePayload pins the function both LogDB backends
// rely on to keep their in-memory logs private.
//
// It is the whole read-side no-aliasing rule in one place. The backends call it
// on every entry they serve, so a change that returned the entry unchanged
// would hand every caller a slice into the live log — and the conformance suite
// would catch it, but only through two backends and a round-trip. Here the
// property is asserted directly on the function that carries it.
func TestCopyEntryCmd_DetachesThePayload(t *testing.T) {
	original := []byte("committed-command")
	src := Entry{Index: 7, Term: 3, Type: 1, Key: 9, ClientID: 2, SeriesID: 4,
		RespondedTo: 1, Cmd: original}

	got := CopyEntryCmd(src)

	// Every field other than Cmd is carried through untouched.
	if got.Index != src.Index || got.Term != src.Term || got.Type != src.Type ||
		got.Key != src.Key || got.ClientID != src.ClientID ||
		got.SeriesID != src.SeriesID || got.RespondedTo != src.RespondedTo ||
		got.OwnerID != src.OwnerID {
		t.Fatalf("CopyEntryCmd changed a field other than Cmd:\n got %+v\nwant %+v",
			got, src)
	}
	if !bytes.Equal(got.Cmd, original) {
		t.Fatalf("CopyEntryCmd returned Cmd %q, want %q", got.Cmd, original)
	}
	if &got.Cmd[0] == &original[0] {
		t.Fatal("CopyEntryCmd returned a Cmd that still points at the caller's " +
			"backing array: the copy did not happen, and every entry a LogDB " +
			"serves would alias the live log it serves it from")
	}

	// Writing through the copy must not reach the original, in either direction.
	for i := range got.Cmd {
		got.Cmd[i] = 'X'
	}
	if !bytes.Equal(original, []byte("committed-command")) {
		t.Fatalf("writing to the copy changed the original: %q", original)
	}

	// A nil or empty payload is returned as-is; there is nothing to detach and
	// allocating for it would put a needless allocation on the read path.
	if e := CopyEntryCmd(Entry{Index: 1}); e.Cmd != nil {
		t.Fatalf("CopyEntryCmd allocated for a nil Cmd: %v", e.Cmd)
	}
	empty := Entry{Index: 1, Cmd: []byte{}}
	if e := CopyEntryCmd(empty); len(e.Cmd) != 0 {
		t.Fatalf("CopyEntryCmd changed an empty Cmd: %v", e.Cmd)
	}
}

// TestEntriesBelowCompactionFloorError pins the typed error's contract: it
// carries enough to LOCATE the caller bug, and errors.Is against the sentinel
// holds so callers can match it without depending on the concrete type.
func TestEntriesBelowCompactionFloorError(t *testing.T) {
	err := &EntriesBelowCompactionFloorError{
		ShardID: 3, ReplicaID: 7, FirstIndex: 5, CompactedTo: 9,
	}
	if !errors.Is(err, ErrEntriesBelowCompactionFloor) {
		t.Fatal("errors.Is against ErrEntriesBelowCompactionFloor does not hold; " +
			"every backend returns this type, so callers cannot match the condition " +
			"portably without it")
	}
	msg := err.Error()
	for _, want := range []string{"shard 3", "replica 7", "first index 5", "compacted to 9"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error message %q does not contain %q: the refusal can be "+
				"observed but the write that caused it cannot be located", msg, want)
		}
	}
	if !strings.HasPrefix(msg, ErrEntriesBelowCompactionFloor.Error()) {
		t.Fatalf("error message %q does not lead with the sentinel's text", msg)
	}
	// Wrapped in another error, both the sentinel match and the detail survive.
	wrapped := fmt.Errorf("SaveState: %w", err)
	var detail *EntriesBelowCompactionFloorError
	if !errors.As(wrapped, &detail) || detail.FirstIndex != 5 {
		t.Fatalf("errors.As through a wrapper lost the detail: %+v", detail)
	}
}

// TestAtRestProtectionString pins every rendering, including an
// out-of-range value, since these strings reach structured logs and errors.
func TestAtRestProtectionString(t *testing.T) {
	for _, tc := range []struct {
		p    AtRestProtection
		want string
	}{
		{AtRestUnprotected, "unprotected"},
		{AtRestNonDurable, "non_durable"},
		{AtRestBarrierEncrypted, "barrier_encrypted"},
		{AtRestProtection(250), "invalid"},
	} {
		if got := tc.p.String(); got != tc.want {
			t.Errorf("AtRestProtection(%d).String() = %q, want %q", tc.p, got, tc.want)
		}
	}
}
