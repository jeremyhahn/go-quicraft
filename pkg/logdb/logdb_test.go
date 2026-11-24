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

// --- Error tests ---

// The sentinel group pins production values: losing the "logdb: " prefix, or
// aliasing two sentinels so errors.Is cannot tell them apart, both fail here.
// That distinction is load-bearing — raft falls back to InstallSnapshot on
// ErrCompacted and retries on ErrUnavailable.

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
		{AtRestHostRouted, "host_routed"},
		{AtRestProtection(250), "invalid"},
	} {
		if got := tc.p.String(); got != tc.want {
			t.Errorf("AtRestProtection(%d).String() = %q, want %q", tc.p, got, tc.want)
		}
	}
}
