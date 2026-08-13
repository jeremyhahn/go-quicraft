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
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// sentinelClassification is one row of the exhaustive classification table.
// Every exported sentinel declared in errors.go must appear exactly once;
// TestTemporary_ClassifiesEverySentinel enforces that against the source.
type sentinelClassification struct {
	name string
	err  error
	// temporary is the answer Temporary must give. A sentinel nobody can
	// confidently classify is false, because retrying a permanent failure
	// forever is worse than failing fast.
	temporary bool
}

func sentinelTable() []sentinelClassification {
	return []sentinelClassification{
		// --- transient: the identical request can succeed on retry ---
		{"ErrBusy", ErrBusy, true},
		{"ErrSystemBusy", ErrSystemBusy, true},
		{"ErrTimeout", ErrTimeout, true},
		{"ErrAborted", ErrAborted, true},
		{"ErrDropped", ErrDropped, true},
		{"ErrShardNotStarted", ErrShardNotStarted, true},
		{"ErrShardNotReady", ErrShardNotReady, true},
		{"ErrNotLeader", ErrNotLeader, true},
		{"ErrReadIndexNotReady", ErrReadIndexNotReady, true},
		{"ErrRotateInProgress", ErrRotateInProgress, true},
		{"ErrSnapshotInProgress", ErrSnapshotInProgress, true},

		// --- permanent ---
		{"ErrClosed", ErrClosed, false},
		{"ErrCanceled", ErrCanceled, false},
		{"ErrRejected", ErrRejected, false},
		{"ErrShardBarriersPlaintextWAL", ErrShardBarriersPlaintextWAL, false},
		{"ErrShardBarrierUnresolved", ErrShardBarrierUnresolved, false},
		{"ErrShardBarrierLogDBMismatch", ErrShardBarrierLogDBMismatch, false},
		{"ErrShardNotFound", ErrShardNotFound, false},
		{"ErrShardAlreadyExists", ErrShardAlreadyExists, false},
		{"ErrShardFailed", ErrShardFailed, false},
		{"ErrInvalidConfig", ErrInvalidConfig, false},
		{"ErrInvalidOperation", ErrInvalidOperation, false},
		{"ErrReleased", ErrReleased, false},
		{"ErrLogDBWrite", ErrLogDBWrite, false},
		{"ErrRecoveredTermUnknown", ErrRecoveredTermUnknown, false},
		{"ErrShutdownTimeout", ErrShutdownTimeout, false},
		{"ErrDecompressionBomb", ErrDecompressionBomb, false},
		{"ErrSessionLimitExceeded", ErrSessionLimitExceeded, false},
		{"ErrTooManySessions", ErrTooManySessions, false},
		{"ErrBarrierSealed", ErrBarrierSealed, false},
		{"ErrBarrierNotInit", ErrBarrierNotInit, false},
		{"ErrDiskFull", ErrDiskFull, false},
		{"ErrDecryptionFailed", ErrDecryptionFailed, false},
		{"ErrSnapshotRecoveryFailed", ErrSnapshotRecoveryFailed, false},
		{"ErrEntryTooLarge", ErrEntryTooLarge, false},
		{"ErrSessionNotFound", ErrSessionNotFound, false},
		{"ErrSessionExpired", ErrSessionExpired, false},
		{"ErrSessionAlreadyExists", ErrSessionAlreadyExists, false},
		{"ErrResponseLimitExceeded", ErrResponseLimitExceeded, false},
		{"ErrInvalidSMType", ErrInvalidSMType, false},
		{"ErrSnapshotNotFound", ErrSnapshotNotFound, false},
		{"ErrStopped", ErrStopped, false},
		{"ErrDraining", ErrDraining, false},
		{"ErrInvalidSession", ErrInvalidSession, false},
		{"ErrSnapshotSizeMismatch", ErrSnapshotSizeMismatch, false},
		{"ErrSnapshotCorrupt", ErrSnapshotCorrupt, false},
		{"ErrShardNotStopped", ErrShardNotStopped, false},
		{"ErrStaleConfigChange", ErrStaleConfigChange, false},
		{"ErrReplicaNotFound", ErrReplicaNotFound, false},
		{"ErrInvalidRange", ErrInvalidRange, false},
		{"ErrInvalidMember", ErrInvalidMember, false},
		{"ErrSnapshotTooLarge", ErrSnapshotTooLarge, false},
		{"ErrTooManyWitnesses", ErrTooManyWitnesses, false},
		{"ErrBreaksFullReplicaMajority", ErrBreaksFullReplicaMajority, false},
		{"ErrRemoveLastFullReplica", ErrRemoveLastFullReplica, false},
		{"ErrNonVotingRoleInMembers", ErrNonVotingRoleInMembers, false},
		{"ErrImportLayoutUnknown", ErrImportLayoutUnknown, false},
		{"ErrImportDataDirMismatch", ErrImportDataDirMismatch, false},
		{"ErrImportSnapshotDirExists", ErrImportSnapshotDirExists, false},
	}
}

// TestTemporary_Sentinels asserts the classification of every exported
// sentinel. It is deliberately exhaustive rather than sampled: a helper that
// answered a constant, or that classified by name prefix, fails here.
func TestTemporary_Sentinels(t *testing.T) {
	for _, tc := range sentinelTable() {
		if got := Temporary(tc.err); got != tc.temporary {
			t.Errorf("Temporary(%s) = %v, want %v", tc.name, got, tc.temporary)
		}
	}
}

// TestTemporary_ClassifiesEverySentinel parses errors.go and fails when it
// declares an exported Err* sentinel that the classification table does not
// mention. Without this, a sentinel added later is silently permanent and
// nobody ever decides whether that is right.
func TestTemporary_ClassifiesEverySentinel(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "errors.go", nil, 0)
	if err != nil {
		t.Fatalf("parse errors.go: %v", err)
	}

	classified := make(map[string]bool, len(sentinelTable()))
	for _, tc := range sentinelTable() {
		classified[tc.name] = true
	}

	var declared []string
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, name := range value.Names {
				if len(name.Name) > 3 && name.Name[:3] == "Err" && ast.IsExported(name.Name) {
					declared = append(declared, name.Name)
				}
			}
		}
	}

	if len(declared) == 0 {
		t.Fatal("parsed no exported Err* sentinels from errors.go; the AST walk is broken")
	}
	for _, name := range declared {
		if !classified[name] {
			t.Errorf("sentinel %s is declared in errors.go but is not classified in "+
				"sentinelTable; decide whether it is temporary and add a row", name)
		}
	}
}

// TestTemporary_Wrapped proves the classification survives wrapping, which is
// the whole reason it uses errors.Is instead of equality.
func TestTemporary_Wrapped(t *testing.T) {
	if !Temporary(fmt.Errorf("proposing to shard 7: %w", ErrTimeout)) {
		t.Error("wrapped ErrTimeout must classify as temporary")
	}
	if Temporary(fmt.Errorf("proposing to shard 7: %w", ErrShardNotFound)) {
		t.Error("wrapped ErrShardNotFound must not classify as temporary")
	}
	// Two layers, and the temporary sentinel is the innermost.
	deep := fmt.Errorf("host: %w", fmt.Errorf("shard 7: %w", ErrBusy))
	if !Temporary(deep) {
		t.Error("doubly wrapped ErrBusy must classify as temporary")
	}
}

// TestTemporary_TypedErrors proves the typed errors classify through their Is
// methods rather than only as bare sentinels.
func TestTemporary_TypedErrors(t *testing.T) {
	busy := &BusyError{ShardID: 3, CurrentSize: 10, MaxSize: 5}
	if !Temporary(busy) {
		t.Error("*BusyError must classify as temporary")
	}
	notLeader := &NotLeaderError{ShardID: 3, LeaderID: 9, LeaderAddress: "n9:9000"}
	if !Temporary(notLeader) {
		t.Error("*NotLeaderError must classify as temporary")
	}
	tooLarge := &EntryTooLargeError{Size: 100, MaxSize: 10}
	if Temporary(tooLarge) {
		t.Error("*EntryTooLargeError must not classify as temporary")
	}
	expired := &SessionExpiredError{ShardID: 3, ClientID: 4}
	if Temporary(expired) {
		t.Error("*SessionExpiredError must not classify as temporary")
	}
}

// TestTemporary_Nil pins the documented nil behaviour.
func TestTemporary_Nil(t *testing.T) {
	if Temporary(nil) {
		t.Error("Temporary(nil) must be false")
	}
}

// TestTemporary_ForeignError pins the conservative default: an error this
// package never produced is permanent, not temporary.
func TestTemporary_ForeignError(t *testing.T) {
	if Temporary(errors.New("some other library: connection reset")) {
		t.Error("an unrecognised error must default to permanent")
	}
}

// TestNotLeader asserts the leader ID is surfaced, not merely that the error
// was recognised — surfacing LeaderID is the entire point of the helper.
func TestNotLeader(t *testing.T) {
	t.Run("typed error carries the leader", func(t *testing.T) {
		id, ok := NotLeader(&NotLeaderError{ShardID: 1, LeaderID: 7, LeaderAddress: "n7:9000"})
		if !ok {
			t.Fatal("NotLeader must recognise *NotLeaderError")
		}
		if id != 7 {
			t.Fatalf("NotLeader returned replicaID %d, want 7", id)
		}
	})

	t.Run("wrapped typed error still carries the leader", func(t *testing.T) {
		wrapped := fmt.Errorf("propose: %w", &NotLeaderError{ShardID: 1, LeaderID: 42})
		id, ok := NotLeader(wrapped)
		if !ok {
			t.Fatal("NotLeader must unwrap to *NotLeaderError")
		}
		if id != 42 {
			t.Fatalf("NotLeader returned replicaID %d, want 42", id)
		}
	})

	t.Run("unknown leader is reported as zero, not as not-a-leader-error", func(t *testing.T) {
		id, ok := NotLeader(&NotLeaderError{ShardID: 1, LeaderID: 0})
		if !ok {
			t.Fatal("an unknown leader is still a not-leader error")
		}
		if id != 0 {
			t.Fatalf("NotLeader returned replicaID %d, want 0", id)
		}
	})

	t.Run("bare sentinel", func(t *testing.T) {
		id, ok := NotLeader(ErrNotLeader)
		if !ok {
			t.Fatal("NotLeader must recognise the bare ErrNotLeader sentinel")
		}
		if id != 0 {
			t.Fatalf("NotLeader returned replicaID %d, want 0 for the bare sentinel", id)
		}
	})

	t.Run("other errors", func(t *testing.T) {
		for _, err := range []error{nil, ErrTimeout, ErrShardNotFound, errors.New("x")} {
			if id, ok := NotLeader(err); ok || id != 0 {
				t.Errorf("NotLeader(%v) = (%d, %v), want (0, false)", err, id, ok)
			}
		}
	})
}

// TestNotLeader_AgreesWithTemporary pins the documented relationship: every
// not-leader error is retryable.
func TestNotLeader_AgreesWithTemporary(t *testing.T) {
	errs := []error{
		ErrNotLeader,
		&NotLeaderError{ShardID: 1, LeaderID: 5},
		fmt.Errorf("wrapped: %w", &NotLeaderError{ShardID: 1}),
	}
	for _, err := range errs {
		if _, ok := NotLeader(err); !ok {
			t.Fatalf("setup: %v is not a not-leader error", err)
		}
		if !Temporary(err) {
			t.Errorf("Temporary(%v) = false; every not-leader error must be temporary", err)
		}
	}
}
