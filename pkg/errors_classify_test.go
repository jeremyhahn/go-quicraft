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
	"os"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
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
		// Temporary: the transfer completes or times out on its own and the
		// identical request then succeeds. Produced BEFORE the entry enters
		// the log, so it is never MaybeApplied.
		{"ErrTransferInProgress", ErrTransferInProgress, true},
		{"ErrSnapshotInProgress", ErrSnapshotInProgress, true},
		{"ErrImportInProgress", ErrImportInProgress, true},

		// --- permanent ---
		{"ErrClosed", ErrClosed, false},
		{"ErrCanceled", ErrCanceled, false},
		{"ErrRejected", ErrRejected, false},
		{"ErrAtRestEncryptionUnspecified", ErrAtRestEncryptionUnspecified, false},
		{"ErrAtRestEncryptionConflict", ErrAtRestEncryptionConflict, false},
		{"ErrBarrierPlaintextLogDB", ErrBarrierPlaintextLogDB, false},
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
		{"ErrSessionBusy", ErrSessionBusy, true},
		{"ErrSnapshotSizeMismatch", ErrSnapshotSizeMismatch, false},
		{"ErrSnapshotCorrupt", ErrSnapshotCorrupt, false},
		{"ErrEntropyDegenerate", ErrEntropyDegenerate, false},
		{"ErrSnapshotDestinationRecorded", ErrSnapshotDestinationRecorded, false},
		{"ErrShardNotStopped", ErrShardNotStopped, false},
		{"ErrReplicaRetired", ErrReplicaRetired, false},
		{"ErrStaleConfigChange", ErrStaleConfigChange, false},
		{"ErrReplicaNotFound", ErrReplicaNotFound, false},
		{"ErrInvalidRange", ErrInvalidRange, false},
		{"ErrInvalidMember", ErrInvalidMember, false},
		{"ErrSnapshotTooLarge", ErrSnapshotTooLarge, false},
		{"ErrTooManyWitnesses", ErrTooManyWitnesses, false},
		{"ErrBreaksFullReplicaMajority", ErrBreaksFullReplicaMajority, false},
		{"ErrRemoveLastFullReplica", ErrRemoveLastFullReplica, false},
		// Membership validation rejections. Every one describes the request
		// itself, not a condition on this host, so the IDENTICAL request fails
		// forever: the caller must re-read the membership and build a
		// different one.
		{"ErrNodeNotFound", ErrNodeNotFound, false},
		{"ErrNodeAlreadyExists", ErrNodeAlreadyExists, false},
		// Permanent: a removed member's identity is banned from the shard for
		// good, under any replica ID, so retrying the same add never succeeds.
		{"ErrRemovedMemberIdentity", ErrRemovedMemberIdentity, false},
		{"ErrNodeRemoved", ErrNodeRemoved, false},
		{"ErrRemoveLastVoter", ErrRemoveLastVoter, false},
		{"ErrTooManyVotingMembers", ErrTooManyVotingMembers, false},
		{"ErrZeroReplicaID", ErrZeroReplicaID, false},
		{"ErrEmptyAddress", ErrEmptyAddress, false},
		{"ErrMemberIdentityRequired", ErrMemberIdentityRequired, false},
		// Permanent: the address or identity stays held until the member
		// holding it is removed, which the caller must do deliberately.
		{"ErrDuplicateMemberAddress", ErrDuplicateMemberAddress, false},
		{"ErrDuplicateMemberIdentity", ErrDuplicateMemberIdentity, false},
		// Permanent: only re-issuing this node's certificate with per-node
		// SANs clears it.
		{"ErrSharedPeerCertificate", ErrSharedPeerCertificate, false},
		// A malformed health value in a decoded document does not become
		// well-formed by decoding it again.
		{"ErrInvalidShardHealth", ErrInvalidShardHealth, false},
		{"ErrNonVotingRoleInMembers", ErrNonVotingRoleInMembers, false},
		{"ErrImportLayoutUnknown", ErrImportLayoutUnknown, false},
		{"ErrImportMembershipInvalid", ErrImportMembershipInvalid, false},
		{"ErrImportDataDirMismatch", ErrImportDataDirMismatch, false},
		{"ErrImportSnapshotDirExists", ErrImportSnapshotDirExists, false},

		// Declared outside errors.go. These were invisible to the
		// exhaustiveness check for as long as it parsed only errors.go, so
		// nobody had ever decided whether they were retryable.
		{"ErrSessionsUnsupportedOnDisk", ErrSessionsUnsupportedOnDisk, false},
		{"ErrSnapshotEncryptedBarrierUnavailable", ErrSnapshotEncryptedBarrierUnavailable, false},
		{"ErrSnapshotBarrierSealed", ErrSnapshotBarrierSealed, false},
		{"ErrInvalidSnapshotOption", ErrInvalidSnapshotOption, false},
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

// declaredSentinels parses every non-test source file of THIS package and
// returns the exported Err* package-level variables it declares, mapped to the
// file that declares them.
//
// It reads the whole package, not errors.go alone. Sentinels are declared next
// to what produces them — ErrSessionsUnsupportedOnDisk and the two snapshot
// barrier re-exports live in host.go, ErrInvalidSnapshotOption in
// snapshot_option.go — and a check scoped to one file simply cannot see them.
// Four exported sentinels were unclassified for exactly that reason: the test
// reported complete coverage of a set it had defined to exclude them.
func declaredSentinels(t *testing.T) map[string]string {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	fset := token.NewFileSet()
	declared := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, name, nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", name, parseErr)
		}
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
				for _, ident := range value.Names {
					if strings.HasPrefix(ident.Name, "Err") && len(ident.Name) > 3 &&
						ast.IsExported(ident.Name) {
						declared[ident.Name] = name
					}
				}
			}
		}
	}
	if len(declared) == 0 {
		t.Fatal("parsed no exported Err* sentinels from the package; the AST walk is broken")
	}
	return declared
}

// TestTemporary_ClassifiesEverySentinel holds the classification table and the
// package's actual declarations against each other in BOTH directions.
//
// The one-directional version could only catch an addition. A sentinel that
// was REMOVED left its table row behind, still asserting a classification for
// something no longer in the API, and a sentinel declared in any file other
// than errors.go was never seen at all. Both directions are needed because the
// table is a record of a human decision and the declarations are the API: they
// are two independent sources, and the test is only worth anything while it
// compares them rather than restating one of them.
func TestTemporary_ClassifiesEverySentinel(t *testing.T) {
	declared := declaredSentinels(t)

	classified := make(map[string]bool, len(sentinelTable()))
	for _, tc := range sentinelTable() {
		if classified[tc.name] {
			t.Errorf("sentinel %s appears twice in sentinelTable", tc.name)
		}
		classified[tc.name] = true
	}

	for name, file := range declared {
		if !classified[name] {
			t.Errorf("sentinel %s is declared in %s but is not classified in "+
				"sentinelTable; decide whether it is temporary and add a row", name, file)
		}
	}
	for name := range classified {
		if _, ok := declared[name]; !ok {
			t.Errorf("sentinelTable classifies %s, which this package no longer "+
				"declares; drop the row so the table describes the real API", name)
		}
	}
}

// TestTemporary_ImplementationMatchesTheDecision compares the temporaryErrors
// LIST — the implementation — against the classification table — the recorded
// decision. They are written independently and must agree.
//
// Without it the suite only ever asserted the list against itself: sentinelTable
// says "temporary" exactly where temporaryErrors has an entry, and Temporary
// answers from temporaryErrors, so TestTemporary_Sentinels passes for any list
// the table was copied from. Appending a sentinel to temporaryErrors and
// nothing else — silently making a permanent failure retried forever — was
// invisible. Here it is not: the sentinel becomes temporary while the table
// still says permanent.
func TestTemporary_ImplementationMatchesTheDecision(t *testing.T) {
	byIdentity := make(map[error]string, len(sentinelTable()))
	wantTemporary := make(map[string]bool, len(sentinelTable()))
	for _, tc := range sentinelTable() {
		byIdentity[tc.err] = tc.name
		wantTemporary[tc.name] = tc.temporary
	}

	seen := make(map[string]bool, len(temporaryErrors))
	for _, listed := range temporaryErrors {
		name, known := byIdentity[listed]
		if !known {
			t.Errorf("temporaryErrors holds %v, which sentinelTable does not "+
				"classify; every retryable sentinel must be a decided one", listed)
			continue
		}
		if seen[name] {
			t.Errorf("temporaryErrors lists %s twice", name)
		}
		seen[name] = true
		if !wantTemporary[name] {
			t.Errorf("temporaryErrors makes %s retryable, but sentinelTable "+
				"classifies it as permanent", name)
		}
	}

	// The other direction, allowing for sentinels that are temporary through
	// another entry's Is (ErrSystemBusy is a *BusyError and matches ErrBusy).
	for name, temporary := range wantTemporary {
		if temporary && !seen[name] && !Temporary(mustSentinel(t, name)) {
			t.Errorf("sentinelTable classifies %s as retryable but nothing in "+
				"temporaryErrors makes Temporary agree", name)
		}
	}
}

// mustSentinel returns the error the table records under name.
func mustSentinel(t *testing.T, name string) error {
	t.Helper()
	for _, tc := range sentinelTable() {
		if tc.name == name {
			return tc.err
		}
	}
	t.Fatalf("no sentinel named %s in sentinelTable", name)
	return nil
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

// TestTransferInProgress_IsTemporaryAndNotMaybeApplied pins the public
// classification of ErrTransferInProgress.
//
// raft.handleLeaderPropose returns ErrTransferInProgress for EVERY proposal
// type while a leadership transfer is in flight, and handleReadIndex returns it
// for linearizable reads. It lives in an internal package, so without the
// re-export no caller could match it with errors.Is and would resort to
// strings.Contains(err.Error(), "transfer in progress") — string matching on
// error text, the shape to avoid.
//
// The three properties a caller depends on, pinned here so a future change to
// the classification has to be deliberate:
//
//   - it is matchable by sentinel, wrapped or not;
//   - Temporary is TRUE — the transfer completes or times out with no operator
//     or caller action, and the identical request then succeeds;
//   - MaybeApplied is FALSE — raft returns it before the entry enters the log,
//     so nothing was submitted and nothing can commit later. A caller that
//     treated it as maybe-applied would go looking for an entry that does not
//     exist, which on the config-change path is how "already exists" gets
//     misread as success.
func TestTransferInProgress_IsTemporaryAndNotMaybeApplied(t *testing.T) {
	if !errors.Is(ErrTransferInProgress, raft.ErrTransferInProgress) {
		t.Fatal("the exported sentinel must BE the engine's error, not a copy of " +
			"it; a copy matches nothing the engine actually returns")
	}
	if !Temporary(ErrTransferInProgress) {
		t.Error("Temporary(ErrTransferInProgress) = false; a transfer clears on " +
			"its own and the identical request then succeeds")
	}
	if MaybeApplied(ErrTransferInProgress) {
		t.Error("MaybeApplied(ErrTransferInProgress) = true; it is produced " +
			"before the entry enters the log, so nothing can commit later")
	}

	// Wrapped, which is how it reaches a caller through the adapter
	// (*AdapterError) and through any fmt.Errorf on the way out.
	wrapped := fmt.Errorf("propose shard 1: %w", ErrTransferInProgress)
	if !errors.Is(wrapped, ErrTransferInProgress) {
		t.Error("wrapping broke errors.Is; the substring match this replaces " +
			"survived wrapping, and the sentinel must too")
	}
	if !Temporary(wrapped) {
		t.Error("wrapping broke Temporary()")
	}
	if MaybeApplied(wrapped) {
		t.Error("wrapping made it MaybeApplied")
	}
}
