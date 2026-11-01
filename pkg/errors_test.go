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
	"strings"
	"testing"
)

// allSentinelErrors enumerates every sentinel error in the package.
// Adding a sentinel error to errors.go without adding it here will
// cause TestSentinelErrors_NonEmptyMessages to miss it, so keep
// this slice in sync.
var allSentinelErrors = []struct {
	name string
	err  error
}{
	{"ErrBusy", ErrBusy},
	{"ErrClosed", ErrClosed},
	{"ErrCanceled", ErrCanceled},
	{"ErrTimeout", ErrTimeout},
	{"ErrRejected", ErrRejected},
	{"ErrAborted", ErrAborted},
	{"ErrDropped", ErrDropped},
	{"ErrShardNotFound", ErrShardNotFound},
	{"ErrShardNotStarted", ErrShardNotStarted},
	{"ErrShardNotReady", ErrShardNotReady},
	{"ErrShardAlreadyExists", ErrShardAlreadyExists},
	{"ErrShardFailed", ErrShardFailed},
	{"ErrInvalidConfig", ErrInvalidConfig},
	{"ErrInvalidOperation", ErrInvalidOperation},
	{"ErrNotLeader", ErrNotLeader},
	{"ErrReleased", ErrReleased},
	{"ErrLogDBWrite", ErrLogDBWrite},
	{"ErrShutdownTimeout", ErrShutdownTimeout},
	{"ErrSystemBusy", ErrSystemBusy},
	{"ErrDecompressionBomb", ErrDecompressionBomb},
	{"ErrSessionLimitExceeded", ErrSessionLimitExceeded},
	{"ErrTooManySessions", ErrTooManySessions},
	{"ErrBarrierSealed", ErrBarrierSealed},
	{"ErrShardPaused", ErrShardPaused},
	{"ErrBarrierNotInit", ErrBarrierNotInit},
	{"ErrDiskFull", ErrDiskFull},
	{"ErrDecryptionFailed", ErrDecryptionFailed},
	{"ErrSnapshotRecoveryFailed", ErrSnapshotRecoveryFailed},
	{"ErrEntryTooLarge", ErrEntryTooLarge},
	{"ErrSessionExpired", ErrSessionExpired},
	{"ErrSessionNotFound", ErrSessionNotFound},
	{"ErrSessionAlreadyExists", ErrSessionAlreadyExists},
	{"ErrResponseLimitExceeded", ErrResponseLimitExceeded},
	{"ErrInvalidSMType", ErrInvalidSMType},
	{"ErrSnapshotNotFound", ErrSnapshotNotFound},
	{"ErrSnapshotInProgress", ErrSnapshotInProgress},
	{"ErrSnapshotDestinationRecorded", ErrSnapshotDestinationRecorded},
	{"ErrEntropyDegenerate", ErrEntropyDegenerate},
	{"ErrStopped", ErrStopped},
	{"ErrReadIndexNotReady", ErrReadIndexNotReady},
	{"ErrNoLeader", ErrNoLeader},
	{"ErrLeadershipChanged", ErrLeadershipChanged},
	{"ErrLeaderHandlerNotRegistered", ErrLeaderHandlerNotRegistered},
	{"ErrDraining", ErrDraining},
	{"ErrInvalidSession", ErrInvalidSession},
	{"ErrSnapshotSizeMismatch", ErrSnapshotSizeMismatch},
	{"ErrShardNotStopped", ErrShardNotStopped},
	{"ErrReplicaNotFound", ErrReplicaNotFound},
	{"ErrStaleConfigChange", ErrStaleConfigChange},
	{"ErrInvalidMember", ErrInvalidMember},
}

func TestSentinelErrors_NonEmptyMessages(t *testing.T) {
	for _, tc := range allSentinelErrors {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.err.Error()
			if msg == "" {
				t.Errorf("%s has an empty error message", tc.name)
			}
			if !strings.HasPrefix(msg, "quicraft") {
				t.Errorf("%s message %q does not start with 'quicraft' prefix", tc.name, msg)
			}
		})
	}
}

func TestSentinelErrors_Distinct(t *testing.T) {
	seen := make(map[string]string, len(allSentinelErrors))
	for _, tc := range allSentinelErrors {
		msg := tc.err.Error()
		if prev, ok := seen[msg]; ok {
			// ErrTooManySessions is an intentional legacy alias; skip it.
			if tc.name == "ErrTooManySessions" || prev == "ErrTooManySessions" {
				continue
			}
			t.Errorf("duplicate error message %q shared by %s and %s", msg, prev, tc.name)
		}
		seen[msg] = tc.name
	}
}

// knownHierarchy lists sentinel error pairs where errors.Is(a, b) should
// return true by design. ErrSystemBusy is a *BusyError that matches ErrBusy.
// ErrTooManySessions is a pointer alias for ErrSessionLimitExceeded.
var knownHierarchy = map[[2]string]bool{
	{"ErrSystemBusy", "ErrBusy"}:                      true,
	{"ErrTooManySessions", "ErrSessionLimitExceeded"}: true,
	{"ErrSessionLimitExceeded", "ErrTooManySessions"}: true,
}

func TestSentinelErrors_NoFalsePositives(t *testing.T) {
	for i, a := range allSentinelErrors {
		for j, b := range allSentinelErrors {
			if i == j {
				continue
			}
			pair := [2]string{a.name, b.name}
			if knownHierarchy[pair] {
				continue
			}
			if errors.Is(a.err, b.err) {
				t.Errorf("errors.Is(%s, %s) unexpectedly returned true", a.name, b.name)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// NotLeaderError
// ---------------------------------------------------------------------------

func TestNotLeaderError_ErrorWithLeader(t *testing.T) {
	err := &NotLeaderError{
		ShardID:       1,
		LeaderID:      2,
		LeaderAddress: "10.0.0.1:5000",
	}
	msg := err.Error()
	if !strings.Contains(msg, "shard 1") {
		t.Errorf("expected shard ID in message, got %q", msg)
	}
	if !strings.Contains(msg, "replica 2") {
		t.Errorf("expected leader replica ID in message, got %q", msg)
	}
	if !strings.Contains(msg, "10.0.0.1:5000") {
		t.Errorf("expected leader address in message, got %q", msg)
	}
}

func TestNotLeaderError_ErrorWithoutLeader(t *testing.T) {
	err := &NotLeaderError{
		ShardID:  3,
		LeaderID: 0,
	}
	msg := err.Error()
	if !strings.Contains(msg, "shard 3") {
		t.Errorf("expected shard ID in message, got %q", msg)
	}
	if !strings.Contains(msg, "unknown") {
		t.Errorf("expected 'unknown' for no-leader case, got %q", msg)
	}
}

func TestNotLeaderError_Is_MatchesSentinel(t *testing.T) {
	err := &NotLeaderError{ShardID: 1, LeaderID: 5, LeaderAddress: "host:9000"}
	if !errors.Is(err, ErrNotLeader) {
		t.Error("errors.Is(NotLeaderError, ErrNotLeader) returned false")
	}
}

func TestNotLeaderError_Is_DoesNotMatchOther(t *testing.T) {
	err := &NotLeaderError{ShardID: 1}
	if errors.Is(err, ErrBusy) {
		t.Error("errors.Is(NotLeaderError, ErrBusy) unexpectedly returned true")
	}
	if errors.Is(err, ErrClosed) {
		t.Error("errors.Is(NotLeaderError, ErrClosed) unexpectedly returned true")
	}
	if errors.Is(err, ErrTimeout) {
		t.Error("errors.Is(NotLeaderError, ErrTimeout) unexpectedly returned true")
	}
}

// ---------------------------------------------------------------------------
// EntryTooLargeError
// ---------------------------------------------------------------------------

func TestEntryTooLargeError_Error(t *testing.T) {
	err := &EntryTooLargeError{Size: 16_000_000, MaxSize: 8_000_000}
	msg := err.Error()
	if !strings.Contains(msg, "16000000") {
		t.Errorf("expected size in message, got %q", msg)
	}
	if !strings.Contains(msg, "8000000") {
		t.Errorf("expected max size in message, got %q", msg)
	}
	if !strings.HasPrefix(msg, "quicraft:") {
		t.Errorf("message does not start with 'quicraft:' prefix, got %q", msg)
	}
}

func TestEntryTooLargeError_Is_MatchesSentinel(t *testing.T) {
	err := &EntryTooLargeError{Size: 100, MaxSize: 50}
	if !errors.Is(err, ErrEntryTooLarge) {
		t.Error("errors.Is(EntryTooLargeError, ErrEntryTooLarge) returned false")
	}
}

func TestEntryTooLargeError_Is_DoesNotMatchOther(t *testing.T) {
	err := &EntryTooLargeError{Size: 100, MaxSize: 50}
	if errors.Is(err, ErrBusy) {
		t.Error("errors.Is(EntryTooLargeError, ErrBusy) unexpectedly returned true")
	}
	if errors.Is(err, ErrNotLeader) {
		t.Error("errors.Is(EntryTooLargeError, ErrNotLeader) unexpectedly returned true")
	}
}

// ---------------------------------------------------------------------------
// BusyError
// ---------------------------------------------------------------------------

func TestBusyError_Error(t *testing.T) {
	err := &BusyError{ShardID: 42, CurrentSize: 300_000_000, MaxSize: 256_000_000}
	msg := err.Error()
	if !strings.Contains(msg, "shard 42") {
		t.Errorf("expected shard ID in message, got %q", msg)
	}
	if !strings.Contains(msg, "300000000") {
		t.Errorf("expected current size in message, got %q", msg)
	}
	if !strings.Contains(msg, "256000000") {
		t.Errorf("expected max size in message, got %q", msg)
	}
	if !strings.HasPrefix(msg, "quicraft:") {
		t.Errorf("message does not start with 'quicraft:' prefix, got %q", msg)
	}
}

func TestBusyError_Is_MatchesSentinel(t *testing.T) {
	err := &BusyError{ShardID: 1, CurrentSize: 100, MaxSize: 50}
	if !errors.Is(err, ErrBusy) {
		t.Error("errors.Is(BusyError, ErrBusy) returned false")
	}
}

func TestBusyError_Is_DoesNotMatchOther(t *testing.T) {
	err := &BusyError{ShardID: 1}
	if errors.Is(err, ErrNotLeader) {
		t.Error("errors.Is(BusyError, ErrNotLeader) unexpectedly returned true")
	}
	if errors.Is(err, ErrClosed) {
		t.Error("errors.Is(BusyError, ErrClosed) unexpectedly returned true")
	}
	if errors.Is(err, ErrEntryTooLarge) {
		t.Error("errors.Is(BusyError, ErrEntryTooLarge) unexpectedly returned true")
	}
}

// ---------------------------------------------------------------------------
// UnauthorizedMessageError
// ---------------------------------------------------------------------------

func TestUnauthorizedMessageError_Error(t *testing.T) {
	err := &UnauthorizedMessageError{
		ShardID: 5,
		From:    42,
		MsgType: 3,
	}
	msg := err.Error()
	if !strings.Contains(msg, "replica 42") {
		t.Errorf("expected 'replica 42' in message, got %q", msg)
	}
	if !strings.Contains(msg, "shard 5") {
		t.Errorf("expected 'shard 5' in message, got %q", msg)
	}
	if !strings.Contains(msg, "type=3") {
		t.Errorf("expected 'type=3' in message, got %q", msg)
	}
	if !strings.HasPrefix(msg, "quicraft:") {
		t.Errorf("message does not start with 'quicraft:' prefix, got %q", msg)
	}
}

// ---------------------------------------------------------------------------
// SessionExpiredError
// ---------------------------------------------------------------------------

func TestSessionExpiredError_Error(t *testing.T) {
	err := &SessionExpiredError{ShardID: 10, ClientID: 42}
	msg := err.Error()
	if !strings.Contains(msg, "client 42") {
		t.Errorf("expected client ID in message, got %q", msg)
	}
	if !strings.Contains(msg, "shard 10") {
		t.Errorf("expected shard ID in message, got %q", msg)
	}
	if !strings.HasPrefix(msg, "quicraft:") {
		t.Errorf("message does not start with 'quicraft:' prefix, got %q", msg)
	}
}

func TestSessionExpiredError_Is_MatchesSentinel(t *testing.T) {
	err := &SessionExpiredError{ShardID: 1, ClientID: 99}
	if !errors.Is(err, ErrSessionExpired) {
		t.Error("errors.Is(SessionExpiredError, ErrSessionExpired) returned false")
	}
}

func TestSessionExpiredError_Is_DoesNotMatchOther(t *testing.T) {
	err := &SessionExpiredError{ShardID: 1, ClientID: 99}
	if errors.Is(err, ErrBusy) {
		t.Error("errors.Is(SessionExpiredError, ErrBusy) unexpectedly returned true")
	}
	if errors.Is(err, ErrNotLeader) {
		t.Error("errors.Is(SessionExpiredError, ErrNotLeader) unexpectedly returned true")
	}
	if errors.Is(err, ErrSessionNotFound) {
		t.Error("errors.Is(SessionExpiredError, ErrSessionNotFound) unexpectedly returned true")
	}
}

// ---------------------------------------------------------------------------
// Error sentinel hierarchy
// ---------------------------------------------------------------------------

func TestErrSystemBusy_Is_ErrBusy(t *testing.T) {
	// ErrSystemBusy is a *BusyError, so errors.Is must match ErrBusy.
	if !errors.Is(ErrSystemBusy, ErrBusy) {
		t.Error("errors.Is(ErrSystemBusy, ErrBusy) returned false")
	}
}

func TestErrSystemBusy_ErrorMessage(t *testing.T) {
	msg := ErrSystemBusy.Error()
	if !strings.Contains(msg, "global rate limit") {
		t.Errorf("ErrSystemBusy message %q does not mention global rate limit", msg)
	}
	if !strings.HasPrefix(msg, "quicraft:") {
		t.Errorf("ErrSystemBusy message %q does not have quicraft: prefix", msg)
	}
}

func TestErrTooManySessions_SamePointer(t *testing.T) {
	// Verify they are truly the same value, not just semantically equivalent.
	if ErrTooManySessions != ErrSessionLimitExceeded {
		t.Error("ErrTooManySessions and ErrSessionLimitExceeded are not the same pointer")
	}
}

// ---------------------------------------------------------------------------
// BusyError with custom message
// ---------------------------------------------------------------------------

func TestBusyError_CustomMessage(t *testing.T) {
	err := &BusyError{message: "quicraft: custom busy message"}
	msg := err.Error()
	if msg != "quicraft: custom busy message" {
		t.Errorf("Error() = %q, want %q", msg, "quicraft: custom busy message")
	}
}

// ---------------------------------------------------------------------------
// StaleConfigChangeError
// ---------------------------------------------------------------------------

func TestStaleConfigChangeError_Error(t *testing.T) {
	err := &StaleConfigChangeError{ShardID: 1, Expected: 5, Got: 3}
	msg := err.Error()
	if !strings.Contains(msg, "stale config change") {
		t.Errorf("expected 'stale config change' in message, got %q", msg)
	}
	if !strings.Contains(msg, "shard 1") {
		t.Errorf("expected shard ID in message, got %q", msg)
	}
	if !strings.Contains(msg, "expected ConfigChangeID 5") {
		t.Errorf("expected expected ID in message, got %q", msg)
	}
	if !strings.Contains(msg, "got 3") {
		t.Errorf("expected got ID in message, got %q", msg)
	}
}

func TestStaleConfigChangeError_Is_MatchesSentinel(t *testing.T) {
	err := &StaleConfigChangeError{ShardID: 1, Expected: 5, Got: 3}
	if !errors.Is(err, ErrStaleConfigChange) {
		t.Error("StaleConfigChangeError should match ErrStaleConfigChange")
	}
}

func TestStaleConfigChangeError_Is_DoesNotMatchOther(t *testing.T) {
	err := &StaleConfigChangeError{ShardID: 1, Expected: 5, Got: 3}
	if errors.Is(err, ErrClosed) {
		t.Error("StaleConfigChangeError should not match ErrClosed")
	}
}

// ---------------------------------------------------------------------------
// SessionError
// ---------------------------------------------------------------------------

func TestSessionError_Error_FormatsMessage(t *testing.T) {
	inner := errors.New("connection lost")
	err := &SessionError{ClientID: 42, Op: "register", Err: inner}

	msg := err.Error()
	if !strings.Contains(msg, "42") {
		t.Errorf("expected ClientID in message, got %q", msg)
	}
	if !strings.Contains(msg, "register") {
		t.Errorf("expected Op in message, got %q", msg)
	}
	if !strings.Contains(msg, "connection lost") {
		t.Errorf("expected inner error in message, got %q", msg)
	}
}

func TestSessionError_Error_NilInnerError(t *testing.T) {
	err := &SessionError{ClientID: 1, Op: "unregister", Err: nil}
	msg := err.Error()
	if !strings.Contains(msg, "unregister") {
		t.Errorf("expected Op in message even with nil Err, got %q", msg)
	}
	if !strings.Contains(msg, "<nil>") {
		t.Errorf("expected <nil> in message for nil Err, got %q", msg)
	}
}

func TestSessionError_Unwrap_ReturnsInner(t *testing.T) {
	inner := errors.New("timeout")
	err := &SessionError{ClientID: 7, Op: "propose", Err: inner}
	if err.Unwrap() != inner {
		t.Error("Unwrap should return the inner error")
	}
}

// ---------------------------------------------------------------------------
// ShardNotStoppedError
// ---------------------------------------------------------------------------

func TestShardNotStoppedError_Error_FormatsMessage(t *testing.T) {
	err := &ShardNotStoppedError{ShardID: 5, ReplicaID: 3}
	msg := err.Error()
	if !strings.Contains(msg, "5") {
		t.Errorf("expected ShardID in message, got %q", msg)
	}
	if !strings.Contains(msg, "3") {
		t.Errorf("expected ReplicaID in message, got %q", msg)
	}
	if !strings.Contains(msg, "still running") {
		t.Errorf("expected 'still running' in message, got %q", msg)
	}
}

func TestShardNotStoppedError_Is_MatchesSentinel(t *testing.T) {
	err := &ShardNotStoppedError{ShardID: 1, ReplicaID: 2}
	if !errors.Is(err, ErrShardNotStopped) {
		t.Error("ShardNotStoppedError should match ErrShardNotStopped")
	}
}

func TestShardNotStoppedError_Is_DoesNotMatchOther(t *testing.T) {
	err := &ShardNotStoppedError{ShardID: 1, ReplicaID: 2}
	if errors.Is(err, ErrClosed) {
		t.Error("ShardNotStoppedError should not match ErrClosed")
	}
}

// ---------------------------------------------------------------------------
// ReplicaNotFoundError
// ---------------------------------------------------------------------------

func TestReplicaNotFoundError_Error_FormatsMessage(t *testing.T) {
	err := &ReplicaNotFoundError{ShardID: 8, ReplicaID: 4}
	msg := err.Error()
	if !strings.Contains(msg, "4") {
		t.Errorf("expected ReplicaID in message, got %q", msg)
	}
	if !strings.Contains(msg, "8") {
		t.Errorf("expected ShardID in message, got %q", msg)
	}
	if !strings.Contains(msg, "not found") {
		t.Errorf("expected 'not found' in message, got %q", msg)
	}
}

func TestReplicaNotFoundError_Is_MatchesSentinel(t *testing.T) {
	err := &ReplicaNotFoundError{ShardID: 1, ReplicaID: 2}
	if !errors.Is(err, ErrReplicaNotFound) {
		t.Error("ReplicaNotFoundError should match ErrReplicaNotFound")
	}
}

func TestReplicaNotFoundError_Is_DoesNotMatchOther(t *testing.T) {
	err := &ReplicaNotFoundError{ShardID: 1, ReplicaID: 2}
	if errors.Is(err, ErrShardNotReady) {
		t.Error("ReplicaNotFoundError should not match ErrShardNotReady")
	}
}

// ---------------------------------------------------------------------------
// LoopPanicError
// ---------------------------------------------------------------------------

func TestLoopPanicError_ErrorMessage(t *testing.T) {
	err := &LoopPanicError{
		LoopName: "key-rotation",
		Value:    "test panic value",
		Stack:    []byte("fake stack"),
	}
	msg := err.Error()
	if !strings.Contains(msg, "key-rotation") {
		t.Errorf("expected loop name in error, got: %s", msg)
	}
	if !strings.Contains(msg, "test panic value") {
		t.Errorf("expected panic value in error, got: %s", msg)
	}
}

func TestInvalidMemberError_Is_MatchesSentinel(t *testing.T) {
	err := &InvalidMemberError{ShardID: 1, ReplicaID: 0, Reason: "zero replica id"}
	if !errors.Is(err, ErrInvalidMember) {
		t.Error("InvalidMemberError should match ErrInvalidMember")
	}
}

func TestInvalidMemberError_Is_DoesNotMatchOther(t *testing.T) {
	err := &InvalidMemberError{ShardID: 1, ReplicaID: 0, Reason: "zero replica id"}
	if errors.Is(err, ErrReplicaNotFound) {
		t.Error("InvalidMemberError should not match ErrReplicaNotFound")
	}
}

// TestMaybeApplied pins the distinction Temporary() cannot make: whether a
// failed request had already reached the raft log.
//
// Temporary answers "is a retry reasonable?"; MaybeApplied answers "did it
// happen?". Conflating them is what made the library's own documented retry
// loop — gate on Temporary, use NotLeader to pick a target — able to
// double-apply an unsessioned proposal that had already committed.
func TestMaybeApplied(t *testing.T) {
	preFlight := &NotLeaderError{ShardID: 1, LeaderID: 2}
	postDelivery := &MaybeAppliedError{Err: &NotLeaderError{ShardID: 1, LeaderID: 2}}

	if MaybeApplied(nil) {
		t.Error("MaybeApplied(nil) = true, want false")
	}
	if MaybeApplied(preFlight) {
		t.Error("a pre-flight NotLeaderError must not report MaybeApplied: " +
			"nothing was submitted, so a retry is safe")
	}
	if !MaybeApplied(postDelivery) {
		t.Fatal("a post-delivery NotLeaderError must report MaybeApplied: " +
			"the entry is in the log and can still commit")
	}

	// Wrapping must not change any existing classification, or callers that
	// predate MaybeApplied would silently change behaviour.
	if !Temporary(postDelivery) {
		t.Error("wrapping broke Temporary(): a wrapped not-leader error is " +
			"still retryable, just not blindly")
	}
	id, ok := NotLeader(postDelivery)
	if !ok || id != 2 {
		t.Errorf("wrapping broke NotLeader(): got (%d, %v), want (2, true)", id, ok)
	}
	var nle *NotLeaderError
	if !errors.As(postDelivery, &nle) || nle.ShardID != 1 {
		t.Error("wrapping broke errors.As unwrapping to *NotLeaderError")
	}
	if !errors.Is(postDelivery, ErrNotLeader) {
		t.Error("wrapping broke errors.Is(err, ErrNotLeader)")
	}

	// The message must say so; an operator reading a log line is the other
	// consumer of this distinction.
	if got := postDelivery.Error(); !strings.Contains(got, "may still commit") {
		t.Errorf("MaybeAppliedError.Error() = %q, want it to state the ambiguity", got)
	}
}
