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
// Typed errors
// ---------------------------------------------------------------------------

// TestTypedErrors_Error pins what each typed error's message carries: the
// identifiers an operator needs to act on it, and the package prefix where the
// type promises one. want, when set, is the whole message.
func TestTypedErrors_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		prefix   string
		want     string
		contains []string
	}{
		{
			name:     "NotLeaderError with leader",
			err:      &NotLeaderError{ShardID: 1, LeaderID: 2, LeaderAddress: "10.0.0.1:5000"},
			contains: []string{"shard 1", "replica 2", "10.0.0.1:5000"},
		},
		{
			name:     "NotLeaderError without leader",
			err:      &NotLeaderError{ShardID: 3, LeaderID: 0},
			contains: []string{"shard 3", "unknown"},
		},
		{
			name:     "EntryTooLargeError",
			err:      &EntryTooLargeError{Size: 16_000_000, MaxSize: 8_000_000},
			prefix:   "quicraft:",
			contains: []string{"16000000", "8000000"},
		},
		{
			name:     "BusyError",
			err:      &BusyError{ShardID: 42, CurrentSize: 300_000_000, MaxSize: 256_000_000},
			prefix:   "quicraft:",
			contains: []string{"shard 42", "300000000", "256000000"},
		},
		{
			name: "BusyError with custom message",
			err:  &BusyError{message: "quicraft: custom busy message"},
			want: "quicraft: custom busy message",
		},
		{
			name:     "ErrSystemBusy",
			err:      ErrSystemBusy,
			prefix:   "quicraft:",
			contains: []string{"global rate limit"},
		},
		{
			name:     "UnauthorizedMessageError",
			err:      &UnauthorizedMessageError{ShardID: 5, From: 42, MsgType: 3},
			prefix:   "quicraft:",
			contains: []string{"replica 42", "shard 5", "type=3"},
		},
		{
			name:     "SessionExpiredError",
			err:      &SessionExpiredError{ShardID: 10, ClientID: 42},
			prefix:   "quicraft:",
			contains: []string{"client 42", "shard 10"},
		},
		{
			name:     "StaleConfigChangeError",
			err:      &StaleConfigChangeError{ShardID: 1, Expected: 5, Got: 3},
			contains: []string{"stale config change", "shard 1", "expected ConfigChangeID 5", "got 3"},
		},
		{
			name:     "SessionError",
			err:      &SessionError{ClientID: 42, Op: "register", Err: errors.New("connection lost")},
			contains: []string{"42", "register", "connection lost"},
		},
		{
			name:     "SessionError with nil inner error",
			err:      &SessionError{ClientID: 1, Op: "unregister", Err: nil},
			contains: []string{"unregister", "<nil>"},
		},
		{
			name:     "ShardNotStoppedError",
			err:      &ShardNotStoppedError{ShardID: 5, ReplicaID: 3},
			contains: []string{"5", "3", "still running"},
		},
		{
			name:     "ReplicaNotFoundError",
			err:      &ReplicaNotFoundError{ShardID: 8, ReplicaID: 4},
			contains: []string{"4", "8", "not found"},
		},
		{
			name:     "LoopPanicError",
			err:      &LoopPanicError{LoopName: "key-rotation", Value: "test panic value", Stack: []byte("fake stack")},
			contains: []string{"key-rotation", "test panic value"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.err.Error()
			if tc.want != "" && msg != tc.want {
				t.Errorf("Error() = %q, want %q", msg, tc.want)
			}
			if tc.prefix != "" && !strings.HasPrefix(msg, tc.prefix) {
				t.Errorf("Error() = %q, want prefix %q", msg, tc.prefix)
			}
			for _, sub := range tc.contains {
				if !strings.Contains(msg, sub) {
					t.Errorf("Error() = %q, want it to contain %q", msg, sub)
				}
			}
		})
	}
}

// TestTypedErrors_Is pins that each typed error matches its own sentinel
// through errors.Is and no unrelated one.
func TestTypedErrors_Is(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		matches error
		others  []error
	}{
		{
			name:    "NotLeaderError",
			err:     &NotLeaderError{ShardID: 1, LeaderID: 5, LeaderAddress: "host:9000"},
			matches: ErrNotLeader,
			others:  []error{ErrBusy, ErrClosed, ErrTimeout},
		},
		{
			name:    "EntryTooLargeError",
			err:     &EntryTooLargeError{Size: 100, MaxSize: 50},
			matches: ErrEntryTooLarge,
			others:  []error{ErrBusy, ErrNotLeader},
		},
		{
			name:    "BusyError",
			err:     &BusyError{ShardID: 1, CurrentSize: 100, MaxSize: 50},
			matches: ErrBusy,
			others:  []error{ErrNotLeader, ErrClosed, ErrEntryTooLarge},
		},
		{
			// ErrSystemBusy is a *BusyError, so it must match ErrBusy.
			name:    "ErrSystemBusy",
			err:     ErrSystemBusy,
			matches: ErrBusy,
		},
		{
			name:    "SessionExpiredError",
			err:     &SessionExpiredError{ShardID: 1, ClientID: 99},
			matches: ErrSessionExpired,
			others:  []error{ErrBusy, ErrNotLeader, ErrSessionNotFound},
		},
		{
			name:    "StaleConfigChangeError",
			err:     &StaleConfigChangeError{ShardID: 1, Expected: 5, Got: 3},
			matches: ErrStaleConfigChange,
			others:  []error{ErrClosed},
		},
		{
			name:    "ShardNotStoppedError",
			err:     &ShardNotStoppedError{ShardID: 1, ReplicaID: 2},
			matches: ErrShardNotStopped,
			others:  []error{ErrClosed},
		},
		{
			name:    "ReplicaNotFoundError",
			err:     &ReplicaNotFoundError{ShardID: 1, ReplicaID: 2},
			matches: ErrReplicaNotFound,
			others:  []error{ErrShardNotReady},
		},
		{
			name:    "InvalidMemberError",
			err:     &InvalidMemberError{ShardID: 1, ReplicaID: 0, Reason: "zero replica id"},
			matches: ErrInvalidMember,
			others:  []error{ErrReplicaNotFound},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, tc.matches) {
				t.Errorf("errors.Is(%s, %v) = false, want true", tc.name, tc.matches)
			}
			for _, other := range tc.others {
				if errors.Is(tc.err, other) {
					t.Errorf("errors.Is(%s, %v) = true, want false", tc.name, other)
				}
			}
		})
	}
}

func TestErrTooManySessions_SamePointer(t *testing.T) {
	// Verify they are truly the same value, not just semantically equivalent.
	if ErrTooManySessions != ErrSessionLimitExceeded {
		t.Error("ErrTooManySessions and ErrSessionLimitExceeded are not the same pointer")
	}
}

func TestSessionError_Unwrap_ReturnsInner(t *testing.T) {
	inner := errors.New("timeout")
	err := &SessionError{ClientID: 7, Op: "propose", Err: inner}
	if err.Unwrap() != inner {
		t.Error("Unwrap should return the inner error")
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
