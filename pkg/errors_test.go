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
	{"ErrRotateInProgress", ErrRotateInProgress},
	{"ErrDecompressionBomb", ErrDecompressionBomb},
	{"ErrSessionLimitExceeded", ErrSessionLimitExceeded},
	{"ErrTooManySessions", ErrTooManySessions},
	{"ErrBarrierSealed", ErrBarrierSealed},
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
	{"ErrStopped", ErrStopped},
	{"ErrReadIndexNotReady", ErrReadIndexNotReady},
	{"ErrDraining", ErrDraining},
	{"ErrInvalidSession", ErrInvalidSession},
	{"ErrSnapshotSizeMismatch", ErrSnapshotSizeMismatch},
	{"ErrShardNotStopped", ErrShardNotStopped},
	{"ErrReplicaNotFound", ErrReplicaNotFound},
	{"ErrStaleConfigChange", ErrStaleConfigChange},
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

func TestSentinelErrors_IdentityMatch(t *testing.T) {
	for _, tc := range allSentinelErrors {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, tc.err) {
				t.Errorf("errors.Is(%s, %s) returned false", tc.name, tc.name)
			}
		})
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

func TestSentinelErrors_WrappedPreservesIdentity(t *testing.T) {
	for _, tc := range allSentinelErrors {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := fmt.Errorf("context: %w", tc.err)
			if !errors.Is(wrapped, tc.err) {
				t.Errorf("errors.Is(wrapped, %s) returned false after wrapping", tc.name)
			}
		})
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

func TestNotLeaderError_StdLibErrorsIs(t *testing.T) {
	var err error = &NotLeaderError{
		ShardID:       10,
		LeaderID:      20,
		LeaderAddress: "leader:5000",
	}
	if !errors.Is(err, ErrNotLeader) {
		t.Error("standard library errors.Is(err, ErrNotLeader) returned false")
	}
}

func TestNotLeaderError_WrappedErrorsIs(t *testing.T) {
	inner := &NotLeaderError{ShardID: 1, LeaderID: 2, LeaderAddress: "host:1234"}
	wrapped := fmt.Errorf("operation failed: %w", inner)
	if !errors.Is(wrapped, ErrNotLeader) {
		t.Error("errors.Is(wrapped NotLeaderError, ErrNotLeader) returned false")
	}
}

func TestNotLeaderError_ErrorsAs(t *testing.T) {
	var err error = &NotLeaderError{
		ShardID:       7,
		LeaderID:      3,
		LeaderAddress: "192.168.1.1:8000",
	}
	wrapped := fmt.Errorf("failed: %w", err)

	var nle *NotLeaderError
	if !errors.As(wrapped, &nle) {
		t.Fatal("errors.As failed to extract NotLeaderError")
	}
	if nle.ShardID != 7 {
		t.Errorf("ShardID = %d, want 7", nle.ShardID)
	}
	if nle.LeaderID != 3 {
		t.Errorf("LeaderID = %d, want 3", nle.LeaderID)
	}
	if nle.LeaderAddress != "192.168.1.1:8000" {
		t.Errorf("LeaderAddress = %q, want %q", nle.LeaderAddress, "192.168.1.1:8000")
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

func TestEntryTooLargeError_WrappedErrorsIs(t *testing.T) {
	inner := &EntryTooLargeError{Size: 200, MaxSize: 100}
	wrapped := fmt.Errorf("propose failed: %w", inner)
	if !errors.Is(wrapped, ErrEntryTooLarge) {
		t.Error("errors.Is(wrapped EntryTooLargeError, ErrEntryTooLarge) returned false")
	}
}

func TestEntryTooLargeError_ErrorsAs(t *testing.T) {
	var err error = &EntryTooLargeError{Size: 500, MaxSize: 200}
	wrapped := fmt.Errorf("failed: %w", err)

	var etle *EntryTooLargeError
	if !errors.As(wrapped, &etle) {
		t.Fatal("errors.As failed to extract EntryTooLargeError")
	}
	if etle.Size != 500 {
		t.Errorf("Size = %d, want 500", etle.Size)
	}
	if etle.MaxSize != 200 {
		t.Errorf("MaxSize = %d, want 200", etle.MaxSize)
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

func TestBusyError_WrappedErrorsIs(t *testing.T) {
	inner := &BusyError{ShardID: 5, CurrentSize: 999, MaxSize: 500}
	wrapped := fmt.Errorf("propose failed: %w", inner)
	if !errors.Is(wrapped, ErrBusy) {
		t.Error("errors.Is(wrapped BusyError, ErrBusy) returned false")
	}
}

func TestBusyError_ErrorsAs(t *testing.T) {
	var err error = &BusyError{ShardID: 99, CurrentSize: 400, MaxSize: 300}
	wrapped := fmt.Errorf("failed: %w", err)

	var be *BusyError
	if !errors.As(wrapped, &be) {
		t.Fatal("errors.As failed to extract BusyError")
	}
	if be.ShardID != 99 {
		t.Errorf("ShardID = %d, want 99", be.ShardID)
	}
	if be.CurrentSize != 400 {
		t.Errorf("CurrentSize = %d, want 400", be.CurrentSize)
	}
	if be.MaxSize != 300 {
		t.Errorf("MaxSize = %d, want 300", be.MaxSize)
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

func TestUnauthorizedMessageError_ErrorsAs(t *testing.T) {
	var err error = &UnauthorizedMessageError{
		ShardID: 10,
		From:    99,
		MsgType: 7,
	}
	wrapped := fmt.Errorf("rejected: %w", err)

	var ume *UnauthorizedMessageError
	if !errors.As(wrapped, &ume) {
		t.Fatal("errors.As failed to extract UnauthorizedMessageError")
	}
	if ume.ShardID != 10 {
		t.Errorf("ShardID = %d, want 10", ume.ShardID)
	}
	if ume.From != 99 {
		t.Errorf("From = %d, want 99", ume.From)
	}
	if ume.MsgType != 7 {
		t.Errorf("MsgType = %d, want 7", ume.MsgType)
	}
}

func TestUnauthorizedMessageError_DoesNotMatchSentinels(t *testing.T) {
	err := &UnauthorizedMessageError{ShardID: 1, From: 2, MsgType: 3}
	if errors.Is(err, ErrBusy) {
		t.Error("errors.Is(UnauthorizedMessageError, ErrBusy) unexpectedly returned true")
	}
	if errors.Is(err, ErrNotLeader) {
		t.Error("errors.Is(UnauthorizedMessageError, ErrNotLeader) unexpectedly returned true")
	}
	if errors.Is(err, ErrClosed) {
		t.Error("errors.Is(UnauthorizedMessageError, ErrClosed) unexpectedly returned true")
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

func TestSessionExpiredError_WrappedErrorsIs(t *testing.T) {
	inner := &SessionExpiredError{ShardID: 5, ClientID: 42}
	wrapped := fmt.Errorf("propose failed: %w", inner)
	if !errors.Is(wrapped, ErrSessionExpired) {
		t.Error("errors.Is(wrapped SessionExpiredError, ErrSessionExpired) returned false")
	}
}

func TestSessionExpiredError_ErrorsAs(t *testing.T) {
	var err error = &SessionExpiredError{ShardID: 7, ClientID: 123}
	wrapped := fmt.Errorf("failed: %w", err)

	var see *SessionExpiredError
	if !errors.As(wrapped, &see) {
		t.Fatal("errors.As failed to extract SessionExpiredError")
	}
	if see.ShardID != 7 {
		t.Errorf("ShardID = %d, want 7", see.ShardID)
	}
	if see.ClientID != 123 {
		t.Errorf("ClientID = %d, want 123", see.ClientID)
	}
}

// ---------------------------------------------------------------------------
// Cross-type error isolation
// ---------------------------------------------------------------------------

func TestTypedErrors_CrossTypeIsolation(t *testing.T) {
	notLeader := &NotLeaderError{ShardID: 1, LeaderID: 2}
	entryTooLarge := &EntryTooLargeError{Size: 100, MaxSize: 50}
	busy := &BusyError{ShardID: 1, CurrentSize: 100, MaxSize: 50}
	sessionExpired := &SessionExpiredError{ShardID: 1, ClientID: 42}

	tests := []struct {
		name   string
		err    error
		target error
		want   bool
	}{
		{"NotLeader-vs-ErrNotLeader", notLeader, ErrNotLeader, true},
		{"NotLeader-vs-ErrBusy", notLeader, ErrBusy, false},
		{"NotLeader-vs-ErrEntryTooLarge", notLeader, ErrEntryTooLarge, false},
		{"EntryTooLarge-vs-ErrEntryTooLarge", entryTooLarge, ErrEntryTooLarge, true},
		{"EntryTooLarge-vs-ErrNotLeader", entryTooLarge, ErrNotLeader, false},
		{"EntryTooLarge-vs-ErrBusy", entryTooLarge, ErrBusy, false},
		{"Busy-vs-ErrBusy", busy, ErrBusy, true},
		{"Busy-vs-ErrNotLeader", busy, ErrNotLeader, false},
		{"Busy-vs-ErrEntryTooLarge", busy, ErrEntryTooLarge, false},
		{"SessionExpired-vs-ErrSessionExpired", sessionExpired, ErrSessionExpired, true},
		{"SessionExpired-vs-ErrNotLeader", sessionExpired, ErrNotLeader, false},
		{"SessionExpired-vs-ErrBusy", sessionExpired, ErrBusy, false},
		{"SessionExpired-vs-ErrSessionNotFound", sessionExpired, ErrSessionNotFound, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := errors.Is(tc.err, tc.target)
			if got != tc.want {
				t.Errorf("errors.Is(%s, %s) = %v, want %v", tc.name, tc.target, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Interface compliance
// ---------------------------------------------------------------------------

func TestTypedErrors_ImplementErrorInterface(t *testing.T) {
	// Compile-time interface compliance check via assignment.
	var _ error = (*NotLeaderError)(nil)
	var _ error = (*EntryTooLargeError)(nil)
	var _ error = (*BusyError)(nil)
	var _ error = (*UnauthorizedMessageError)(nil)
	var _ error = (*SessionExpiredError)(nil)
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

func TestErrSystemBusy_Is_Self(t *testing.T) {
	if !errors.Is(ErrSystemBusy, ErrSystemBusy) {
		t.Error("errors.Is(ErrSystemBusy, ErrSystemBusy) returned false")
	}
}

func TestErrSystemBusy_ErrorsAs_BusyError(t *testing.T) {
	// errors.As must extract *BusyError from ErrSystemBusy.
	var be *BusyError
	if !errors.As(ErrSystemBusy, &be) {
		t.Fatal("errors.As(ErrSystemBusy, *BusyError) returned false")
	}
	if be.message == "" {
		t.Error("extracted BusyError has empty message")
	}
}

func TestErrSystemBusy_WrappedIs_ErrBusy(t *testing.T) {
	wrapped := fmt.Errorf("propose failed: %w", ErrSystemBusy)
	if !errors.Is(wrapped, ErrBusy) {
		t.Error("errors.Is(wrapped ErrSystemBusy, ErrBusy) returned false")
	}
	if !errors.Is(wrapped, ErrSystemBusy) {
		t.Error("errors.Is(wrapped ErrSystemBusy, ErrSystemBusy) returned false")
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

func TestErrSystemBusy_DoesNotMatchUnrelated(t *testing.T) {
	if errors.Is(ErrSystemBusy, ErrNotLeader) {
		t.Error("errors.Is(ErrSystemBusy, ErrNotLeader) unexpectedly returned true")
	}
	if errors.Is(ErrSystemBusy, ErrClosed) {
		t.Error("errors.Is(ErrSystemBusy, ErrClosed) unexpectedly returned true")
	}
	if errors.Is(ErrSystemBusy, ErrEntryTooLarge) {
		t.Error("errors.Is(ErrSystemBusy, ErrEntryTooLarge) unexpectedly returned true")
	}
}

func TestShardBusyAndGlobalBusy_BothMatchErrBusy(t *testing.T) {
	// Both per-shard BusyError and global ErrSystemBusy match ErrBusy.
	shardBusy := &BusyError{ShardID: 42, CurrentSize: 100, MaxSize: 50}
	if !errors.Is(shardBusy, ErrBusy) {
		t.Error("errors.Is(shard BusyError, ErrBusy) returned false")
	}
	if !errors.Is(ErrSystemBusy, ErrBusy) {
		t.Error("errors.Is(ErrSystemBusy, ErrBusy) returned false")
	}
}

func TestShardBusyAndGlobalBusy_ErrorsAs(t *testing.T) {
	// errors.As extracts *BusyError from both shard and global busy.
	shardBusy := fmt.Errorf("op failed: %w", &BusyError{ShardID: 7, CurrentSize: 200, MaxSize: 100})
	globalBusy := fmt.Errorf("op failed: %w", ErrSystemBusy)

	var be *BusyError
	if !errors.As(shardBusy, &be) {
		t.Fatal("errors.As failed to extract BusyError from shard busy")
	}
	if be.ShardID != 7 {
		t.Errorf("shard BusyError.ShardID = %d, want 7", be.ShardID)
	}

	be = nil
	if !errors.As(globalBusy, &be) {
		t.Fatal("errors.As failed to extract BusyError from global busy")
	}
	if be.ShardID != 0 {
		t.Errorf("global BusyError.ShardID = %d, want 0", be.ShardID)
	}
}

func TestErrTooManySessions_Is_ErrSessionLimitExceeded(t *testing.T) {
	// ErrTooManySessions is a pointer alias for ErrSessionLimitExceeded.
	if !errors.Is(ErrTooManySessions, ErrSessionLimitExceeded) {
		t.Error("errors.Is(ErrTooManySessions, ErrSessionLimitExceeded) returned false")
	}
}

func TestErrSessionLimitExceeded_Is_ErrTooManySessions(t *testing.T) {
	// Reverse direction must also hold since they are the same pointer.
	if !errors.Is(ErrSessionLimitExceeded, ErrTooManySessions) {
		t.Error("errors.Is(ErrSessionLimitExceeded, ErrTooManySessions) returned false")
	}
}

func TestErrTooManySessions_WrappedIs(t *testing.T) {
	wrapped := fmt.Errorf("session op: %w", ErrTooManySessions)
	if !errors.Is(wrapped, ErrSessionLimitExceeded) {
		t.Error("errors.Is(wrapped ErrTooManySessions, ErrSessionLimitExceeded) returned false")
	}
	if !errors.Is(wrapped, ErrTooManySessions) {
		t.Error("errors.Is(wrapped ErrTooManySessions, ErrTooManySessions) returned false")
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

func TestBusyError_DefaultMessage_WhenNoCustom(t *testing.T) {
	err := &BusyError{ShardID: 5, CurrentSize: 100, MaxSize: 50}
	msg := err.Error()
	if !strings.Contains(msg, "shard 5") {
		t.Errorf("expected shard ID in default message, got %q", msg)
	}
	if !strings.Contains(msg, "100") {
		t.Errorf("expected current size in default message, got %q", msg)
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

func TestStaleConfigChangeError_ErrorsAs(t *testing.T) {
	err := &StaleConfigChangeError{ShardID: 1, Expected: 5, Got: 3}
	var sce *StaleConfigChangeError
	if !errors.As(err, &sce) {
		t.Fatal("errors.As should extract StaleConfigChangeError")
	}
	if sce.ShardID != 1 || sce.Expected != 5 || sce.Got != 3 {
		t.Errorf("extracted fields: ShardID=%d Expected=%d Got=%d", sce.ShardID, sce.Expected, sce.Got)
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

func TestSessionError_Unwrap_NilInner(t *testing.T) {
	err := &SessionError{ClientID: 7, Op: "propose", Err: nil}
	if err.Unwrap() != nil {
		t.Error("Unwrap should return nil when Err is nil")
	}
}

func TestSessionError_ErrorsIs_MatchesInner(t *testing.T) {
	inner := fmt.Errorf("wrapped: %w", ErrClosed)
	err := &SessionError{ClientID: 1, Op: "op", Err: inner}
	if !errors.Is(err, ErrClosed) {
		t.Error("errors.Is should match through Unwrap chain")
	}
}

func TestSessionError_ErrorsAs(t *testing.T) {
	err := &SessionError{ClientID: 99, Op: "lookup", Err: errors.New("fail")}
	wrapped := fmt.Errorf("outer: %w", err)
	var se *SessionError
	if !errors.As(wrapped, &se) {
		t.Fatal("errors.As should extract SessionError from wrapped chain")
	}
	if se.ClientID != 99 {
		t.Errorf("ClientID = %d, want 99", se.ClientID)
	}
	if se.Op != "lookup" {
		t.Errorf("Op = %q, want %q", se.Op, "lookup")
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

func TestShardNotStoppedError_Error_ZeroValues(t *testing.T) {
	err := &ShardNotStoppedError{}
	msg := err.Error()
	if !strings.Contains(msg, "shard 0") {
		t.Errorf("expected 'shard 0' for zero-value ShardID, got %q", msg)
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

func TestShardNotStoppedError_ErrorsAs(t *testing.T) {
	err := &ShardNotStoppedError{ShardID: 10, ReplicaID: 20}
	wrapped := fmt.Errorf("outer: %w", err)
	var snse *ShardNotStoppedError
	if !errors.As(wrapped, &snse) {
		t.Fatal("errors.As should extract ShardNotStoppedError")
	}
	if snse.ShardID != 10 || snse.ReplicaID != 20 {
		t.Errorf("extracted fields: ShardID=%d ReplicaID=%d", snse.ShardID, snse.ReplicaID)
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

func TestReplicaNotFoundError_Error_ZeroValues(t *testing.T) {
	err := &ReplicaNotFoundError{}
	msg := err.Error()
	if !strings.Contains(msg, "replica 0") {
		t.Errorf("expected 'replica 0' for zero-value, got %q", msg)
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

func TestReplicaNotFoundError_ErrorsAs(t *testing.T) {
	err := &ReplicaNotFoundError{ShardID: 15, ReplicaID: 25}
	wrapped := fmt.Errorf("outer: %w", err)
	var rnfe *ReplicaNotFoundError
	if !errors.As(wrapped, &rnfe) {
		t.Fatal("errors.As should extract ReplicaNotFoundError")
	}
	if rnfe.ShardID != 15 || rnfe.ReplicaID != 25 {
		t.Errorf("extracted fields: ShardID=%d ReplicaID=%d", rnfe.ShardID, rnfe.ReplicaID)
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

func TestLoopPanicError_ErrorMessageDifferentLoop(t *testing.T) {
	err := &LoopPanicError{
		LoopName: "pending-sweep",
		Value:    42,
		Stack:    []byte("goroutine 1"),
	}
	msg := err.Error()
	if !strings.Contains(msg, "pending-sweep") {
		t.Errorf("expected 'pending-sweep' in error, got: %s", msg)
	}
	if !strings.Contains(msg, "42") {
		t.Errorf("expected panic value '42' in error, got: %s", msg)
	}
}
