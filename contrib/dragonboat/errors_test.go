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

package dragonboat

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

func TestSentinelMappingsAreNonNil(t *testing.T) {
	sentinels := map[string]error{
		"ErrClosed":                  ErrClosed,
		"ErrShardNotFound":           ErrShardNotFound,
		"ErrShardAlreadyExist":       ErrShardAlreadyExist,
		"ErrShardNotStopped":         ErrShardNotStopped,
		"ErrSystemBusy":              ErrSystemBusy,
		"ErrTimeout":                 ErrTimeout,
		"ErrCanceled":                ErrCanceled,
		"ErrRejected":                ErrRejected,
		"ErrAborted":                 ErrAborted,
		"ErrInvalidSession":          ErrInvalidSession,
		"ErrPayloadTooBig":           ErrPayloadTooBig,
		"ErrShardNotReady":           ErrShardNotReady,
		"ErrShardClosed":             ErrShardClosed,
		"ErrShardNotInitialized":     ErrShardNotInitialized,
		"ErrInvalidShardSettings":    ErrInvalidShardSettings,
		"ErrInvalidAddress":          ErrInvalidAddress,
		"ErrInvalidOperation":        ErrInvalidOperation,
		"ErrInvalidRange":            ErrInvalidRange,
		"ErrReplicaRemoved":          ErrReplicaRemoved,
		"ErrShardNotBootstrapped":    ErrShardNotBootstrapped,
		"ErrDeadlineNotSet":          ErrDeadlineNotSet,
		"ErrInvalidDeadline":         ErrInvalidDeadline,
		"ErrTimeoutTooSmall":         ErrTimeoutTooSmall,
		"ErrDirNotExist":             ErrDirNotExist,
		"ErrInvalidOption":           ErrInvalidOption,
		"ErrInvalidTarget":           ErrInvalidTarget,
		"ErrLogDBNotCreatedOrClosed": ErrLogDBNotCreatedOrClosed,
		"ErrNoSnapshot":              ErrNoSnapshot,
	}

	for name, sentinel := range sentinels {
		t.Run(name, func(t *testing.T) {
			if sentinel == nil {
				t.Errorf("sentinel %s is nil", name)
			}
		})
	}
}

func TestSentinelMappingsMatchQuicraft(t *testing.T) {
	tests := []struct {
		name        string
		dragonboat  error
		quicraftErr error
	}{
		{"ErrClosed", ErrClosed, quicraft.ErrClosed},
		{"ErrShardNotFound", ErrShardNotFound, quicraft.ErrShardNotFound},
		{"ErrShardAlreadyExist", ErrShardAlreadyExist, quicraft.ErrShardAlreadyExists},
		{"ErrShardNotStopped", ErrShardNotStopped, quicraft.ErrShardNotStopped},
		{"ErrSystemBusy", ErrSystemBusy, quicraft.ErrSystemBusy},
		{"ErrTimeout", ErrTimeout, quicraft.ErrTimeout},
		{"ErrCanceled", ErrCanceled, quicraft.ErrCanceled},
		{"ErrRejected", ErrRejected, quicraft.ErrRejected},
		{"ErrAborted", ErrAborted, quicraft.ErrAborted},
		{"ErrInvalidSession", ErrInvalidSession, quicraft.ErrInvalidSession},
		{"ErrPayloadTooBig", ErrPayloadTooBig, quicraft.ErrEntryTooLarge},
		{"ErrShardNotReady", ErrShardNotReady, quicraft.ErrShardNotReady},
		// ErrShardClosed, ErrShardNotInitialized, and ErrInvalidShardSettings
		// are distinct sentinels (errors.New) and intentionally do NOT alias
		// quicraft errors. They are tested via translateError instead.
		{"ErrInvalidOperation", ErrInvalidOperation, quicraft.ErrInvalidOperation},
		{"ErrInvalidRange", ErrInvalidRange, quicraft.ErrInvalidRange},
		// ErrReplicaRemoved is a distinct sentinel (permanent removal
		// guard) and intentionally does NOT alias quicraft's
		// ErrReplicaNotFound; the alias is ErrReplicaNotFound.
		{"ErrReplicaNotFound", ErrReplicaNotFound, quicraft.ErrReplicaNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !errors.Is(tt.dragonboat, tt.quicraftErr) {
				t.Errorf("expected dragonboat %s to match quicraft error %v", tt.name, tt.quicraftErr)
			}
		})
	}
}

func TestNewSentinels_AreStandalone(t *testing.T) {
	// ErrLogDBNotCreatedOrClosed and ErrNoSnapshot are standalone errors,
	// not mapped to quicraft sentinels.
	if ErrLogDBNotCreatedOrClosed.Error() != "logdb is not created yet or closed already" {
		t.Errorf("unexpected error message: %s", ErrLogDBNotCreatedOrClosed.Error())
	}
	if ErrNoSnapshot.Error() != "no snapshot available" {
		t.Errorf("unexpected error message: %s", ErrNoSnapshot.Error())
	}
}

func TestIsTempError_TemporaryErrors(t *testing.T) {
	tempErrors := []struct {
		name string
		err  error
	}{
		{"ErrSystemBusy", ErrSystemBusy},
		{"ErrShardNotReady", ErrShardNotReady},
		{"ErrTimeout", ErrTimeout},
		{"ErrShardClosed", ErrShardClosed},
		{"ErrShardNotInitialized", ErrShardNotInitialized},
		{"ErrClosed", ErrClosed},
		{"ErrAborted", ErrAborted},
	}

	for _, tt := range tempErrors {
		t.Run(tt.name, func(t *testing.T) {
			if !IsTempError(tt.err) {
				t.Errorf("expected IsTempError(%s) to return true", tt.name)
			}
		})
	}
}

func TestIsTempError_NonTemporaryErrors(t *testing.T) {
	nonTempErrors := []struct {
		name string
		err  error
	}{
		{"ErrShardNotFound", ErrShardNotFound},
		{"ErrRejected", ErrRejected},
		{"ErrInvalidSession", ErrInvalidSession},
		{"ErrPayloadTooBig", ErrPayloadTooBig},
		{"ErrInvalidOperation", ErrInvalidOperation},
		{"ErrInvalidRange", ErrInvalidRange},
		{"ErrLogDBNotCreatedOrClosed", ErrLogDBNotCreatedOrClosed},
		{"ErrNoSnapshot", ErrNoSnapshot},
		{"nil", nil},
	}

	for _, tt := range nonTempErrors {
		t.Run(tt.name, func(t *testing.T) {
			if IsTempError(tt.err) {
				t.Errorf("expected IsTempError(%s) to return false", tt.name)
			}
		})
	}
}

func TestTranslateError_NilReturnsNil(t *testing.T) {
	result := translateError(nil)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestTranslateError_MapsQuicraftToDragonboat(t *testing.T) {
	tests := []struct {
		name     string
		input    error
		expected error
	}{
		{"Closed", quicraft.ErrClosed, ErrClosed},
		{"ShardNotFound", quicraft.ErrShardNotFound, ErrShardNotFound},
		{"ShardAlreadyExists", quicraft.ErrShardAlreadyExists, ErrShardAlreadyExist},
		{"ShardNotStopped", quicraft.ErrShardNotStopped, ErrShardNotStopped},
		{"SystemBusy", quicraft.ErrSystemBusy, ErrSystemBusy},
		{"Timeout", quicraft.ErrTimeout, ErrTimeout},
		{"Canceled", quicraft.ErrCanceled, ErrCanceled},
		{"Rejected", quicraft.ErrRejected, ErrRejected},
		{"Aborted", quicraft.ErrAborted, ErrAborted},
		{"InvalidSession", quicraft.ErrInvalidSession, ErrInvalidSession},
		{"EntryTooLarge", quicraft.ErrEntryTooLarge, ErrPayloadTooBig},
		{"ShardNotReady", quicraft.ErrShardNotReady, ErrShardNotReady},
		{"ShardNotStarted", quicraft.ErrShardNotStarted, ErrShardNotInitialized},
		{"InvalidConfig", quicraft.ErrInvalidConfig, ErrInvalidShardSettings},
		{"InvalidOperation", quicraft.ErrInvalidOperation, ErrInvalidOperation},
		{"InvalidRange", quicraft.ErrInvalidRange, ErrInvalidRange},
		{"ReplicaNotFound", quicraft.ErrReplicaNotFound, ErrReplicaNotFound},
		{"NotLeader", quicraft.ErrNotLeader, ErrShardNotReady},
		{"StaleConfigChange", quicraft.ErrStaleConfigChange, ErrStaleConfigChange},
		{"SnapshotNotFound", quicraft.ErrSnapshotNotFound, ErrSnapshotNotFound},
		{"SnapshotInProgress", quicraft.ErrSnapshotInProgress, ErrSnapshotInProgress},
		{"ImportInProgress", quicraft.ErrImportInProgress, ErrSystemBusy},
		{"SessionNotFound", quicraft.ErrSessionNotFound, ErrInvalidSession},
		{"SessionExpired", quicraft.ErrSessionExpired, ErrRejected},
		{"SessionAlreadyExists", quicraft.ErrSessionAlreadyExists, ErrRejected},
		{"ReadIndexNotReady", quicraft.ErrReadIndexNotReady, ErrShardNotReady},
		{"ShardFailed", quicraft.ErrShardFailed, ErrShardClosed},
		{"Busy", quicraft.ErrBusy, ErrSystemBusy},
		{"Draining", quicraft.ErrDraining, ErrSystemBusy},
		{"Dropped", quicraft.ErrDropped, ErrShardNotReady},
		{"Stopped", quicraft.ErrStopped, ErrShardClosed},
		{"SessionLimitExceeded", quicraft.ErrSessionLimitExceeded, ErrSystemBusy},
		{"LogDBWrite", quicraft.ErrLogDBWrite, ErrSystemBusy},
		{"DiskFull", quicraft.ErrDiskFull, ErrSystemBusy},
		{"SnapshotRecoveryFailed", quicraft.ErrSnapshotRecoveryFailed, ErrAborted},
		{"SnapshotSizeMismatch", quicraft.ErrSnapshotSizeMismatch, ErrAborted},
		{"ResponseLimitExceeded", quicraft.ErrResponseLimitExceeded, ErrPayloadTooBig},
		{"InvalidSMType", quicraft.ErrInvalidSMType, ErrInvalidShardSettings},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := translateError(tt.input)
			if !errors.Is(result, tt.expected) {
				t.Errorf("translateError(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// wrappedError is a test helper that wraps an error for testing errors.Is chains.
type wrappedError struct {
	msg   string
	inner error
}

func (e *wrappedError) Error() string { return e.msg }
func (e *wrappedError) Unwrap() error { return e.inner }

func TestTranslateError_WrappedQuicraftErrors(t *testing.T) {
	wrapped := &wrappedError{
		msg:   "operation failed",
		inner: quicraft.ErrShardNotFound,
	}

	result := translateError(wrapped)
	if !errors.Is(result, ErrShardNotFound) {
		t.Errorf("expected wrapped error to translate to ErrShardNotFound, got %v", result)
	}
}

func TestTranslateError_UnknownErrorWrappedInAdapterError(t *testing.T) {
	unknown := errors.New("unknown error")
	result := translateError(unknown)

	var ae *AdapterError
	if !errors.As(result, &ae) {
		t.Fatalf("expected *AdapterError, got %T (%v)", result, result)
	}
	if !errors.Is(result, unknown) {
		t.Error("AdapterError must keep the original error reachable via errors.Is")
	}
}

func TestTranslateError_AdapterSentinelNotReWrapped(t *testing.T) {
	for _, sentinel := range []error{ErrInvalidTarget, ErrDeadlineNotSet, ErrLogDBNotCreatedOrClosed} {
		if got := translateError(sentinel); got != sentinel {
			t.Errorf("translateError(%v) = %v, want the sentinel unchanged", sentinel, got)
		}
	}
}

func TestTranslateError_InvalidMemberErrorMapsToInvalidTarget(t *testing.T) {
	err := &quicraft.InvalidMemberError{ShardID: 1, ReplicaID: 0, Reason: "zero replica id"}
	if got := translateError(err); !errors.Is(got, ErrInvalidTarget) {
		t.Errorf("translateError(InvalidMemberError) = %v, want ErrInvalidTarget", got)
	}
}

func TestTranslateError_ContextErrors(t *testing.T) {
	if got := translateError(context.DeadlineExceeded); !errors.Is(got, ErrTimeout) {
		t.Errorf("translateError(DeadlineExceeded) = %v, want ErrTimeout", got)
	}
	if got := translateError(context.Canceled); !errors.Is(got, ErrCanceled) {
		t.Errorf("translateError(Canceled) = %v, want ErrCanceled", got)
	}
}

func TestIsTempError_NotLeaderIsTemporary(t *testing.T) {
	// A raw quicraft not-leader error reaching a migrated retry loop must
	// not be treated as fatal: an election is a routine, temporary state.
	if !IsTempError(ErrNotLeader) {
		t.Error("IsTempError(ErrNotLeader) = false, want true")
	}
}

func TestIsTempError_WrappedTemporaryError(t *testing.T) {
	wrapped := fmt.Errorf("context: %w", ErrTimeout)
	if !IsTempError(wrapped) {
		t.Error("expected IsTempError to return true for wrapped ErrTimeout")
	}
}

// ---------------------------------------------------------------------------
// Rate-limiting / busy error tests
// ---------------------------------------------------------------------------

func TestErrSystemBusy_IsTemp(t *testing.T) {
	if !IsTempError(ErrSystemBusy) {
		t.Fatal("ErrSystemBusy should be temporary")
	}
}

func TestErrPayloadTooBig_Exists(t *testing.T) {
	if ErrPayloadTooBig == nil {
		t.Fatal("ErrPayloadTooBig should be non-nil")
	}
}

func TestErrPayloadTooBig_IsNotTemp(t *testing.T) {
	if IsTempError(ErrPayloadTooBig) {
		t.Fatal("ErrPayloadTooBig should not be temporary")
	}
}

func TestErrPayloadTooBig_MapsToEntryTooLarge(t *testing.T) {
	if !errors.Is(ErrPayloadTooBig, quicraft.ErrEntryTooLarge) {
		t.Fatal("ErrPayloadTooBig should match quicraft.ErrEntryTooLarge")
	}
}

// ---------------------------------------------------------------------------
// Additional sentinel coverage
// ---------------------------------------------------------------------------

func TestErrShardNotBootstrapped_IsNotTemp(t *testing.T) {
	// ErrShardNotBootstrapped is a distinct sentinel and is NOT in the
	// IsTempError set — it indicates a configuration/setup problem, not
	// a transient condition.
	if IsTempError(ErrShardNotBootstrapped) {
		t.Fatal("ErrShardNotBootstrapped should NOT be temporary")
	}
}

func TestErrDeadlineNotSet_Exists(t *testing.T) {
	if ErrDeadlineNotSet == nil {
		t.Fatal("ErrDeadlineNotSet should be non-nil")
	}
}

func TestErrInvalidDeadline_Exists(t *testing.T) {
	if ErrInvalidDeadline == nil {
		t.Fatal("ErrInvalidDeadline should be non-nil")
	}
}

func TestErrTimeoutTooSmall_Exists(t *testing.T) {
	if ErrTimeoutTooSmall == nil {
		t.Fatal("ErrTimeoutTooSmall should be non-nil")
	}
}

func TestErrDirNotExist_Exists(t *testing.T) {
	if ErrDirNotExist == nil {
		t.Fatal("ErrDirNotExist should be non-nil")
	}
}

func TestErrInvalidTarget_Exists(t *testing.T) {
	if ErrInvalidTarget == nil {
		t.Fatal("ErrInvalidTarget should be non-nil")
	}
}

func TestErrInvalidOption_Exists(t *testing.T) {
	if ErrInvalidOption == nil {
		t.Fatal("ErrInvalidOption should be non-nil")
	}
}

func TestErrInvalidAddress_Exists(t *testing.T) {
	if ErrInvalidAddress == nil {
		t.Fatal("ErrInvalidAddress should be non-nil")
	}
}

// ---------------------------------------------------------------------------
// TranslateError edge cases
// ---------------------------------------------------------------------------

func TestTranslateError_DoubleWrappedError(t *testing.T) {
	inner := fmt.Errorf("layer1: %w", quicraft.ErrTimeout)
	outer := fmt.Errorf("layer2: %w", inner)

	result := translateError(outer)
	if !errors.Is(result, ErrTimeout) {
		t.Errorf("expected ErrTimeout for double-wrapped, got %v", result)
	}
}

func TestIsTempError_WrappedAborted(t *testing.T) {
	wrapped := fmt.Errorf("operation failed: %w", ErrAborted)
	if !IsTempError(wrapped) {
		t.Error("expected IsTempError to return true for wrapped ErrAborted")
	}
}

func TestIsTempError_WrappedSystemBusy(t *testing.T) {
	wrapped := fmt.Errorf("rate limited: %w", ErrSystemBusy)
	if !IsTempError(wrapped) {
		t.Error("expected IsTempError to return true for wrapped ErrSystemBusy")
	}
}

func TestIsTempError_CanceledIsNotTemp(t *testing.T) {
	if IsTempError(ErrCanceled) {
		t.Error("expected IsTempError to return false for ErrCanceled")
	}
}

func TestIsTempError_WrappedCanceledIsNotTemp(t *testing.T) {
	wrapped := fmt.Errorf("context canceled: %w", ErrCanceled)
	if IsTempError(wrapped) {
		t.Error("expected IsTempError to return false for wrapped ErrCanceled")
	}
}

func TestIsTempError_CanceledIsNotTemporary(t *testing.T) {
	// Verify that ErrCanceled is NOT classified as temporary,
	// matching dragonboat v4 behavior where canceled is not retryable.
	if IsTempError(ErrCanceled) {
		t.Error("ErrCanceled should not be temporary (dragonboat v4 does not treat canceled as retryable)")
	}
}

// ---------------------------------------------------------------------------
// CompactionIndexNotSupportedError
// ---------------------------------------------------------------------------

func TestCompactionIndexNotSupportedError_Message(t *testing.T) {
	err := &CompactionIndexNotSupportedError{}
	msg := err.Error()
	if msg == "" {
		t.Fatal("expected non-empty error message")
	}
	if !strings.Contains(msg, "CompactionIndex") {
		t.Errorf("error message should mention CompactionIndex: %s", msg)
	}
	if !strings.Contains(msg, "CompactionOverhead") {
		t.Errorf("error message should mention CompactionOverhead: %s", msg)
	}
}

func TestCompactionIndexNotSupportedError_IsErrInvalidOption(t *testing.T) {
	err := &CompactionIndexNotSupportedError{}
	if !errors.Is(err, ErrInvalidOption) {
		t.Error("CompactionIndexNotSupportedError should match ErrInvalidOption via errors.Is")
	}
}

// ---------------------------------------------------------------------------
// translateError: ErrSessionLimitExceeded mapping
// ---------------------------------------------------------------------------

func TestTranslateError_SessionLimitExceeded(t *testing.T) {
	result := translateError(quicraft.ErrSessionLimitExceeded)
	if !errors.Is(result, ErrSystemBusy) {
		t.Errorf("translateError(ErrSessionLimitExceeded) = %v, want ErrSystemBusy", result)
	}
}

func TestTranslateError_TooManySessions(t *testing.T) {
	// ErrTooManySessions is an alias for ErrSessionLimitExceeded.
	result := translateError(quicraft.ErrTooManySessions)
	if !errors.Is(result, ErrSystemBusy) {
		t.Errorf("translateError(ErrTooManySessions) = %v, want ErrSystemBusy", result)
	}
}

func TestTranslateError_PackageLevelMappings(t *testing.T) {
	if errorMappings == nil {
		t.Fatal("errorMappings package-level variable must not be nil")
	}

	const expectedCount = 41
	if len(errorMappings) != expectedCount {
		t.Errorf("errorMappings has %d entries, want %d", len(errorMappings), expectedCount)
	}

	// Verify every entry has non-nil source and destination errors.
	for i, m := range errorMappings {
		if m.quicraftErr == nil {
			t.Errorf("errorMappings[%d].quicraftErr is nil", i)
		}
		if m.dragonboatErr == nil {
			t.Errorf("errorMappings[%d].dragonboatErr is nil", i)
		}
	}
}

// ---------------------------------------------------------------------------
// New error mappings
// ---------------------------------------------------------------------------

func TestTranslateError_LogDBWrite(t *testing.T) {
	result := translateError(quicraft.ErrLogDBWrite)
	if !errors.Is(result, ErrSystemBusy) {
		t.Errorf("translateError(ErrLogDBWrite) = %v, want ErrSystemBusy", result)
	}
}

func TestTranslateError_LogDBWrite_ErrorsIs(t *testing.T) {
	result := translateError(quicraft.ErrLogDBWrite)
	if result != ErrSystemBusy {
		t.Errorf("expected exact ErrSystemBusy pointer, got %v", result)
	}
}

func TestTranslateError_DiskFull(t *testing.T) {
	result := translateError(quicraft.ErrDiskFull)
	if !errors.Is(result, ErrSystemBusy) {
		t.Errorf("translateError(ErrDiskFull) = %v, want ErrSystemBusy", result)
	}
}

func TestTranslateError_DiskFull_ErrorsIs(t *testing.T) {
	result := translateError(quicraft.ErrDiskFull)
	if result != ErrSystemBusy {
		t.Errorf("expected exact ErrSystemBusy pointer, got %v", result)
	}
}

func TestTranslateError_SnapshotRecoveryFailed(t *testing.T) {
	result := translateError(quicraft.ErrSnapshotRecoveryFailed)
	if !errors.Is(result, ErrAborted) {
		t.Errorf("translateError(ErrSnapshotRecoveryFailed) = %v, want ErrAborted", result)
	}
}

func TestTranslateError_SnapshotRecoveryFailed_ErrorsIs(t *testing.T) {
	result := translateError(quicraft.ErrSnapshotRecoveryFailed)
	if result != ErrAborted {
		t.Errorf("expected exact ErrAborted pointer, got %v", result)
	}
}

func TestTranslateError_SnapshotSizeMismatch(t *testing.T) {
	result := translateError(quicraft.ErrSnapshotSizeMismatch)
	if !errors.Is(result, ErrAborted) {
		t.Errorf("translateError(ErrSnapshotSizeMismatch) = %v, want ErrAborted", result)
	}
}

func TestTranslateError_SnapshotSizeMismatch_ErrorsIs(t *testing.T) {
	result := translateError(quicraft.ErrSnapshotSizeMismatch)
	if result != ErrAborted {
		t.Errorf("expected exact ErrAborted pointer, got %v", result)
	}
}

func TestTranslateError_ResponseLimitExceeded(t *testing.T) {
	result := translateError(quicraft.ErrResponseLimitExceeded)
	if !errors.Is(result, ErrPayloadTooBig) {
		t.Errorf("translateError(ErrResponseLimitExceeded) = %v, want ErrPayloadTooBig", result)
	}
}

func TestTranslateError_ResponseLimitExceeded_ErrorsIs(t *testing.T) {
	result := translateError(quicraft.ErrResponseLimitExceeded)
	if result != ErrPayloadTooBig {
		t.Errorf("expected exact ErrPayloadTooBig pointer, got %v", result)
	}
}

func TestTranslateError_InvalidSMType(t *testing.T) {
	result := translateError(quicraft.ErrInvalidSMType)
	if !errors.Is(result, ErrInvalidShardSettings) {
		t.Errorf("translateError(ErrInvalidSMType) = %v, want ErrInvalidShardSettings", result)
	}
}

func TestTranslateError_InvalidSMType_ErrorsIs(t *testing.T) {
	result := translateError(quicraft.ErrInvalidSMType)
	if result != ErrInvalidShardSettings {
		t.Errorf("expected exact ErrInvalidShardSettings pointer, got %v", result)
	}
}

func BenchmarkTranslateError(b *testing.B) {
	// Benchmark with a known error (first entry) to measure the fast path.
	b.Run("KnownError_First", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = translateError(quicraft.ErrClosed)
		}
	})

	// Benchmark with a known error near the end of the table.
	b.Run("KnownError_Last", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = translateError(quicraft.ErrSessionLimitExceeded)
		}
	})

	// Benchmark with nil (early return).
	b.Run("NilError", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = translateError(nil)
		}
	})

	// Benchmark with an unknown error (full table scan, no match).
	b.Run("UnknownError", func(b *testing.B) {
		unknown := errors.New("unknown")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = translateError(unknown)
		}
	})
}

// TestTranslateError_PreservesTheMaybeAppliedBit pins that translateError keeps
// the submission bit.
//
// quicraft marks failures that happen AFTER an entry reached Raft by wrapping
// them in *MaybeAppliedError. Discarding that wrapper and returning the bare
// mapped sentinel would make the post-delivery leadership change and the
// pre-flight refusal indistinguishable here — both ErrShardNotReady, both
// MaybeApplied false.
//
// That is not a cosmetic loss. On the config-change path the submission bit is
// what separates "the leader refused this, nothing happened" from "the leader
// took it and then lost the shard, so it may still commit", which decides
// whether a later "already exists" is this caller's own change landing or
// somebody else's. Without the bit the adapter's helper would have to widen its
// predicate to cover the fold, and an engine applying a change it had reported
// as refused PRE-FLIGHT would go undetected here while the root module caught
// it.
func TestTranslateError_PreservesTheMaybeAppliedBit(t *testing.T) {
	postDelivery := &quicraft.MaybeAppliedError{Err: &quicraft.NotLeaderError{ShardID: 1}}
	translated := translateError(postDelivery)

	// The mapping still holds: every existing comparison keeps working,
	// because errors.Is walks Unwrap.
	if !errors.Is(translated, ErrShardNotReady) {
		t.Errorf("errors.Is(%v, ErrShardNotReady) = false; wrapping must not "+
			"break the sentinel a migrated caller matches on", translated)
	}
	// And the bit now survives.
	if !quicraft.MaybeApplied(translated) {
		t.Error("quicraft.MaybeApplied = false for a post-delivery failure; the " +
			"submission bit was discarded by the mapping")
	}
	// IsTempError is unaffected, since it is errors.Is-based too.
	if !IsTempError(translated) {
		t.Error("IsTempError = false; the wrapper must not change temporariness")
	}

	// The pre-flight refusal is the control: same sentinel, no bit. Without
	// this the test would pass against an implementation that wrapped
	// EVERYTHING, which would be just as wrong in the other direction.
	preFlight := translateError(&quicraft.NotLeaderError{ShardID: 1})
	if !errors.Is(preFlight, ErrShardNotReady) {
		t.Errorf("errors.Is(%v, ErrShardNotReady) = false for the pre-flight refusal", preFlight)
	}
	if quicraft.MaybeApplied(preFlight) {
		t.Error("quicraft.MaybeApplied = true for a PRE-FLIGHT refusal; nothing " +
			"was submitted, and treating it as maybe-applied is what makes a " +
			"stale \"already exists\" read as success")
	}
}

// TestTranslateError_PreservesTheMaybeAppliedBitForEveryMapping is the generic
// half. translateError rebuilds the wrapper for ANY mapped sentinel,
// not only the not-leader one the focused test above pins, so a mutation that
// preserved the bit for ErrNotLeader alone would pass that test and silently
// drop it for a wrapped timeout, a wrapped shard-closed, or whatever quicraft
// wraps next. Every row of errorMappings is exercised in both directions: the
// wrapped input keeps the bit and still errors.Is the mapped sentinel, and the
// bare input is the control that must NOT acquire one.
func TestTranslateError_PreservesTheMaybeAppliedBitForEveryMapping(t *testing.T) {
	if len(errorMappings) == 0 {
		t.Fatal("errorMappings is empty; this test would verify nothing")
	}
	for _, m := range errorMappings {
		t.Run(m.quicraftErr.Error(), func(t *testing.T) {
			wrapped := translateError(&quicraft.MaybeAppliedError{Err: m.quicraftErr})
			if !errors.Is(wrapped, m.dragonboatErr) {
				t.Errorf("errors.Is(%v, %v) = false; wrapping must not break the mapping",
					wrapped, m.dragonboatErr)
			}
			if !quicraft.MaybeApplied(wrapped) {
				t.Errorf("quicraft.MaybeApplied = false for a wrapped %v; the submission "+
					"bit was discarded by the mapping", m.quicraftErr)
			}

			bare := translateError(m.quicraftErr)
			if !errors.Is(bare, m.dragonboatErr) {
				t.Errorf("errors.Is(%v, %v) = false for the bare input", bare, m.dragonboatErr)
			}
			if quicraft.MaybeApplied(bare) {
				t.Errorf("quicraft.MaybeApplied = true for a BARE %v; nothing was "+
					"submitted and the mapping must not invent the bit", m.quicraftErr)
			}
		})
	}
}
