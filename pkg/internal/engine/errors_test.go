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
	"errors"
	"fmt"
	"testing"
)

func TestSaveStateError_Error(t *testing.T) {
	inner := errors.New("disk write failed")
	e := &SaveStateError{ShardID: 1, ReplicaID: 2, Err: inner}
	msg := e.Error()
	if msg == "" {
		t.Fatal("expected non-empty error message")
	}
	if !contains(msg, "shard 1") || !contains(msg, "replica 2") {
		t.Fatalf("error message missing shard/replica info: %s", msg)
	}
}

func TestSaveStateError_Unwrap(t *testing.T) {
	inner := errors.New("disk write failed")
	e := &SaveStateError{ShardID: 1, ReplicaID: 2, Err: inner}
	if !errors.Is(e, inner) {
		t.Fatal("expected Unwrap to return inner error")
	}
}

func TestApplyError_Error(t *testing.T) {
	inner := errors.New("sm.Apply failed")
	e := &ApplyError{ShardID: 3, ReplicaID: 4, Err: inner}
	msg := e.Error()
	if !contains(msg, "shard 3") || !contains(msg, "replica 4") {
		t.Fatalf("error message missing shard/replica info: %s", msg)
	}
}

func TestApplyError_Unwrap(t *testing.T) {
	inner := errors.New("sm.Apply failed")
	e := &ApplyError{ShardID: 3, ReplicaID: 4, Err: inner}
	if !errors.Is(e, inner) {
		t.Fatal("expected Unwrap to return inner error")
	}
}

func TestCircuitBreakerError_Error(t *testing.T) {
	inner := errors.New("permanent failure")
	e := &CircuitBreakerError{
		ShardID:          5,
		ReplicaID:        6,
		ConsecutiveFails: 100,
		LastErr:          inner,
	}
	msg := e.Error()
	if !contains(msg, "circuit breaker") || !contains(msg, "shard 5") {
		t.Fatalf("error message missing expected content: %s", msg)
	}
}

func TestCircuitBreakerError_Is(t *testing.T) {
	inner := errors.New("failure")
	e := &CircuitBreakerError{LastErr: inner}
	if !errors.Is(e, ErrShardFailed) {
		t.Fatal("expected CircuitBreakerError to match ErrShardFailed")
	}
}

func TestCircuitBreakerError_Unwrap(t *testing.T) {
	inner := errors.New("failure")
	e := &CircuitBreakerError{LastErr: inner}
	if !errors.Is(e, inner) {
		t.Fatal("expected Unwrap to return inner error")
	}
}

func TestSnapshotDuplicateError_Error(t *testing.T) {
	e := &SnapshotDuplicateError{ShardID: 10}
	msg := e.Error()
	if !contains(msg, "shard 10") || !contains(msg, "in-flight") {
		t.Fatalf("error message missing expected content: %s", msg)
	}
}

func TestSnapshotPoolBusyError_Error(t *testing.T) {
	e := &SnapshotPoolBusyError{ShardID: 5}
	msg := e.Error()
	if !contains(msg, "shard 5") || !contains(msg, "busy") {
		t.Fatalf("error message missing expected content: %s", msg)
	}
}

func TestSnapshotError_Error_Save(t *testing.T) {
	inner := errors.New("io error")
	e := &SnapshotError{ShardID: 7, ReplicaID: 8, Save: true, Err: inner}
	msg := e.Error()
	if !contains(msg, "save") || !contains(msg, "shard 7") {
		t.Fatalf("error message missing expected content: %s", msg)
	}
}

func TestSnapshotError_Error_Recover(t *testing.T) {
	inner := errors.New("io error")
	e := &SnapshotError{ShardID: 7, ReplicaID: 8, Save: false, Err: inner}
	msg := e.Error()
	if !contains(msg, "recover") {
		t.Fatalf("error message should contain 'recover': %s", msg)
	}
}

func TestSnapshotError_Unwrap(t *testing.T) {
	inner := errors.New("io error")
	e := &SnapshotError{Err: inner}
	if !errors.Is(e, inner) {
		t.Fatal("expected Unwrap to return inner error")
	}
}

func TestNodeNotLoadedError_Error(t *testing.T) {
	e := &NodeNotLoadedError{ShardID: 99}
	msg := e.Error()
	if !contains(msg, "shard 99") || !contains(msg, "not loaded") {
		t.Fatalf("error message missing expected content: %s", msg)
	}
}

func TestMessageHandleError_Error(t *testing.T) {
	inner := errors.New("unknown sender")
	e := &MessageHandleError{
		ShardID:   10,
		ReplicaID: 2,
		MsgType:   3,
		From:      5,
		Err:       inner,
	}
	msg := e.Error()
	if !contains(msg, "shard 10") || !contains(msg, "replica 2") {
		t.Fatalf("error message missing shard/replica info: %s", msg)
	}
	if !contains(msg, "Handle failed") {
		t.Fatalf("error message missing operation context: %s", msg)
	}
}

func TestMessageHandleError_Unwrap(t *testing.T) {
	inner := errors.New("stale term")
	e := &MessageHandleError{
		ShardID:   1,
		ReplicaID: 1,
		MsgType:   1,
		From:      2,
		Err:       inner,
	}
	if !errors.Is(e, inner) {
		t.Fatal("expected Unwrap to return inner error")
	}
}

func TestSnapshotTooLargeError_Error(t *testing.T) {
	e := &SnapshotTooLargeError{
		ShardID:   10,
		ReplicaID: 2,
		Size:      5000000000,
		Limit:     4294967296,
	}
	msg := e.Error()
	if !contains(msg, "shard 10") || !contains(msg, "replica 2") {
		t.Fatalf("error message missing shard/replica info: %s", msg)
	}
	if !contains(msg, "5000000000") || !contains(msg, "4294967296") {
		t.Fatalf("error message missing size/limit: %s", msg)
	}
}

func TestSnapshotTooLargeError_Is(t *testing.T) {
	e := &SnapshotTooLargeError{ShardID: 1, ReplicaID: 1, Size: 100, Limit: 50}
	if !errors.Is(e, ErrSnapshotTooLarge) {
		t.Fatal("expected Is(ErrSnapshotTooLarge) to be true")
	}
	if errors.Is(e, ErrSnapshotCorrupt) {
		t.Fatal("expected Is(ErrSnapshotCorrupt) to be false")
	}
}

func TestSentinelErrors_AreDistinct(t *testing.T) {
	if errors.Is(ErrShardFailed, ErrStopped) {
		t.Fatal("ErrShardFailed and ErrStopped should be distinct")
	}
}

// ---------------------------------------------------------------------------
// CommitError
// ---------------------------------------------------------------------------

func TestCommitError_Error(t *testing.T) {
	inner := errors.New("log gap detected")
	e := &CommitError{ShardID: 11, ReplicaID: 3, Err: inner}
	msg := e.Error()
	if !contains(msg, "Commit failed") {
		t.Fatalf("error message missing operation context: %s", msg)
	}
	if !contains(msg, "shard 11") || !contains(msg, "replica 3") {
		t.Fatalf("error message missing shard/replica info: %s", msg)
	}
	if !contains(msg, "log gap detected") {
		t.Fatalf("error message missing inner error: %s", msg)
	}
}

func TestCommitError_Error_PreservesInnerMessage(t *testing.T) {
	// Verify the full inner error message is preserved, not truncated.
	inner := errors.New("entry range [100, 200) missing from LogReader")
	e := &CommitError{ShardID: 42, ReplicaID: 7, Err: inner}
	msg := e.Error()
	if !contains(msg, "entry range [100, 200) missing from LogReader") {
		t.Fatalf("inner error message not fully preserved: %s", msg)
	}
}

func TestCommitError_Unwrap(t *testing.T) {
	inner := errors.New("log gap detected")
	e := &CommitError{ShardID: 11, ReplicaID: 3, Err: inner}
	if !errors.Is(e, inner) {
		t.Fatal("expected Unwrap to return inner error")
	}
}

func TestCommitError_Unwrap_ChainedErrors(t *testing.T) {
	root := errors.New("disk I/O error")
	wrapped := fmt.Errorf("log compaction failed: %w", root)
	e := &CommitError{ShardID: 1, ReplicaID: 1, Err: wrapped}
	if !errors.Is(e, root) {
		t.Fatal("expected errors.Is to reach root cause through Unwrap chain")
	}
}

func TestSnapshotExportError_Error(t *testing.T) {
	inner := errors.New("permission denied")
	e := &SnapshotExportError{ExportPath: "/tmp/export", Err: inner}
	msg := e.Error()
	if !contains(msg, "export") {
		t.Fatalf("error message missing 'export': %s", msg)
	}
	if !contains(msg, "/tmp/export") {
		t.Fatalf("error message missing export path: %s", msg)
	}
	if !contains(msg, "permission denied") {
		t.Fatalf("error message missing inner error: %s", msg)
	}
}

func TestSnapshotExportError_Unwrap(t *testing.T) {
	inner := errors.New("permission denied")
	e := &SnapshotExportError{ExportPath: "/tmp/export", Err: inner}
	if !errors.Is(e, inner) {
		t.Fatal("expected Unwrap to return inner error")
	}
}

// ---------------------------------------------------------------------------
// TypeAssertionError
// ---------------------------------------------------------------------------

func TestTypeAssertionError_Error(t *testing.T) {
	e := &TypeAssertionError{
		Context:      "DeliverMessage node lookup",
		ExpectedType: "*Node",
		ActualValue:  "not a node",
	}
	msg := e.Error()
	if !contains(msg, "type assertion failed") {
		t.Fatalf("error message missing 'type assertion failed': %s", msg)
	}
	if !contains(msg, "DeliverMessage node lookup") {
		t.Fatalf("error message missing context: %s", msg)
	}
	if !contains(msg, "*Node") {
		t.Fatalf("error message missing expected type: %s", msg)
	}
	if !contains(msg, "string") {
		t.Fatalf("error message missing actual type: %s", msg)
	}
}

func TestTypeAssertionError_Error_NilValue(t *testing.T) {
	e := &TypeAssertionError{
		Context:      "GetNode",
		ExpectedType: "*Node",
		ActualValue:  nil,
	}
	msg := e.Error()
	if !contains(msg, "type assertion failed") {
		t.Fatalf("error message missing 'type assertion failed': %s", msg)
	}
	if !contains(msg, "<nil>") {
		t.Fatalf("error message missing nil indicator: %s", msg)
	}
}

// contains is a helper that checks whether s contains substr.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
