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
	"testing"
)

// TestEngineErrors_Error verifies that each engine error type's message
// carries the operation and every identifying field an operator triages it by.
func TestEngineErrors_Error(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want []string
	}{
		{
			name: "SaveStateError",
			err:  &SaveStateError{ShardID: 1, ReplicaID: 2, Err: errors.New("disk write failed")},
			want: []string{"shard 1", "replica 2"},
		},
		{
			name: "ApplyError",
			err:  &ApplyError{ShardID: 3, ReplicaID: 4, Err: errors.New("sm.Apply failed")},
			want: []string{"shard 3", "replica 4"},
		},
		{
			name: "SnapshotDuplicateError",
			err:  &SnapshotDuplicateError{ShardID: 10},
			want: []string{"shard 10", "in-flight"},
		},
		{
			name: "SnapshotPoolBusyError",
			err:  &SnapshotPoolBusyError{ShardID: 5},
			want: []string{"shard 5", "busy"},
		},
		{
			name: "SnapshotError save",
			err:  &SnapshotError{ShardID: 7, ReplicaID: 8, Save: true, Err: errors.New("io error")},
			want: []string{"save", "shard 7"},
		},
		{
			name: "SnapshotError recover",
			err:  &SnapshotError{ShardID: 7, ReplicaID: 8, Save: false, Err: errors.New("io error")},
			want: []string{"recover"},
		},
		{
			name: "NodeNotLoadedError",
			err:  &NodeNotLoadedError{ShardID: 99},
			want: []string{"shard 99", "not loaded"},
		},
		{
			name: "MessageHandleError",
			err: &MessageHandleError{
				ShardID: 10, ReplicaID: 2, MsgType: 3, From: 5,
				Err: errors.New("unknown sender"),
			},
			want: []string{"shard 10", "replica 2", "Handle failed"},
		},
		{
			name: "SnapshotTooLargeError",
			err: &SnapshotTooLargeError{
				ShardID: 10, ReplicaID: 2, Size: 5000000000, Limit: 4294967296,
			},
			want: []string{"shard 10", "replica 2", "5000000000", "4294967296"},
		},
		{
			name: "CommitError",
			err:  &CommitError{ShardID: 11, ReplicaID: 3, Err: errors.New("log gap detected")},
			want: []string{"Commit failed", "shard 11", "replica 3", "log gap detected"},
		},
		{
			name: "SnapshotExportError",
			err:  &SnapshotExportError{ExportPath: "/tmp/export", Err: errors.New("permission denied")},
			want: []string{"export", "/tmp/export", "permission denied"},
		},
		{
			name: "TypeAssertionError",
			err: &TypeAssertionError{
				Context:      "DeliverMessage node lookup",
				ExpectedType: "*Node",
				ActualValue:  "not a node",
			},
			want: []string{"type assertion failed", "DeliverMessage node lookup", "*Node", "string"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := tt.err.Error()
			for _, want := range tt.want {
				if !contains(msg, want) {
					t.Fatalf("error message missing %q: %s", want, msg)
				}
			}
		})
	}
}

// TestEngineErrors_Unwrap verifies that each wrapping engine error type
// exposes its cause to errors.Is.
func TestEngineErrors_Unwrap(t *testing.T) {
	inner := errors.New("inner cause")
	tests := []struct {
		name string
		err  error
	}{
		{"SaveStateError", &SaveStateError{ShardID: 1, ReplicaID: 2, Err: inner}},
		{"ApplyError", &ApplyError{ShardID: 3, ReplicaID: 4, Err: inner}},
		{"SnapshotError", &SnapshotError{Err: inner}},
		{"MessageHandleError", &MessageHandleError{ShardID: 1, ReplicaID: 1, MsgType: 1, From: 2, Err: inner}},
		{"CommitError", &CommitError{ShardID: 11, ReplicaID: 3, Err: inner}},
		{"SnapshotExportError", &SnapshotExportError{ExportPath: "/tmp/export", Err: inner}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !errors.Is(tt.err, inner) {
				t.Fatal("expected Unwrap to return inner error")
			}
		})
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

// ---------------------------------------------------------------------------
// Snapshot recovery bound errors
// ---------------------------------------------------------------------------

func TestSnapshotRecoveryBehindError_Error(t *testing.T) {
	e := &SnapshotRecoveryBehindError{
		ShardID:       9,
		ReplicaID:     2,
		SnapshotIndex: 300,
		Committed:     120,
		Processed:     110,
	}
	msg := e.Error()
	// Operators triage these from logs: every bound that made the recovery
	// inadmissible must be present.
	for _, want := range []string{"shard 9", "replica 2", "index 300", "committed 120", "processed 110"} {
		if !contains(msg, want) {
			t.Fatalf("error message missing %q: %s", want, msg)
		}
	}
	if !e.Retryable() {
		t.Fatal("SnapshotRecoveryBehindError must report itself retryable")
	}
}

func TestSnapshotRecoveryStaleError_Error(t *testing.T) {
	e := &SnapshotRecoveryStaleError{
		ShardID:       9,
		ReplicaID:     2,
		SnapshotIndex: 100,
		LastApplied:   400,
	}
	msg := e.Error()
	for _, want := range []string{"shard 9", "replica 2", "index 100", "applied index 400"} {
		if !contains(msg, want) {
			t.Fatalf("error message missing %q: %s", want, msg)
		}
	}
	// A stale recovery is permanent, not retryable: it must NOT expose the
	// retryable behavior that would make a caller re-request it forever.
	var retryable interface{ Retryable() bool }
	if errors.As(error(e), &retryable) {
		t.Fatal("SnapshotRecoveryStaleError must not be retryable: recovering it would regress the state machine")
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
