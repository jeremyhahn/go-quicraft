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
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestSnapshotReceive_ValidationCausesAreTyped pins that the three refusals
// that used to be fmt.Errorf strings inside a SnapshotReceiveError (#193) are
// reachable by errors.As with their fields: a caller can tell a truncated or
// forged size from a defective chunk set, and which chunk, without reading
// the message.
func TestSnapshotReceive_ValidationCausesAreTyped(t *testing.T) {
	handler, _ := newSnapshotHandler(t)

	t.Run("size mismatch", func(t *testing.T) {
		chunks := []proto.SnapshotChunk{
			{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 2, FileSize: 100, Data: []byte("abc")},
			{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 1, ChunkCount: 2, FileSize: 100, Data: []byte("def")},
		}
		err := handler.HandleSnapshot(chunks, authorizeAllSenders)
		var recv *SnapshotReceiveError
		if !errors.As(err, &recv) || recv.Op != "validate" {
			t.Fatalf("want *SnapshotReceiveError{Op: validate}, got %T: %v", err, err)
		}
		var size *SnapshotSizeMismatchError
		if !errors.As(err, &size) {
			t.Fatalf("the cause is not a *SnapshotSizeMismatchError: %v", err)
		}
		if size.Declared != 100 || size.Actual != 6 {
			t.Fatalf("size mismatch = declared %d actual %d, want 100/6", size.Declared, size.Actual)
		}
	})

	for _, tc := range []struct {
		name    string
		chunks  []proto.SnapshotChunk
		chunkID uint64
		reason  string
	}{
		{
			name: "identity mismatch",
			chunks: []proto.SnapshotChunk{
				{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 2, Data: []byte("a")},
				{ShardID: 1, ReplicaID: 1, From: 2, Index: 101, Term: 5, ChunkID: 1, ChunkCount: 2, Data: []byte("b")},
			},
			chunkID: 1, reason: "identity mismatch with chunk 0",
		},
		{
			name: "chunk count disagrees",
			chunks: []proto.SnapshotChunk{
				{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 2, Data: []byte("a")},
				{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 1, ChunkCount: 3, Data: []byte("b")},
			},
			chunkID: 1, reason: "declares ChunkCount 3 but 2 chunks were received",
		},
		{
			name: "duplicate chunk",
			chunks: []proto.SnapshotChunk{
				{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 2, Data: []byte("a")},
				{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 2, Data: []byte("b")},
			},
			chunkID: 0, reason: "duplicate chunk ID",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateSnapshotChunks(tc.chunks)
			var recv *SnapshotReceiveError
			if !errors.As(err, &recv) || recv.Op != "validate" {
				t.Fatalf("want *SnapshotReceiveError{Op: validate}, got %T: %v", err, err)
			}
			var chunk *SnapshotChunkError
			if !errors.As(err, &chunk) {
				t.Fatalf("the cause is not a *SnapshotChunkError: %v", err)
			}
			if chunk.ChunkID != tc.chunkID || chunk.ChunkCount != 2 {
				t.Fatalf("chunk = %d of %d, want %d of 2", chunk.ChunkID, chunk.ChunkCount, tc.chunkID)
			}
			if len(chunk.Reason) < len(tc.reason) || chunk.Reason[:len(tc.reason)] != tc.reason {
				t.Fatalf("reason = %q, want it to start with %q", chunk.Reason, tc.reason)
			}
		})
	}
}

// TestRequestUpdateWitnessPolicy_NegativeMaxWitnessesIsAValidationError pins
// that the refusal of a negative MaxWitnesses is a config.ValidationError,
// the type every other config validation already uses, inside the
// HostInitError callers already match.
func TestRequestUpdateWitnessPolicy_NegativeMaxWitnessesIsAValidationError(t *testing.T) {
	h := newTestHost(t)
	_, err := h.RequestUpdateWitnessPolicy(context.Background(), 1, config.WitnessConfig{MaxWitnesses: -1}, 0)
	var initErr *HostInitError
	if !errors.As(err, &initErr) || initErr.Field != "witness_policy" {
		t.Fatalf("want *HostInitError{Field: witness_policy}, got %T: %v", err, err)
	}
	var v *config.ValidationError
	if !errors.As(err, &v) {
		t.Fatalf("the cause is not a *config.ValidationError: %v", err)
	}
	if v.Field != "MaxWitnesses" {
		t.Fatalf("validation field = %q, want MaxWitnesses", v.Field)
	}
}
