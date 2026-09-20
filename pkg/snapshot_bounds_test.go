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
	"math"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestSnapshotReceive_RefusesPositionOutsideProtocolRange pins #197 C at the
// transport door: a chunk set that agrees on a position the protocol cannot
// do arithmetic on is refused during validation — before the artifact is
// written, before LogDB and before raft see it — as a SnapshotReceiveError
// whose cause is the proto sentinel.
func TestSnapshotReceive_RefusesPositionOutsideProtocolRange(t *testing.T) {
	for _, tc := range []struct {
		name        string
		index, term uint64
		want        error
	}{
		{"index at the ceiling", math.MaxUint64, 5, proto.ErrLogIndexOutOfRange},
		{"index at the bound", proto.MaxLogIndex, 5, proto.ErrLogIndexOutOfRange},
		{"term at the ceiling", 100, math.MaxUint64, proto.ErrTermOutOfRange},
		{"term at the bound", 100, proto.MaxTerm, proto.ErrTermOutOfRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chunks := []proto.SnapshotChunk{
				{ShardID: 1, ReplicaID: 1, From: 2, Index: tc.index, Term: tc.term, ChunkID: 0, ChunkCount: 2, Data: []byte("a")},
				{ShardID: 1, ReplicaID: 1, From: 2, Index: tc.index, Term: tc.term, ChunkID: 1, ChunkCount: 2, Data: []byte("b")},
			}
			ordered, err := validateSnapshotChunks(chunks)
			if ordered != nil {
				t.Fatalf("validateSnapshotChunks returned an ordered set for a refused position")
			}
			var recv *SnapshotReceiveError
			if !errors.As(err, &recv) || recv.Op != "validate" {
				t.Fatalf("want *SnapshotReceiveError{Op: validate}, got %T: %v", err, err)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("cause = %v, want %v", err, tc.want)
			}
		})
	}
	// One below the bound validates like any other set.
	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 1, From: 2, Index: proto.MaxLogIndex - 1, Term: proto.MaxTerm - 1, ChunkID: 0, ChunkCount: 1, Data: []byte("a")},
	}
	if _, err := validateSnapshotChunks(chunks); err != nil {
		t.Fatalf("validateSnapshotChunks one below the bound = %v, want nil", err)
	}
}
