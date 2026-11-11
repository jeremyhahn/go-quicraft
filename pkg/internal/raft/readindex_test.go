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

package raft

// ReadIndex request tracking. Request addition, quorum confirmation, the
// recorded index, clearing on role change, the no-op readiness gate and the
// overload limits are tested through step in raft_test.go,
// readindex_context_regression_test.go and spec_gap_regression_test.go.

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestReadIndex_DuplicateKeyGetsDistinctContext verifies that the same client key
// can be added multiple times (they get different ctx values). Each request
// is independent and tracked separately.
func TestReadIndex_DuplicateKeyGetsDistinctContext(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	// Send two ReadIndex requests with the same key.
	err := r.step(proto.Message{Type: proto.ReadIndex, Origin: proto.OriginLocal, Hint: 42})
	if err != nil {
		t.Fatalf("unexpected error on first request: %v", err)
	}
	err = r.step(proto.Message{Type: proto.ReadIndex, Origin: proto.OriginLocal, Hint: 42})
	if err != nil {
		t.Fatalf("unexpected error on second request: %v", err)
	}

	// Both should be tracked as separate pending entries with different ctx.
	if len(r.pendingReadIndex) != 2 {
		t.Fatalf("expected 2 pending read index entries, got %d", len(r.pendingReadIndex))
	}
	if r.pendingReadIndex[0].ctx == r.pendingReadIndex[1].ctx {
		t.Fatal("expected different ctx values for duplicate key requests")
	}
	if r.pendingReadIndex[0].key != 42 || r.pendingReadIndex[1].key != 42 {
		t.Fatal("expected both entries to have key=42")
	}
}
