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

package memdb

import (
	"errors"
	"math"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestSaveSnapshot_RefusesPositionOutsideProtocolRange is the MemDB half of
// the waldb test of the same name: the same bound on the same door.
func TestSaveSnapshot_RefusesPositionOutsideProtocolRange(t *testing.T) {
	db := New()
	for _, tc := range []struct {
		name string
		ss   logdb.Snapshot
		want error
	}{
		{"index at the ceiling", logdb.Snapshot{Index: math.MaxUint64, Term: 5}, proto.ErrLogIndexOutOfRange},
		{"index at the bound", logdb.Snapshot{Index: proto.MaxLogIndex, Term: 5}, proto.ErrLogIndexOutOfRange},
		{"term at the ceiling", logdb.Snapshot{Index: 10, Term: math.MaxUint64}, proto.ErrTermOutOfRange},
		{"term at the bound", logdb.Snapshot{Index: 10, Term: proto.MaxTerm}, proto.ErrTermOutOfRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := db.SaveSnapshot(1, 1, tc.ss); !errors.Is(err, tc.want) {
				t.Fatalf("SaveSnapshot(%+v) = %v, want %v", tc.ss, err, tc.want)
			}
			if _, err := db.GetSnapshot(1, 1); !errors.Is(err, logdb.ErrNoSnapshot) {
				t.Fatalf("GetSnapshot after a refused snapshot = %v, want ErrNoSnapshot", err)
			}
		})
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: proto.MaxLogIndex - 1, Term: proto.MaxTerm - 1}); err != nil {
		t.Fatalf("SaveSnapshot one below the bound = %v, want nil", err)
	}
}

// TestSaveState_RefusesSnapshotOutsideProtocolRange is the MemDB half of the
// waldb test of the same name.
func TestSaveState_RefusesSnapshotOutsideProtocolRange(t *testing.T) {
	db := New()
	err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		State:    logdb.State{Term: 1, Commit: 1},
		Entries:  []logdb.Entry{{Index: 1, Term: 1}},
		Snapshot: logdb.Snapshot{Index: 10, Term: proto.MaxTerm},
	}})
	if !errors.Is(err, proto.ErrTermOutOfRange) {
		t.Fatalf("SaveState with a snapshot term at the bound = %v, want ErrTermOutOfRange", err)
	}
	if first, count, _ := db.EntryRange(1, 1); first != 0 || count != 0 {
		t.Fatalf("the batch's entries were stored (range %d+%d) although the batch was refused", first, count)
	}
}
