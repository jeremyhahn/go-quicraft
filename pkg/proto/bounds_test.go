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

package proto

import (
	"errors"
	"math"
	"testing"
)

// TestSnapshotCheckBounds pins the one predicate every snapshot admission
// path applies (#197 C): the position must lie below MaxLogIndex / MaxTerm so
// that Index+1 and term arithmetic cannot wrap.
func TestSnapshotCheckBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		ss   Snapshot
		want error
	}{
		{"zero position is not this predicate's concern", Snapshot{}, nil},
		{"ordinary position", Snapshot{Index: 1000, Term: 7}, nil},
		{"index just below the bound", Snapshot{Index: MaxLogIndex - 1, Term: 7}, nil},
		{"term just below the bound", Snapshot{Index: 1000, Term: MaxTerm - 1}, nil},
		{"index at the bound", Snapshot{Index: MaxLogIndex, Term: 7}, ErrLogIndexOutOfRange},
		{"index at the ceiling", Snapshot{Index: math.MaxUint64, Term: 7}, ErrLogIndexOutOfRange},
		{"term at the bound", Snapshot{Index: 1000, Term: MaxTerm}, ErrTermOutOfRange},
		{"term at the ceiling", Snapshot{Index: 1000, Term: math.MaxUint64}, ErrTermOutOfRange},
		{"both out of range reports the index first", Snapshot{Index: math.MaxUint64, Term: math.MaxUint64}, ErrLogIndexOutOfRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.ss.CheckBounds(); !errors.Is(err, tc.want) {
				t.Fatalf("CheckBounds(%+v) = %v, want %v", tc.ss, err, tc.want)
			}
		})
	}
	if MaxTerm != MaxLogIndex {
		t.Fatalf("MaxTerm %d != MaxLogIndex %d: the two bounds were introduced together and share a headroom argument", MaxTerm, MaxLogIndex)
	}
	if math.MaxUint64-MaxTerm < 1024 {
		t.Fatalf("MaxTerm leaves %d of headroom, want at least 1024", math.MaxUint64-MaxTerm)
	}
}
