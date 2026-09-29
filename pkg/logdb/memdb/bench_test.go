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
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// benchEntries builds count entries with a cmdSize-byte payload each.
func benchEntries(start, count uint64, term uint64, cmdSize int) []logdb.Entry {
	entries := make([]logdb.Entry, count)
	for i := uint64(0); i < count; i++ {
		cmd := make([]byte, cmdSize)
		for j := range cmd {
			cmd[j] = byte(j % 251)
		}
		entries[i] = logdb.Entry{Index: start + i, Term: term, Cmd: cmd}
	}
	return entries
}

// BenchmarkMemDBIterateEntries measures the replication read path: the call
// raft makes to fill an AppendEntries message. It mirrors
// waldb.BenchmarkDBIterateEntries (1000 stored entries of 128 bytes, 99 read
// per call, destination slice reused) so the two backends' read costs are
// directly comparable.
func BenchmarkMemDBIterateEntries(b *testing.B) {
	b.ReportAllocs()
	db := New()
	b.Cleanup(func() { db.Close() })

	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1, Entries: benchEntries(1, 1000, 1, 128),
	}}); err != nil {
		b.Fatalf("SaveState: %v", err)
	}

	buf := make([]logdb.Entry, 0, 100)

	b.ResetTimer()
	for b.Loop() {
		buf = buf[:0]
		if _, _, err := db.IterateEntries(buf, 0, 1, 1, 1, 100, 0); err != nil {
			b.Fatalf("IterateEntries: %v", err)
		}
	}
}

// BenchmarkMemDBSaveStateParallel measures concurrent SaveState batches on
// distinct nodes — the shape of many Raft groups committing at once — so any
// lock every writer shares shows up as contention.
func BenchmarkMemDBSaveStateParallel(b *testing.B) {
	db := New()
	var next atomic.Uint64
	cmd := make([]byte, 64)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		shard := next.Add(1)
		var index uint64
		for pb.Next() {
			index++
			if err := db.SaveState([]logdb.Update{{
				ShardID:   shard,
				ReplicaID: 1,
				State:     logdb.State{Term: 1, Commit: index},
				Entries:   []logdb.Entry{{Index: index, Term: 1, Cmd: cmd}},
			}}); err != nil {
				b.Fatalf("SaveState: %v", err)
			}
			if index%1024 == 0 {
				if err := db.RemoveEntriesTo(shard, 1, index-1); err != nil {
					b.Fatalf("RemoveEntriesTo: %v", err)
				}
			}
		}
	})
}
