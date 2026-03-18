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

import "sync"

// defaultEntrySliceCap is the initial capacity for pooled entry slices.
// Sized for typical committed entry batches (1-16 entries). Larger batches
// grow naturally and the grown slice is returned to the pool for reuse.
const defaultEntrySliceCap = 16

// entrySlicePool pools reusable []Entry slices to reduce allocation
// pressure on the proposal/commit/apply hot path. Each Raft GetUpdate
// call allocates a []Entry for CommittedEntries; pooling avoids per-call
// heap allocations under sustained proposal load.
//
// Callers obtain a slice via GetEntrySlice and must return it via
// PutEntrySlice after the entries are no longer referenced. PutEntrySlice
// zeroes all elements to release Cmd byte slice references and allow GC
// to collect the underlying data.
var entrySlicePool = sync.Pool{
	New: func() any {
		s := make([]Entry, 0, defaultEntrySliceCap)
		return &s
	},
}

// GetEntrySlice returns a pooled []Entry slice with length 0 and at
// least defaultEntrySliceCap capacity. The caller must call
// PutEntrySlice when done to return the slice to the pool.
func GetEntrySlice() *[]Entry {
	return entrySlicePool.Get().(*[]Entry)
}

// PutEntrySlice returns a []Entry slice to the pool after clearing all
// elements. Each Entry is zeroed to nil out the Cmd byte slice pointer,
// allowing the garbage collector to reclaim the referenced data. The
// slice length is reset to 0 while preserving the underlying capacity
// for reuse.
//
// Passing a nil pointer is a no-op for caller convenience.
func PutEntrySlice(s *[]Entry) {
	if s == nil {
		return
	}
	slice := *s
	for i := range slice {
		slice[i] = Entry{}
	}
	*s = slice[:0]
	entrySlicePool.Put(s)
}
