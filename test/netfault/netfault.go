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

// Package netfault drops messages on the sending side to partition live replicas silently; active only in builds tagged quicraft_netfault, inert otherwise.
package netfault

import (
	"sync"
	"sync/atomic"
)

// link identifies one directed edge between two replicas.
type link struct {
	from uint64
	to   uint64
}

// state is the immutable blocked set published to the send path.
type state struct {
	// blocked holds directed edges. An edge is present iff messages
	// travelling it are dropped.
	blocked map[link]struct{}
	// isolated holds replicas cut off in BOTH directions from everyone.
	isolated map[uint64]struct{}
}

var (
	// mu serialises mutators; readers never take it.
	mu sync.Mutex
	// cur is nil until the first mutator runs, which keeps the send path at a
	// single nil check for suites that never inject.
	cur atomic.Pointer[state]
)

// Drop reports whether a message must be dropped; the transport calls it for every outbound message under quicraft_netfault. shardID is unused.
func Drop(_, from, to uint64) bool {
	s := cur.Load()
	if s == nil {
		return false
	}
	if _, ok := s.isolated[from]; ok {
		return true
	}
	if _, ok := s.isolated[to]; ok {
		return true
	}
	_, ok := s.blocked[link{from: from, to: to}]
	return ok
}

// mutate applies fn to a copy of the current state and publishes it.
// Copy-on-write keeps readers lock-free and never mutates a published map.
func mutate(fn func(*state)) {
	mu.Lock()
	defer mu.Unlock()

	next := &state{
		blocked:  make(map[link]struct{}),
		isolated: make(map[uint64]struct{}),
	}
	if c := cur.Load(); c != nil {
		for k := range c.blocked {
			next.blocked[k] = struct{}{}
		}
		for k := range c.isolated {
			next.isolated[k] = struct{}{}
		}
	}
	fn(next)
	cur.Store(next)
}

// BlockLink drops every message from replica from to replica to, leaving the reverse direction intact.
func BlockLink(from, to uint64) {
	mutate(func(s *state) { s.blocked[link{from: from, to: to}] = struct{}{} })
}

// HealLink restores a single directed edge.
func HealLink(from, to uint64) {
	mutate(func(s *state) { delete(s.blocked, link{from: from, to: to}) })
}

// Isolate cuts a replica off in both directions from every other replica
// without stopping it. It keeps its term, its role and its leadership belief,
// and keeps ticking — the live-but-deposed leader stopNode cannot produce.
func Isolate(replica uint64) {
	mutate(func(s *state) { s.isolated[replica] = struct{}{} })
}

// Rejoin restores a replica isolated by Isolate. Edges blocked individually
// with BlockLink are unaffected.
func Rejoin(replica uint64) {
	mutate(func(s *state) { delete(s.isolated, replica) })
}

// Partition splits the cluster along a cut, blocking both directions between
// every member of groupA and every member of groupB. Traffic within each group
// is untouched, so each side stays internally connected and can elect among
// itself if it holds quorum.
func Partition(groupA, groupB []uint64) {
	mutate(func(s *state) {
		for _, a := range groupA {
			for _, b := range groupB {
				s.blocked[link{from: a, to: b}] = struct{}{}
				s.blocked[link{from: b, to: a}] = struct{}{}
			}
		}
	})
}

// Heal removes every injected fault, restoring full connectivity. Tests must
// call it on cleanup: the state is process-global, so a partition left standing
// leaks into whatever test runs next.
func Heal() {
	mu.Lock()
	defer mu.Unlock()
	cur.Store(nil)
}

// BlockedCount reports installed faults (directed edges plus isolated replicas); assert on it, since everything here is inert without quicraft_netfault.
func BlockedCount() int {
	s := cur.Load()
	if s == nil {
		return 0
	}
	return len(s.blocked) + len(s.isolated)
}
