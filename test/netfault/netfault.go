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

// Package netfault injects network partitions between live replicas.
//
// # Why this exists
//
// Every "partition" test in the suite used to call stopNode — a process kill.
// A killed node is not a partitioned node: it holds no leadership belief, it
// answers nothing, and it returns with cold in-memory state. The single most
// dangerous configuration in Raft — a LIVE leader deposed by a quorum it can no
// longer reach — could not be constructed anywhere in the suite. The cost of
// that blind spot is measurable: with it, weakening the election quorum to n/2
// passed 18 of 19 linearizability tests.
//
// # How it is wired
//
// The transport consults this package from its send path, and only in a build
// tagged quicraft_netfault. In a normal build the check is a false compile-time
// constant, the branch folds away, and this package is not in the binary's
// dependency graph at all — the same discipline test/errfs uses for filesystem
// faults. Calling anything here in a normal build is therefore inert.
//
// Drops are applied on the SENDING side, which expresses any directed cut: the
// edge A->B is controlled entirely at A. Both nodes stay alive, keep ticking,
// and keep whatever term and role they held.
//
// # Model
//
// Drops are silent, as on a real network: the sender's connections stay up and
// no error is surfaced. Raft cannot distinguish an unanswered message from an
// undelivered one, and neither can this.
//
// The blocked set is published as a whole through an atomic.Pointer, so the
// send path reads it without a lock and a test may flip a partition while
// traffic is in flight.
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

// Drop reports whether a message must be dropped. The transport calls this on
// every outbound message when built with quicraft_netfault.
//
// shardID is accepted for symmetry with the call site and for future per-shard
// cuts; partitions today are node-level, which is what a network partition
// actually is.
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

// BlockLink drops every message sent from replica `from` to replica `to`,
// leaving the reverse direction intact. This is the primitive for asymmetric
// partitions — a leader that still hears acknowledgements it can no longer
// answer, or answers nothing it still hears.
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

// BlockedCount reports how many faults are currently installed — directed edges
// plus isolated replicas.
//
// It exists so a test can assert its own fault setup took effect rather than
// silently exercising a healthy cluster. That is not hypothetical: a partition
// suite that secretly tested nothing is the reason this package exists, and a
// suite built with the wrong tag would reproduce it exactly, because every
// function here is inert without quicraft_netfault. Assert on this.
func BlockedCount() int {
	s := cur.Load()
	if s == nil {
		return 0
	}
	return len(s.blocked) + len(s.isolated)
}
