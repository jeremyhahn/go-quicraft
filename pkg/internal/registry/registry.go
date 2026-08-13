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

// Package registry provides the address registry that maps (shardID, replicaID)
// pairs to network addresses. It is used by the transport layer to resolve
// peer addresses for Raft message delivery.
//
// Registry is a concrete type (not an interface) because there is exactly one
// implementation. Adding an interface would be unnecessary indirection on the
// hot path.
//
// The underlying sync.Map provides lock-free reads on the Resolve hot path.
// Addresses are stable after shard startup (written once on bootstrap, rarely
// updated on membership change), making sync.Map's read-biased design ideal.
// This eliminates RWMutex reader-count atomic contention that previously
// caused cache-line bouncing across concurrent step workers and receive
// handlers.
package registry

import (
	"sync"
	"sync/atomic"
)

// NodeInfo uniquely identifies a replica within a shard.
type NodeInfo struct {
	ShardID   uint64
	ReplicaID uint64
}

// Registry maps (shardID, replicaID) pairs to network addresses. It is
// safe for concurrent use. The registry is created by Host and passed
// to Transport at construction time. Updated by the engine when
// membership changes.
//
// Resolve is lock-free via sync.Map. Register and Remove take the
// slower sync.Map Store/Delete path, which is acceptable since they
// only occur during membership changes (rare after startup).
type Registry struct {
	addresses sync.Map // NodeInfo -> string
	count     atomic.Int64
}

// NewRegistry creates a new empty address registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Resolve returns the network address for the given (shardID, replicaID)
// pair. Returns a *NodeNotFoundError if the node is not registered.
//
// Resolve is lock-free: it uses sync.Map.Load which requires no mutex
// acquisition, only an atomic pointer read from the read-only map.
func (r *Registry) Resolve(shardID, replicaID uint64) (string, error) {
	key := NodeInfo{ShardID: shardID, ReplicaID: replicaID}
	val, ok := r.addresses.Load(key)
	if !ok {
		return "", &NodeNotFoundError{ShardID: shardID, ReplicaID: replicaID}
	}
	return val.(string), nil
}

// Register adds or updates the network address for the given
// (shardID, replicaID) pair. If the node already exists, the address
// is overwritten.
func (r *Registry) Register(shardID, replicaID uint64, address string) {
	key := NodeInfo{ShardID: shardID, ReplicaID: replicaID}
	_, loaded := r.addresses.Swap(key, address)
	if !loaded {
		r.count.Add(1)
	}
}

// RegisterIfAbsent records address for (shardID, replicaID) only when no entry
// exists yet. It returns the address the entry holds afterwards — the one just
// stored, or the pre-existing one that was kept — and whether this call is what
// stored it.
//
// It is the ATOMIC form of Resolve-then-Register. The message handler's address
// learning did those as two separate sync.Map operations, which does not
// actually enforce the invariant it was written for: a sender the committed
// configuration does not name may ESTABLISH a mapping but must never REPLACE
// one. Two concurrent receives for the same (shard, replica) both observe no
// entry and both Register, so the loser's address silently replaces the
// winner's — the exact hijack the check exists to deny. LoadOrStore closes the
// window, leaving Register and CompareAndRegister as the only paths that can
// replace an established mapping.
func (r *Registry) RegisterIfAbsent(shardID, replicaID uint64, address string) (string, bool) {
	key := NodeInfo{ShardID: shardID, ReplicaID: replicaID}
	actual, loaded := r.addresses.LoadOrStore(key, address)
	if !loaded {
		r.count.Add(1)
	}
	return actual.(string), !loaded
}

// Remove deletes the address entry for the given (shardID, replicaID)
// pair. If the node does not exist, Remove is a no-op.
func (r *Registry) Remove(shardID, replicaID uint64) {
	key := NodeInfo{ShardID: shardID, ReplicaID: replicaID}
	_, loaded := r.addresses.LoadAndDelete(key)
	if loaded {
		r.count.Add(-1)
	}
}

// CompareAndRemove deletes the entry for (shardID, replicaID) only if its
// current address equals want. It returns true if the entry was removed.
//
// This is the atomic form of Resolve-then-Remove: a speculative pre-register
// (used while a config-change proposal is in flight) can be rolled back on a
// deadline sweep without racing the apply path that commits the same replica
// at a possibly DIFFERENT address. Without the compare, a rollback could delete
// the mapping the apply path just wrote, silently black-holing traffic to a
// committed member. CompareAndDelete removes only when the mapping still holds
// the speculative value, so a mapping already overwritten by the apply path is
// left intact.
func (r *Registry) CompareAndRemove(shardID, replicaID uint64, want string) bool {
	key := NodeInfo{ShardID: shardID, ReplicaID: replicaID}
	if r.addresses.CompareAndDelete(key, want) {
		r.count.Add(-1)
		return true
	}
	return false
}

// CompareAndRegister overwrites the entry for (shardID, replicaID) with next
// only if its current address equals want. It returns true if the swap
// happened. The count is unchanged: the entry exists both before and after.
//
// This is the atomic restore counterpart to CompareAndRemove: a speculative
// pre-register that overwrote a PRIOR mapping rolls back by restoring the prior
// value, but only while the mapping still holds the speculative value — a later
// config change (or the apply of this one after it committed) that has already
// claimed the entry with a different address must win over the late rollback.
func (r *Registry) CompareAndRegister(shardID, replicaID uint64, want, next string) bool {
	key := NodeInfo{ShardID: shardID, ReplicaID: replicaID}
	return r.addresses.CompareAndSwap(key, want, next)
}

// AddressInUse reports whether ANY registered (shardID, replicaID) pair still
// maps to address. It exists because one transport connection is keyed by
// ADDRESS and multiplexed across every shard that talks to that host: before a
// caller may tear the connection down on behalf of one shard's committed
// membership removal, it has to establish that no other shard still resolves a
// replica to the same address. Tearing down while another shard is using the
// peer would kill healthy traffic.
//
// The registry keeps no reverse (address -> members) index, so this is an O(n)
// scan over registered members via sync.Map.Range, exiting at the first match.
// It is intended for INFREQUENT, event-driven use — a committed membership
// removal — and must never be placed on a message or resolve hot path.
//
// The scan is not a snapshot: sync.Map.Range makes no atomicity guarantee
// across entries, so a concurrent Register may or may not be observed. The
// caller must therefore treat a false result as "no member was registered at
// this address during the scan", which is exactly the ordering a committed
// removal already has (the removal is applied, hence out of the registry,
// before the check runs).
func (r *Registry) AddressInUse(address string) bool {
	inUse := false
	r.addresses.Range(func(_, val any) bool {
		if val.(string) != address {
			return true
		}
		inUse = true
		return false
	})
	return inUse
}

// Count returns the number of registered nodes.
func (r *Registry) Count() int {
	return int(r.count.Load())
}
