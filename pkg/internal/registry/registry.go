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

// Package registry maps (shardID, replicaID) pairs to network addresses for the transport, with lock-free reads via sync.Map.
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

// Registry maps (shardID, replicaID) pairs to network addresses; safe for concurrent use, with lock-free Resolve.
type Registry struct {
	addresses sync.Map // NodeInfo -> string
	count     atomic.Int64
}

// NewRegistry creates a new empty address registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Resolve returns the address for (shardID, replicaID), or a *NodeNotFoundError; it is lock-free.
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

// RegisterIfAbsent atomically stores address only when no entry exists, returning the held address and whether this call stored it.
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

// CompareAndRemove deletes the entry for (shardID, replicaID) only if its address equals want, reporting whether it did.
func (r *Registry) CompareAndRemove(shardID, replicaID uint64, want string) bool {
	key := NodeInfo{ShardID: shardID, ReplicaID: replicaID}
	if r.addresses.CompareAndDelete(key, want) {
		r.count.Add(-1)
		return true
	}
	return false
}

// CompareAndRegister replaces the entry's address with next only if it equals want, reporting whether it did.
func (r *Registry) CompareAndRegister(shardID, replicaID uint64, want, next string) bool {
	key := NodeInfo{ShardID: shardID, ReplicaID: replicaID}
	return r.addresses.CompareAndSwap(key, want, next)
}

// AddressInUse reports whether any registered pair maps to address; an O(n) scan for infrequent use, not a snapshot.
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

// ReplicasAt returns every (shardID, replicaID) registered at address. Like
// AddressInUse it is a scan, not a snapshot, and is meant for rare paths —
// the transport asks it once per outbound dial — never the per-message one.
func (r *Registry) ReplicasAt(address string) []NodeInfo {
	var at []NodeInfo
	r.addresses.Range(func(key, val any) bool {
		if val.(string) == address {
			at = append(at, key.(NodeInfo))
		}
		return true
	})
	return at
}

// Addresses returns every distinct address registered, in no particular
// order. A certificate reload uses it to re-run the SAN-exclusivity check
// against every peer this host currently knows.
func (r *Registry) Addresses() []string {
	seen := make(map[string]struct{})
	var addrs []string
	r.addresses.Range(func(_, val any) bool {
		addr := val.(string)
		if _, ok := seen[addr]; !ok {
			seen[addr] = struct{}{}
			addrs = append(addrs, addr)
		}
		return true
	})
	return addrs
}

// Count returns the number of registered nodes.
func (r *Registry) Count() int {
	return int(r.count.Load())
}
