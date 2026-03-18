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

// Remove deletes the address entry for the given (shardID, replicaID)
// pair. If the node does not exist, Remove is a no-op.
func (r *Registry) Remove(shardID, replicaID uint64) {
	key := NodeInfo{ShardID: shardID, ReplicaID: replicaID}
	_, loaded := r.addresses.LoadAndDelete(key)
	if loaded {
		r.count.Add(-1)
	}
}

// Count returns the number of registered nodes.
func (r *Registry) Count() int {
	return int(r.count.Load())
}
