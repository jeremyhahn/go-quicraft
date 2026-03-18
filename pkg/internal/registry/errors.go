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

package registry

import "fmt"

// NodeNotFoundError is returned when Resolve cannot find an address
// for the given (shardID, replicaID) pair.
type NodeNotFoundError struct {
	ShardID   uint64
	ReplicaID uint64
}

// Error returns a human-readable description of the lookup failure.
func (e *NodeNotFoundError) Error() string {
	return fmt.Sprintf(
		"registry: node not found (shard=%d, replica=%d)",
		e.ShardID, e.ReplicaID,
	)
}

// Is reports whether target matches the sentinel ErrNodeNotFound error.
func (e *NodeNotFoundError) Is(target error) bool {
	_, ok := target.(*NodeNotFoundError)
	return ok
}

// ErrNodeNotFound is a sentinel value for errors.Is() comparisons.
// Use errors.Is(err, &NodeNotFoundError{}) to match any NodeNotFoundError.
var ErrNodeNotFound = &NodeNotFoundError{}
