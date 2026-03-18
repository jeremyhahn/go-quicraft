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

package sm

// CreateFunc is the factory function type for creating state machine instances.
// It returns one of StateMachine, ConcurrentStateMachine, or DiskStateMachine.
// The engine inspects the concrete type at startup to determine the state machine
// variant. This avoids three separate Start methods on Host in favor of one
// StartShard method.
//
// The returned interface{} must implement exactly one of the three state machine
// interfaces. If none match, StartShard returns an error.
type CreateFunc func(shardID, replicaID uint64) interface{}

// CreateStateMachineFunc creates an in-memory StateMachine instance.
// Used with NewCreateFunc to provide type-safe factory creation.
type CreateStateMachineFunc func(shardID uint64, replicaID uint64) StateMachine

// CreateConcurrentStateMachineFunc creates a ConcurrentStateMachine instance.
// Used with NewConcurrentCreateFunc to provide type-safe factory creation.
type CreateConcurrentStateMachineFunc func(shardID uint64, replicaID uint64) ConcurrentStateMachine

// CreateDiskStateMachineFunc creates a DiskStateMachine instance.
// Used with NewDiskCreateFunc to provide type-safe factory creation.
type CreateDiskStateMachineFunc func(shardID uint64, replicaID uint64) DiskStateMachine

// NewCreateFunc wraps a type-safe StateMachine factory into a CreateFunc.
// This catches type errors at compile time rather than runtime.
func NewCreateFunc(fn CreateStateMachineFunc) CreateFunc {
	return func(shardID, replicaID uint64) interface{} {
		return fn(shardID, replicaID)
	}
}

// NewConcurrentCreateFunc wraps a type-safe ConcurrentStateMachine factory
// into a CreateFunc.
func NewConcurrentCreateFunc(fn CreateConcurrentStateMachineFunc) CreateFunc {
	return func(shardID, replicaID uint64) interface{} {
		return fn(shardID, replicaID)
	}
}

// NewDiskCreateFunc wraps a type-safe DiskStateMachine factory
// into a CreateFunc.
func NewDiskCreateFunc(fn CreateDiskStateMachineFunc) CreateFunc {
	return func(shardID, replicaID uint64) interface{} {
		return fn(shardID, replicaID)
	}
}
