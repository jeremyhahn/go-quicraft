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

package statemachine

import qcSM "github.com/jeremyhahn/go-quicraft/pkg/sm"

// WrapCreateFunc wraps a dragonboat CreateStateMachineFunc as a quicraft
// sm.CreateFunc. The returned factory creates regularAdapter instances
// that translate between dragonboat's single-entry Update convention and
// quicraft's batch Update interface.
func WrapCreateFunc(f CreateStateMachineFunc) qcSM.CreateFunc {
	return func(shardID, replicaID uint64) interface{} {
		inner := f(shardID, replicaID)
		return &regularAdapter{inner: inner}
	}
}

// WrapConcurrentCreateFunc wraps a dragonboat CreateConcurrentStateMachineFunc
// as a quicraft sm.CreateFunc. The returned factory creates concurrentAdapter
// instances that translate between the two batch-entry representations.
func WrapConcurrentCreateFunc(f CreateConcurrentStateMachineFunc) qcSM.CreateFunc {
	return func(shardID, replicaID uint64) interface{} {
		inner := f(shardID, replicaID)
		return &concurrentAdapter{inner: inner}
	}
}

// WrapOnDiskCreateFunc wraps a dragonboat CreateOnDiskStateMachineFunc
// as a quicraft sm.CreateFunc. The returned factory creates diskAdapter
// instances that translate between the on-disk state machine representations.
func WrapOnDiskCreateFunc(f CreateOnDiskStateMachineFunc) qcSM.CreateFunc {
	return func(shardID, replicaID uint64) interface{} {
		inner := f(shardID, replicaID)
		return &diskAdapter{inner: inner}
	}
}
