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

// WrapCreateFunc adapts a dragonboat CreateStateMachineFunc to a quicraft sm.CreateFunc.
func WrapCreateFunc(f CreateStateMachineFunc) qcSM.CreateFunc {
	return func(shardID, replicaID uint64) interface{} {
		inner := f(shardID, replicaID)
		return &regularAdapter{inner: inner}
	}
}

// WrapConcurrentCreateFunc adapts a dragonboat CreateConcurrentStateMachineFunc to a quicraft sm.CreateFunc.
func WrapConcurrentCreateFunc(f CreateConcurrentStateMachineFunc) qcSM.CreateFunc {
	return func(shardID, replicaID uint64) interface{} {
		inner := f(shardID, replicaID)
		return &concurrentAdapter{inner: inner}
	}
}

// WrapOnDiskCreateFunc adapts a dragonboat CreateOnDiskStateMachineFunc to a quicraft sm.CreateFunc; Open's snapshot directory is discarded.
func WrapOnDiskCreateFunc(f CreateOnDiskStateMachineFunc) qcSM.CreateFunc {
	return func(shardID, replicaID uint64) interface{} {
		inner := f(shardID, replicaID)
		return &diskAdapter{inner: inner}
	}
}
