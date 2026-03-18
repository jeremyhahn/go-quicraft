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

package kv

import "github.com/jeremyhahn/go-quicraft/pkg/sm"

// NewMemoryCreateFunc returns a CreateFunc that creates MemoryStore instances.
func NewMemoryCreateFunc() sm.CreateFunc {
	return sm.NewCreateFunc(func(_, _ uint64) sm.StateMachine {
		return NewMemoryStore()
	})
}

// NewConcurrentCreateFunc returns a CreateFunc that creates ConcurrentStore instances.
func NewConcurrentCreateFunc() sm.CreateFunc {
	return sm.NewConcurrentCreateFunc(func(_, _ uint64) sm.ConcurrentStateMachine {
		return NewConcurrentStore()
	})
}
