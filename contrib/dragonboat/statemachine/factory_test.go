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

import (
	"testing"

	qcSM "github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// TestWrapCreateFuncs_NilCreateResultReturnsNil verifies that each wrapper
// returns an untyped nil when the user's create function returns nil, so the
// host refuses it with ErrInvalidSMType instead of starting a replica whose
// adapter panics on first use.
func TestWrapCreateFuncs_NilCreateResultReturnsNil(t *testing.T) {
	tests := []struct {
		name    string
		factory qcSM.CreateFunc
	}{
		{
			name: "regular",
			factory: WrapCreateFunc(func(uint64, uint64) IStateMachine {
				return nil
			}),
		},
		{
			name: "concurrent",
			factory: WrapConcurrentCreateFunc(func(uint64, uint64) IConcurrentStateMachine {
				return nil
			}),
		},
		{
			name: "on-disk",
			factory: WrapOnDiskCreateFunc(func(uint64, uint64) IOnDiskStateMachine {
				return nil
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.factory(1, 1); got != nil {
				t.Fatalf("factory(1, 1) = %T, want untyped nil", got)
			}
		})
	}
}

// TestWrapCreateFuncs_NonNilCreateResultWrapped verifies that a non-nil
// create result is still wrapped in the matching quicraft interface.
func TestWrapCreateFuncs_NonNilCreateResultWrapped(t *testing.T) {
	t.Run("regular", func(t *testing.T) {
		got := WrapCreateFunc(func(uint64, uint64) IStateMachine {
			return &counterSM{}
		})(1, 1)
		if _, ok := got.(qcSM.StateMachine); !ok {
			t.Fatalf("factory(1, 1) = %T, want qcSM.StateMachine", got)
		}
	})
	t.Run("concurrent", func(t *testing.T) {
		got := WrapConcurrentCreateFunc(func(uint64, uint64) IConcurrentStateMachine {
			return &concurrentCounterSM{}
		})(1, 1)
		if _, ok := got.(qcSM.ConcurrentStateMachine); !ok {
			t.Fatalf("factory(1, 1) = %T, want qcSM.ConcurrentStateMachine", got)
		}
	})
	t.Run("on-disk", func(t *testing.T) {
		got := WrapOnDiskCreateFunc(func(uint64, uint64) IOnDiskStateMachine {
			return &diskCounterSM{}
		})(1, 1)
		if _, ok := got.(qcSM.DiskStateMachine); !ok {
			t.Fatalf("factory(1, 1) = %T, want qcSM.DiskStateMachine", got)
		}
	})
}
