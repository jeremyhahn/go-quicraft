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

package rsm

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// TestStateMachine_ClosedGuard verifies the defence-in-depth guard on the
// basic tier: Apply, SaveSnapshot and RecoverFromSnapshot on a closed state
// machine return ErrClosed without running user code.
func TestStateMachine_ClosedGuard(t *testing.T) {
	cases := []struct {
		name string
		call func(*StateMachine) error
	}{
		{"Apply", func(m *StateMachine) error {
			entries := []proto.Entry{{Index: 1, Term: 1, Cmd: []byte{1, 'k', 'v'}}}
			return m.Apply(entries, make([]sm.Result, 1))
		}},
		{"SaveSnapshot", func(m *StateMachine) error {
			var buf bytes.Buffer
			return m.SaveSnapshot(context.Background(), &buf, make(chan struct{}))
		}},
		{"RecoverFromSnapshot", func(m *StateMachine) error {
			return m.RecoverFromSnapshot(context.Background(), bytes.NewReader(nil), make(chan struct{}))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			createFn := func(_, _ uint64) interface{} { return newTestKV() }
			machine, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
			if err != nil {
				t.Fatalf("NewStateMachine failed: %v", err)
			}
			if err := machine.Close(); err != nil {
				t.Fatalf("Close failed: %v", err)
			}
			if err := c.call(machine); !errors.Is(err, ErrClosed) {
				t.Fatalf("%s on closed SM returned %v, want ErrClosed", c.name, err)
			}
		})
	}
}
