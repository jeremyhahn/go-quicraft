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

func newClosedGuardSM(t *testing.T) *StateMachine {
	t.Helper()
	createFn := func(_, _ uint64) interface{} { return newTestKV() }
	machine, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine failed: %v", err)
	}
	return machine
}

// TestStateMachine_Apply_Closed verifies the defence-in-depth guard:
// Apply on a closed state machine must return ErrClosed without running
// user code.
func TestStateMachine_Apply_Closed(t *testing.T) {
	machine := newClosedGuardSM(t)
	if err := machine.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	entries := []proto.Entry{{Index: 1, Term: 1, Cmd: []byte{1, 'k', 'v'}}}
	results := make([]sm.Result, 1)
	if err := machine.Apply(entries, results); !errors.Is(err, ErrClosed) {
		t.Fatalf("Apply on closed SM returned %v, want ErrClosed", err)
	}
}

// TestStateMachine_SaveSnapshot_Closed verifies SaveSnapshot on a closed
// state machine returns ErrClosed.
func TestStateMachine_SaveSnapshot_Closed(t *testing.T) {
	machine := newClosedGuardSM(t)
	if err := machine.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	var buf bytes.Buffer
	stopper := make(chan struct{})
	if err := machine.SaveSnapshot(context.Background(), &buf, stopper); !errors.Is(err, ErrClosed) {
		t.Fatalf("SaveSnapshot on closed SM returned %v, want ErrClosed", err)
	}
}

// TestStateMachine_RecoverFromSnapshot_Closed verifies RecoverFromSnapshot
// on a closed state machine returns ErrClosed.
func TestStateMachine_RecoverFromSnapshot_Closed(t *testing.T) {
	machine := newClosedGuardSM(t)
	if err := machine.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	stopper := make(chan struct{})
	if err := machine.RecoverFromSnapshot(context.Background(), bytes.NewReader(nil), stopper); !errors.Is(err, ErrClosed) {
		t.Fatalf("RecoverFromSnapshot on closed SM returned %v, want ErrClosed", err)
	}
}
