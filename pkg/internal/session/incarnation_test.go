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

package session

import (
	"bytes"
	"errors"
	"testing"
)

// TestCheckDuplicate_RejectsEntryFromAPreviousIncarnation: an entry below the index that created a re-registered session's current incarnation is rejected, not applied again.
func TestCheckDuplicate_RejectsEntryFromAPreviousIncarnation(t *testing.T) {
	m := NewManager(10, 10, 0)

	// The client's first incarnation, registered at index 10, proposes at 20.
	if err := m.Register(1, 0, 10); err != nil {
		t.Fatalf("Register (first incarnation): %v", err)
	}
	// It expires and is re-registered at a much later index.
	if err := m.Unregister(1); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if err := m.Register(1, 0, 5000); err != nil {
		t.Fatalf("Register (second incarnation): %v", err)
	}

	// The stale entry from the first incarnation commits.
	_, isDup, err := m.CheckDuplicate(1, 7, 20)
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("CheckDuplicate for an entry from a previous incarnation "+
			"returned (dup=%v, err=%v); want ErrSessionNotFound. Without the "+
			"incarnation floor the entry looks brand new and the command is "+
			"applied a second time on every replica", isDup, err)
	}

	// Control: an entry from the current incarnation is unaffected.
	if _, _, err := m.CheckDuplicate(1, 7, 5001); err != nil {
		t.Fatalf("CheckDuplicate for the current incarnation returned %v; "+
			"the floor must not reject live traffic", err)
	}
}

// TestRegisteredIndex_SurvivesASnapshot pins that the floor is part of the
// replicated state. If it did not survive marshalling, it would vanish on
// exactly the restart-then-stale-commit path it exists to close.
func TestRegisteredIndex_SurvivesASnapshot(t *testing.T) {
	m := NewManager(10, 10, 0)
	if err := m.Register(1, 0, 5000); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var buf bytes.Buffer
	if err := m.MarshalTo(&buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	restored := NewManager(10, 10, 0)
	if err := restored.UnmarshalFrom(&buf); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}

	if _, _, err := restored.CheckDuplicate(1, 7, 20); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("after a snapshot round-trip the incarnation floor was lost: "+
			"CheckDuplicate(index=20) returned %v, want ErrSessionNotFound", err)
	}
	if _, _, err := restored.CheckDuplicate(1, 7, 5001); err != nil {
		t.Fatalf("the restored floor rejects live traffic: %v", err)
	}
}
