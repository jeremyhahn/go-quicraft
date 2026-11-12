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

package quicraft

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/session"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// sessionErrorCases pairs each public session error with the error a real
// session manager returns for that condition. The session manager is what
// the state machine consults on every session-managed entry, so this is the
// error an embedder is handed; errors.Is against the public name must hold.
func sessionErrorCases(t *testing.T) []struct {
	name     string
	exported error
	internal error
	returned error
} {
	t.Helper()

	full := session.NewManager(1, 1, 0)
	if err := full.Register(1, 0, 100); err != nil {
		t.Fatalf("Register(1) = %v", err)
	}
	limitErr := full.Register(2, 0, 101)
	existsErr := full.Register(1, 0, 102)
	notFoundErr := full.CanRecord(99, 0, 0)
	if err := full.RecordResult(1, 1, 0, 103, sm.Result{Value: 1}); err != nil {
		t.Fatalf("RecordResult = %v", err)
	}
	responseErr := full.CanRecord(1, 0, 0)

	return []struct {
		name     string
		exported error
		internal error
		returned error
	}{
		{"ErrSessionLimitExceeded", ErrSessionLimitExceeded, session.ErrSessionLimitExceeded, limitErr},
		{"ErrTooManySessions", ErrTooManySessions, session.ErrSessionLimitExceeded, limitErr},
		{"ErrSessionAlreadyExists", ErrSessionAlreadyExists, session.ErrSessionAlreadyExists, existsErr},
		{"ErrSessionNotFound", ErrSessionNotFound, session.ErrSessionNotFound, notFoundErr},
		{"ErrResponseLimitExceeded", ErrResponseLimitExceeded, session.ErrResponseLimitExceeded, responseErr},
	}
}

// TestSessionErrorsAreTheSessionManagersValues checks that each public
// session error is the very value the session manager returns, so
// errors.Is holds both ways, through wrapping, with no translation layer.
func TestSessionErrorsAreTheSessionManagersValues(t *testing.T) {
	for _, c := range sessionErrorCases(t) {
		t.Run(c.name, func(t *testing.T) {
			if c.returned == nil {
				t.Fatalf("the session manager returned nil for the %s condition", c.name)
			}
			if !errors.Is(c.returned, c.internal) {
				t.Fatalf("session manager returned %v, want the internal %v", c.returned, c.internal)
			}
			if c.exported != c.internal {
				t.Fatalf("quicraft.%s is not the value the session manager returns", c.name)
			}
			if !errors.Is(c.returned, c.exported) {
				t.Fatalf("errors.Is(%v, quicraft.%s) = false", c.returned, c.name)
			}
			if !errors.Is(c.exported, c.internal) || !errors.Is(c.internal, c.exported) {
				t.Fatalf("errors.Is between quicraft.%s and the internal value is not symmetric", c.name)
			}
			wrapped := fmt.Errorf("apply: %w", c.returned)
			if !errors.Is(wrapped, c.exported) {
				t.Fatalf("errors.Is(%v, quicraft.%s) = false through wrapping", wrapped, c.name)
			}
			if errors.Is(errors.New(c.exported.Error()), c.exported) {
				t.Fatalf("a look-alike of %s with identical text matched; identity is not tested", c.name)
			}
		})
	}
}

// TestTooManySessionsStaysTheLimitAlias checks the legacy alias still names
// the same value as ErrSessionLimitExceeded.
func TestTooManySessionsStaysTheLimitAlias(t *testing.T) {
	if ErrTooManySessions != ErrSessionLimitExceeded {
		t.Fatal("ErrTooManySessions is no longer the same value as ErrSessionLimitExceeded")
	}
}
