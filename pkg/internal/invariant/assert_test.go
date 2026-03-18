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

package invariant

import (
	"strings"
	"testing"
)

func TestAssert_TrueCondition_NoPanic(t *testing.T) {
	// Assert with a true condition must not panic.
	Assert(true, "this should not panic")
}

func TestAssert_FalseCondition_Panics(t *testing.T) {
	const msg = "expected positive value"
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic but did not get one")
		}
		s, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if !strings.Contains(s, msg) {
			t.Fatalf("panic message %q does not contain %q", s, msg)
		}
		if !strings.HasPrefix(s, "invariant violated: ") {
			t.Fatalf("panic message %q missing 'invariant violated:' prefix", s)
		}
	}()
	Assert(false, msg)
}

func TestAssert_EmptyMessage_StillPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic but did not get one")
		}
		s, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if s != "invariant violated: " {
			t.Fatalf("unexpected panic message: %q", s)
		}
	}()
	Assert(false, "")
}
