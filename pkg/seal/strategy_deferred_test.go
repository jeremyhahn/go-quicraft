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

package seal

import "testing"

func TestDeferredSoftwareStrategy_Properties(t *testing.T) {
	s := NewDeferredSoftwareStrategy()
	defer s.Close()

	if s.ID() != StrategyPassphrase {
		t.Fatalf("expected ID=%q, got %q", StrategyPassphrase, s.ID())
	}
	if !s.Available() {
		t.Fatal("expected Available() to return true")
	}
	if s.HardwareBacked() {
		t.Fatal("expected HardwareBacked() to return false")
	}
	if s.passphrase.Load() != nil {
		t.Fatal("expected passphrase to be nil for deferred strategy")
	}
	if !s.deferPassphrase {
		t.Fatal("expected deferPassphrase to be true")
	}
}
