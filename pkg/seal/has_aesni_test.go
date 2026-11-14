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

import (
	"runtime"
	"testing"

	"golang.org/x/sys/cpu"
)

// TestHasAESNI_MatchesCPUFlagForArchitecture asserts the documented contract:
// on amd64 HasAESNI reports the AES-NI flag, on arm64 the cryptographic
// extension AES flag, and on every other architecture false.
func TestHasAESNI_MatchesCPUFlagForArchitecture(t *testing.T) {
	var want bool
	switch runtime.GOARCH {
	case "amd64":
		want = cpu.X86.HasAES
	case "arm64":
		want = cpu.ARM64.HasAES
	default:
		want = false
	}
	if got := HasAESNI(); got != want {
		t.Fatalf("HasAESNI() on %s = %v, want %v", runtime.GOARCH, got, want)
	}
}

// TestHasAESNI_Stable asserts that repeated calls return the same value: the
// CPU feature flags are detected once and never change during the process.
func TestHasAESNI_Stable(t *testing.T) {
	first := HasAESNI()
	for i := 0; i < 100; i++ {
		if got := HasAESNI(); got != first {
			t.Fatalf("HasAESNI() call %d = %v, first call = %v", i, got, first)
		}
	}
}

// TestHasAESNI_SelectOptimalAlgorithm asserts that SelectOptimalAlgorithm
// picks AES-256-GCM whenever HasAESNI is true or the operation is hardware
// backed, and the build's software fallback otherwise; in FIPS mode it picks
// AES-256-GCM unconditionally.
func TestHasAESNI_SelectOptimalAlgorithm(t *testing.T) {
	if FIPSEnabled() {
		for _, hw := range []bool{false, true} {
			if got := SelectOptimalAlgorithm(hw); got != AlgAES256GCM {
				t.Fatalf("FIPS mode: SelectOptimalAlgorithm(%v) = %s, want %s", hw, got, AlgAES256GCM)
			}
		}
		return
	}

	t.Run("software path follows HasAESNI", func(t *testing.T) {
		want := softwareFallbackAlgorithm()
		if HasAESNI() {
			want = AlgAES256GCM
		}
		if got := SelectOptimalAlgorithm(false); got != want {
			t.Fatalf("SelectOptimalAlgorithm(false) with HasAESNI()=%v = %s, want %s",
				HasAESNI(), got, want)
		}
	})

	t.Run("hardware backed is AES-256-GCM either way", func(t *testing.T) {
		if got := SelectOptimalAlgorithm(true); got != AlgAES256GCM {
			t.Fatalf("SelectOptimalAlgorithm(true) with HasAESNI()=%v = %s, want %s",
				HasAESNI(), got, AlgAES256GCM)
		}
	})
}
