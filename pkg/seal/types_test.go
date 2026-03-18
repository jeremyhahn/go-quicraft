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
	"testing"
	"time"
)

func TestDefaultConfig_ReturnsNonNil(t *testing.T) {
	cfg := DefaultConfig()
	if cfg == nil {
		t.Fatal("DefaultConfig() returned nil")
	}
}

func TestDefaultConfig_Mode(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Mode != ModeManual {
		t.Fatalf("Mode: got %q, want %q", cfg.Mode, ModeManual)
	}
}

func TestDefaultConfig_ShamirThreshold(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Threshold != 3 {
		t.Fatalf("Threshold: got %d, want %d", cfg.Threshold, 3)
	}
}

func TestDefaultConfig_ShamirTotalShares(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.TotalShares != 5 {
		t.Fatalf("TotalShares: got %d, want %d", cfg.TotalShares, 5)
	}
}

func TestDefaultConfig_ShamirConfiguration_IsValid(t *testing.T) {
	cfg := DefaultConfig()
	// Threshold must be >= 2 and <= TotalShares for valid M-of-N Shamir sharing.
	if cfg.Threshold < 2 {
		t.Fatalf("Threshold must be >= 2, got %d", cfg.Threshold)
	}
	if cfg.Threshold > cfg.TotalShares {
		t.Fatalf("Threshold (%d) must be <= TotalShares (%d)", cfg.Threshold, cfg.TotalShares)
	}
}

func TestDefaultConfig_RecoveryKeysEnabled(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.RecoveryKeysEnabled {
		t.Fatal("RecoveryKeysEnabled: got true, want false")
	}
}

func TestDefaultConfig_RecoveryThreshold(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.RecoveryThreshold != 3 {
		t.Fatalf("RecoveryThreshold: got %d, want %d", cfg.RecoveryThreshold, 3)
	}
}

func TestDefaultConfig_RecoveryTotalKeys(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.RecoveryTotalKeys != 5 {
		t.Fatalf("RecoveryTotalKeys: got %d, want %d", cfg.RecoveryTotalKeys, 5)
	}
}

func TestDefaultConfig_RecoveryConfiguration_IsValid(t *testing.T) {
	cfg := DefaultConfig()
	// Recovery threshold must follow same rules as Shamir threshold.
	if cfg.RecoveryThreshold < 2 {
		t.Fatalf("RecoveryThreshold must be >= 2, got %d", cfg.RecoveryThreshold)
	}
	if cfg.RecoveryThreshold > cfg.RecoveryTotalKeys {
		t.Fatalf("RecoveryThreshold (%d) must be <= RecoveryTotalKeys (%d)",
			cfg.RecoveryThreshold, cfg.RecoveryTotalKeys)
	}
}

func TestDefaultConfig_ShareExchangeTimeout(t *testing.T) {
	cfg := DefaultConfig()
	expected := 30 * time.Second
	if cfg.ShareExchangeTimeout != expected {
		t.Fatalf("ShareExchangeTimeout: got %v, want %v", cfg.ShareExchangeTimeout, expected)
	}
}

func TestDefaultConfig_ShareExchangeTimeout_IsPositive(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ShareExchangeTimeout <= 0 {
		t.Fatalf("ShareExchangeTimeout must be positive, got %v", cfg.ShareExchangeTimeout)
	}
}

func TestDefaultConfig_DeploymentID(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.DeploymentID != 0 {
		t.Fatalf("DeploymentID: got %d, want %d", cfg.DeploymentID, 0)
	}
}

func TestDefaultConfig_AllFieldsSet(t *testing.T) {
	cfg := DefaultConfig()

	// Verify all fields have expected default values.
	// This serves as a regression test if new fields are added.
	tests := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"Mode", cfg.Mode, ModeManual},
		{"Threshold", cfg.Threshold, 3},
		{"TotalShares", cfg.TotalShares, 5},
		{"RecoveryKeysEnabled", cfg.RecoveryKeysEnabled, false},
		{"RecoveryThreshold", cfg.RecoveryThreshold, 3},
		{"RecoveryTotalKeys", cfg.RecoveryTotalKeys, 5},
		{"ShareExchangeTimeout", cfg.ShareExchangeTimeout, 30 * time.Second},
		{"DeploymentID", cfg.DeploymentID, uint64(0)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("%s: got %v, want %v", tc.name, tc.got, tc.want)
			}
		})
	}
}

func TestDefaultConfig_ReturnsNewInstance(t *testing.T) {
	cfg1 := DefaultConfig()
	cfg2 := DefaultConfig()

	if cfg1 == cfg2 {
		t.Fatal("DefaultConfig() should return a new instance each time")
	}
}

func TestDefaultConfig_MutationsDoNotAffectOtherInstances(t *testing.T) {
	cfg1 := DefaultConfig()
	cfg2 := DefaultConfig()

	// Mutate cfg1.
	cfg1.Mode = ModeAuto
	cfg1.Threshold = 10
	cfg1.TotalShares = 15

	// cfg2 should be unaffected.
	if cfg2.Mode != ModeManual {
		t.Fatalf("cfg2.Mode was affected by cfg1 mutation: got %q, want %q", cfg2.Mode, ModeManual)
	}
	if cfg2.Threshold != 3 {
		t.Fatalf("cfg2.Threshold was affected by cfg1 mutation: got %d, want %d", cfg2.Threshold, 3)
	}
	if cfg2.TotalShares != 5 {
		t.Fatalf("cfg2.TotalShares was affected by cfg1 mutation: got %d, want %d", cfg2.TotalShares, 5)
	}
}

// --- Mode Tests ---

func TestMode_ManualConstant(t *testing.T) {
	if ModeManual != "manual" {
		t.Fatalf("ModeManual: got %q, want %q", ModeManual, "manual")
	}
}

func TestMode_AutoConstant(t *testing.T) {
	if ModeAuto != "auto" {
		t.Fatalf("ModeAuto: got %q, want %q", ModeAuto, "auto")
	}
}

func TestMode_AreDistinct(t *testing.T) {
	if ModeManual == ModeAuto {
		t.Fatal("ModeManual and ModeAuto should be distinct")
	}
}

// --- ShareExchangeType Tests ---

func TestShareExchangeType_Constants(t *testing.T) {
	tests := []struct {
		name     string
		value    ShareExchangeType
		expected int
	}{
		{"ShareExchangeRequest", ShareExchangeRequest, 0},
		{"ShareExchangeResponse", ShareExchangeResponse, 1},
		{"ShareExchangeAck", ShareExchangeAck, 2},
		{"ShareExchangeReady", ShareExchangeReady, 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if int(tc.value) != tc.expected {
				t.Fatalf("%s: got %d, want %d", tc.name, tc.value, tc.expected)
			}
		})
	}
}

func TestShareExchangeType_AreDistinct(t *testing.T) {
	types := []ShareExchangeType{
		ShareExchangeRequest,
		ShareExchangeResponse,
		ShareExchangeAck,
		ShareExchangeReady,
	}

	for i := range types {
		for j := range types {
			if i != j && types[i] == types[j] {
				t.Fatalf("ShareExchangeType %d and %d should be distinct", i, j)
			}
		}
	}
}
