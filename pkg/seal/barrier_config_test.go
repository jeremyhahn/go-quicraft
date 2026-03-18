// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package seal

import (
	"errors"
	"testing"
)

// TestDefaultBarrierConfig verifies that DefaultBarrierConfig returns sane defaults:
// a valid algorithm, the provided DeploymentID, nonce tracking enabled, and default
// invocation limit.
func TestDefaultBarrierConfig(t *testing.T) {
	cfg := DefaultBarrierConfig(12345)

	// Verify algorithm is valid and set
	if !cfg.Algorithm.Valid() {
		t.Fatalf("Algorithm is invalid: %v", cfg.Algorithm)
	}

	// Verify DeploymentID is passed through
	if cfg.DeploymentID != 12345 {
		t.Fatalf("expected DeploymentID=12345, got %d", cfg.DeploymentID)
	}

	// Verify nonce tracking is enabled
	if !cfg.NonceTracking {
		t.Fatal("NonceTracking should be enabled by default")
	}

	// Verify invocation limit defaults to zero (resolved to DefaultInvocationLimit
	// at counter creation time).
	if cfg.InvocationLimit != 0 {
		t.Fatalf("expected InvocationLimit=0 (default), got %d", cfg.InvocationLimit)
	}
}

// TestDefaultBarrierConfig_DeploymentID verifies that the provided DeploymentID
// is correctly passed through to the returned config.
func TestDefaultBarrierConfig_DeploymentID(t *testing.T) {
	tests := []struct {
		name         string
		deploymentID uint64
	}{
		{"zero", 0},
		{"small", 1},
		{"large", 18446744073709551615},  // max uint64
		{"typical", 9223372036854775807}, // large but typical
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultBarrierConfig(tt.deploymentID)
			if cfg.DeploymentID != tt.deploymentID {
				t.Fatalf("expected DeploymentID=%d, got %d", tt.deploymentID, cfg.DeploymentID)
			}
		})
	}
}

// TestBarrierConfig_Validate verifies that a valid config returns nil error.
func TestBarrierConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		algorithm SymmetricAlgorithm
	}{
		{"AES128GCM", AlgAES128GCM},
		{"AES192GCM", AlgAES192GCM},
		{"AES256GCM", AlgAES256GCM},
		{"ChaCha20Poly1305", AlgChaCha20Poly1305},
		{"XChaCha20Poly1305", AlgXChaCha20Poly1305},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := BarrierConfig{
				Algorithm:     tt.algorithm,
				DeploymentID:  1,
				NonceTracking: true,
			}
			err := cfg.validate()
			if err != nil {
				t.Fatalf("validate() failed for valid config: %v", err)
			}
		})
	}
}

// TestBarrierConfig_Validate_InvalidAlgorithm verifies that an invalid
// algorithm returns ErrUnsupportedAlgorithm.
func TestBarrierConfig_Validate_InvalidAlgorithm(t *testing.T) {
	cfg := BarrierConfig{
		Algorithm:     SymmetricAlgorithm(0xFF), // Invalid algorithm identifier
		DeploymentID:  1,
		NonceTracking: true,
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() should return error for invalid algorithm")
	}

	if !errors.Is(err, ErrUnsupportedAlgorithm) {
		t.Fatalf("expected ErrUnsupportedAlgorithm, got %v", err)
	}
}

// TestBarrierConfig_Algorithm_Selection verifies that DefaultBarrierConfig
// selects a valid algorithm (either AES256GCM or ChaCha20Poly1305) based on
// the CPU capabilities.
func TestBarrierConfig_Algorithm_Selection(t *testing.T) {
	cfg := DefaultBarrierConfig(1)

	// SelectOptimalAlgorithm chooses between AES256GCM and ChaCha20Poly1305
	// based on AES-NI availability
	validAlgorithms := []SymmetricAlgorithm{AlgAES256GCM, AlgChaCha20Poly1305}

	found := false
	for _, validAlg := range validAlgorithms {
		if cfg.Algorithm == validAlg {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("algorithm %v is neither AES256GCM nor ChaCha20Poly1305", cfg.Algorithm)
	}

	// Ensure the algorithm is valid
	if !cfg.Algorithm.Valid() {
		t.Fatalf("selected algorithm is invalid: %v", cfg.Algorithm)
	}
}
