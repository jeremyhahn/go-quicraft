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
	"fmt"
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
	if !cfg.GetNonceTracking() {
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
	for _, tt := range knownAlgorithms {
		t.Run(tt.name, func(t *testing.T) {
			cfg := BarrierConfig{
				Algorithm:     tt.alg,
				DeploymentID:  1,
				NonceTracking: BoolPtr(true),
			}
			err := cfg.Validate()
			if err != nil {
				t.Fatalf("validate() failed for valid config: %v", err)
			}
		})
	}

	// An algorithm this build does not register is refused at configuration
	// time as an unsupported identifier, not as a policy violation — in a FIPS
	// build the ChaCha20 family is not "disallowed", it is absent. The runtime
	// FIPS rejection (ErrAlgorithmNotFIPSApproved) is a separate check that
	// still fires in a non-FIPS build with GOFIPS140 set; see
	// TestFIPSConstrainsDataPathCipher.
	for _, alg := range unregisteredAlgorithms {
		t.Run(fmt.Sprintf("unregistered_%#x", byte(alg)), func(t *testing.T) {
			cfg := BarrierConfig{
				Algorithm:     alg,
				DeploymentID:  1,
				NonceTracking: BoolPtr(true),
			}
			if err := cfg.Validate(); !errors.Is(err, ErrUnsupportedAlgorithm) {
				t.Fatalf("Validate() = %v, want ErrUnsupportedAlgorithm", err)
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
		NonceTracking: BoolPtr(true),
	}

	err := cfg.Validate()
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

	// SelectOptimalAlgorithm chooses between AES256GCM and this build's
	// software fallback based on AES-NI availability. The fallback is
	// ChaCha20-Poly1305 normally and AES-256-GCM in a -tags fips build, which
	// has no non-approved cipher to fall back to.
	validAlgorithms := []SymmetricAlgorithm{AlgAES256GCM, softwareFallbackAlgorithm()}

	found := false
	for _, validAlg := range validAlgorithms {
		if cfg.Algorithm == validAlg {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("algorithm %v is neither AES256GCM nor this build's software fallback", cfg.Algorithm)
	}

	// Ensure the algorithm is valid
	if !cfg.Algorithm.Valid() {
		t.Fatalf("selected algorithm is invalid: %v", cfg.Algorithm)
	}
}

// TestBarrierConfigValidate_ExportedForRuntimeConfigs asserts the validation
// is reachable as an error, so an embedder assembling a config at runtime can
// reject it at admission time.
func TestBarrierConfigValidate_ExportedForRuntimeConfigs(t *testing.T) {
	valid := DefaultBarrierConfig(1)
	if err := valid.Validate(); err != nil {
		t.Fatalf("default config rejected: %v", err)
	}

	// An unknown algorithm is exactly what NewBarrier rejects.
	bad := DefaultBarrierConfig(1)
	bad.Algorithm = SymmetricAlgorithm(0xFE)
	if err := bad.Validate(); !errors.Is(err, ErrUnsupportedAlgorithm) {
		t.Fatalf("Validate() = %v, want ErrUnsupportedAlgorithm", err)
	}

	// NewBarrier applies the same rule and returns the same error identity, so
	// Validate is an admission-time convenience rather than a safety
	// requirement the caller can forget.
	b, err := NewBarrier(bad)
	if !errors.Is(err, ErrUnsupportedAlgorithm) {
		t.Fatalf("NewBarrier(bad) err = %v, want ErrUnsupportedAlgorithm", err)
	}
	if b != nil {
		t.Fatal("NewBarrier(bad) must return a nil Barrier")
	}
}
