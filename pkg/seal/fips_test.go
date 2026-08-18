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
	"crypto/fips140"
	"os"
	"testing"
)

// skipIfModuleActive skips a test whose premise is that this binary has NO
// active FIPS 140 module, so that clearing GOFIPS140 is enough to make
// FIPSEnabled report false. Under GOFIPS140=latest the module is active and
// fips140.Enabled() is true independently of the environment — the condition
// cannot be constructed, which is the correct outcome and not a failure.
func skipIfModuleActive(t *testing.T) {
	t.Helper()
	if fips140.Enabled() {
		t.Skip("the Go FIPS 140 module is active in this binary; the " +
			"environment-only detection path cannot be exercised here")
	}
}

func TestFIPSEnabled_WithEnvSet(t *testing.T) {
	original := os.Getenv(envGOFIPS140)
	t.Cleanup(func() {
		if original == "" {
			os.Unsetenv(envGOFIPS140)
		} else {
			os.Setenv(envGOFIPS140, original)
		}
	})

	os.Setenv(envGOFIPS140, "1")
	if !FIPSEnabled() {
		t.Fatal("expected FIPSEnabled() to return true when GOFIPS140 is set")
	}
}

func TestFIPSEnabled_WithEnvUnset(t *testing.T) {
	skipIfModuleActive(t)

	original := os.Getenv(envGOFIPS140)
	t.Cleanup(func() {
		if original == "" {
			os.Unsetenv(envGOFIPS140)
		} else {
			os.Setenv(envGOFIPS140, original)
		}
	})

	os.Unsetenv(envGOFIPS140)
	if FIPSEnabled() {
		t.Fatal("expected FIPSEnabled() to return false when GOFIPS140 is unset")
	}
}

func TestFIPSEnabled_WithEmptyEnv(t *testing.T) {
	skipIfModuleActive(t)

	original := os.Getenv(envGOFIPS140)
	t.Cleanup(func() {
		if original == "" {
			os.Unsetenv(envGOFIPS140)
		} else {
			os.Setenv(envGOFIPS140, original)
		}
	})

	os.Setenv(envGOFIPS140, "")
	if FIPSEnabled() {
		t.Fatal("expected FIPSEnabled() to return false when GOFIPS140 is empty string")
	}
}

func TestDefaultKDFVersion_StandardMode(t *testing.T) {
	skipIfModuleActive(t)

	original := os.Getenv(envGOFIPS140)
	t.Cleanup(func() {
		if original == "" {
			os.Unsetenv(envGOFIPS140)
		} else {
			os.Setenv(envGOFIPS140, original)
		}
	})

	os.Unsetenv(envGOFIPS140)
	assertDefaultKDFVersionOutsideFIPS(t)
}

func TestDefaultKDFVersion_FIPSMode(t *testing.T) {
	original := os.Getenv(envGOFIPS140)
	t.Cleanup(func() {
		if original == "" {
			os.Unsetenv(envGOFIPS140)
		} else {
			os.Setenv(envGOFIPS140, original)
		}
	})

	os.Setenv(envGOFIPS140, "1")
	if v := DefaultKDFVersion(); v != KDFPbkdf2SHA256 {
		t.Fatalf("expected KDFPbkdf2SHA256 (%d), got %d", KDFPbkdf2SHA256, v)
	}
}

func TestFIPSEnabled_NonEmptyArbitraryValue(t *testing.T) {
	original := os.Getenv(envGOFIPS140)
	t.Cleanup(func() {
		if original == "" {
			os.Unsetenv(envGOFIPS140)
		} else {
			os.Setenv(envGOFIPS140, original)
		}
	})

	os.Setenv(envGOFIPS140, "true")
	if !FIPSEnabled() {
		t.Fatal("expected FIPSEnabled() to return true for any non-empty GOFIPS140 value")
	}
}

// TestFIPSEnabledOffValue verifies GOFIPS140=off is honored as DISABLED
// (the old detection treated any non-empty value, including "off", as
// FIPS mode).
func TestFIPSEnabledOffValue(t *testing.T) {
	skipIfModuleActive(t)

	t.Setenv("GOFIPS140", "off")
	if FIPSEnabled() {
		t.Fatal("GOFIPS140=off must not enable FIPS mode")
	}
}

// TestFIPSConstrainsDataPathCipher verifies that FIPS mode forces
// AES-256-GCM from SelectOptimalAlgorithm regardless of AES-NI, and that
// BarrierConfig.validate refuses the non-approved ChaCha20 family. The REASON
// it is refused differs by build — policy in a normal build, absence in a
// -tags fips one — which is what assertChaChaRefusedUnderFIPS captures.
func TestFIPSConstrainsDataPathCipher(t *testing.T) {
	t.Setenv("GOFIPS140", "latest")

	if alg := SelectOptimalAlgorithm(false); alg != AlgAES256GCM {
		t.Fatalf("SelectOptimalAlgorithm under FIPS = %v, want AES-256-GCM", alg)
	}
	if alg := SelectOptimalAlgorithm(true); alg != AlgAES256GCM {
		t.Fatalf("SelectOptimalAlgorithm(hardware) under FIPS = %v, want AES-256-GCM", alg)
	}

	assertChaChaRefusedUnderFIPS(t)

	// AES variants remain valid.
	for _, alg := range []SymmetricAlgorithm{AlgAES128GCM, AlgAES192GCM, AlgAES256GCM} {
		cfg := BarrierConfig{Algorithm: alg, DeploymentID: 1}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("validate(%v) under FIPS = %v, want nil", alg, err)
		}
	}
}

// TestChaChaOutsideFIPS pins what this build does with ChaCha20 when FIPS mode
// is NOT requested: selectable in a normal build, still absent in a -tags fips
// one. See assertChaChaOutsideFIPS.
func TestChaChaOutsideFIPS(t *testing.T) {
	t.Setenv("GOFIPS140", "")
	assertChaChaOutsideFIPS(t)
}
