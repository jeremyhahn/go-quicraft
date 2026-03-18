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
	"os"
	"testing"
)

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
	original := os.Getenv(envGOFIPS140)
	t.Cleanup(func() {
		if original == "" {
			os.Unsetenv(envGOFIPS140)
		} else {
			os.Setenv(envGOFIPS140, original)
		}
	})

	os.Unsetenv(envGOFIPS140)
	if v := DefaultKDFVersion(); v != KDFArgon2id {
		t.Fatalf("expected KDFArgon2id (%d), got %d", KDFArgon2id, v)
	}
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
