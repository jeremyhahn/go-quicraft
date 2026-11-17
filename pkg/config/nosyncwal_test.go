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

package config

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// TestHostConfig_Validate_NoSyncWALInTestBinary asserts that a test binary
// (the default isTestBinary seam) accepts NoSyncWAL and logs the warning.
func TestHostConfig_Validate_NoSyncWALInTestBinary(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	if !isTestBinary() {
		t.Fatal("isTestBinary() = false inside a test binary")
	}
	hc := validHostConfig()
	hc.NoSyncWAL = true
	if err := hc.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil in a test binary", err)
	}
	if !strings.Contains(buf.String(), "NoSyncWAL") {
		t.Fatalf("expected a NoSyncWAL warning, got %q", buf.String())
	}
}

// TestHostConfig_Validate_NoSyncWALOutsideTestBinary asserts that a binary
// that is not a test binary refuses NoSyncWAL with a typed error naming the
// field. It swaps the package-level seam, so it must not run in parallel.
func TestHostConfig_Validate_NoSyncWALOutsideTestBinary(t *testing.T) {
	prev := isTestBinary
	isTestBinary = func() bool { return false }
	t.Cleanup(func() { isTestBinary = prev })

	t.Run("refused when set", func(t *testing.T) {
		hc := validHostConfig()
		hc.NoSyncWAL = true
		err := hc.Validate()
		if err == nil {
			t.Fatal("Validate() = nil, want ErrNoSyncWALOutsideTest")
		}
		if !errors.Is(err, ErrNoSyncWALOutsideTest) {
			t.Fatalf("Validate() = %v, want errors.Is ErrNoSyncWALOutsideTest", err)
		}
		var typed *NoSyncWALOutsideTestError
		if !errors.As(err, &typed) {
			t.Fatalf("Validate() = %T, want *NoSyncWALOutsideTestError", err)
		}
		if !strings.Contains(err.Error(), "NoSyncWAL") {
			t.Fatalf("error %q does not name NoSyncWAL", err.Error())
		}
	})

	t.Run("valid when unset", func(t *testing.T) {
		hc := validHostConfig()
		if err := hc.Validate(); err != nil {
			t.Fatalf("Validate() = %v, want nil", err)
		}
	})
}
