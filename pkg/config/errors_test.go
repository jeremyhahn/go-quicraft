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
	"errors"
	"strings"
	"testing"
	"time"
)

func TestValidationError_Error(t *testing.T) {
	t.Run("formats field and message", func(t *testing.T) {
		err := &ValidationError{
			Field:   "ElectionRTT",
			Message: "must be >= 5",
		}
		want := "config: invalid ElectionRTT: must be >= 5"
		if got := err.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})
}

func TestLogValidationError_Error(t *testing.T) {
	t.Run("formats field message and value", func(t *testing.T) {
		err := &LogValidationError{
			Field:   "Level",
			Message: "must be one of debug, info, warn, error",
			Value:   "invalid",
		}
		want := `config: invalid LogConfig.Level: must be one of debug, info, warn, error (got "invalid")`
		if got := err.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})
}

func TestMaxProposalPayloadSizeExceededError_Error(t *testing.T) {
	t.Run("formats configured and protocol maximum", func(t *testing.T) {
		err := &MaxProposalPayloadSizeExceededError{
			Configured:      16 * 1024 * 1024,
			ProtocolMaximum: 8 * 1024 * 1024,
		}
		msg := err.Error()
		if !strings.Contains(msg, "MaxProposalPayloadSize") {
			t.Fatalf("error message missing field name: %s", msg)
		}
		if !strings.Contains(msg, "16777216") {
			t.Fatalf("error message missing configured value: %s", msg)
		}
		if !strings.Contains(msg, "8388608") {
			t.Fatalf("error message missing protocol maximum: %s", msg)
		}
	})
}

func TestKeyRotationIntervalTooSmallError_Error(t *testing.T) {
	t.Run("formats configured and minimum", func(t *testing.T) {
		err := &KeyRotationIntervalTooSmallError{
			Configured: 500 * time.Millisecond,
			Minimum:    time.Second,
		}
		msg := err.Error()
		if !strings.Contains(msg, "KeyRotationInterval") {
			t.Fatalf("error message missing field name: %s", msg)
		}
		if !strings.Contains(msg, "500ms") {
			t.Fatalf("error message missing configured value: %s", msg)
		}
		if !strings.Contains(msg, "1s") {
			t.Fatalf("error message missing minimum value: %s", msg)
		}
	})
}

func TestSessionExpiryEntriesTooSmallError_Error(t *testing.T) {
	t.Run("formats configured and minimum", func(t *testing.T) {
		err := &SessionExpiryEntriesTooSmallError{
			Configured: 50,
			Minimum:    100,
		}
		msg := err.Error()
		if !strings.Contains(msg, "SessionExpiryEntries") {
			t.Fatalf("error message missing field name: %s", msg)
		}
		if !strings.Contains(msg, "50") {
			t.Fatalf("error message missing configured value: %s", msg)
		}
		if !strings.Contains(msg, "100") {
			t.Fatalf("error message missing minimum value: %s", msg)
		}
	})
}

func TestMaxEntrySizeExceededError_Error(t *testing.T) {
	err := &MaxEntrySizeExceededError{
		Configured:      16 * 1024 * 1024,
		ProtocolMaximum: 8 * 1024 * 1024,
	}
	msg := err.Error()
	for _, want := range []string{"MaxEntrySize", "16777216", "8388608"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("Error() = %q, missing %q", msg, want)
		}
	}

	var typed *MaxEntrySizeExceededError
	if !errors.As(error(err), &typed) {
		t.Fatal("errors.As should match *MaxEntrySizeExceededError")
	}
}

func TestMaxApplyEntrySizeExceededError_Error(t *testing.T) {
	err := &MaxApplyEntrySizeExceededError{
		Configured:   MaxReplicationPayloadBytes + 1,
		FrameMaximum: MaxReplicationPayloadBytes,
	}
	msg := err.Error()
	for _, want := range []string{"MaxApplyEntrySize", "16711673", "16711672", "frame"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("Error() = %q, missing %q", msg, want)
		}
	}

	var typed *MaxApplyEntrySizeExceededError
	if !errors.As(error(err), &typed) {
		t.Fatal("errors.As should match *MaxApplyEntrySizeExceededError")
	}
	if typed.FrameMaximum != MaxReplicationPayloadBytes {
		t.Errorf("FrameMaximum = %d, want %d", typed.FrameMaximum, MaxReplicationPayloadBytes)
	}
}

// TestMTLSConfigRequiredError verifies both the message and the sentinel
// matching that callers use to distinguish "mTLS material missing" from any
// other validation failure. mTLS is mandatory; there is no insecure fallback.
func TestMTLSConfigRequiredError(t *testing.T) {
	err := &MTLSConfigRequiredError{}
	if !strings.Contains(err.Error(), "MTLSConfig is required") {
		t.Fatalf("Error() = %q, missing the requirement statement", err.Error())
	}
	if !errors.Is(err, ErrMTLSConfigRequired) {
		t.Fatal("errors.Is should match ErrMTLSConfigRequired")
	}
	if errors.Is(err, ErrLeaseReadRequiresCheckQuorum) {
		t.Fatal("errors.Is must not match an unrelated sentinel")
	}

	// The sentinel must match the error Validate actually returns.
	var tc TransportConfig
	tc.SetDefaults()
	tc.MTLSConfig = nil
	if verr := tc.Validate(); !errors.Is(verr, ErrMTLSConfigRequired) {
		t.Fatalf("Validate() = %v, want ErrMTLSConfigRequired", verr)
	}
}

// TestLeaseReadRequiresCheckQuorumError verifies the message and sentinel for
// the lease-read safety precondition (Raft PhD thesis 6.4): without CheckQuorum
// a partitioned leader serves stale reads indefinitely.
func TestLeaseReadRequiresCheckQuorumError(t *testing.T) {
	err := &LeaseReadRequiresCheckQuorumError{}
	for _, want := range []string{"LeaseRead requires CheckQuorum", "stale reads"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Error() = %q, missing %q", err.Error(), want)
		}
	}
	if !errors.Is(err, ErrLeaseReadRequiresCheckQuorum) {
		t.Fatal("errors.Is should match ErrLeaseReadRequiresCheckQuorum")
	}
	if errors.Is(err, ErrMTLSConfigRequired) {
		t.Fatal("errors.Is must not match an unrelated sentinel")
	}
}
