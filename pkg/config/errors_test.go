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

	t.Run("empty field and message", func(t *testing.T) {
		err := &ValidationError{}
		want := "config: invalid : "
		if got := err.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})
}

func TestValidationError_Unwrap(t *testing.T) {
	t.Run("errors.As matches typed error", func(t *testing.T) {
		err := newValidationError("ShardID", "must be > 0")
		var ve *ValidationError
		if !errors.As(err, &ve) {
			t.Fatal("errors.As should match *ValidationError")
		}
		if ve.Field != "ShardID" {
			t.Errorf("Field = %q, want %q", ve.Field, "ShardID")
		}
		if ve.Message != "must be > 0" {
			t.Errorf("Message = %q, want %q", ve.Message, "must be > 0")
		}
	})

	t.Run("non-matching type fails errors.As", func(t *testing.T) {
		err := errors.New("some other error")
		var ve *ValidationError
		if errors.As(err, &ve) {
			t.Fatal("errors.As should not match for non-validation error")
		}
	})
}

func TestNewValidationError(t *testing.T) {
	t.Run("creates error with correct fields", func(t *testing.T) {
		err := newValidationError("WALDir", "must not be empty")
		if err.Field != "WALDir" {
			t.Errorf("Field = %q, want %q", err.Field, "WALDir")
		}
		if err.Message != "must not be empty" {
			t.Errorf("Message = %q, want %q", err.Message, "must not be empty")
		}
	})

	t.Run("error satisfies error interface", func(t *testing.T) {
		var err error = newValidationError("test", "msg")
		if err == nil {
			t.Fatal("error should not be nil")
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

	t.Run("handles empty value", func(t *testing.T) {
		err := &LogValidationError{
			Field:   "Output",
			Message: "must not be empty",
			Value:   "",
		}
		want := `config: invalid LogConfig.Output: must not be empty (got "")`
		if got := err.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("handles special characters in value", func(t *testing.T) {
		err := &LogValidationError{
			Field:   "Format",
			Message: "unsupported format",
			Value:   "json\nwith\tnewlines",
		}
		want := `config: invalid LogConfig.Format: unsupported format (got "json\nwith\tnewlines")`
		if got := err.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("errors.As matches typed error", func(t *testing.T) {
		var err error = &LogValidationError{
			Field:   "Level",
			Message: "invalid",
			Value:   "bad",
		}
		var lve *LogValidationError
		if !errors.As(err, &lve) {
			t.Fatal("errors.As should match *LogValidationError")
		}
		if lve.Field != "Level" {
			t.Errorf("Field = %q, want %q", lve.Field, "Level")
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

	t.Run("errors.As matches typed error", func(t *testing.T) {
		var err error = &MaxProposalPayloadSizeExceededError{
			Configured:      16 * 1024 * 1024,
			ProtocolMaximum: 8 * 1024 * 1024,
		}
		var exceeded *MaxProposalPayloadSizeExceededError
		if !errors.As(err, &exceeded) {
			t.Fatal("errors.As should match *MaxProposalPayloadSizeExceededError")
		}
		if exceeded.Configured != 16*1024*1024 {
			t.Errorf("Configured = %d, want %d", exceeded.Configured, 16*1024*1024)
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

	t.Run("errors.As matches typed error", func(t *testing.T) {
		var err error = &KeyRotationIntervalTooSmallError{
			Configured: 500 * time.Millisecond,
			Minimum:    time.Second,
		}
		var tooSmall *KeyRotationIntervalTooSmallError
		if !errors.As(err, &tooSmall) {
			t.Fatal("errors.As should match *KeyRotationIntervalTooSmallError")
		}
		if tooSmall.Configured != 500*time.Millisecond {
			t.Errorf("Configured = %v, want 500ms", tooSmall.Configured)
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

	t.Run("errors.As matches typed error", func(t *testing.T) {
		var err error = &SessionExpiryEntriesTooSmallError{
			Configured: 10,
			Minimum:    100,
		}
		var tooSmall *SessionExpiryEntriesTooSmallError
		if !errors.As(err, &tooSmall) {
			t.Fatal("errors.As should match *SessionExpiryEntriesTooSmallError")
		}
		if tooSmall.Configured != 10 {
			t.Errorf("Configured = %d, want 10", tooSmall.Configured)
		}
	})
}
