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

package writemode

import (
	"errors"
	"testing"
)

func TestMode_String(t *testing.T) {
	tests := []struct {
		name string
		mode Mode
		want string
	}{
		{name: "sync", mode: Sync, want: "sync"},
		{name: "wal_durable", mode: WALDurable, want: "wal_durable"},
		{name: "batched", mode: Batched, want: "batched"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mode.String(); got != tt.want {
				t.Errorf("Mode.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMode_String_Unknown(t *testing.T) {
	m := Mode(255)
	got := m.String()
	want := "unknown(255)"
	if got != want {
		t.Errorf("Mode.String() for unknown = %q, want %q", got, want)
	}
}

func TestFromString_ValidModes(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Mode
	}{
		{name: "empty defaults to sync", input: "", want: Sync},
		{name: "sync", input: "sync", want: Sync},
		{name: "wal_durable", input: "wal_durable", want: WALDurable},
		{name: "wal alias", input: "wal", want: WALDurable},
		{name: "async alias", input: "async", want: WALDurable},
		{name: "batched", input: "batched", want: Batched},
		{name: "batch alias", input: "batch", want: Batched},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FromString(tt.input)
			if err != nil {
				t.Fatalf("FromString(%q) returned unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("FromString(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestFromString_InvalidMode(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "unknown string", input: "turbo"},
		{name: "uppercase", input: "SYNC"},
		{name: "mixed case", input: "Batched"},
		{name: "numeric", input: "0"},
		{name: "whitespace", input: " sync"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := FromString(tt.input)
			if err == nil {
				t.Fatalf("FromString(%q) expected error, got nil", tt.input)
			}
			var target *InvalidModeError
			if !errors.As(err, &target) {
				t.Fatalf("FromString(%q) error should be *InvalidModeError, got %T", tt.input, err)
			}
			if target.Value != tt.input {
				t.Errorf("InvalidModeError.Value = %q, want %q", target.Value, tt.input)
			}
		})
	}
}

func TestMode_Validate_Valid(t *testing.T) {
	tests := []struct {
		name string
		mode Mode
	}{
		{name: "sync", mode: Sync},
		{name: "wal_durable", mode: WALDurable},
		{name: "batched", mode: Batched},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.mode.Validate(); err != nil {
				t.Errorf("Mode(%d).Validate() returned unexpected error: %v", tt.mode, err)
			}
		})
	}
}

func TestMode_Validate_Invalid(t *testing.T) {
	tests := []struct {
		name string
		mode Mode
	}{
		{name: "out of range 3", mode: Mode(3)},
		{name: "out of range 255", mode: Mode(255)},
		{name: "out of range 100", mode: Mode(100)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.mode.Validate()
			if err == nil {
				t.Fatalf("Mode(%d).Validate() expected error, got nil", tt.mode)
			}
			var target *InvalidModeError
			if !errors.As(err, &target) {
				t.Fatalf("Mode(%d).Validate() error should be *InvalidModeError, got %T", tt.mode, err)
			}
		})
	}
}

func TestMode_IsSynchronous(t *testing.T) {
	tests := []struct {
		name string
		mode Mode
		want bool
	}{
		{name: "sync is synchronous", mode: Sync, want: true},
		{name: "wal_durable is not synchronous", mode: WALDurable, want: false},
		{name: "batched is synchronous", mode: Batched, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mode.IsSynchronous(); got != tt.want {
				t.Errorf("Mode(%d).IsSynchronous() = %v, want %v", tt.mode, got, tt.want)
			}
		})
	}
}

func TestMode_IsAsync(t *testing.T) {
	tests := []struct {
		name string
		mode Mode
		want bool
	}{
		{name: "sync is not async", mode: Sync, want: false},
		{name: "wal_durable is async", mode: WALDurable, want: true},
		{name: "batched is not async", mode: Batched, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mode.IsAsync(); got != tt.want {
				t.Errorf("Mode(%d).IsAsync() = %v, want %v", tt.mode, got, tt.want)
			}
		})
	}
}

func TestMode_RequiresQuorum(t *testing.T) {
	tests := []struct {
		name string
		mode Mode
		want bool
	}{
		{name: "sync requires quorum", mode: Sync, want: true},
		{name: "wal_durable does not require quorum", mode: WALDurable, want: false},
		{name: "batched requires quorum", mode: Batched, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mode.RequiresQuorum(); got != tt.want {
				t.Errorf("Mode(%d).RequiresQuorum() = %v, want %v", tt.mode, got, tt.want)
			}
		})
	}
}

func TestMode_IsSynchronous_And_IsAsync_AreMutuallyExclusive(t *testing.T) {
	modes := []Mode{Sync, WALDurable, Batched}
	for _, m := range modes {
		if m.IsSynchronous() && m.IsAsync() {
			t.Errorf("Mode(%d) reports both IsSynchronous and IsAsync", m)
		}
		if !m.IsSynchronous() && !m.IsAsync() {
			t.Errorf("Mode(%d) reports neither IsSynchronous nor IsAsync", m)
		}
	}
}

func TestFromString_RoundTrip(t *testing.T) {
	modes := []Mode{Sync, WALDurable, Batched}
	for _, m := range modes {
		t.Run(m.String(), func(t *testing.T) {
			parsed, err := FromString(m.String())
			if err != nil {
				t.Fatalf("FromString(%q) returned unexpected error: %v", m.String(), err)
			}
			if parsed != m {
				t.Errorf("round-trip failed: %v -> %q -> %v", m, m.String(), parsed)
			}
		})
	}
}

func TestMode_Constants(t *testing.T) {
	// Verify iota ordering matches expected uint8 values.
	if Sync != 0 {
		t.Errorf("Sync = %d, want 0", Sync)
	}
	if WALDurable != 1 {
		t.Errorf("WALDurable = %d, want 1", WALDurable)
	}
	if Batched != 2 {
		t.Errorf("Batched = %d, want 2", Batched)
	}
}
