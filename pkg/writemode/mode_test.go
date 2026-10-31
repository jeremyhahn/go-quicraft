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
	"fmt"
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

// TestFromString_RejectsAnAliasForAnUndescribedMode is the gate on Validate's
// production caller. parseModes and modeSpecs are separate tables — the first
// is many-to-one — and FromString is the only thing that checks them against
// each other. An alias pointing at a Mode modeSpecs does not describe must be
// REFUSED, not returned: a Mode that parses cleanly and then answers
// "unknown(N)" from String and false from every durability predicate is a
// configuration this package accepted while promising nothing.
//
// The drift is injected here rather than left to a future edit, because the
// whole point is that the mistake compiles.
func TestFromString_RejectsAnAliasForAnUndescribedMode(t *testing.T) {
	const undescribed Mode = 200
	if _, ok := modeSpecs[undescribed]; ok {
		t.Fatalf("Mode(%d) is described by modeSpecs; pick a value that is not",
			undescribed)
	}

	parseModes["drifted"] = undescribed
	t.Cleanup(func() { delete(parseModes, "drifted") })

	got, err := FromString("drifted")
	if err == nil {
		t.Fatalf("FromString(%q) = %v with no error; an alias for a Mode that "+
			"modeSpecs does not describe must be refused", "drifted", got)
	}
	var target *InvalidModeError
	if !errors.As(err, &target) {
		t.Fatalf("FromString error = %T, want *InvalidModeError", err)
	}
	if got != Sync {
		t.Errorf("FromString returned Mode(%d) alongside its error, want the zero "+
			"value Sync", got)
	}
}

// TestMode_UnknownValueGuaranteesNothing pins the answer an out-of-range Mode
// gives, which the Mode doc comment relies on and which nothing tested.
//
// Mode is an open uint8, so this is the state a value decoded from
// configuration or a wire field lands in when it does not name a real mode. All
// three predicates report false — in particular RequiresQuorum, so an embedder
// that branches on it treats an unnameable mode as one that needs no quorum.
// That is why Validate exists and why the doc comment tells callers to run it
// at the boundary; if the answers here are ever changed, this test and that
// comment must move together.
func TestMode_UnknownValueGuaranteesNothing(t *testing.T) {
	for _, m := range []Mode{Mode(3), Mode(100), Mode(255)} {
		var target *InvalidModeError
		if err := m.Validate(); !errors.As(err, &target) {
			t.Fatalf("Mode(%d).Validate() = %v, want *InvalidModeError", m, err)
		}
		if m.IsSynchronous() {
			t.Errorf("Mode(%d).IsSynchronous() = true, want false", m)
		}
		if m.IsAsync() {
			t.Errorf("Mode(%d).IsAsync() = true, want false", m)
		}
		if m.RequiresQuorum() {
			t.Errorf("Mode(%d).RequiresQuorum() = true, want false", m)
		}
		if want := fmt.Sprintf("unknown(%d)", m); m.String() != want {
			t.Errorf("Mode(%d).String() = %q, want %q", m, m.String(), want)
		}
	}
}

// TestModeSpecs_CoverEveryDeclaredMode pins that the three exported constants
// are exactly the Modes modeSpecs describes. A constant added without a spec
// would validate as invalid and answer false to every durability question while
// still being part of the package's public API.
func TestModeSpecs_CoverEveryDeclaredMode(t *testing.T) {
	declared := []Mode{Sync, WALDurable, Batched}
	for _, m := range declared {
		if _, ok := modeSpecs[m]; !ok {
			t.Errorf("Mode %d is declared but modeSpecs does not describe it", m)
		}
	}
	if len(modeSpecs) != len(declared) {
		t.Errorf("modeSpecs describes %d modes, but %d are declared: a spec "+
			"without a constant is unreachable through FromString",
			len(modeSpecs), len(declared))
	}
}

// TestParseModes_OnlyNameDescribedModes is the static half of the drift guard:
// every alias must already resolve to a described Mode, so the runtime check in
// FromString never has to fire in a correct build.
func TestParseModes_OnlyNameDescribedModes(t *testing.T) {
	for alias, m := range parseModes {
		if err := m.Validate(); err != nil {
			t.Errorf("parseModes[%q] = Mode(%d), which modeSpecs does not describe: %v",
				alias, m, err)
		}
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
