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

//go:build linux

package lockedmem

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// This package is a witness for other packages' tests, so a defect in it is
// invisible where it is used: a parser that reported the wrong mapping, or none,
// would make every freeze assertion SKIP and every one of them would still read
// as a pass. These tests exist so that cannot happen quietly.

// TestProtectionOf_ReportsTheRealProtections runs the parser against the two
// states it exists to tell apart, on real enclave pages rather than a fixture:
// a sized enclave is writable and the same enclave is read-only after Freeze.
//
// Using go-enclave rather than a hand-rolled mmap is deliberate. What the
// consumers ask this package is "did MY call to Freeze take", so the thing under
// test has to be the mapping go-enclave actually produces.
func TestProtectionOf_ReportsTheRealProtections(t *testing.T) {
	e, err := enclave.NewMemorySized(32)
	if err != nil {
		t.Skipf("could not allocate locked memory: %v", err)
	}
	defer e.Destroy()

	// Each reading is taken inside its own borrow: Freeze waits out in-flight
	// borrows, so it cannot run inside one.
	protection := func() (Protection, error) {
		var (
			p    Protection
			perr error
		)
		if err := e.WithBytes(func(view []byte) error {
			p, perr = ProtectionOf(view)
			return nil
		}); err != nil {
			t.Fatalf("WithBytes: %v", err)
		}
		return p, perr
	}

	before, err := protection()
	if err != nil {
		t.Skipf("this kernel cannot report mapping protections: %v", err)
	}
	if before.ReadOnly() {
		t.Fatalf("a sized enclave reported %q: it must be writable before Freeze, "+
			"or every consumer's freeze assertion passes vacuously", before)
	}

	var f enclave.Freezer
	if !enclave.As(e, &f) {
		t.Fatal("the memory enclave lacks the Freezer capability")
	}
	if err := f.Freeze(); err != nil {
		t.Fatalf("Freeze: %v", err)
	}
	after, err := protection()
	if err != nil {
		t.Fatalf("ProtectionOf after Freeze: %v", err)
	}
	if !after.ReadOnly() {
		t.Fatalf("a frozen enclave reported %q, want a read-only mapping", after)
	}
}

// TestProtectionOf_RefusesWhatItCannotAnswer pins the two "cannot answer" paths
// as ERRORS rather than as a permissive default. A version that returned the
// zero Protection for an unknown address would report ReadOnly()==false, which a
// consumer reads as "this was not frozen" — a false failure that looks exactly
// like a real one.
func TestProtectionOf_RefusesWhatItCannotAnswer(t *testing.T) {
	if _, err := ProtectionOf(nil); err == nil {
		t.Fatal("an empty slice has no mapping to report; it must be an error")
	}
	if _, err := ProtectionOf([]byte{}); err == nil {
		t.Fatal("an empty slice has no mapping to report; it must be an error")
	}
}

// TestProtection_ReadOnly is the table the whole package turns on: which
// permission strings /proc/self/maps can produce count as frozen.
func TestProtection_ReadOnly(t *testing.T) {
	for _, tc := range []struct {
		perms Protection
		want  bool
	}{
		{"r--p", true},
		{"r--s", true},
		{"rw-p", false},
		{"rwxp", false},
		{"---p", false},
		{"r", false}, // truncated: not enough to decide, so not a claim
		{"", false},
	} {
		if got := tc.perms.ReadOnly(); got != tc.want {
			t.Errorf("Protection(%q).ReadOnly() = %v, want %v", tc.perms, got, tc.want)
		}
	}
}

// TestParseRange covers the malformed lines a maps file should never contain and
// which must not be mistaken for a match: a missing bound is not address zero.
func TestParseRange(t *testing.T) {
	lo, hi, ok := parseRange("7f0000000000-7f0000001000")
	if !ok || lo != 0x7f0000000000 || hi != 0x7f0000001000 {
		t.Fatalf("parseRange = (%#x, %#x, %v), want the two bounds", lo, hi, ok)
	}
	for _, bad := range []string{"", "-", "-7f0", "zz-7f0", "7f0-zz", "7f0"} {
		if _, _, ok := parseRange(bad); ok {
			t.Errorf("parseRange(%q) reported a range", bad)
		}
	}
}

// TestProtectionOf_ErrorsAreDistinguishable keeps the two failure causes apart
// for a caller deciding between skipping and failing. Both are wrapped, so
// errors.Unwrap reaches the cause where there is one.
func TestProtectionOf_ErrorsAreDistinguishable(t *testing.T) {
	_, err := ProtectionOf(nil)
	if err == nil {
		t.Fatal("expected an error for an empty slice")
	}
	if errors.Unwrap(err) != nil {
		t.Fatalf("the empty-slice refusal wraps nothing; got %v", errors.Unwrap(err))
	}
}
