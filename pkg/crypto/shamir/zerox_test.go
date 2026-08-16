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

package shamir

import (
	"bytes"
	"encoding/base64"
	"errors"
	"testing"
)

// TestCombineRejectsZeroXCoordinate pins the rule that makes Combine's output
// un-choosable.
//
// Deleting the x == 0 branch from parseRawShare left the whole package suite
// green — the duplicate-x and format-version guards beside it are both tested,
// only this one was not.
//
// Why it matters: reconstruction evaluates the Lagrange basis at x = 0. For a
// share whose own x-coordinate IS 0, the basis coefficient is
// Π(x_m / (0 ⊕ x_m)) = 1, while every OTHER share's basis contains a factor
// evaluated at x_m = 0, which is 0 in GF(2^8). The interpolation therefore
// returns exactly that share's y bytes as the "reconstructed secret",
// regardless of the genuine shares supplied alongside it. One crafted share
// dictates the output of a recovery ceremony.
func TestCombineRejectsZeroXCoordinate(t *testing.T) {
	secret := []byte("root-key-material-32-bytes-long!")
	shares, err := Split(secret, 2, 3)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}

	// Forge share 0 with x = 0 and an attacker-chosen payload.
	chosen := bytes.Repeat([]byte{0xAA}, len(secret))
	forged := append([]byte{shareFormatVersion, 0x00}, chosen...)
	shares[0].Value = base64.StdEncoding.EncodeToString(forged)

	got, err := Combine(shares[:2])
	if err == nil {
		t.Fatalf("Combine ACCEPTED a zero x-coordinate share and returned %x. "+
			"With x=0 the Lagrange basis collapses to 1 for that share and 0 "+
			"for every other, so the attacker's bytes ARE the reconstructed "+
			"secret", got)
	}
	var combineErr *CombineError
	if !errors.As(err, &combineErr) {
		t.Fatalf("err = %T (%v), want *CombineError", err, err)
	}

	// The byte-slice path must reject it identically; it is the path a
	// memory-locked enclave caller uses.
	vals := [][]byte{[]byte(shares[0].Value), []byte(shares[1].Value)}
	if _, err := CombineBytes(vals, 2); err == nil {
		t.Fatal("CombineBytes accepted a zero x-coordinate share")
	}
}

// TestCombineRejectsZeroX_ControlStillReconstructs is the control: the
// rejection above must be caused by the zero x-coordinate specifically, not by
// the test having produced shares that cannot combine at all.
func TestCombineRejectsZeroX_ControlStillReconstructs(t *testing.T) {
	secret := []byte("root-key-material-32-bytes-long!")
	shares, err := Split(secret, 2, 3)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	got, err := Combine(shares[:2])
	if err != nil {
		t.Fatalf("Combine on untouched shares: %v", err)
	}
	if !bytes.Equal(got, secret) {
		t.Fatalf("Combine = %x, want %x", got, secret)
	}
}
