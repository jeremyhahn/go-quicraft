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
	"crypto/rand"
	"encoding/base64"
	"errors"
	"testing"
)

// refGFMul is an independent reference implementation of GF(2^8)
// multiplication (carry-less multiply followed by reduction mod 0x11B)
// used to cross-check the constant-time gfMul.
func refGFMul(a, b byte) byte {
	var product uint16
	for i := 0; i < 8; i++ {
		if b&(1<<i) != 0 {
			product ^= uint16(a) << i
		}
	}
	// Reduce modulo x^8 + x^4 + x^3 + x + 1 (0x11B).
	for i := 15; i >= 8; i-- {
		if product&(1<<i) != 0 {
			product ^= 0x11B << (i - 8)
		}
	}
	return byte(product)
}

// TestGFMulMatchesReference exhaustively cross-checks gfMul against an
// independent reference implementation over all 65536 operand pairs.
func TestGFMulMatchesReference(t *testing.T) {
	for a := 0; a < 256; a++ {
		for b := 0; b < 256; b++ {
			got := gfMul(byte(a), byte(b))
			want := refGFMul(byte(a), byte(b))
			if got != want {
				t.Fatalf("gfMul(%d, %d) = %d, want %d", a, b, got, want)
			}
		}
	}
}

// TestGFInv verifies a * gfInv(a) == 1 for all non-zero a, and
// gfInv(0) == 0.
func TestGFInv(t *testing.T) {
	if gfInv(0) != 0 {
		t.Fatalf("gfInv(0) = %d, want 0", gfInv(0))
	}
	for a := 1; a < 256; a++ {
		inv := gfInv(byte(a))
		if p := gfMul(byte(a), inv); p != 1 {
			t.Fatalf("%d * gfInv(%d)=%d gives %d, want 1", a, a, inv, p)
		}
	}
}

// TestGFDiv verifies (a*b)/b == a for non-zero b.
func TestGFDiv(t *testing.T) {
	for a := 0; a < 256; a++ {
		for _, b := range []byte{1, 2, 3, 0x53, 0xCA, 0xFF} {
			prod := gfMul(byte(a), b)
			if q := gfDiv(prod, b); q != byte(a) {
				t.Fatalf("(%d*%d)/%d = %d, want %d", a, b, b, q, a)
			}
		}
	}
}

// TestGFPolyEval verifies Horner evaluation against direct expansion.
func TestGFPolyEval(t *testing.T) {
	// p(x) = 7 + 3x + 5x^2
	coeffs := []byte{7, 3, 5}
	for x := 0; x < 256; x++ {
		xb := byte(x)
		want := byte(7) ^ gfMul(3, xb) ^ gfMul(5, gfMul(xb, xb))
		if got := gfPolyEval(coeffs, xb); got != want {
			t.Fatalf("p(%d) = %d, want %d", x, got, want)
		}
	}
	// Evaluation at 0 returns the constant term (the secret byte).
	if got := gfPolyEval(coeffs, 0); got != 7 {
		t.Fatalf("p(0) = %d, want 7", got)
	}
}

// TestSplitCombineRoundTripAllSubsets verifies that EVERY
// threshold-sized subset of shares reconstructs a random secret exactly,
// for several (threshold, total) configurations and secret sizes.
func TestSplitCombineRoundTripAllSubsets(t *testing.T) {
	configs := []struct{ threshold, total int }{
		{2, 2}, {2, 3}, {3, 5}, {5, 5}, {4, 7},
	}
	for _, cfg := range configs {
		for _, size := range []int{1, 31, 32, 33, 64} {
			secret := make([]byte, size)
			if _, err := rand.Read(secret); err != nil {
				t.Fatalf("rand: %v", err)
			}
			shares, err := Split(secret, cfg.threshold, cfg.total)
			if err != nil {
				t.Fatalf("Split(%d,%d): %v", cfg.threshold, cfg.total, err)
			}

			// All threshold-sized combinations.
			idx := make([]int, cfg.threshold)
			var rec func(start, k int)
			rec = func(start, k int) {
				if k == cfg.threshold {
					subset := make([]*Share, cfg.threshold)
					for i, j := range idx {
						subset[i] = shares[j]
					}
					got, cerr := Combine(subset)
					if cerr != nil {
						t.Fatalf("Combine subset %v: %v", idx, cerr)
					}
					if !bytes.Equal(got, secret) {
						t.Fatalf("subset %v reconstructed wrong secret", idx)
					}
					return
				}
				for j := start; j < cfg.total; j++ {
					idx[k] = j
					rec(j+1, k+1)
				}
			}
			rec(0, 0)

			// All shares together also reconstruct exactly.
			got, err := Combine(shares)
			if err != nil {
				t.Fatalf("Combine all: %v", err)
			}
			if !bytes.Equal(got, secret) {
				t.Fatal("all-shares reconstruction mismatch")
			}
		}
	}
}

// TestCombineTamperedShareYieldsGarbage verifies the M4 contract: flipping a
// byte in a share is NOT self-detected (shares carry no secret digest — that
// would be an offline oracle). Combine returns garbage, not an error; the caller
// must catch this via out-of-band authentication.
func TestCombineTamperedShareYieldsGarbage(t *testing.T) {
	secret := []byte("tamper detection secret")
	shares, err := Split(secret, 3, 5)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}

	// Tamper with a payload byte of share 1 (re-encode base64).
	raw, err := shares[1].Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	raw[rawShareOverhead] ^= 0x01
	shares[1].Value = encodeB64(raw)

	got, err := Combine(shares[:3])
	if err != nil {
		t.Fatalf("Combine must not error on a tampered share: %v", err)
	}
	if bytes.Equal(got, secret) {
		t.Fatal("tampered share unexpectedly reconstructed the original secret")
	}
}

// TestCombineRejectsLegacyFormat verifies that a share value without the
// current format version byte is rejected explicitly rather than
// misinterpreted (gates legacy pre-GF(256) shares).
func TestCombineRejectsLegacyFormat(t *testing.T) {
	secret := []byte("format version gate")
	shares, err := Split(secret, 2, 3)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	raw, _ := shares[0].Bytes()
	raw[0] = 0x7F // unknown version
	shares[0].Value = encodeB64(raw)

	_, err = CombineUnverified(shares[:2])
	var combineErr *CombineError
	if !errors.As(err, &combineErr) {
		t.Fatalf("err = %v, want CombineError", err)
	}
}

// TestCombineRejectsDuplicateXCoordinate verifies that two shares with
// the same field coordinate are rejected (zero Lagrange denominator).
func TestCombineRejectsDuplicateXCoordinate(t *testing.T) {
	secret := []byte("dup x")
	shares, err := Split(secret, 2, 3)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	// Same value under a different Share.Index: the embedded x is what
	// counts.
	dup := *shares[0]
	dup.Index = 2
	_, err = Combine([]*Share{shares[0], &dup})
	var combineErr *CombineError
	if !errors.As(err, &combineErr) {
		t.Fatalf("err = %v, want CombineError for duplicate x", err)
	}
}

// TestCombineRequiresDigest verifies that Combine refuses digest-less
// shares and CombineUnverified accepts them (explicit opt-out, not a
// silent sentinel).
// TestCombineEqualsCombineUnverified verifies the M4 contract: shares carry no
// secret digest, so Combine performs no self-verification and is identical to
// CombineUnverified — both reconstruct the secret and leave integrity to the
// caller's out-of-band authentication.
func TestCombineEqualsCombineUnverified(t *testing.T) {
	secret := []byte("digest policy")
	shares, err := Split(secret, 2, 3)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}

	gotCombine, err := Combine(shares[:2])
	if err != nil {
		t.Fatalf("Combine: %v", err)
	}
	gotUnverified, err := CombineUnverified(shares[:2])
	if err != nil {
		t.Fatalf("CombineUnverified: %v", err)
	}
	if !bytes.Equal(gotCombine, secret) || !bytes.Equal(gotUnverified, secret) {
		t.Fatal("reconstruction mismatch")
	}
	if !bytes.Equal(gotCombine, gotUnverified) {
		t.Fatal("Combine and CombineUnverified produced different results")
	}
}

// TestFewerThanThresholdSharesRevealNothing verifies (probabilistically)
// that threshold-1 shares do not reconstruct the secret.
func TestFewerThanThresholdSharesRevealNothing(t *testing.T) {
	secret := []byte("below threshold")
	shares, err := Split(secret, 3, 5)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if _, err := CombineUnverified(shares[:2]); err == nil {
		t.Fatal("threshold check must reject 2 of 3 shares")
	}
}

// encodeB64 is a test helper mirroring the share value encoding.
func encodeB64(raw []byte) string {
	return base64.StdEncoding.EncodeToString(raw)
}
