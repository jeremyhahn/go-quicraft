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
	"fmt"
	"strings"
	"testing"
)

func TestSplitProtected_Success(t *testing.T) {
	secret := []byte("This is a secret message for testing!")
	threshold := 3
	total := 5

	shares := splitProtectedShares(t, secret, threshold, total)
	if len(shares) != total {
		t.Fatalf("expected %d shares, got %d", total, len(shares))
	}

	for i, share := range shares {
		if share.Index != i+1 {
			t.Errorf("share %d: expected index %d, got %d", i, i+1, share.Index)
		}
		if share.Threshold != threshold {
			t.Errorf("share %d: expected threshold %d, got %d", i, threshold, share.Threshold)
		}
		if share.Total != total {
			t.Errorf("share %d: expected total %d, got %d", i, total, share.Total)
		}
		if len(valueCopy(share)) == 0 {
			t.Errorf("share %d: value is empty", i)
		}
		if err := share.Validate(); err != nil {
			t.Errorf("share %d: validation failed: %v", i, err)
		}
	}
}

func TestSplitBytes_InvalidThreshold(t *testing.T) {
	_, err := SplitBytes([]byte("secret"), 1, 5)
	if err == nil {
		t.Fatal("expected error for threshold < 2, got nil")
	}
	var splitErr *SplitError
	if !errors.As(err, &splitErr) {
		t.Fatalf("expected *SplitError, got %T: %v", err, err)
	}
	if splitErr.Reason != "threshold must be at least 2, got 1" {
		t.Errorf("unexpected reason: %s", splitErr.Reason)
	}
}

func TestSplitBytes_TotalLessThanThreshold(t *testing.T) {
	_, err := SplitBytes([]byte("secret"), 5, 3)
	if err == nil {
		t.Fatal("expected error for total < threshold, got nil")
	}
	var splitErr *SplitError
	if !errors.As(err, &splitErr) {
		t.Fatalf("expected *SplitError, got %T: %v", err, err)
	}
	if splitErr.Reason != "total shares (3) must be >= threshold (5)" {
		t.Errorf("unexpected reason: %s", splitErr.Reason)
	}
}

func TestSplitBytes_ThresholdExceeds255(t *testing.T) {
	_, err := SplitBytes([]byte("secret"), 256, 300)
	if err == nil {
		t.Fatal("expected error for threshold > 255, got nil")
	}
	var splitErr *SplitError
	if !errors.As(err, &splitErr) {
		t.Fatalf("expected *SplitError, got %T: %v", err, err)
	}
	if splitErr.Reason != "threshold cannot exceed 255, got 256" {
		t.Errorf("unexpected reason: %s", splitErr.Reason)
	}
}

func TestSplitBytes_TotalExceeds255(t *testing.T) {
	_, err := SplitBytes([]byte("secret"), 3, 256)
	if err == nil {
		t.Fatal("expected error for total > 255, got nil")
	}
	var splitErr *SplitError
	if !errors.As(err, &splitErr) {
		t.Fatalf("expected *SplitError, got %T: %v", err, err)
	}
	if splitErr.Reason != "total shares cannot exceed 255, got 256" {
		t.Errorf("unexpected reason: %s", splitErr.Reason)
	}
}

func TestSplitBytes_EmptySecret(t *testing.T) {
	_, err := SplitBytes([]byte{}, 3, 5)
	if err == nil {
		t.Fatal("expected error for empty secret, got nil")
	}
	var splitErr *SplitError
	if !errors.As(err, &splitErr) {
		t.Fatalf("expected *SplitError, got %T: %v", err, err)
	}
	if splitErr.Reason != "secret cannot be empty" {
		t.Errorf("unexpected reason: %s", splitErr.Reason)
	}
}

func TestCombineBytes_Success(t *testing.T) {
	secret := []byte("Reconstruct this secret via raw byte shares")
	threshold := 3
	total := 5

	values := splitValues(t, secret, threshold, total)
	reconstructed, err := CombineBytes(pick(values, 0, 2, 4), threshold)
	if err != nil {
		t.Fatalf("CombineBytes failed: %v", err)
	}
	if !bytes.Equal(secret, reconstructed) {
		t.Errorf("reconstructed secret does not match original\noriginal:      %q\nreconstructed: %q", secret, reconstructed)
	}
}

func TestCombineBytes_SubsetOfShares(t *testing.T) {
	secret := []byte("Only need threshold shares")
	threshold := 3
	total := 7

	values := splitValues(t, secret, threshold, total)
	subsets := [][]int{{0, 2, 4}, {1, 3, 6}, {0, 5, 6}}
	for i, subset := range subsets {
		reconstructed, err := CombineBytes(pick(values, subset...), threshold)
		if err != nil {
			t.Fatalf("subset %d: CombineBytes failed: %v", i, err)
		}
		if !bytes.Equal(secret, reconstructed) {
			t.Errorf("subset %d: reconstructed secret does not match original", i)
		}
	}
}

func TestCombineBytes_MatchesCombineProtected(t *testing.T) {
	secret := []byte("byte-combine and protected-combine must agree")
	threshold, total := 2, 4

	restore := testOnlyRandReader(bytes.NewReader(bytes.Repeat([]byte{0x3c}, 4096)))
	values := splitValues(t, secret, threshold, total)
	restore()
	shares := splitProtectedShares(t, secret, threshold, total)

	viaBytes, err := CombineBytes(pick(values, 0, 3), threshold)
	if err != nil {
		t.Fatalf("CombineBytes failed: %v", err)
	}
	viaShares, err := CombineProtected([]*ProtectedShare{shares[0], shares[3]})
	if err != nil {
		t.Fatalf("CombineProtected failed: %v", err)
	}
	defer viaShares.Destroy()
	if !bytes.Equal(encCopy(viaShares), viaBytes) {
		t.Errorf("CombineBytes disagrees with CombineProtected:\nCombineProtected: %q\nCombineBytes:     %q", encCopy(viaShares), viaBytes)
	}
}

func TestCombineBytes_InvalidThreshold(t *testing.T) {
	values := splitValues(t, []byte("threshold below 2 must be rejected"), 2, 3)
	for _, threshold := range []int{1, 0, -1} {
		_, err := CombineBytes(values[:2], threshold)
		var combineErr *CombineError
		if !errors.As(err, &combineErr) || !strings.Contains(combineErr.Reason, "threshold must be at least 2") {
			t.Fatalf("threshold %d: expected threshold CombineError, got %v", threshold, err)
		}
	}
}

func TestCombineBytes_NoShares(t *testing.T) {
	_, err := CombineBytes(nil, 3)
	var combineErr *CombineError
	if !errors.As(err, &combineErr) || combineErr.Reason != "no shares provided" {
		t.Fatalf("expected no-shares CombineError, got %v", err)
	}
}

func TestCombineBytes_InsufficientShares(t *testing.T) {
	threshold, total := 4, 7
	values := splitValues(t, []byte("need more byte shares"), threshold, total)
	subset := pick(values, 0, 2, 5)
	_, err := CombineBytes(subset, threshold)
	var combineErr *CombineError
	if !errors.As(err, &combineErr) {
		t.Fatalf("expected *CombineError, got %T: %v", err, err)
	}
	expected := fmt.Sprintf("need at least %d shares, got %d", threshold, len(subset))
	if combineErr.Reason != expected {
		t.Errorf("unexpected reason: %s (expected: %s)", combineErr.Reason, expected)
	}
}

func TestCombineBytes_InvalidBase64(t *testing.T) {
	_, err := CombineBytes([][]byte{[]byte("!!!not-base64!!!"), []byte("@@@@")}, 2)
	var combineErr *CombineError
	if !errors.As(err, &combineErr) {
		t.Fatalf("expected *CombineError for bad base64, got %T: %v", err, err)
	}

	// One corrupt value among valid ones is refused too.
	values := splitValues(t, []byte("test secret"), 3, 5)
	values[1] = []byte("!!!invalid-base64!!!")
	if _, err := CombineBytes(values[:3], 3); !errors.As(err, &combineErr) {
		t.Fatalf("expected *CombineError for one bad value, got %T: %v", err, err)
	}
}

func TestCombineBytes_DuplicateXCoordinate(t *testing.T) {
	threshold, total := 2, 5
	values := splitValues(t, []byte("dup x-coordinate is rejected"), threshold, total)
	// Same share twice → duplicate x-coordinate.
	_, err := CombineBytes(pick(values, 0, 0), threshold)
	var combineErr *CombineError
	if !errors.As(err, &combineErr) || !strings.Contains(combineErr.Reason, "duplicate share") {
		t.Fatalf("expected duplicate-share CombineError, got %v", err)
	}
}

func TestVerifyShare_Success(t *testing.T) {
	shares := splitProtectedShares(t, []byte("verify me"), 3, 5)

	// Verify each share against the others
	for i, share := range shares {
		others := make([]*ProtectedShare, 0, len(shares)-1)
		for j, s := range shares {
			if j != i {
				others = append(others, s)
			}
		}
		if err := VerifyShare(share, others); err != nil {
			t.Errorf("share %d: VerifyShare failed: %v", i, err)
		}
	}
}

func TestVerifyShare_InvalidShare(t *testing.T) {
	badShare := splitProtectedShares(t, []byte("invalid share"), 3, 5)[0]
	badShare.Index = 0 // invalid: must be >= 1

	err := VerifyShare(badShare, nil)
	if err == nil {
		t.Fatal("expected error for invalid share, got nil")
	}
	var valErr *ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if valErr.Field != "index" {
		t.Errorf("expected field 'index', got %q", valErr.Field)
	}
}

func TestVerifyShare_DuplicateIndex(t *testing.T) {
	shares := splitProtectedShares(t, []byte("duplicate test"), 3, 5)

	// A share from another split with the same index as shares[0].
	duplicate := splitProtectedShares(t, []byte("another split"), 3, 5)[0]

	err := VerifyShare(duplicate, shares)
	if err == nil {
		t.Fatal("expected error for duplicate index, got nil")
	}
	var valErr *ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if valErr.Field != "index" {
		t.Errorf("expected field 'index', got %q", valErr.Field)
	}
}

func TestVerifyShare_ThresholdMismatch(t *testing.T) {
	shares := splitProtectedShares(t, []byte("threshold test"), 3, 5)
	// Index 4, a valid index within total, under a different threshold.
	badShare := splitProtectedShares(t, []byte("threshold test"), 4, 5)[3]

	err := VerifyShare(badShare, shares[:2])
	if err == nil {
		t.Fatal("expected error for threshold mismatch, got nil")
	}
	var valErr *ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if valErr.Field != "threshold" {
		t.Errorf("expected field 'threshold', got %q", valErr.Field)
	}
}

func TestVerifyShare_TotalMismatch(t *testing.T) {
	shares := splitProtectedShares(t, []byte("total test"), 3, 5)
	// Index 4 of a split with a different total.
	badShare := splitProtectedShares(t, []byte("total test"), 3, 9)[3]

	err := VerifyShare(badShare, shares[:2])
	if err == nil {
		t.Fatal("expected error for total mismatch, got nil")
	}
	var valErr *ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if valErr.Field != "total" {
		t.Errorf("expected field 'total', got %q", valErr.Field)
	}
}

func TestSplitBytes_BinarySecret(t *testing.T) {
	secret := make([]byte, 256)
	if _, err := rand.Read(secret); err != nil {
		t.Fatalf("failed to generate random secret: %v", err)
	}

	values := splitValues(t, secret, 3, 5)
	reconstructed, err := CombineBytes(pick(values, 1, 2, 4), 3)
	if err != nil {
		t.Fatalf("CombineBytes failed: %v", err)
	}

	if !bytes.Equal(secret, reconstructed) {
		t.Error("reconstructed binary secret does not match original")
	}
}

func TestSplitBytes_LargeSecret(t *testing.T) {
	secret := make([]byte, 1024)
	if _, err := rand.Read(secret); err != nil {
		t.Fatalf("failed to generate random secret: %v", err)
	}

	values := splitValues(t, secret, 5, 9)
	if len(values) != 9 {
		t.Fatalf("expected 9 shares, got %d", len(values))
	}

	reconstructed, err := CombineBytes(pick(values, 0, 2, 4, 6, 8), 5)
	if err != nil {
		t.Fatalf("CombineBytes failed: %v", err)
	}

	if !bytes.Equal(secret, reconstructed) {
		t.Error("reconstructed large secret does not match original")
	}
}

func TestCombineBytes_MoreThanThreshold(t *testing.T) {
	secret := []byte("more than threshold test")
	threshold := 3
	total := 5

	values := splitValues(t, secret, threshold, total)

	// Use 4 shares (more than threshold of 3)
	reconstructed, err := CombineBytes(pick(values, 0, 1, 3, 4), threshold)
	if err != nil {
		t.Fatalf("CombineBytes failed: %v", err)
	}

	if !bytes.Equal(secret, reconstructed) {
		t.Error("reconstructed secret does not match original when using more than threshold shares")
	}
}

func TestErrors_Format(t *testing.T) {
	splitErr := &SplitError{Reason: "test reason"}
	if splitErr.Error() != "quicraft/shamir: split failed: test reason" {
		t.Errorf("SplitError.Error() = %q", splitErr.Error())
	}

	combineErr := &CombineError{Reason: "combine reason"}
	if combineErr.Error() != "quicraft/shamir: combine failed: combine reason" {
		t.Errorf("CombineError.Error() = %q", combineErr.Error())
	}

	valErr := &ValidationError{Field: "index", Reason: "out of range"}
	if valErr.Error() != "quicraft/shamir: validation failed for index: out of range" {
		t.Errorf("ValidationError.Error() = %q", valErr.Error())
	}
}

func TestCombineBytes_ReconstructsSecret(t *testing.T) {
	secret := []byte("reconstruction test secret")
	values := splitValues(t, secret, 3, 5)

	// A threshold subset reconstructs the original.
	reconstructed, err := CombineBytes(pick(values, 0, 2, 4), 3)
	if err != nil {
		t.Fatalf("CombineBytes failed: %v", err)
	}
	if !bytes.Equal(secret, reconstructed) {
		t.Error("reconstructed secret does not match original")
	}
}

// TestCombineBytes_TamperedShareYieldsGarbageNotError documents the contract:
// shares carry NO secret digest (it would be an offline oracle), so a tampered
// share is NOT self-detected — CombineBytes returns garbage, not an error. The
// caller must authenticate the reconstructed secret out-of-band.
func TestCombineBytes_TamperedShareYieldsGarbageNotError(t *testing.T) {
	secret := []byte("tamper test secret of sufficient length")
	values := splitValues(t, secret, 3, 5)

	// Tamper with one share's y-payload.
	raw := decodeValue(t, values[0])
	raw[len(raw)-1] ^= 0xFF
	values[0] = encodeB64(raw)

	got, err := CombineBytes(pick(values, 0, 1, 2), 3)
	if err != nil {
		t.Fatalf("CombineBytes must not error on a tampered share (no self-verification): %v", err)
	}
	if bytes.Equal(got, secret) {
		t.Fatal("tampered share unexpectedly reconstructed the original secret")
	}
}

// TestCombineBytes_ForgedLowThreshold documents that claiming a lower
// threshold than the split used is not self-detected (no digest): it yields
// garbage that the caller must reject via out-of-band authentication. A SILENT
// correct reconstruction of the real secret from a sub-threshold set would be
// a failure.
func TestCombineBytes_ForgedLowThreshold(t *testing.T) {
	secret := []byte("forged threshold test secret")
	values := splitValues(t, secret, 4, 7)

	got, err := CombineBytes(pick(values, 0, 1), 2)
	if err == nil && bytes.Equal(got, secret) {
		t.Fatal("forged low-threshold combine unexpectedly reconstructed the original secret")
	}
}

func TestSplitBytes_NilSecret(t *testing.T) {
	_, err := SplitBytes(nil, 3, 5)
	if err == nil {
		t.Fatal("expected error for nil secret, got nil")
	}
	var splitErr *SplitError
	if !errors.As(err, &splitErr) {
		t.Fatalf("expected *SplitError, got %T: %v", err, err)
	}
}

// BenchmarkSplitBytes_SmallSecret benchmarks splitting a small secret.
func BenchmarkSplitBytes_SmallSecret(b *testing.B) {
	secret := []byte("Small secret for benchmarking")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = SplitBytes(secret, 3, 5)
	}
}

// BenchmarkSplitBytes_LargeSecret benchmarks splitting a large secret.
func BenchmarkSplitBytes_LargeSecret(b *testing.B) {
	secret := make([]byte, 1024)
	_, _ = rand.Read(secret)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = SplitBytes(secret, 5, 9)
	}
}

// BenchmarkCombineBytes benchmarks combining shares.
func BenchmarkCombineBytes(b *testing.B) {
	secret := []byte("Benchmark secret")
	values := splitValues(b, secret, 3, 5)
	subset := pick(values, 0, 2, 4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CombineBytes(subset, 3)
	}
}

// TestSplitBytes_CoefficientsUsedVerbatim asserts that SplitBytes uses the raw
// crypto/rand coefficient bytes verbatim, without resampling zero top
// coefficients. Excluding c_{t-1}=0 would let a t-1 coalition eliminate one
// candidate secret per byte, so a zero top coefficient MUST be accepted.
// With an all-zeros randReader every sharing polynomial degenerates to its
// constant term, so each share's y-payload must equal the secret exactly.
func TestSplitBytes_CoefficientsUsedVerbatim(t *testing.T) {
	secret := []byte{0x00, 0x01, 0x7F, 0xFF}
	threshold, total := 3, 5

	// All-zero coefficients: p(x) = secret byte for every x.
	restore := testOnlyRandReader(zeroReader{})
	defer restore()
	values := splitValues(t, secret, threshold, total)
	for i, v := range values {
		raw := decodeValue(t, v)
		if !bytes.Equal(raw[rawShareOverhead:], secret) {
			t.Fatalf("share %d: zero coefficients were not used verbatim: y-payload %x, want %x",
				i, raw[rawShareOverhead:], secret)
		}
	}
	got, err := CombineBytes(values[:threshold], threshold)
	if err != nil {
		t.Fatalf("CombineBytes: %v", err)
	}
	if !bytes.Equal(got, secret) {
		t.Fatalf("round-trip with zero top coefficients: got %x, want %x", got, secret)
	}

	// Known non-zero coefficients: shares must match direct polynomial
	// evaluation with exactly the supplied bytes.
	coeffBytes := []byte{0x11, 0x00, 0x22, 0x00, 0x33, 0x00, 0x44, 0x00}
	testOnlyRandReader(bytes.NewReader(coeffBytes))
	values = splitValues(t, secret, threshold, total)
	for i, v := range values {
		raw := decodeValue(t, v)
		x := raw[1]
		for b := range secret {
			coeffs := append([]byte{secret[b]}, coeffBytes[b*(threshold-1):(b+1)*(threshold-1)]...)
			want := gfPolyEval(coeffs, x)
			if raw[rawShareOverhead+b] != want {
				t.Fatalf("share %d byte %d: got %#x, want %#x (coefficients not used verbatim)",
					i, b, raw[rawShareOverhead+b], want)
			}
		}
	}
	got, err = CombineBytes(values[:threshold], threshold)
	if err != nil {
		t.Fatalf("CombineBytes: %v", err)
	}
	if !bytes.Equal(got, secret) {
		t.Fatalf("round-trip with fixed coefficients: got %x, want %x", got, secret)
	}
}

// zeroReader yields an endless stream of zero bytes.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// TestWipe asserts the shared wipe helper zeroes its argument. wipe delegates
// to go-enclave's Wipe, whose zeroing is resistant to dead-store
// elimination; every secret-derived scratch buffer in this package — including
// the per-share raw buffer SplitBytes builds before base64-encoding it — goes
// through it.
func TestWipe(t *testing.T) {
	b := []byte{1, 2, 3, 4, 5}
	wipe(b)
	for i, v := range b {
		if v != 0 {
			t.Fatalf("b[%d] = %d after wipe, want 0", i, v)
		}
	}
	wipe(nil) // must not panic
}

// TestVerifyShare_NilShare pins that a nil share reports a validation failure
// rather than panicking, whether it is the share or one of the others.
func TestVerifyShare_NilShare(t *testing.T) {
	if err := VerifyShare(nil, nil); err == nil {
		t.Fatal("VerifyShare(nil, nil) returned nil, want a validation error")
	}

	valid := splitProtectedShares(t, []byte("root key material"), 2, 3)
	if err := VerifyShare(valid[0], []*ProtectedShare{nil}); err == nil {
		t.Fatal("VerifyShare with a nil other-share returned nil, want a validation error")
	}
}

// TestSplitBytes_RoundTripsThroughCombineBytes pins the wipeable generation
// path: SplitBytes hands back caller-owned buffers that CombineBytes consumes
// directly, so share material never has to become an un-wipeable Go string.
func TestSplitBytes_RoundTripsThroughCombineBytes(t *testing.T) {
	secret := []byte("32-byte-ish root key material...")
	values, err := SplitBytes(secret, 3, 5)
	if err != nil {
		t.Fatalf("SplitBytes: %v", err)
	}
	if len(values) != 5 {
		t.Fatalf("SplitBytes returned %d values, want 5", len(values))
	}

	got, err := CombineBytes([][]byte{values[4], values[1], values[2]}, 3)
	if err != nil {
		t.Fatalf("CombineBytes: %v", err)
	}
	if !bytes.Equal(got, secret) {
		t.Fatalf("reconstructed %q, want %q", got, secret)
	}

	// values[i] must be exactly the value of share Index i+1, so the two
	// generation paths are interchangeable.
	for i, v := range values {
		raw, derr := base64.StdEncoding.DecodeString(string(v))
		if derr != nil {
			t.Fatalf("value %d is not base64: %v", i, derr)
		}
		if raw[0] != shareFormatVersion {
			t.Errorf("value %d has format version %d, want %d", i, raw[0], shareFormatVersion)
		}
		if int(raw[1]) != i+1 {
			t.Errorf("value %d carries x-coordinate %d, want %d", i, raw[1], i+1)
		}
	}

	// The caller owns them and can wipe them.
	for _, v := range values {
		wipe(v)
		for _, b := range v {
			if b != 0 {
				t.Fatal("SplitBytes returned a buffer that could not be wiped")
			}
		}
	}
}

// TestSplitBytes_RejectsInvalidParameters keeps the parameter contract
// pinned for each invalid shape.
func TestSplitBytes_RejectsInvalidParameters(t *testing.T) {
	for _, tc := range []struct {
		name      string
		secret    []byte
		threshold int
		total     int
	}{
		{"threshold below 2", []byte("s"), 1, 3},
		{"total below threshold", []byte("s"), 3, 2},
		{"threshold above 255", []byte("s"), 256, 300},
		{"total above 255", []byte("s"), 2, 256},
		{"empty secret", nil, 2, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := SplitBytes(tc.secret, tc.threshold, tc.total); err == nil {
				t.Fatal("SplitBytes accepted invalid parameters")
			}
		})
	}
}
