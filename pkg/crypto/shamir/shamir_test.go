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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestSplit_Success(t *testing.T) {
	secret := []byte("This is a secret message for testing!")
	threshold := 3
	total := 5

	shares, err := Split(secret, threshold, total)
	if err != nil {
		t.Fatalf("Split returned unexpected error: %v", err)
	}
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
		if share.Value == "" {
			t.Errorf("share %d: value is empty", i)
		}
		if err := share.Validate(); err != nil {
			t.Errorf("share %d: validation failed: %v", i, err)
		}
	}
}

func TestSplit_InvalidThreshold(t *testing.T) {
	_, err := Split([]byte("secret"), 1, 5)
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

func TestSplit_TotalLessThanThreshold(t *testing.T) {
	_, err := Split([]byte("secret"), 5, 3)
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

func TestSplit_ThresholdExceeds255(t *testing.T) {
	_, err := Split([]byte("secret"), 256, 300)
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

func TestSplit_TotalExceeds255(t *testing.T) {
	_, err := Split([]byte("secret"), 3, 256)
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

func TestSplit_EmptySecret(t *testing.T) {
	_, err := Split([]byte{}, 3, 5)
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

func TestCombine_Success(t *testing.T) {
	secret := []byte("Reconstruct this secret perfectly")
	threshold := 3
	total := 5

	shares, err := Split(secret, threshold, total)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	reconstructed, err := Combine(shares)
	if err != nil {
		t.Fatalf("Combine failed: %v", err)
	}

	if !bytes.Equal(secret, reconstructed) {
		t.Errorf("reconstructed secret does not match original\noriginal:      %q\nreconstructed: %q", secret, reconstructed)
	}
}

func TestCombine_SubsetOfShares(t *testing.T) {
	secret := []byte("Only need threshold shares")
	threshold := 3
	total := 7

	shares, err := Split(secret, threshold, total)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	// Use exactly threshold shares (not all of them)
	subsets := [][]*Share{
		{shares[0], shares[2], shares[4]},
		{shares[1], shares[3], shares[6]},
		{shares[0], shares[5], shares[6]},
	}

	for i, subset := range subsets {
		reconstructed, err := Combine(subset)
		if err != nil {
			t.Fatalf("subset %d: Combine failed: %v", i, err)
		}
		if !bytes.Equal(secret, reconstructed) {
			t.Errorf("subset %d: reconstructed secret does not match original", i)
		}
	}
}

func TestCombine_NoShares(t *testing.T) {
	_, err := Combine([]*Share{})
	if err == nil {
		t.Fatal("expected error for empty shares, got nil")
	}
	var combineErr *CombineError
	if !errors.As(err, &combineErr) {
		t.Fatalf("expected *CombineError, got %T: %v", err, err)
	}
	if combineErr.Reason != "no shares provided" {
		t.Errorf("unexpected reason: %s", combineErr.Reason)
	}
}

func TestCombine_InsufficientShares(t *testing.T) {
	secret := []byte("need more shares")
	threshold := 4
	total := 7

	shares, err := Split(secret, threshold, total)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	// Provide fewer than threshold
	subset := []*Share{shares[0], shares[2], shares[5]}
	_, err = Combine(subset)
	if err == nil {
		t.Fatal("expected error for insufficient shares, got nil")
	}
	var combineErr *CombineError
	if !errors.As(err, &combineErr) {
		t.Fatalf("expected *CombineError, got %T: %v", err, err)
	}
	expected := fmt.Sprintf("need at least %d shares, got %d", threshold, len(subset))
	if combineErr.Reason != expected {
		t.Errorf("unexpected reason: %s (expected: %s)", combineErr.Reason, expected)
	}
}

func TestCombine_MismatchedThreshold(t *testing.T) {
	shares := []*Share{
		{Index: 1, Threshold: 3, Total: 5, Value: "dGVzdA=="},
		{Index: 2, Threshold: 4, Total: 5, Value: "dGVzdA=="},
		{Index: 3, Threshold: 3, Total: 5, Value: "dGVzdA=="},
	}

	_, err := Combine(shares)
	if err == nil {
		t.Fatal("expected error for mismatched threshold, got nil")
	}
	var combineErr *CombineError
	if !errors.As(err, &combineErr) {
		t.Fatalf("expected *CombineError, got %T: %v", err, err)
	}
	expected := "share 1 has different threshold (4) than share 0 (3)"
	if combineErr.Reason != expected {
		t.Errorf("unexpected reason: %s (expected: %s)", combineErr.Reason, expected)
	}
}

func TestCombine_MismatchedTotal(t *testing.T) {
	shares := []*Share{
		{Index: 1, Threshold: 3, Total: 5, Value: "dGVzdA=="},
		{Index: 2, Threshold: 3, Total: 7, Value: "dGVzdA=="},
		{Index: 3, Threshold: 3, Total: 5, Value: "dGVzdA=="},
	}

	_, err := Combine(shares)
	if err == nil {
		t.Fatal("expected error for mismatched total, got nil")
	}
	var combineErr *CombineError
	if !errors.As(err, &combineErr) {
		t.Fatalf("expected *CombineError, got %T: %v", err, err)
	}
	expected := "share 1 has different total (7) than share 0 (5)"
	if combineErr.Reason != expected {
		t.Errorf("unexpected reason: %s (expected: %s)", combineErr.Reason, expected)
	}
}

func TestCombine_InvalidShare(t *testing.T) {
	shares := []*Share{
		{Index: 1, Threshold: 3, Total: 5, Value: "dGVzdA=="},
		{Index: 2, Threshold: 3, Total: 5, Value: ""}, // empty value fails validation
		{Index: 3, Threshold: 3, Total: 5, Value: "dGVzdA=="},
	}

	_, err := Combine(shares)
	if err == nil {
		t.Fatal("expected error for invalid share, got nil")
	}
	var combineErr *CombineError
	if !errors.As(err, &combineErr) {
		t.Fatalf("expected *CombineError, got %T: %v", err, err)
	}
	if combineErr.Reason == "" {
		t.Error("expected non-empty reason")
	}
}

func TestCombine_InvalidBase64(t *testing.T) {
	shares, err := Split([]byte("test secret"), 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	// Corrupt one share's base64 value
	shares[1].Value = "!!!invalid-base64!!!"

	_, err = Combine([]*Share{shares[0], shares[1], shares[2]})
	if err == nil {
		t.Fatal("expected error for invalid base64, got nil")
	}
	var combineErr *CombineError
	if !errors.As(err, &combineErr) {
		t.Fatalf("expected *CombineError, got %T: %v", err, err)
	}
}

func TestVerifyShare_Success(t *testing.T) {
	shares, err := Split([]byte("verify me"), 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	// Verify each share against the others
	for i, share := range shares {
		others := make([]*Share, 0, len(shares)-1)
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
	badShare := &Share{
		Index:     0, // invalid: must be >= 1
		Threshold: 3,
		Total:     5,
		Value:     "dGVzdA==",
	}

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
	shares, err := Split([]byte("duplicate test"), 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	// Create a share with the same index as shares[0]
	duplicate := &Share{
		Index:     shares[0].Index,
		Threshold: shares[0].Threshold,
		Total:     shares[0].Total,
		Value:     "dGVzdA==",
	}

	err = VerifyShare(duplicate, shares)
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
	shares, err := Split([]byte("threshold test"), 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	badShare := &Share{
		Index:     4, // valid index within total
		Threshold: 4, // different threshold
		Total:     5,
		Value:     "dGVzdA==",
	}

	err = VerifyShare(badShare, shares[:2])
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
	shares, err := Split([]byte("total test"), 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	badShare := &Share{
		Index:     4,
		Threshold: 3,
		Total:     9, // different total
		Value:     "dGVzdA==",
	}

	err = VerifyShare(badShare, shares[:2])
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

func TestShare_Bytes(t *testing.T) {
	shares, err := Split([]byte("bytes test secret"), 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	for i, share := range shares {
		b, err := share.Bytes()
		if err != nil {
			t.Errorf("share %d: Bytes() returned error: %v", i, err)
		}
		if len(b) == 0 {
			t.Errorf("share %d: Bytes() returned empty slice", i)
		}
	}
}

func TestShare_Bytes_InvalidBase64(t *testing.T) {
	share := &Share{
		Index:     1,
		Threshold: 3,
		Total:     5,
		Value:     "!!!not-valid-base64!!!",
	}

	_, err := share.Bytes()
	if err == nil {
		t.Fatal("expected error for invalid base64, got nil")
	}
}

func TestShare_String(t *testing.T) {
	shares, err := Split([]byte("string test"), 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	for i, share := range shares {
		s := share.String()
		expected := fmt.Sprintf("Index: %d", i+1)
		if len(s) == 0 {
			t.Errorf("share %d: String() returned empty", i)
		}
		if !strings.Contains(s, expected) {
			t.Errorf("share %d: String() %q does not contain %q", i, s, expected)
		}
		if !strings.Contains(s, "Share{Index:") {
			t.Errorf("share %d: String() %q does not contain 'Share{Index:'", i, s)
		}
	}
}

func TestShare_String_ShortValue(t *testing.T) {
	share := &Share{
		Index:     1,
		Threshold: 3,
		Total:     5,
		Value:     "abc",
	}
	s := share.String()
	if !strings.Contains(s, "abc") {
		t.Errorf("String() %q does not contain short value 'abc'", s)
	}
}

func TestShare_Validate_AllCases(t *testing.T) {
	tests := []struct {
		name      string
		share     *Share
		wantErr   bool
		wantField string
	}{
		{
			name:    "valid share",
			share:   &Share{Index: 1, Threshold: 3, Total: 5, Value: "dGVzdA=="},
			wantErr: false,
		},
		{
			name:      "index zero",
			share:     &Share{Index: 0, Threshold: 3, Total: 5, Value: "dGVzdA=="},
			wantErr:   true,
			wantField: "index",
		},
		{
			name:      "index negative",
			share:     &Share{Index: -1, Threshold: 3, Total: 5, Value: "dGVzdA=="},
			wantErr:   true,
			wantField: "index",
		},
		{
			name:      "threshold too low",
			share:     &Share{Index: 1, Threshold: 1, Total: 5, Value: "dGVzdA=="},
			wantErr:   true,
			wantField: "threshold",
		},
		{
			name:      "threshold zero",
			share:     &Share{Index: 1, Threshold: 0, Total: 5, Value: "dGVzdA=="},
			wantErr:   true,
			wantField: "threshold",
		},
		{
			name:      "total less than threshold",
			share:     &Share{Index: 1, Threshold: 5, Total: 3, Value: "dGVzdA=="},
			wantErr:   true,
			wantField: "total",
		},
		{
			name:      "index exceeds total",
			share:     &Share{Index: 6, Threshold: 3, Total: 5, Value: "dGVzdA=="},
			wantErr:   true,
			wantField: "index",
		},
		{
			name:      "empty value",
			share:     &Share{Index: 1, Threshold: 3, Total: 5, Value: ""},
			wantErr:   true,
			wantField: "value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.share.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				var valErr *ValidationError
				if !errors.As(err, &valErr) {
					t.Fatalf("expected *ValidationError, got %T: %v", err, err)
				}
				if valErr.Field != tt.wantField {
					t.Errorf("expected field %q, got %q", tt.wantField, valErr.Field)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestShare_MarshalUnmarshal(t *testing.T) {
	original := &Share{
		Index:     2,
		Threshold: 3,
		Total:     5,
		Value:     "dGVzdCBkYXRh",
		Metadata:  map[string]string{"owner": "alice", "created": "2026-01-15"},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	var decoded Share
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("UnmarshalJSON failed: %v", err)
	}

	if decoded.Index != original.Index {
		t.Errorf("Index: expected %d, got %d", original.Index, decoded.Index)
	}
	if decoded.Threshold != original.Threshold {
		t.Errorf("Threshold: expected %d, got %d", original.Threshold, decoded.Threshold)
	}
	if decoded.Total != original.Total {
		t.Errorf("Total: expected %d, got %d", original.Total, decoded.Total)
	}
	if decoded.Value != original.Value {
		t.Errorf("Value: expected %q, got %q", original.Value, decoded.Value)
	}
	if decoded.Metadata["owner"] != "alice" {
		t.Errorf("Metadata[owner]: expected 'alice', got %q", decoded.Metadata["owner"])
	}
	if decoded.Metadata["created"] != "2026-01-15" {
		t.Errorf("Metadata[created]: expected '2026-01-15', got %q", decoded.Metadata["created"])
	}
}

func TestShare_UnmarshalJSON_InvalidData(t *testing.T) {
	var share Share
	err := share.UnmarshalJSON([]byte("not valid json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestShare_MarshalUnmarshal_NoMetadata(t *testing.T) {
	original := &Share{
		Index:     1,
		Threshold: 2,
		Total:     3,
		Value:     "dGVzdA==",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	var decoded Share
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("UnmarshalJSON failed: %v", err)
	}

	if decoded.Index != original.Index {
		t.Errorf("Index: expected %d, got %d", original.Index, decoded.Index)
	}
	if decoded.Value != original.Value {
		t.Errorf("Value: expected %q, got %q", original.Value, decoded.Value)
	}
}

func TestSplit_BinarySecret(t *testing.T) {
	secret := make([]byte, 256)
	if _, err := rand.Read(secret); err != nil {
		t.Fatalf("failed to generate random secret: %v", err)
	}

	shares, err := Split(secret, 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	reconstructed, err := Combine([]*Share{shares[1], shares[2], shares[4]})
	if err != nil {
		t.Fatalf("Combine failed: %v", err)
	}

	if !bytes.Equal(secret, reconstructed) {
		t.Error("reconstructed binary secret does not match original")
	}
}

func TestSplit_LargeSecret(t *testing.T) {
	secret := make([]byte, 1024)
	if _, err := rand.Read(secret); err != nil {
		t.Fatalf("failed to generate random secret: %v", err)
	}

	shares, err := Split(secret, 5, 9)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}
	if len(shares) != 9 {
		t.Fatalf("expected 9 shares, got %d", len(shares))
	}

	subset := []*Share{shares[0], shares[2], shares[4], shares[6], shares[8]}
	reconstructed, err := Combine(subset)
	if err != nil {
		t.Fatalf("Combine failed: %v", err)
	}

	if !bytes.Equal(secret, reconstructed) {
		t.Error("reconstructed large secret does not match original")
	}
}

func TestCombine_MoreThanThreshold(t *testing.T) {
	secret := []byte("more than threshold test")
	threshold := 3
	total := 5

	shares, err := Split(secret, threshold, total)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	// Use 4 shares (more than threshold of 3)
	subset := []*Share{shares[0], shares[1], shares[3], shares[4]}
	reconstructed, err := Combine(subset)
	if err != nil {
		t.Fatalf("Combine failed: %v", err)
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

func TestCombine_DigestVerification(t *testing.T) {
	secret := []byte("digest verification test secret")
	shares, err := Split(secret, 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	// All shares should carry the same digest.
	for i := 1; i < len(shares); i++ {
		if shares[i].Digest != shares[0].Digest {
			t.Fatalf("share %d digest differs from share 0", i)
		}
	}

	// Combine with correct shares should succeed.
	reconstructed, err := Combine([]*Share{shares[0], shares[2], shares[4]})
	if err != nil {
		t.Fatalf("Combine failed: %v", err)
	}
	if !bytes.Equal(secret, reconstructed) {
		t.Error("reconstructed secret does not match original")
	}
}

func TestCombine_TamperedShareFailsDigest(t *testing.T) {
	secret := []byte("tamper test secret")
	shares, err := Split(secret, 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	// Tamper with one share's digest to simulate a forged share set.
	shares[0].Digest[0] ^= 0xFF

	_, err = Combine([]*Share{shares[0], shares[1], shares[2]})
	if err == nil {
		t.Fatal("expected error for tampered share digest, got nil")
	}
	var intErr *IntegrityError
	if !errors.As(err, &intErr) {
		t.Fatalf("expected *IntegrityError, got %T: %v", err, err)
	}
}

func TestCombine_ForgedLowThresholdFails(t *testing.T) {
	secret := []byte("forged threshold test")
	shares, err := Split(secret, 4, 7)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	// Forge a lower threshold on all shares. With only 2 shares and
	// the real threshold being 4, reconstruction should produce wrong
	// data and fail digest verification.
	forged := []*Share{shares[0], shares[1]}
	for _, s := range forged {
		s.Threshold = 2
		s.Total = 3
	}

	_, err = Combine(forged)
	if err == nil {
		t.Fatal("expected error for forged threshold, got nil")
	}
	// The error should be either IntegrityError (digest mismatch) or
	// CombineError (sssa combine failure with wrong share count).
	var intErr *IntegrityError
	var combErr *CombineError
	if !errors.As(err, &intErr) && !errors.As(err, &combErr) {
		t.Fatalf("expected *IntegrityError or *CombineError, got %T: %v", err, err)
	}
}

func TestIntegrityError_Format(t *testing.T) {
	e := &IntegrityError{}
	want := "quicraft/shamir: integrity verification failed: reconstructed secret digest does not match share digest"
	if e.Error() != want {
		t.Errorf("IntegrityError.Error() = %q, want %q", e.Error(), want)
	}
}

func TestSplit_NilSecret(t *testing.T) {
	_, err := Split(nil, 3, 5)
	if err == nil {
		t.Fatal("expected error for nil secret, got nil")
	}
	var splitErr *SplitError
	if !errors.As(err, &splitErr) {
		t.Fatalf("expected *SplitError, got %T: %v", err, err)
	}
}

// BenchmarkSplit_SmallSecret benchmarks splitting a small secret.
func BenchmarkSplit_SmallSecret(b *testing.B) {
	secret := []byte("Small secret for benchmarking")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Split(secret, 3, 5)
	}
}

// BenchmarkSplit_LargeSecret benchmarks splitting a large secret.
func BenchmarkSplit_LargeSecret(b *testing.B) {
	secret := make([]byte, 1024)
	_, _ = rand.Read(secret)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Split(secret, 5, 9)
	}
}

// BenchmarkCombine benchmarks combining shares.
func BenchmarkCombine(b *testing.B) {
	secret := []byte("Benchmark secret")
	shares, _ := Split(secret, 3, 5)
	subset := []*Share{shares[0], shares[2], shares[4]}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Combine(subset)
	}
}
