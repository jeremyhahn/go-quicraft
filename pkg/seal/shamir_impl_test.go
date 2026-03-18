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

package seal

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// SSSASplitter
// ---------------------------------------------------------------------------

func TestSSSASplitter_Split(t *testing.T) {
	splitter := NewSSSASplitter()
	secret := []byte("this-is-a-32-byte-root-key-val!")

	shares, err := splitter.Split(secret, 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}
	if len(shares) != 5 {
		t.Fatalf("expected 5 shares, got %d", len(shares))
	}

	// Verify each share is non-empty and unique.
	seen := make(map[string]struct{}, len(shares))
	for i, share := range shares {
		if share == "" {
			t.Fatalf("share %d is empty", i)
		}
		if _, dup := seen[share]; dup {
			t.Fatalf("duplicate share at index %d", i)
		}
		seen[share] = struct{}{}
	}
}

func TestSSSASplitter_Split_RoundTrip(t *testing.T) {
	splitter := NewSSSASplitter()

	tests := []struct {
		name      string
		secret    []byte
		threshold int
		total     int
	}{
		{
			name:      "3-of-5 text secret",
			secret:    []byte("quicraft-root-key-material-here"),
			threshold: 3,
			total:     5,
		},
		{
			name:      "2-of-3 binary secret",
			secret:    make([]byte, 32),
			threshold: 2,
			total:     3,
		},
		{
			name:      "2-of-2 minimum shares",
			secret:    []byte("minimal"),
			threshold: 2,
			total:     2,
		},
		{
			name:      "5-of-10 large share set",
			secret:    []byte("larger-share-set-testing-secret!"),
			threshold: 5,
			total:     10,
		},
	}

	// Fill binary secret with random bytes.
	if _, err := rand.Read(tests[1].secret); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shares, err := splitter.Split(tc.secret, tc.threshold, tc.total)
			if err != nil {
				t.Fatalf("Split failed: %v", err)
			}
			if len(shares) != tc.total {
				t.Fatalf("expected %d shares, got %d", tc.total, len(shares))
			}

			// Reconstruct using exactly threshold shares.
			acc := NewSSSAAccumulator(tc.threshold, tc.total, 5*time.Minute)
			ctx := context.Background()
			for i := 0; i < tc.threshold; i++ {
				progress, err := acc.AddShare(ctx, shares[i])
				if err != nil {
					t.Fatalf("AddShare(%d) failed: %v", i, err)
				}
				if i < tc.threshold-1 && progress.Complete {
					t.Fatalf("should not be complete after %d shares", i+1)
				}
			}

			reconstructed, err := acc.Reconstruct(ctx)
			if err != nil {
				t.Fatalf("Reconstruct failed: %v", err)
			}
			if !bytes.Equal(reconstructed, tc.secret) {
				t.Fatalf("reconstructed secret mismatch:\n  got:  %x\n  want: %x", reconstructed, tc.secret)
			}
		})
	}
}

func TestSSSASplitter_Split_InvalidArgs(t *testing.T) {
	splitter := NewSSSASplitter()
	validSecret := []byte("valid-secret-data")

	tests := []struct {
		name      string
		secret    []byte
		threshold int
		total     int
		wantErr   error
	}{
		{
			name:      "nil secret",
			secret:    nil,
			threshold: 2,
			total:     3,
			wantErr:   ErrShamirEmptySecret,
		},
		{
			name:      "empty secret",
			secret:    []byte{},
			threshold: 2,
			total:     3,
			wantErr:   ErrShamirEmptySecret,
		},
		{
			name:      "threshold less than 2",
			secret:    validSecret,
			threshold: 1,
			total:     3,
			wantErr:   ErrShamirThresholdInvalid,
		},
		{
			name:      "threshold zero",
			secret:    validSecret,
			threshold: 0,
			total:     3,
			wantErr:   ErrShamirThresholdInvalid,
		},
		{
			name:      "threshold negative",
			secret:    validSecret,
			threshold: -1,
			total:     3,
			wantErr:   ErrShamirThresholdInvalid,
		},
		{
			name:      "total less than threshold",
			secret:    validSecret,
			threshold: 5,
			total:     3,
			wantErr:   ErrShamirThresholdInvalid,
		},
		{
			name:      "total exceeds 255",
			secret:    validSecret,
			threshold: 2,
			total:     256,
			wantErr:   ErrShamirTotalExceedsMax,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := splitter.Split(tc.secret, tc.threshold, tc.total)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected %v, got %v", tc.wantErr, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SSSAAccumulator
// ---------------------------------------------------------------------------

func TestSSSAAccumulator_AddShare(t *testing.T) {
	splitter := NewSSSASplitter()
	secret := []byte("accumulator-test-secret-data-32!")

	shares, err := splitter.Split(secret, 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	acc := NewSSSAAccumulator(3, 5, 5*time.Minute)
	ctx := context.Background()

	// Submit shares progressively.
	for i := 0; i < 3; i++ {
		progress, err := acc.AddShare(ctx, shares[i])
		if err != nil {
			t.Fatalf("AddShare(%d) failed: %v", i, err)
		}
		if progress.Required != 3 {
			t.Fatalf("expected required=3, got %d", progress.Required)
		}
		if progress.Submitted != i+1 {
			t.Fatalf("expected submitted=%d, got %d", i+1, progress.Submitted)
		}
		expectedComplete := (i + 1) >= 3
		if progress.Complete != expectedComplete {
			t.Fatalf("expected complete=%v at share %d, got %v", expectedComplete, i+1, progress.Complete)
		}
	}
}

func TestSSSAAccumulator_AddShare_Dedup(t *testing.T) {
	splitter := NewSSSASplitter()
	secret := []byte("dedup-test-secret")

	shares, err := splitter.Split(secret, 2, 3)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	acc := NewSSSAAccumulator(2, 3, 5*time.Minute)
	ctx := context.Background()

	// Add the same share twice.
	progress1, err := acc.AddShare(ctx, shares[0])
	if err != nil {
		t.Fatalf("AddShare(0) failed: %v", err)
	}
	if progress1.Submitted != 1 {
		t.Fatalf("expected submitted=1, got %d", progress1.Submitted)
	}

	progress2, err := acc.AddShare(ctx, shares[0])
	if err != nil {
		t.Fatalf("AddShare(0 dup) failed: %v", err)
	}
	if progress2.Submitted != 1 {
		t.Fatalf("expected submitted=1 after dedup, got %d", progress2.Submitted)
	}
	if progress2.Complete {
		t.Fatal("should not be complete with only 1 unique share")
	}

	// Add a different share to reach threshold.
	progress3, err := acc.AddShare(ctx, shares[1])
	if err != nil {
		t.Fatalf("AddShare(1) failed: %v", err)
	}
	if progress3.Submitted != 2 {
		t.Fatalf("expected submitted=2, got %d", progress3.Submitted)
	}
	if !progress3.Complete {
		t.Fatal("expected complete after 2 unique shares (threshold=2)")
	}
}

func TestSSSAAccumulator_AddShare_EmptyShare(t *testing.T) {
	acc := NewSSSAAccumulator(2, 3, 5*time.Minute)
	ctx := context.Background()

	_, err := acc.AddShare(ctx, "")
	if !errors.Is(err, ErrShamirEmptyShare) {
		t.Fatalf("expected ErrShamirEmptyShare, got %v", err)
	}
}

func TestSSSAAccumulator_AddShare_Expired(t *testing.T) {
	// Create an accumulator with a reasonable TTL, then backdate to force expiry.
	acc := NewSSSAAccumulator(2, 3, 5*time.Minute)
	acc.createdAt = time.Now().Add(-10 * time.Minute)

	ctx := context.Background()
	_, err := acc.AddShare(ctx, "some-share")
	if !errors.Is(err, ErrShamirSessionExpired) {
		t.Fatalf("expected ErrShamirSessionExpired, got %v", err)
	}
}

func TestSSSAAccumulator_Reconstruct(t *testing.T) {
	splitter := NewSSSASplitter()
	secret := []byte("reconstruct-round-trip-secret!!")

	shares, err := splitter.Split(secret, 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	acc := NewSSSAAccumulator(3, 5, 5*time.Minute)
	ctx := context.Background()

	// Add exactly threshold shares.
	for i := 0; i < 3; i++ {
		if _, err := acc.AddShare(ctx, shares[i]); err != nil {
			t.Fatalf("AddShare(%d) failed: %v", i, err)
		}
	}

	reconstructed, err := acc.Reconstruct(ctx)
	if err != nil {
		t.Fatalf("Reconstruct failed: %v", err)
	}
	if !bytes.Equal(reconstructed, secret) {
		t.Fatalf("reconstructed secret mismatch:\n  got:  %x\n  want: %x", reconstructed, secret)
	}
}

func TestSSSAAccumulator_Reconstruct_MoreThanThreshold(t *testing.T) {
	splitter := NewSSSASplitter()
	secret := []byte("over-threshold-test")

	shares, err := splitter.Split(secret, 2, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	acc := NewSSSAAccumulator(2, 5, 5*time.Minute)
	ctx := context.Background()

	// Add more shares than threshold (all 5).
	for i := 0; i < 5; i++ {
		if _, err := acc.AddShare(ctx, shares[i]); err != nil {
			t.Fatalf("AddShare(%d) failed: %v", i, err)
		}
	}

	reconstructed, err := acc.Reconstruct(ctx)
	if err != nil {
		t.Fatalf("Reconstruct failed: %v", err)
	}
	if !bytes.Equal(reconstructed, secret) {
		t.Fatalf("reconstructed secret mismatch:\n  got:  %x\n  want: %x", reconstructed, secret)
	}
}

func TestSSSAAccumulator_Reconstruct_InsufficientShares(t *testing.T) {
	splitter := NewSSSASplitter()
	secret := []byte("insufficient-test")

	shares, err := splitter.Split(secret, 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	acc := NewSSSAAccumulator(3, 5, 5*time.Minute)
	ctx := context.Background()

	// Add only 2 shares when 3 are needed.
	for i := 0; i < 2; i++ {
		if _, err := acc.AddShare(ctx, shares[i]); err != nil {
			t.Fatalf("AddShare(%d) failed: %v", i, err)
		}
	}

	_, err = acc.Reconstruct(ctx)
	if !errors.Is(err, ErrShamirThresholdNotMet) {
		t.Fatalf("expected ErrShamirThresholdNotMet, got %v", err)
	}
}

func TestSSSAAccumulator_Reconstruct_NoShares(t *testing.T) {
	acc := NewSSSAAccumulator(2, 3, 5*time.Minute)
	ctx := context.Background()

	_, err := acc.Reconstruct(ctx)
	if !errors.Is(err, ErrShamirThresholdNotMet) {
		t.Fatalf("expected ErrShamirThresholdNotMet, got %v", err)
	}
}

func TestSSSAAccumulator_Reset(t *testing.T) {
	splitter := NewSSSASplitter()
	secret := []byte("reset-test-secret")

	shares, err := splitter.Split(secret, 2, 3)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	acc := NewSSSAAccumulator(2, 3, 5*time.Minute)
	ctx := context.Background()

	// Add a share.
	progress, err := acc.AddShare(ctx, shares[0])
	if err != nil {
		t.Fatalf("AddShare failed: %v", err)
	}
	if progress.Submitted != 1 {
		t.Fatalf("expected submitted=1, got %d", progress.Submitted)
	}

	// Reset clears all state.
	acc.Reset()

	// Verify shares are cleared by checking Reconstruct fails.
	_, err = acc.Reconstruct(ctx)
	if !errors.Is(err, ErrShamirThresholdNotMet) {
		t.Fatalf("expected ErrShamirThresholdNotMet after reset, got %v", err)
	}

	// Verify we can add shares again after reset.
	progress, err = acc.AddShare(ctx, shares[0])
	if err != nil {
		t.Fatalf("AddShare after reset failed: %v", err)
	}
	if progress.Submitted != 1 {
		t.Fatalf("expected submitted=1 after reset, got %d", progress.Submitted)
	}
}

func TestSSSAAccumulator_Reset_RestartsTTL(t *testing.T) {
	// Create with a 5-minute TTL, then backdate createdAt to force expiration.
	acc := NewSSSAAccumulator(2, 3, 5*time.Minute)
	acc.createdAt = time.Now().Add(-10 * time.Minute)

	if !acc.Expired() {
		t.Fatal("expected accumulator to be expired before reset")
	}

	acc.Reset()

	// After reset, createdAt is refreshed so the 5-minute TTL is not exceeded.
	if acc.Expired() {
		t.Fatal("expected accumulator NOT to be expired after reset")
	}
}

func TestSSSAAccumulator_Expired(t *testing.T) {
	t.Run("not expired within TTL", func(t *testing.T) {
		acc := NewSSSAAccumulator(2, 3, 5*time.Minute)
		if acc.Expired() {
			t.Fatal("freshly created accumulator should not be expired")
		}
	})

	t.Run("expired after TTL", func(t *testing.T) {
		acc := NewSSSAAccumulator(2, 3, 5*time.Minute)
		// Backdate createdAt to ensure TTL is exceeded.
		acc.createdAt = time.Now().Add(-10 * time.Minute)
		if !acc.Expired() {
			t.Fatal("accumulator should be expired after TTL exceeded")
		}
	})
}

func TestSSSAAccumulator_GetProgress(t *testing.T) {
	splitter := NewSSSASplitter()
	secret := []byte("getprogress-test-secret-data!!")

	shares, err := splitter.Split(secret, 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	acc := NewSSSAAccumulator(3, 5, 5*time.Minute)
	ctx := context.Background()

	// GetProgress before any shares added.
	progress := acc.GetProgress()
	if progress == nil {
		t.Fatal("expected non-nil progress")
	}
	if progress.Required != 3 {
		t.Fatalf("expected Required=3, got %d", progress.Required)
	}
	if progress.Submitted != 0 {
		t.Fatalf("expected Submitted=0, got %d", progress.Submitted)
	}
	if progress.Complete {
		t.Fatal("expected Complete=false with no shares")
	}

	// Add one share.
	if _, addErr := acc.AddShare(ctx, shares[0]); addErr != nil {
		t.Fatalf("AddShare failed: %v", addErr)
	}

	progress = acc.GetProgress()
	if progress.Submitted != 1 {
		t.Fatalf("expected Submitted=1, got %d", progress.Submitted)
	}
	if progress.Complete {
		t.Fatal("expected Complete=false with 1 share")
	}

	// Add two more shares to reach threshold.
	if _, addErr := acc.AddShare(ctx, shares[1]); addErr != nil {
		t.Fatalf("AddShare failed: %v", addErr)
	}
	if _, addErr := acc.AddShare(ctx, shares[2]); addErr != nil {
		t.Fatalf("AddShare failed: %v", addErr)
	}

	progress = acc.GetProgress()
	if progress.Submitted != 3 {
		t.Fatalf("expected Submitted=3, got %d", progress.Submitted)
	}
	if !progress.Complete {
		t.Fatal("expected Complete=true at threshold")
	}
}

func TestSSSAAccumulator_GetProgress_EmptyAccumulator(t *testing.T) {
	acc := NewSSSAAccumulator(2, 3, 5*time.Minute)

	progress := acc.GetProgress()
	if progress == nil {
		t.Fatal("expected non-nil progress")
	}
	if progress.Required != 2 {
		t.Fatalf("expected Required=2, got %d", progress.Required)
	}
	if progress.Submitted != 0 {
		t.Fatalf("expected Submitted=0, got %d", progress.Submitted)
	}
	if progress.Complete {
		t.Fatal("expected Complete=false")
	}
}

func TestSSSAAccumulator_GetProgress_Concurrent(t *testing.T) {
	splitter := NewSSSASplitter()
	secret := []byte("concurrent-getprogress-test!!")

	shares, err := splitter.Split(secret, 2, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	acc := NewSSSAAccumulator(2, 5, 5*time.Minute)
	ctx := context.Background()

	const numReaders = 10
	const numWriters = 3
	done := make(chan struct{})

	// Start reader goroutines (GetProgress).
	for i := 0; i < numReaders; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				progress := acc.GetProgress()
				if progress == nil {
					t.Error("GetProgress returned nil")
					return
				}
				if progress.Required != 2 {
					t.Errorf("expected Required=2, got %d", progress.Required)
					return
				}
			}
		}()
	}

	// Start writer goroutines (AddShare).
	for i := 0; i < numWriters; i++ {
		shareIdx := i % len(shares)
		go func(idx int) {
			defer func() { done <- struct{}{} }()
			_, _ = acc.AddShare(ctx, shares[idx])
		}(shareIdx)
	}

	// Wait for all goroutines.
	for i := 0; i < numReaders+numWriters; i++ {
		<-done
	}

	// The test passes if no race conditions are detected.
}

// ---------------------------------------------------------------------------
// Interface compliance (compile-time checks)
// ---------------------------------------------------------------------------

var _ ShareSplitter = (*SSSASplitter)(nil)
var _ ShareAccumulator = (*SSSAAccumulator)(nil)

// ---------------------------------------------------------------------------
// Error type assertions
// ---------------------------------------------------------------------------

func TestShamirSplitError_Unwrap(t *testing.T) {
	inner := errors.New("sssa create failed")
	err := &ShamirSplitError{Err: inner}

	if err.Error() != "quicraft/seal: shamir split failed: sssa create failed" {
		t.Fatalf("unexpected error message: %s", err.Error())
	}
	if !errors.Is(err, inner) {
		t.Fatal("Unwrap should return the inner error")
	}
}

func TestShamirCombineError_Unwrap(t *testing.T) {
	inner := errors.New("invalid share")
	err := &ShamirCombineError{Err: inner}

	if err.Error() != "quicraft/seal: shamir combine failed: invalid share" {
		t.Fatalf("unexpected error message: %s", err.Error())
	}
	if !errors.Is(err, inner) {
		t.Fatal("Unwrap should return the inner error")
	}
}

func TestShamirDecodeError_Unwrap(t *testing.T) {
	inner := errors.New("hex decode fail")
	err := &ShamirDecodeError{Err: inner}

	if err.Error() != "quicraft/seal: shamir hex decode failed: hex decode fail" {
		t.Fatalf("unexpected error message: %s", err.Error())
	}
	if !errors.Is(err, inner) {
		t.Fatal("Unwrap should return the inner error")
	}
}

// ---------------------------------------------------------------------------
// Full end-to-end: binary secret round-trip with random key material
// ---------------------------------------------------------------------------

func TestSSSA_EndToEnd_RandomSecret(t *testing.T) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	splitter := NewSSSASplitter()
	shares, err := splitter.Split(secret, 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	// Use shares [1], [3], [4] (non-contiguous subset) to prove any
	// threshold-sized subset works.
	acc := NewSSSAAccumulator(3, 5, 5*time.Minute)
	ctx := context.Background()

	for _, idx := range []int{1, 3, 4} {
		if _, err := acc.AddShare(ctx, shares[idx]); err != nil {
			t.Fatalf("AddShare(shares[%d]) failed: %v", idx, err)
		}
	}

	reconstructed, err := acc.Reconstruct(ctx)
	if err != nil {
		t.Fatalf("Reconstruct failed: %v", err)
	}
	if !bytes.Equal(reconstructed, secret) {
		t.Fatalf("round-trip failed:\n  got:  %x\n  want: %x", reconstructed, secret)
	}
}

// ---------------------------------------------------------------------------
// Concurrent access tests (thread-safety verification)
// ---------------------------------------------------------------------------

func TestSSSAAccumulator_ConcurrentAddShare(t *testing.T) {
	splitter := NewSSSASplitter()
	secret := []byte("concurrent-test-secret-material!")

	shares, err := splitter.Split(secret, 3, 10)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	acc := NewSSSAAccumulator(3, 10, 5*time.Minute)
	ctx := context.Background()

	// Launch multiple goroutines to add shares concurrently.
	const numWorkers = 10
	done := make(chan struct{})

	for i := 0; i < numWorkers; i++ {
		shareIdx := i % len(shares)
		go func(idx int) {
			defer func() { done <- struct{}{} }()
			_, addErr := acc.AddShare(ctx, shares[idx])
			if addErr != nil {
				t.Errorf("AddShare(%d) failed: %v", idx, addErr)
			}
		}(shareIdx)
	}

	// Wait for all workers to complete.
	for i := 0; i < numWorkers; i++ {
		<-done
	}

	// Verify accumulator state is consistent after concurrent access.
	// Due to deduplication, we should have exactly len(shares) unique entries.
	progress, err := acc.AddShare(ctx, "trigger-check")
	if err != nil {
		t.Fatalf("final AddShare failed: %v", err)
	}
	// We added 10 unique shares + 1 trigger = 11, but threshold is 3.
	if progress.Submitted != 11 {
		t.Fatalf("expected 11 unique shares, got %d", progress.Submitted)
	}
	if !progress.Complete {
		t.Fatal("expected quorum to be complete")
	}
}

func TestSSSAAccumulator_ConcurrentAddShareAndReconstruct(t *testing.T) {
	splitter := NewSSSASplitter()
	secret := []byte("concurrent-reconstruct-test-sec!")

	shares, err := splitter.Split(secret, 3, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	acc := NewSSSAAccumulator(3, 5, 5*time.Minute)
	ctx := context.Background()

	// Add exactly threshold shares first.
	for i := 0; i < 3; i++ {
		if _, addErr := acc.AddShare(ctx, shares[i]); addErr != nil {
			t.Fatalf("AddShare(%d) failed: %v", i, addErr)
		}
	}

	// Now run concurrent Reconstruct calls while also adding more shares.
	const numReaders = 5
	const numWriters = 2
	done := make(chan struct{})
	errChan := make(chan error, numReaders+numWriters)

	// Start reader goroutines (Reconstruct).
	for i := 0; i < numReaders; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			reconstructed, reconstructErr := acc.Reconstruct(ctx)
			if reconstructErr != nil {
				errChan <- reconstructErr
				return
			}
			if !bytes.Equal(reconstructed, secret) {
				errChan <- errors.New("reconstructed secret mismatch")
			}
		}()
	}

	// Start writer goroutines (AddShare with remaining shares).
	for i := 0; i < numWriters; i++ {
		shareIdx := 3 + i
		go func(idx int) {
			defer func() { done <- struct{}{} }()
			_, addErr := acc.AddShare(ctx, shares[idx])
			if addErr != nil {
				errChan <- addErr
			}
		}(shareIdx)
	}

	// Wait for all goroutines.
	for i := 0; i < numReaders+numWriters; i++ {
		<-done
	}
	close(errChan)

	for err := range errChan {
		t.Errorf("concurrent operation error: %v", err)
	}
}

func TestSSSAAccumulator_ConcurrentReset(t *testing.T) {
	splitter := NewSSSASplitter()
	secret := []byte("concurrent-reset-test-secret!!")

	shares, err := splitter.Split(secret, 2, 5)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	acc := NewSSSAAccumulator(2, 5, 5*time.Minute)
	ctx := context.Background()

	const iterations = 100
	done := make(chan struct{})

	// Writer 1: repeatedly adds shares.
	go func() {
		defer func() { done <- struct{}{} }()
		for i := 0; i < iterations; i++ {
			shareIdx := i % len(shares)
			_, _ = acc.AddShare(ctx, shares[shareIdx])
		}
	}()

	// Writer 2: repeatedly resets.
	go func() {
		defer func() { done <- struct{}{} }()
		for i := 0; i < iterations/10; i++ {
			acc.Reset()
		}
	}()

	// Reader: repeatedly checks Expired.
	go func() {
		defer func() { done <- struct{}{} }()
		for i := 0; i < iterations; i++ {
			_ = acc.Expired()
		}
	}()

	// Wait for all goroutines.
	for i := 0; i < 3; i++ {
		<-done
	}

	// No panics or data races means success.
	// The race detector (go test -race) will catch any issues.
}

func TestSSSAAccumulator_ConcurrentExpiredCheck(t *testing.T) {
	acc := NewSSSAAccumulator(2, 3, 5*time.Minute)

	const numGoroutines = 50
	done := make(chan struct{})

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				_ = acc.Expired()
			}
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// The test passes if no race conditions are detected.
}
