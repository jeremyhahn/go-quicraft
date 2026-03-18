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
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir"
)

// Shamir sentinel errors for typed error handling.
var (
	// ErrShamirEmptySecret is returned when Split receives a nil or empty secret.
	ErrShamirEmptySecret = errors.New("quicraft/seal: secret must not be empty")

	// ErrShamirTotalExceedsMax is returned when total shares exceeds 255.
	ErrShamirTotalExceedsMax = errors.New("quicraft/seal: total shares must not exceed 255")

	// ErrShamirSessionExpired is returned when AddShare is called on an
	// expired accumulator session.
	ErrShamirSessionExpired = errors.New("quicraft/seal: accumulator session expired")

	// ErrShamirThresholdNotMet is returned when Reconstruct is called before
	// the threshold number of shares has been accumulated.
	ErrShamirThresholdNotMet = errors.New("quicraft/seal: threshold not met, insufficient shares")

	// ErrShamirEmptyShare is returned when AddShare receives an empty string.
	ErrShamirEmptyShare = errors.New("quicraft/seal: share must not be empty")
)

// ShamirSplitError is returned when the underlying Shamir library fails to split
// a secret into shares.
type ShamirSplitError struct {
	Err error
}

// Error returns a human-readable description of the split failure.
func (e *ShamirSplitError) Error() string {
	return fmt.Sprintf("quicraft/seal: shamir split failed: %v", e.Err)
}

// Unwrap returns the underlying error.
func (e *ShamirSplitError) Unwrap() error {
	return e.Err
}

// ShamirCombineError is returned when the underlying Shamir library fails to
// reconstruct a secret from the accumulated shares.
type ShamirCombineError struct {
	Err error
}

// Error returns a human-readable description of the combine failure.
func (e *ShamirCombineError) Error() string {
	return fmt.Sprintf("quicraft/seal: shamir combine failed: %v", e.Err)
}

// Unwrap returns the underlying error.
func (e *ShamirCombineError) Unwrap() error {
	return e.Err
}

// ShamirDecodeError is returned when the reconstructed hex string cannot be
// decoded back to the original secret bytes.
type ShamirDecodeError struct {
	Err error
}

// Error returns a human-readable description of the decode failure.
func (e *ShamirDecodeError) Error() string {
	return fmt.Sprintf("quicraft/seal: shamir hex decode failed: %v", e.Err)
}

// Unwrap returns the underlying error.
func (e *ShamirDecodeError) Unwrap() error {
	return e.Err
}

// SSSASplitter implements ShareSplitter using the canonical crypto/shamir
// package for Shamir's Secret Sharing. The underlying implementation
// hex-encodes the secret before splitting so that arbitrary binary data can
// be processed by the string-based SSSA library.
type SSSASplitter struct{}

// NewSSSASplitter returns a new SSSASplitter.
func NewSSSASplitter() *SSSASplitter {
	return &SSSASplitter{}
}

// Split divides the secret into the specified number of shares using Shamir's
// Secret Sharing Algorithm. At least threshold shares are required to
// reconstruct the original secret.
//
// Constraints:
//   - threshold must be >= 2
//   - total must be >= threshold
//   - total must be <= 255
//   - secret must not be nil or empty
func (s *SSSASplitter) Split(secret []byte, threshold, total int) ([]string, error) {
	if len(secret) == 0 {
		return nil, ErrShamirEmptySecret
	}
	if threshold < 2 || total < threshold {
		return nil, ErrShamirThresholdInvalid
	}
	if total > 255 {
		return nil, ErrShamirTotalExceedsMax
	}

	structuredShares, err := shamir.Split(secret, threshold, total)
	if err != nil {
		return nil, &ShamirSplitError{Err: err}
	}

	// Convert structured shares back to raw SSSA strings for the
	// string-based ShareSplitter interface used by the barrier ceremony.
	rawShares := make([]string, len(structuredShares))
	for i, share := range structuredShares {
		decoded, decErr := base64.StdEncoding.DecodeString(share.Value)
		if decErr != nil {
			return nil, &ShamirSplitError{Err: decErr}
		}
		rawShares[i] = string(decoded)
	}

	return rawShares, nil
}

// SSSAAccumulator implements ShareAccumulator by collecting Shamir shares from
// individual custodians until the threshold is met. It handles deduplication of
// duplicate share submissions and enforces a TTL-based session expiry.
// All methods are safe for concurrent use.
type SSSAAccumulator struct {
	mu        sync.RWMutex
	shares    []string
	seen      map[string]struct{}
	threshold int
	total     int
	ttl       time.Duration
	createdAt time.Time
}

// NewSSSAAccumulator returns a new SSSAAccumulator configured with the given
// threshold, total share count, and session TTL. The TTL timer starts from the
// moment of construction.
func NewSSSAAccumulator(threshold, total int, ttl time.Duration) *SSSAAccumulator {
	return &SSSAAccumulator{
		shares:    make([]string, 0, threshold),
		seen:      make(map[string]struct{}, threshold),
		threshold: threshold,
		total:     total,
		ttl:       ttl,
		createdAt: time.Now(),
	}
}

// AddShare adds a single share to the accumulator. Duplicate shares are silently
// deduplicated. Returns a QuorumProgress indicating the current state.
// Returns ErrShamirSessionExpired if the TTL has been exceeded.
// Returns ErrShamirEmptyShare if the share string is empty.
// This method is safe for concurrent use.
func (a *SSSAAccumulator) AddShare(_ context.Context, share string) (*QuorumProgress, error) {
	if share == "" {
		return nil, ErrShamirEmptyShare
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.expiredLocked() {
		return nil, ErrShamirSessionExpired
	}

	// Deduplicate: skip shares already seen.
	if _, exists := a.seen[share]; !exists {
		a.seen[share] = struct{}{}
		a.shares = append(a.shares, share)
	}

	return &QuorumProgress{
		Required:  a.threshold,
		Submitted: len(a.shares),
		Complete:  len(a.shares) >= a.threshold,
	}, nil
}

// Reconstruct attempts to reconstruct the original secret from accumulated
// shares. The raw SSSA share strings are wrapped into structured Share objects
// and passed to the canonical crypto/shamir.Combine function for reconstruction.
// Returns ErrShamirThresholdNotMet if fewer than threshold shares have been
// accumulated. This method is safe for concurrent use.
func (a *SSSAAccumulator) Reconstruct(_ context.Context) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.shares) < a.threshold {
		return nil, ErrShamirThresholdNotMet
	}

	// Convert raw SSSA share strings to structured Share objects for the
	// canonical crypto/shamir.Combine function.
	structured := make([]*shamir.Share, len(a.shares))
	for i, raw := range a.shares {
		structured[i] = &shamir.Share{
			Index:     i + 1,
			Threshold: a.threshold,
			Total:     a.total,
			Value:     base64.StdEncoding.EncodeToString([]byte(raw)),
		}
	}

	secret, err := shamir.Combine(structured)
	if err != nil {
		var combineErr *shamir.CombineError
		if errors.As(err, &combineErr) {
			return nil, &ShamirCombineError{Err: err}
		}
		return nil, &ShamirCombineError{Err: err}
	}

	return secret, nil
}

// Reset clears all accumulated shares and restarts the TTL timer.
// This method is safe for concurrent use.
func (a *SSSAAccumulator) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.shares = make([]string, 0, a.threshold)
	a.seen = make(map[string]struct{}, a.threshold)
	a.createdAt = time.Now()
}

// Expired reports whether the accumulator session has exceeded its TTL.
// This method is safe for concurrent use.
func (a *SSSAAccumulator) Expired() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.expiredLocked()
}

// expiredLocked reports whether the session has exceeded its TTL.
// Caller must hold at least a read lock on a.mu.
func (a *SSSAAccumulator) expiredLocked() bool {
	return time.Since(a.createdAt) > a.ttl
}

// GetProgress returns the current quorum progress without modifying state.
// This allows callers to check the accumulator's state without submitting
// a new share. This method is safe for concurrent use.
func (a *SSSAAccumulator) GetProgress() *QuorumProgress {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return &QuorumProgress{
		Required:  a.threshold,
		Submitted: len(a.shares),
		Complete:  len(a.shares) >= a.threshold,
	}
}
