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
	"time"
)

// DefaultQuorumTTL is the default time-to-live for a quorum accumulator session.
// If the threshold is not met within this duration, the accumulated shares are
// discarded and custodians must restart the unseal process.
const DefaultQuorumTTL = 5 * time.Minute

// ShareAccumulator collects shares from individual custodians until the
// threshold is met. Implementations handle deduplication, ordering, and TTL
// expiration. The accumulator is not safe for concurrent use without external
// synchronization (the Barrier provides this via its mutex).
type ShareAccumulator interface {
	// AddShare adds a single share to the accumulator. Returns progress
	// indicating how many shares have been submitted and whether the
	// threshold has been reached.
	AddShare(ctx context.Context, share string) (*QuorumProgress, error)

	// GetProgress returns the current quorum progress without modifying state.
	// This allows callers to check the accumulator's state without submitting
	// a new share.
	GetProgress() *QuorumProgress

	// Reconstruct attempts to reconstruct the secret from accumulated
	// shares. Returns an error if the threshold has not been met.
	Reconstruct(ctx context.Context) ([]byte, error)

	// Reset clears all accumulated shares and resets the TTL timer.
	Reset()

	// Expired reports whether the accumulator session has exceeded its TTL.
	Expired() bool
}

// ShareSplitter splits a secret into M-of-N Shamir secret shares.
type ShareSplitter interface {
	// Split divides the secret into the specified number of shares with the
	// given threshold. At least threshold shares are required to reconstruct
	// the original secret.
	Split(secret []byte, threshold, total int) ([]string, error)
}

// ShamirConfig configures Shamir secret sharing for barrier operations.
type ShamirConfig struct {
	// Threshold is the minimum number of shares required to reconstruct
	// the root key (M in M-of-N).
	Threshold int `json:"threshold"`

	// TotalShares is the total number of shares to generate (N in M-of-N).
	TotalShares int `json:"total_shares"`

	// QuorumTTL is the maximum duration to wait for all shares to arrive.
	// Defaults to DefaultQuorumTTL if zero.
	QuorumTTL time.Duration `json:"quorum_ttl,omitempty"`
}

// ShamirInitResult contains the output of a Shamir-based initialization.
type ShamirInitResult struct {
	// Shares contains the generated Shamir shares, one per custodian.
	Shares []string `json:"shares"`

	// Threshold is the minimum number of shares required for reconstruction.
	Threshold int `json:"threshold"`

	// TotalShares is the total number of shares generated.
	TotalShares int `json:"total_shares"`
}

// QuorumProgress reports the current state of a threshold quorum accumulator.
type QuorumProgress struct {
	// Required is the number of shares needed to reach the threshold.
	Required int `json:"required"`

	// Submitted is the number of shares submitted so far.
	Submitted int `json:"submitted"`

	// Complete indicates whether the threshold has been met and the
	// secret can be reconstructed.
	Complete bool `json:"complete"`
}
