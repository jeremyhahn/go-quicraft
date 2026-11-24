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
	"encoding/json"
	"time"

	"github.com/jeremyhahn/go-enclave"
	"github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir"
)

// jsonNull is the JSON literal a nil slice marshals to; an init result decoded
// from it — or from an absent "shares" — holds no shares rather than being
// rejected as an empty set.
var jsonNull = []byte("null")

// DefaultQuorumTTL is the default time-to-live for a quorum accumulator session.
// If the threshold is not met within this duration, the accumulated shares are
// discarded and custodians must restart the unseal process.
const DefaultQuorumTTL = 5 * time.Minute

// ProtectedShareSplitter splits a secret into M-of-N Shamir shares held in protected memory.
type ProtectedShareSplitter interface {
	// SplitProtected divides secret into total shares with the given threshold, each in an mlock'd, guard-paged buffer.
	SplitProtected(secret []byte, threshold, total int) ([]*shamir.ProtectedShare, error)
}

// ProtectedShareAccumulator collects custodians' shares in protected memory until the threshold is met.
type ProtectedShareAccumulator interface {
	// AddProtectedShare adds share and takes ownership of it, destroying it whether or not it is accepted.
	AddProtectedShare(ctx context.Context, share *shamir.ProtectedShare) (*QuorumProgress, error)

	// GetProgress returns the current quorum progress without modifying state.
	GetProgress() *QuorumProgress

	// ReconstructProtected reconstructs the secret from the accumulated shares
	// into locked memory. It returns an error if the threshold has not been
	// met. The caller owns the returned enclave and must Destroy it.
	ReconstructProtected(ctx context.Context) (enclave.ByteAccess, error)

	// Reset destroys every accumulated share and resets the TTL timer.
	Reset()

	// Expired reports whether the accumulator session has exceeded its TTL.
	Expired() bool
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

// ShamirInitResult owns the N shares of a Shamir initialization, encoded as share objects; Destroy it once they are distributed and marshal it only at the transport boundary.
type ShamirInitResult struct {
	// Shares contains the generated Shamir shares, one per custodian, each
	// value in an mlock'd, guard-paged buffer. The result owns them; see
	// Destroy.
	Shares []*shamir.ProtectedShare `json:"shares"`

	// Threshold is the minimum number of shares required for reconstruction.
	Threshold int `json:"threshold"`

	// TotalShares is the total number of shares generated.
	TotalShares int `json:"total_shares"`
}

// Destroy wipes and releases every share and clears the slice; safe on nil and when called twice.
func (r *ShamirInitResult) Destroy() {
	if r == nil {
		return
	}
	for _, s := range r.Shares {
		s.Destroy()
	}
	r.Shares = nil
}

// UnmarshalJSON decodes an init result, enforcing the share-set bound.
func (r *ShamirInitResult) UnmarshalJSON(data []byte) error {
	var aux struct {
		Shares      json.RawMessage `json:"shares"`
		Threshold   int             `json:"threshold"`
		TotalShares int             `json:"total_shares"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	// aux.Shares is a heap copy of the shares array — share material — so it is
	// wiped on every path out. UnmarshalShareSet wipes only the internal copies
	// it makes, not the slice it is handed.
	defer enclave.Wipe(aux.Shares)

	var shares []*shamir.ProtectedShare
	// An absent or explicitly null "shares" decodes to no shares, mirroring the
	// zero-value round trip; an empty array is a malformed set and
	// UnmarshalShareSet rejects it. Anything else is bounded and decoded there.
	if trimmed := bytes.TrimSpace(aux.Shares); len(trimmed) > 0 && !bytes.Equal(trimmed, jsonNull) {
		decoded, err := shamir.UnmarshalShareSet(aux.Shares)
		if err != nil {
			return err
		}
		shares = decoded
	}

	// Assign last: destroy anything a reused receiver already held so decoding
	// into a non-empty result strands no locked buffer, then take the new set.
	r.Destroy()
	r.Shares = shares
	r.Threshold = aux.Threshold
	r.TotalShares = aux.TotalShares
	return nil
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
