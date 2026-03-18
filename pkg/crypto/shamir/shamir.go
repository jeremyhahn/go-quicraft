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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/SSSaaS/sssa-golang"
)

// Split divides a secret into N shares where any M shares can reconstruct it.
func Split(secret []byte, threshold, total int) ([]*Share, error) {
	if threshold < 2 {
		return nil, &SplitError{Reason: fmt.Sprintf("threshold must be at least 2, got %d", threshold)}
	}
	if total < threshold {
		return nil, &SplitError{Reason: fmt.Sprintf("total shares (%d) must be >= threshold (%d)", total, threshold)}
	}
	if threshold > 255 {
		return nil, &SplitError{Reason: fmt.Sprintf("threshold cannot exceed 255, got %d", threshold)}
	}
	if total > 255 {
		return nil, &SplitError{Reason: fmt.Sprintf("total shares cannot exceed 255, got %d", total)}
	}
	if len(secret) == 0 {
		return nil, &SplitError{Reason: "secret cannot be empty"}
	}

	secretHex := hex.EncodeToString(secret)
	shareStrings, err := sssa.Create(threshold, total, secretHex)
	if err != nil {
		return nil, &SplitError{Reason: fmt.Sprintf("sssa split failed: %v", err)}
	}

	digest := sha256.Sum256(secret)

	shares := make([]*Share, len(shareStrings))
	for i, shareStr := range shareStrings {
		shares[i] = &Share{
			Index:     i + 1,
			Threshold: threshold,
			Total:     total,
			Value:     base64.StdEncoding.EncodeToString([]byte(shareStr)),
			Digest:    digest,
			Metadata:  make(map[string]string),
		}
	}

	return shares, nil
}

// Combine reconstructs the original secret from M or more shares.
func Combine(shares []*Share) ([]byte, error) {
	if len(shares) == 0 {
		return nil, &CombineError{Reason: "no shares provided"}
	}

	threshold := shares[0].Threshold
	total := shares[0].Total

	for i, share := range shares {
		if err := share.Validate(); err != nil {
			return nil, &CombineError{Reason: fmt.Sprintf("invalid share %d: %v", i, err)}
		}
		if share.Threshold != threshold {
			return nil, &CombineError{Reason: fmt.Sprintf("share %d has different threshold (%d) than share 0 (%d)", i, share.Threshold, threshold)}
		}
		if share.Total != total {
			return nil, &CombineError{Reason: fmt.Sprintf("share %d has different total (%d) than share 0 (%d)", i, share.Total, total)}
		}
	}

	if len(shares) < threshold {
		return nil, &CombineError{Reason: fmt.Sprintf("need at least %d shares, got %d", threshold, len(shares))}
	}

	shareStrings := make([]string, len(shares))
	for i, share := range shares {
		decoded, err := base64.StdEncoding.DecodeString(share.Value)
		if err != nil {
			return nil, &CombineError{Reason: fmt.Sprintf("failed to decode share %d: %v", i, err)}
		}
		shareStrings[i] = string(decoded)
	}

	secretHex, err := sssa.Combine(shareStrings)
	if err != nil {
		return nil, &CombineError{Reason: fmt.Sprintf("sssa combine failed: %v", err)}
	}

	secret, err := hex.DecodeString(secretHex)
	if err != nil {
		return nil, &CombineError{Reason: fmt.Sprintf("failed to decode hex secret: %v", err)}
	}

	// Verify integrity: compare SHA-256 of reconstructed secret against
	// the digest stored in shares. All shares carry the same digest
	// (set during Split), so we use the first share's digest.
	// Skip verification when the digest is zero (shares constructed
	// without Split, e.g., by the seal adapter's accumulator).
	var zeroDigest [32]byte
	if shares[0].Digest != zeroDigest {
		computedDigest := sha256.Sum256(secret)
		if computedDigest != shares[0].Digest {
			return nil, &IntegrityError{}
		}
	}

	return secret, nil
}

// VerifyShare checks if a share is valid and consistent with other shares.
func VerifyShare(share *Share, otherShares []*Share) error {
	if err := share.Validate(); err != nil {
		return err
	}

	for i, other := range otherShares {
		if other.Threshold != share.Threshold {
			return &ValidationError{Field: "threshold", Reason: fmt.Sprintf("mismatch with share %d: %d != %d", i, other.Threshold, share.Threshold)}
		}
		if other.Total != share.Total {
			return &ValidationError{Field: "total", Reason: fmt.Sprintf("mismatch with share %d: %d != %d", i, other.Total, share.Total)}
		}
		if other.Index == share.Index {
			return &ValidationError{Field: "index", Reason: fmt.Sprintf("duplicate share index: %d", share.Index)}
		}
	}

	return nil
}
