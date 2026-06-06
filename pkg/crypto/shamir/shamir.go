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
//
// Integrity verification: shares produced by Split carry a SHA-256 digest of
// the original secret, and Combine verifies that the reconstructed secret
// matches that digest, returning an IntegrityError on mismatch. As a special
// case, when the first share's Digest is the zero value ([32]byte{}) the
// SHA-256 integrity check is SKIPPED. This supports callers that construct
// shares without Split (e.g. the seal adapter's ShareAccumulator, which has no
// access to the original digest). When a zero digest disables verification the
// caller MUST validate the reconstructed secret out-of-band (for example by
// authenticating it against an independent commitment or by using it in an
// authenticated decrypt that will fail on a wrong key).
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
	seen := make(map[string]struct{}, len(shares))
	for i, share := range shares {
		decoded, err := base64.StdEncoding.DecodeString(share.Value)
		if err != nil {
			return nil, &CombineError{Reason: fmt.Sprintf("failed to decode share %d: %v", i, err)}
		}
		s := string(decoded)
		// Reject duplicate shares. Lagrange interpolation divides by
		// (x_i - x_j); a repeated share yields a zero denominator (panic or
		// garbage secret) in the underlying library. De-dup defensively.
		if _, dup := seen[s]; dup {
			return nil, &CombineError{Reason: fmt.Sprintf("duplicate share at index %d", i)}
		}
		seen[s] = struct{}{}
		shareStrings[i] = s
	}

	// sssa.Combine is third-party and can panic on crafted/malformed shares
	// (e.g. a duplicate x-coordinate producing a zero Lagrange denominator).
	// Convert any panic into a clean error so a bad unseal ceremony cannot
	// crash the process (availability/DoS).
	secretHex, err := safeSSSACombine(shareStrings)
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

// safeSSSACombine wraps the third-party sssa.Combine with panic recovery so a
// malformed/crafted share set (e.g. a zero Lagrange denominator from duplicate
// x-coordinates) returns an error instead of crashing the process.
func safeSSSACombine(shareStrings []string) (secretHex string, err error) {
	defer func() {
		if r := recover(); r != nil {
			secretHex = ""
			err = fmt.Errorf("panic in sssa.Combine (malformed shares?): %v", r)
		}
	}()
	return sssa.Combine(shareStrings)
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
