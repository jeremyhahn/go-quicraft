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
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

// This file implements Shamir's Secret Sharing over GF(2^8) (see
// gf256.go for the constant-time field arithmetic). Every secret byte is
// shared independently with its own random polynomial of degree
// threshold-1 whose constant term is the secret byte; share j's value is
// the polynomial evaluated at the share's non-zero x-coordinate.
//
// The implementation is self-contained (no third-party dependency) and
// constant-time with respect to secret data: secret bytes flow only
// through branchless GF(2^8) multiplications.

// shareFormatVersion is the first byte of every raw share value. It
// gates the wire format so a future change (or a legacy share produced
// by the pre-GF(256) implementation) is detected explicitly instead of
// being misinterpreted.
const shareFormatVersion = 0x01

// rawShareOverhead is the number of non-payload bytes in a raw share
// value: [version:1][x:1].
const rawShareOverhead = 2

// randReader is the source of cryptographic randomness for polynomial
// coefficients. Injectable for testing error paths.
var randReader io.Reader = rand.Reader

// Split divides a secret into N shares where any M shares can reconstruct it.
//
// Each share's Value is the base64 encoding of [version:1][x:1][y...]
// where x is the share's field coordinate (equal to Share.Index) and y
// has one byte per secret byte. The x-coordinate embedded in the value is
// authoritative for reconstruction, so a share string is self-contained.
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

	// Random coefficients: one degree-(threshold-1) polynomial per secret
	// byte, sharing a single bulk read of crypto/rand.
	randCoeffs := make([]byte, len(secret)*(threshold-1))
	if _, err := io.ReadFull(randReader, randCoeffs); err != nil {
		return nil, &SplitError{Reason: fmt.Sprintf("reading random coefficients: %v", err)}
	}

	// Ensure every sharing polynomial is exactly degree (threshold-1): the top
	// coefficient must be non-zero, else that secret byte degenerates to a
	// lower-degree polynomial reconstructable from fewer than `threshold` shares
	// (happens for ~1/256 of bytes with uniform randomness). Resample any zero
	// top coefficient. The resampling depends only on random bytes, not the
	// secret, so it leaks nothing via timing.
	topIdx := threshold - 2 // index of c_{t-1} within each byte's (t-1) coefficients
	var oneByte [1]byte
	for b := range secret {
		off := b*(threshold-1) + topIdx
		for randCoeffs[off] == 0 {
			if _, err := io.ReadFull(randReader, oneByte[:]); err != nil {
				return nil, &SplitError{Reason: fmt.Sprintf("resampling top coefficient: %v", err)}
			}
			randCoeffs[off] = oneByte[0]
		}
	}

	// No secret digest is embedded in shares: storing sha256(secret) per share
	// is an offline oracle that defeats information-theoretic secrecy for
	// low-entropy secrets. Integrity is verified out-of-band by the caller (see
	// Combine).
	shares := make([]*Share, total)
	coeffs := make([]byte, threshold) // [secret byte, c1..c_{t-1}]
	for i := 0; i < total; i++ {
		x := byte(i + 1) // non-zero x-coordinate
		raw := make([]byte, rawShareOverhead+len(secret))
		raw[0] = shareFormatVersion
		raw[1] = x
		for b := range secret {
			coeffs[0] = secret[b]
			copy(coeffs[1:], randCoeffs[b*(threshold-1):(b+1)*(threshold-1)])
			raw[rawShareOverhead+b] = gfPolyEval(coeffs, x)
		}
		shares[i] = &Share{
			Index:     i + 1,
			Threshold: threshold,
			Total:     total,
			Value:     base64.StdEncoding.EncodeToString(raw),
			Metadata:  make(map[string]string),
		}
	}

	// Wipe the secret-derived scratch material.
	wipe(coeffs)
	wipe(randCoeffs)

	return shares, nil
}

// Combine reconstructs the original secret from M (== threshold) or more shares
// via GF(2^8) Lagrange interpolation at x=0.
//
// Combine performs NO self-verification of the reconstructed secret. Shares
// carry no digest of the secret — embedding one would be an offline brute-force
// oracle that defeats Shamir's information-theoretic secrecy for low-entropy
// secrets (see Share). With fewer-than-threshold genuine shares, or a corrupted
// or mismatched share, reconstruction yields indistinguishable garbage, NOT an
// error. The caller MUST authenticate the result out-of-band — for example by
// using it as the key in an authenticated decrypt that fails on a wrong key.
// Combine is identical to CombineUnverified; both names are retained to let
// callers express intent.
func Combine(shares []*Share) ([]byte, error) {
	return CombineUnverified(shares)
}

// CombineUnverified reconstructs the original secret from M or more
// shares WITHOUT integrity verification. The caller MUST validate the
// reconstructed secret out-of-band (for example by authenticating it
// against an independent commitment or by using it in an authenticated
// decrypt that will fail on a wrong key): with fewer-than-threshold
// genuine shares or a corrupted share, reconstruction yields
// indistinguishable garbage, not an error.
func CombineUnverified(shares []*Share) ([]byte, error) {
	if len(shares) == 0 {
		return nil, &CombineError{Reason: "no shares provided"}
	}
	return combine(shares)
}

// combine performs the shared validation and GF(2^8) Lagrange
// interpolation at x=0. Digest policy is applied by the callers.
func combine(shares []*Share) ([]byte, error) {
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

	// Decode the raw share values. The x-coordinate embedded in the value
	// is authoritative (a share string is self-contained); duplicate or
	// zero x-coordinates are rejected — interpolation divides by
	// (x_j ⊕ x_m), and a repeated coordinate yields a zero denominator.
	xs := make([]byte, len(shares))
	ys := make([][]byte, len(shares))
	seenX := make(map[byte]struct{}, len(shares))
	secretLen := -1
	for i, share := range shares {
		raw, err := base64.StdEncoding.DecodeString(share.Value)
		if err != nil {
			return nil, &CombineError{Reason: fmt.Sprintf("failed to decode share %d: %v", i, err)}
		}
		if len(raw) < rawShareOverhead+1 {
			return nil, &CombineError{Reason: fmt.Sprintf("share %d too short (%d bytes)", i, len(raw))}
		}
		if raw[0] != shareFormatVersion {
			return nil, &CombineError{Reason: fmt.Sprintf(
				"share %d has unsupported format version %d (legacy or foreign share)", i, raw[0])}
		}
		x := raw[1]
		if x == 0 {
			return nil, &CombineError{Reason: fmt.Sprintf("share %d has zero x-coordinate", i)}
		}
		if _, dup := seenX[x]; dup {
			return nil, &CombineError{Reason: fmt.Sprintf("duplicate share at index %d (x-coordinate %d)", i, x)}
		}
		seenX[x] = struct{}{}

		y := raw[rawShareOverhead:]
		if secretLen == -1 {
			secretLen = len(y)
		} else if len(y) != secretLen {
			return nil, &CombineError{Reason: fmt.Sprintf(
				"share %d payload length %d differs from share 0 length %d", i, len(y), secretLen)}
		}
		xs[i] = x
		ys[i] = y
	}

	// Lagrange interpolation at x=0. The basis coefficients depend only
	// on the public x-coordinates and are computed once; secret-dependent
	// y bytes flow only through constant-time gfMul.
	basis := gfLagrangeZero(xs)
	secret := make([]byte, secretLen)
	for b := 0; b < secretLen; b++ {
		var v byte
		for j := range ys {
			v ^= gfMul(ys[j][b], basis[j])
		}
		secret[b] = v
	}
	return secret, nil
}

// wipe zeroes a byte slice.
func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
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
