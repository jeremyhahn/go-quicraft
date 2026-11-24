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

// Package shamir implements Shamir's M-of-N secret sharing over GF(256), with memory-locked share variants; integrity of the result is the caller's to establish.
package shamir

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// Each secret byte is shared with its own random polynomial over GF(2^8); arithmetic is constant-time (gf256.go).

// shareFormatVersion is the first byte of every raw share value.
const shareFormatVersion = 0x01

// rawShareOverhead is the number of non-payload bytes in a raw share
// value: [version:1][x:1].
const rawShareOverhead = 2

// MaxSecretBytes bounds the secret length (64 KiB) so unauthenticated shares cannot force large work; envelope larger payloads under a key.
const MaxSecretBytes = 64 << 10

// maxShares is 255, the non-zero x-coordinates of GF(2^8).
const maxShares = 255

// randReader is the coefficient randomness source; only testOnlyRandReader swaps it.
var randReader io.Reader = rand.Reader

// testOnlyRandReader replaces randReader and returns a restore function; it panics outside a test binary.
func testOnlyRandReader(r io.Reader) func() {
	if !testing.Testing() {
		panic("shamir.testOnlyRandReader: test-only randomness override called outside a test binary")
	}
	prev := randReader
	randReader = r
	return func() { randReader = prev }
}

// validateSplitParams checks the parameters shared by SplitBytes and SplitProtected.
func validateSplitParams(secret []byte, threshold, total int) error {
	if threshold < 2 {
		return &SplitError{Reason: fmt.Sprintf("threshold must be at least 2, got %d", threshold)}
	}
	if total < threshold {
		return &SplitError{Reason: fmt.Sprintf("total shares (%d) must be >= threshold (%d)", total, threshold)}
	}
	if threshold > maxShares {
		return &SplitError{Reason: fmt.Sprintf("threshold cannot exceed %d, got %d", maxShares, threshold)}
	}
	if total > maxShares {
		return &SplitError{Reason: fmt.Sprintf("total shares cannot exceed %d, got %d", maxShares, total)}
	}
	if len(secret) == 0 {
		return &SplitError{Reason: "secret cannot be empty"}
	}
	if len(secret) > MaxSecretBytes {
		return &SplitError{Reason: fmt.Sprintf(
			"secret is %d bytes, exceeding the %d-byte limit; share a key that envelopes the payload, not the payload",
			len(secret), MaxSecretBytes)}
	}
	return nil
}

// SplitBytes divides a secret into N base64 share values in Index order; the caller must wipe the returned buffers.
func SplitBytes(secret []byte, threshold, total int) ([][]byte, error) {
	if err := validateSplitParams(secret, threshold, total); err != nil {
		return nil, err
	}

	// Random coefficients: one degree-(threshold-1) polynomial per secret
	// byte, sharing a single bulk read of crypto/rand.
	randCoeffs := make([]byte, len(secret)*(threshold-1))
	if _, err := io.ReadFull(randReader, randCoeffs); err != nil {
		return nil, &SplitError{Reason: fmt.Sprintf("reading random coefficients: %v", err)}
	}

	// Use coefficients verbatim, including a zero top coefficient: resampling would leak information.

	// Shares carry no secret digest: it would be an offline oracle for low-entropy secrets.
	values := make([][]byte, total)
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
		enc := make([]byte, base64.StdEncoding.EncodedLen(len(raw)))
		base64.StdEncoding.Encode(enc, raw)
		// raw holds this share's y-values; the encoded buffer is the only
		// copy that must survive, and it is the caller's to wipe.
		wipe(raw)
		values[i] = enc
	}

	// Wipe the secret-derived scratch material.
	wipe(coeffs)
	wipe(randCoeffs)

	return values, nil
}

// parseRawShare validates a [version][x][y...] payload and returns x and y; seenX tracks duplicates, secretLen is -1 until known.
func parseRawShare(raw []byte, i int, seenX map[byte]struct{}, secretLen int) (byte, []byte, error) {
	if len(raw) < rawShareOverhead+1 {
		return 0, nil, &CombineError{Reason: fmt.Sprintf("share %d too short (%d bytes)", i, len(raw))}
	}
	if raw[0] != shareFormatVersion {
		return 0, nil, &CombineError{Reason: fmt.Sprintf(
			"share %d has unsupported format version %d (legacy or foreign share)", i, raw[0])}
	}
	// Bound the payload on the first share, before the rest of the unauthenticated set is decoded.
	if payload := len(raw) - rawShareOverhead; payload > MaxSecretBytes {
		return 0, nil, &CombineError{Reason: fmt.Sprintf(
			"share %d payload is %d bytes, exceeding the %d-byte limit", i, payload, MaxSecretBytes)}
	}
	x := raw[1]
	if x == 0 {
		return 0, nil, &CombineError{Reason: fmt.Sprintf("share %d has zero x-coordinate", i)}
	}
	if _, dup := seenX[x]; dup {
		return 0, nil, &CombineError{Reason: fmt.Sprintf("duplicate share at index %d (x-coordinate %d)", i, x)}
	}
	seenX[x] = struct{}{}
	y := raw[rawShareOverhead:]
	if secretLen != -1 && len(y) != secretLen {
		return 0, nil, &CombineError{Reason: fmt.Sprintf(
			"share %d payload length %d differs from share 0 length %d", i, len(y), secretLen)}
	}
	return x, y, nil
}

// interpolateSecretInto reconstructs the secret at x=0 into dst; only gfMul touches secret bytes.
func interpolateSecretInto(dst []byte, xs []byte, ys [][]byte) {
	basis := gfLagrangeZero(xs)
	for b := range dst {
		var v byte
		for j := range ys {
			v ^= gfMul(ys[j][b], basis[j])
		}
		dst[b] = v
	}
}

// accumulateShareInto adds one share's Lagrange term to dst (dst ^= coeff * y); dst starts zeroed.
func accumulateShareInto(dst, y []byte, coeff byte) {
	for b := range dst {
		dst[b] ^= gfMul(y[b], coeff)
	}
}

// interpolateSecret is interpolateSecretInto writing into a freshly allocated
// ordinary heap buffer, for the callers that return one.
func interpolateSecret(xs []byte, ys [][]byte, secretLen int) []byte {
	secret := make([]byte, secretLen)
	interpolateSecretInto(secret, xs, ys)
	return secret
}

// CombineBytes reconstructs the secret from threshold-or-more share values without integrity checks; the caller must authenticate the result and wipe the inputs.
func CombineBytes(values [][]byte, threshold int) ([]byte, error) {
	if threshold < 2 {
		return nil, &CombineError{Reason: fmt.Sprintf("threshold must be at least 2, got %d", threshold)}
	}
	if len(values) == 0 {
		return nil, &CombineError{Reason: "no shares provided"}
	}
	if len(values) < threshold {
		return nil, &CombineError{Reason: fmt.Sprintf("need at least %d shares, got %d", threshold, len(values))}
	}
	// Same bound as CombineProtected: no distinct share set can exceed the 255 non-zero
	// x-coordinates GF(2^8) provides.
	if len(values) > maxShares {
		return nil, &CombineError{Reason: fmt.Sprintf("too many shares: %d exceeds the maximum of %d", len(values), maxShares)}
	}
	xs := make([]byte, len(values))
	ys := make([][]byte, len(values))
	// Register each decode buffer before writing to it, so the deferred wipe covers rejected shares too.
	decoded := make([][]byte, 0, len(values))
	defer func() {
		for _, d := range decoded {
			wipe(d)
		}
	}()
	seenX := make(map[byte]struct{}, len(values))
	secretLen := -1
	for i, val := range values {
		raw := make([]byte, base64.StdEncoding.DecodedLen(len(val)))
		decoded = append(decoded, raw)
		n, err := base64.StdEncoding.Decode(raw, val)
		if err != nil {
			return nil, &CombineError{Reason: fmt.Sprintf("failed to decode share %d: %v", i, err)}
		}
		x, y, err := parseRawShare(raw[:n], i, seenX, secretLen)
		if err != nil {
			return nil, err
		}
		secretLen = len(y)
		xs[i] = x
		ys[i] = y
	}
	return interpolateSecret(xs, ys, secretLen), nil
}

// wipe zeroes a byte slice through go-enclave's Wipe, which survives dead-store elimination.
func wipe(b []byte) {
	enclave.Wipe(b)
}

// VerifyShare checks if a share is valid and consistent with other shares.
// A nil share — or a nil element of otherShares — is reported as a validation
// error naming its position, never dereferenced (see CombineProtected).
func VerifyShare(share *ProtectedShare, otherShares []*ProtectedShare) error {
	if err := share.Validate(); err != nil {
		return err
	}

	for i, other := range otherShares {
		if other == nil {
			return &ValidationError{Field: "share", Reason: fmt.Sprintf("other share %d is nil", i)}
		}
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
