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
	"testing"

	"github.com/jeremyhahn/go-enclave"
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

// MaxSecretBytes bounds the per-share payload this package will produce or
// consume, and therefore the secret length.
//
// Shamir here is a KEY-custody primitive: the secret it splits is a 32-byte
// root key, and every larger payload costs O(shares x len) field operations
// with roughly 1.75x the input held in memory at once while combining. Without
// a bound, a caller (or an attacker submitting shares to a recovery ceremony)
// could hand CombineBytes 255 shares of a megabyte each — ~340 MB of input, ~0.75x
// of it copied, and seconds of CPU — from unauthenticated material, since
// shares cannot be authenticated before they are combined. 64 KiB is four
// orders of magnitude above any legitimate key and caps the work at a few
// megabytes and milliseconds. Split large payloads by enveloping them under a
// key that IS this size, never by sharing the payload itself.
const MaxSecretBytes = 64 << 10

// maxShares is the number of distinct shares the scheme can express: the
// x-coordinate is a non-zero byte, so 255 is a property of GF(2^8), not a
// policy. Every combine path applies it up front so an oversized share list
// is rejected before any of it is decoded.
const maxShares = 255

// randReader is the source of cryptographic randomness for polynomial
// coefficients. It is package-private and swapped ONLY through
// testOnlyRandReader, which panics outside a test binary: it sits on the
// root-key path, where a mutable global source of randomness is exactly the
// thing that must not be reachable from production code.
var randReader io.Reader = rand.Reader

// testOnlyRandReader replaces the package randomness source and returns a
// function restoring the previous one. It PANICS outside a test binary.
//
// The rest of this codebase carries its randomness in a per-instance field
// (see seal.Barrier.randReader and seal.SoftwareStrategy.randReader) precisely
// so that no such global exists. This package's API is a set of free functions
// with no instance to hang it on, so the global stays — behind a gate that
// makes it provably unreachable from production, and that no future caller can
// quietly step around.
func testOnlyRandReader(r io.Reader) func() {
	if !testing.Testing() {
		panic("shamir.testOnlyRandReader: test-only randomness override called outside a test binary")
	}
	prev := randReader
	randReader = r
	return func() { randReader = prev }
}

// validateSplitParams checks the scheme parameters every generation path
// shares. It is the single copy of those rules, used by SplitBytes and by
// SplitProtected, so the plain and protected generation paths cannot drift
// into accepting different arguments — the same reason parseRawShare is the
// single copy of the wire-format rules.
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

// SplitBytes divides a secret into N share VALUES — the base64-encoded
// [version:1][x:1][y...] payloads that are exactly the bytes of a
// ProtectedShare's value, where x is the share's field coordinate (equal to
// its Index) and y has one byte per secret byte. They are
// returned in Index order, so values[i] is the value of share Index i+1.
//
// It is the generation-side counterpart of CombineBytes: the returned buffers
// are owned by the caller and are wipeable, so share material can be carried in
// memory-locked buffers from the moment it exists and zeroed the moment it is
// distributed, never becoming an un-wipeable Go string. SplitProtected is the
// form that returns them already in protected memory.
//
// The caller MUST wipe the returned buffers when done with them.
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

	// The raw crypto/rand coefficients are used verbatim, INCLUDING a zero
	// top coefficient. Uniform i.i.d. coefficients are what make Shamir
	// information-theoretically secure: with t-1 shares every candidate
	// secret byte remains exactly equally likely, and an adversary cannot
	// distinguish a polynomial whose top coefficient happens to be zero
	// from any other. Excluding c_{t-1}=0 (as a "degenerate polynomial"
	// defense) would instead LEAK information — a t-1 coalition could rule
	// out the one candidate secret consistent with c_{t-1}=0, reducing
	// per-byte entropy from 8 bits to log2(255). Do not resample.

	// No secret digest is embedded in shares: storing sha256(secret) per share
	// is an offline oracle that defeats information-theoretic secrecy for
	// low-entropy secrets. Integrity is verified out-of-band by the caller (see
	// CombineBytes).
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

// parseRawShare validates a decoded [version][x][y...] share payload and
// returns its x-coordinate and y-payload. It is the single copy of the share
// wire-format rules shared by CombineBytes and CombineProtected, so the byte
// and protected reconstruction paths can never diverge on which shares they accept.
// seenX tracks duplicate x-coordinates across the share set; secretLen is
// the y-payload length established by share 0, or -1 when not yet known.
func parseRawShare(raw []byte, i int, seenX map[byte]struct{}, secretLen int) (byte, []byte, error) {
	if len(raw) < rawShareOverhead+1 {
		return 0, nil, &CombineError{Reason: fmt.Sprintf("share %d too short (%d bytes)", i, len(raw))}
	}
	if raw[0] != shareFormatVersion {
		return 0, nil, &CombineError{Reason: fmt.Sprintf(
			"share %d has unsupported format version %d (legacy or foreign share)", i, raw[0])}
	}
	// Bound the payload BEFORE the caller decodes the rest of the set: shares
	// arriving at a recovery ceremony are unauthenticated by construction (they
	// cannot be checked until they are combined), so an oversized submission
	// must be rejected on the first share rather than after 255 of them have
	// been decoded and copied. See MaxSecretBytes.
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

// interpolateSecretInto reconstructs the secret via GF(2^8) Lagrange
// interpolation at x=0 from the public x-coordinates and the per-share
// y-payloads, writing it into dst — which is len(secret) bytes and may be
// memory-protected storage, so the protected reconstruction path never
// materializes the secret anywhere else. The basis coefficients depend only on
// the public x-coordinates and are computed once; secret-dependent y bytes flow
// only through constant-time gfMul.
//
// basis and xs are deliberately ORDINARY heap allocations: an x-coordinate is a
// share index, published with the share and known to anyone holding one, and
// the Lagrange basis is a function of those indices alone. Nothing derived from
// the secret is written outside dst.
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

// accumulateShareInto adds ONE share's contribution to a partially interpolated
// secret: dst ^= coeff * y over GF(2^8), where coeff is that share's Lagrange
// basis value at x=0. It is interpolateSecretInto's inner sum turned inside
// out, so a caller that cannot afford to hold every decoded y-payload at once
// (CombineProtected, where each one is locked memory) can fold each share in as
// it decodes it and reuse the buffer. dst must be zeroed before the first call
// and is the running total; y must be the same length as dst. Secret-dependent
// bytes flow only through gfMul, as in interpolateSecretInto.
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

// CombineBytes reconstructs the secret from M-or-more raw share VALUES — the
// base64-encoded [version][x][y...] payloads that are exactly the bytes of
// a ProtectedShare's value — WITHOUT integrity verification. It exists so callers that hold
// share material in wipeable []byte buffers (e.g. memory-locked enclaves) never
// have to copy it into an immutable, un-wipeable Go string just to reconstruct.
// threshold is the M of the M-of-N scheme and must be at least 2, matching
// SplitBytes.
//
// It performs NO self-verification of the reconstructed secret. Shares carry
// no digest of the secret — embedding one would be an offline brute-force
// oracle that defeats Shamir's information-theoretic secrecy for low-entropy
// secrets. With fewer-than-threshold genuine shares, or a corrupted or
// mismatched share, reconstruction yields indistinguishable garbage, NOT an
// error. The caller MUST authenticate the result out-of-band — for example by
// using it as the key in an authenticated decrypt that fails on a wrong key.
// Every decoded buffer — including partially decoded or validation-rejected
// shares — is wiped on every return path; the caller still owns (and must
// wipe) the input buffers.
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
	// Every decode buffer is registered in decoded BEFORE any bytes are
	// written into it, so the deferred wipe covers partially decoded and
	// validation-rejected shares too — not just the fully accepted ys. The
	// reconstructed secret returned to the caller is a separate buffer.
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

// wipe zeroes a byte slice. It delegates to go-enclave's Wipe, whose zeroing is
// resistant to dead-store elimination; a plain range-assign loop over a slice
// the compiler can prove dead may be optimized away.
//
// It is a one-line adapter and stays one deliberately: the package-local name is
// what every scratch buffer here is wiped through, so the primitive behind it
// can move without touching forty call sites.
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
