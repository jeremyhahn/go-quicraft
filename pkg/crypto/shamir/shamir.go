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

	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
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
//
// Share.Value is a Go STRING and therefore immortal: it cannot be zeroed, and
// every share stays readable in the heap until the GC reclaims it. Split wipes
// the byte buffer it encoded from, but the string it hands back is the one copy
// it cannot take away. A ceremony holds all N shares at once, so this is the
// point of widest exposure in the whole scheme — callers that can keep share
// material in wipeable buffers should use SplitBytes and feed CombineBytes.
func Split(secret []byte, threshold, total int) ([]*Share, error) {
	values, err := SplitBytes(secret, threshold, total)
	if err != nil {
		return nil, err
	}
	shares := make([]*Share, total)
	for i, v := range values {
		shares[i] = &Share{
			Index:     i + 1,
			Threshold: threshold,
			Total:     total,
			Value:     string(v),
			Metadata:  make(map[string]string),
		}
		// string(v) copied the bytes, so the buffer SplitBytes allocated is
		// now redundant share material. Wipe it; the string copy is the only
		// one that must survive, and the only one that cannot be wiped.
		wipe(v)
	}
	return shares, nil
}

// SplitBytes divides a secret into N share VALUES — the base64-encoded
// [version:1][x:1][y...] payloads that are exactly the bytes of Share.Value,
// returned in Index order, so values[i] is the value of share Index i+1.
//
// It is the generation-side counterpart of CombineBytes: the returned buffers
// are owned by the caller and are wipeable, so share material can be carried in
// memory-locked buffers from the moment it exists and zeroed the moment it is
// distributed, never becoming an un-wipeable Go string. Split is a thin wrapper
// that encodes these into Share structs and wipes them.
//
// The caller MUST wipe the returned buffers when done with them.
func SplitBytes(secret []byte, threshold, total int) ([][]byte, error) {
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
	// Combine).
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
	// Reject nil elements BEFORE any dereference, including the shares[0]
	// reads below. A nil *Share is not an exotic input: custody lives above
	// this package and necessarily decodes operator-submitted shares, and
	// json.Unmarshal of `[null]` into a []*Share produces exactly this. One
	// malformed submission must not panic the process running an unseal
	// ceremony.
	for i, share := range shares {
		if share == nil {
			return nil, &CombineError{Reason: fmt.Sprintf("share %d is nil", i)}
		}
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

	// Decode the raw share values. The x-coordinate embedded in the value
	// is authoritative (a share string is self-contained); duplicate or
	// zero x-coordinates are rejected — interpolation divides by
	// (x_j ⊕ x_m), and a repeated coordinate yields a zero denominator.
	xs := make([]byte, len(shares))
	ys := make([][]byte, len(shares))
	seenX := make(map[byte]struct{}, len(shares))
	secretLen := -1
	// Wipe every decoded buffer on every return path, mirroring
	// CombineBytes. The share strings themselves are immutable and cannot
	// be wiped, but leaving additional decoded copies of share material on
	// the heap — including on validation-error returns — needlessly widens
	// the exposure. The reconstructed secret is a separate buffer.
	decoded := make([][]byte, 0, len(shares))
	defer func() {
		for _, d := range decoded {
			wipe(d)
		}
	}()
	for i, share := range shares {
		raw, err := base64.StdEncoding.DecodeString(share.Value)
		// Register before the error check: DecodeString returns the bytes
		// decoded so far alongside a corrupt-input error, and those partial
		// bytes are share material that must be wiped too.
		decoded = append(decoded, raw)
		if err != nil {
			return nil, &CombineError{Reason: fmt.Sprintf("failed to decode share %d: %v", i, err)}
		}
		x, y, err := parseRawShare(raw, i, seenX, secretLen)
		if err != nil {
			return nil, err
		}
		secretLen = len(y)
		xs[i] = x
		ys[i] = y
	}

	return interpolateSecret(xs, ys, secretLen), nil
}

// parseRawShare validates a decoded [version][x][y...] share payload and
// returns its x-coordinate and y-payload. It is the single copy of the share
// wire-format rules shared by combine and CombineBytes, so the string and
// byte reconstruction paths can never diverge on which shares they accept.
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

// interpolateSecret reconstructs the secret via GF(2^8) Lagrange interpolation
// at x=0 from the public x-coordinates and the per-share y-payloads. The basis
// coefficients depend only on the public x-coordinates and are computed once;
// secret-dependent y bytes flow only through constant-time gfMul.
func interpolateSecret(xs []byte, ys [][]byte, secretLen int) []byte {
	basis := gfLagrangeZero(xs)
	secret := make([]byte, secretLen)
	for b := 0; b < secretLen; b++ {
		var v byte
		for j := range ys {
			v ^= gfMul(ys[j][b], basis[j])
		}
		secret[b] = v
	}
	return secret
}

// CombineBytes reconstructs the secret from M-or-more raw share VALUES — the
// base64-encoded [version][x][y...] payloads that are exactly the bytes of
// Share.Value — WITHOUT integrity verification. It exists so callers that hold
// share material in wipeable []byte buffers (e.g. memory-locked enclaves) never
// have to copy it into an immutable, un-wipeable Go string just to reconstruct.
// threshold is the M of the M-of-N scheme and must be at least 2, matching
// Split. The same out-of-band-validation caveat as CombineUnverified applies.
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

// wipe zeroes a byte slice. It delegates to enclave.WipeBytes (memguard),
// whose zeroing is resistant to dead-store elimination; a plain range-assign
// loop over a slice the compiler can prove dead may be optimized away.
// pkg/enclave depends only on the standard library and memguard, so the
// import cannot cycle back into pkg/crypto/shamir.
func wipe(b []byte) {
	enclave.WipeBytes(b)
}

// VerifyShare checks if a share is valid and consistent with other shares.
// A nil share — or a nil element of otherShares — is reported as a validation
// error naming its position, never dereferenced (see combine).
func VerifyShare(share *Share, otherShares []*Share) error {
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
