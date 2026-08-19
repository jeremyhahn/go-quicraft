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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
)

// maxProtectedValueBytes is the largest base64 share value NewProtectedShare
// will move into locked memory: the encoding of a full
// [version:1][x:1][y:MaxSecretBytes] share. It is the locked-memory analogue
// of parseRawShare's MaxSecretBytes bound, applied one step earlier because
// the allocation it guards is a kernel resource rather than heap.
var maxProtectedValueBytes = base64.StdEncoding.EncodedLen(rawShareOverhead + MaxSecretBytes)

// MaxShareSetBytes is the locked memory this package will spend on ONE share
// set. Both paths that can allocate a whole set enforce it before they allocate
// any of it: SplitProtected against the footprint its parameters imply, and
// UnmarshalShareSet against the size of the undecoded document (every share
// value is a substring of it, so the document bounds the sum of the buffers).
//
// It is a POLICY bound, unlike maxProtectedValueBytes and maxShares which are
// properties of the scheme, and it exists because the per-value bounds are
// per value. Legal dimensions multiply: 255 shares that are each individually
// legal add up to ~22 MiB of locked payload, and a 64 KiB secret split 255
// ways needs ~16 MiB of polynomial coefficients on top. Both are comfortably
// past the 8 MiB RLIMIT_MEMLOCK a Linux host commonly defaults to, and memguard
// answers a failed lock by TERMINATING THE PROCESS — so a combination of legal
// arguments must produce an error, not a dead service.
//
// 1 MiB is far above any key-custody ceremony this primitive exists to serve
// and far below any plausible locked-memory allowance. It admits the full
// 255-share set of a 32-byte root key (~24 KiB) and a 5-of-5 split of the
// largest secret the package accepts (~750 KiB); it refuses combinations like
// 255 shares of a 4 KiB secret, which is sharing a payload rather than a key.
// Envelope large payloads under a key that IS this size, exactly as
// MaxSecretBytes says. A caller that genuinely needs more does its own
// accounting and works share by share with SplitBytes and NewProtectedShare —
// the policy is then explicitly the caller's, which is where a number that
// cannot be derived from the scheme belongs.
const MaxShareSetBytes = 1 << 20

// splitLockedBytes reports the locked memory SplitProtected holds at its peak
// for these parameters: the random polynomial coefficients, the per-share
// coefficient vector and raw payload it reuses, and the encoded value of every
// share it returns.
//
// The multiplications cannot overflow because validateSplitParams runs first
// and bounds each factor — the secret to MaxSecretBytes (64 KiB) and both
// threshold and total to maxShares (255) — so the largest product this can
// compute is ~39 MB.
func splitLockedBytes(secretLen, threshold, total int) int {
	coefficients := secretLen * (threshold - 1)
	scratch := threshold + rawShareOverhead + secretLen
	values := total * base64.StdEncoding.EncodedLen(rawShareOverhead+secretLen)
	return coefficients + scratch + values
}

// ProtectedShare is a Shamir share whose VALUE lives in memory-protected
// storage instead of a Go string.
//
// It is the protected-memory counterpart of Share and carries the same wire
// shape: the value is the base64 encoding of [version:1][x:1][y...], the JSON
// encoding is byte-for-byte what Share produces, and the same warnings apply —
// shares carry no digest of the secret, and Threshold and Total are
// unauthenticated caller-supplied metadata (see Share).
//
// The difference is lifetime. Share.Value is an immutable Go string: it cannot
// be zeroed, so every share stays readable in the heap until the GC reclaims
// it, and a ceremony holding all N shares is the point of widest exposure in
// the whole scheme. A ProtectedShare's value is held in an
// enclave.MemoryEnclave — mlock'd (excluded from swap), guard-paged,
// canary-checked, and genuinely WIPED by Destroy — so share material can be
// released the moment a custodian is done with it rather than waiting on the
// collector.
//
// Two consequences the caller owns:
//
//   - Every ProtectedShare that is created must be Destroyed, INCLUDING ones
//     produced by json.Unmarshal. An abandoned share holds an mlock'd,
//     guard-paged allocation for the life of the process.
//   - Locked memory is a finite kernel resource (RLIMIT_MEMLOCK), and each
//     share is one locked allocation. A ceremony holding the full 255 shares
//     holds 255 of them. Destroy as shares are consumed rather than at the end
//     of the ceremony.
//
// Index, Threshold, Total and Metadata stay exported: they are non-secret
// metadata, they mirror Share field-for-field so migrating a caller is a move
// rather than a rewrite, and none of them is what this type exists to protect.
// The value is unexported precisely because it IS: routing it through
// NewProtectedShare and Value is what makes "share material never becomes a
// string" an invariant of the type rather than a convention.
type ProtectedShare struct {
	// Index is the share's 1-based position, equal to the x-coordinate
	// embedded in the value.
	Index int
	// Threshold is the M of the M-of-N scheme. It is UNAUTHENTICATED
	// caller-supplied metadata, exactly as on Share: CombineProtected takes
	// the threshold from shares[0], so a relabelled share set reconstructs
	// garbage rather than returning an error.
	Threshold int
	// Total is the N of the M-of-N scheme. Unauthenticated, like Threshold.
	Total int
	// Metadata is caller-defined and carries no cryptographic meaning. It is
	// NOT protected memory — do not put secret material here.
	Metadata map[string]string

	// value is the base64-encoded [version][x][y...] payload — exactly the
	// bytes of Share.Value — in an mlock'd, guard-paged buffer.
	value *enclave.MemoryEnclave
}

// NewProtectedShare builds a share whose value is copied into protected memory.
//
// value is the base64-encoded [version:1][x:1][y...] payload, i.e. exactly the
// bytes SplitBytes returns and exactly the bytes of Share.Value. It is WIPED:
// ownership of the share material transfers to the returned share, which is
// the point — a custody caller can carry share bytes in a wipeable buffer from
// the moment they exist to the moment they enter protected memory, with no
// un-wipeable copy in between. On a validation failure the source is left
// untouched for the caller to wipe.
//
// The value must be syntactically valid standard base64 and no longer than the
// encoding of a MaxSecretBytes payload; both are checked BEFORE any locked
// memory is allocated, because that allocation is the resource an
// unauthenticated share submission would otherwise get to size.
//
// The returned share must be Destroyed.
func NewProtectedShare(index, threshold, total int, value []byte) (*ProtectedShare, error) {
	if err := validateShareHeader(index, threshold, total); err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, &ValidationError{Field: "value", Reason: "cannot be empty"}
	}
	// Reject anything that is not standard base64 BEFORE it reaches protected
	// memory. Two reasons: a value that cannot decode is a malformed share
	// whatever else happens to it, and MarshalJSON writes these bytes into a
	// JSON string literal without escaping them (see MarshalJSON) — which is
	// sound only because the standard alphabet contains no character JSON
	// would need to escape.
	if !isStandardBase64(value) {
		return nil, &ValidationError{Field: "value", Reason: "must be standard base64"}
	}
	// Bound the value BEFORE it is allocated, not after it is decoded.
	//
	// On the string path an oversized share costs an ordinary heap allocation
	// and parseRawShare rejects it during combine. Here the allocation is
	// mlock'd memory: a finite kernel resource (RLIMIT_MEMLOCK) whose
	// exhaustion memguard answers by TERMINATING THE PROCESS. UnmarshalJSON
	// constructs from operator-supplied JSON, which at a recovery ceremony is
	// unauthenticated by construction, so a value made of arbitrarily many
	// legal base64 characters must not be able to reach the locking allocator
	// at all. parseRawShare's MaxSecretBytes check runs far too late to help:
	// it sees the payload only after CombineProtected has locked it.
	//
	// The bound is the encoded length of the largest share this package will
	// ever produce or consume, so it rejects exactly what parseRawShare would
	// have rejected and nothing more.
	if len(value) > maxProtectedValueBytes {
		return nil, &ValidationError{Field: "value", Reason: fmt.Sprintf(
			"is %d bytes, exceeding the %d-byte maximum for a %d-byte payload",
			len(value), maxProtectedValueBytes, MaxSecretBytes)}
	}
	return &ProtectedShare{
		Index:     index,
		Threshold: threshold,
		Total:     total,
		Metadata:  make(map[string]string),
		value:     enclave.NewMemoryEnclaveFromBytes(value),
	}, nil
}

// SplitProtected divides a secret into N shares, any M of which reconstruct it,
// with every share value held in protected memory.
//
// It is the protected-memory counterpart of Split and produces identical
// shares; only the custody of the value differs. Use it instead of Split
// wherever the shares are held rather than immediately handed off — Split's
// values are immortal Go strings.
//
// Generation is protected-NATIVE. The random polynomial coefficients, the
// per-share coefficient vector, the raw [version][x][y...] payload and each
// share's encoded value are allocated in locked memory BEFORE they hold
// anything, so no secret-derived byte is ever written to an ordinary heap
// buffer — not even one that is wiped afterwards. It deliberately does NOT
// route through SplitBytes, which by contract returns ordinary caller-owned
// buffers.
//
// The secret argument itself is the caller's: pass a slice that is already
// protected memory (an enclave.MemoryEnclave's Bytes, say) to keep the material
// protected end to end. This function neither copies nor wipes it.
//
// Every returned share must be Destroyed. Each is one mlock'd allocation
// against RLIMIT_MEMLOCK, and memguard terminates the process if a lock fails,
// so a ceremony splitting into the full 255 shares should destroy them as they
// are distributed rather than holding the set.
//
// Because that failure is fatal rather than reportable, the whole footprint —
// coefficients, scratch and every returned value — is priced against
// MaxShareSetBytes before the first allocation and refused with a *SplitError
// if it does not fit. Arguments that are individually legal can multiply into
// one that is not: see MaxShareSetBytes for what fits and what to do instead.
func SplitProtected(secret []byte, threshold, total int) ([]*ProtectedShare, error) {
	if err := validateSplitParams(secret, threshold, total); err != nil {
		return nil, err
	}
	// The individual bounds are each satisfiable and still multiply into a
	// footprint no host can lock: a 64 KiB secret split 255 ways is ~16 MiB of
	// coefficients before a single share exists. Priced and refused BEFORE the
	// first allocation, because the allocation that would fail is the one
	// memguard answers by terminating the process — a caller that asks for too
	// much must get an error back, not lose the process.
	if need := splitLockedBytes(len(secret), threshold, total); need > MaxShareSetBytes {
		return nil, &SplitError{Reason: fmt.Sprintf(
			"%d shares of a %d-byte secret need %d bytes of locked memory, exceeding the %d-byte budget",
			total, len(secret), need, MaxShareSetBytes)}
	}

	// One degree-(threshold-1) polynomial per secret byte, sharing a single
	// bulk read of crypto/rand — as in SplitBytes, but read directly into
	// locked memory. The coefficients are as sensitive as the secret: with
	// them and one share, the secret follows.
	randCoeffs := enclave.NewMemoryEnclave(len(secret) * (threshold - 1))
	defer randCoeffs.Destroy()
	if _, err := io.ReadFull(randReader, randCoeffs.Bytes()); err != nil {
		return nil, &SplitError{Reason: fmt.Sprintf("reading random coefficients: %v", err)}
	}
	// Raw coefficients are used verbatim, including a zero top coefficient,
	// and no secret digest is embedded in shares — see SplitBytes for why
	// both of those are deliberate and must not be "fixed".

	coeffs := enclave.NewMemoryEnclave(threshold) // [secret byte, c1..c_{t-1}]
	defer coeffs.Destroy()
	// One raw payload buffer serves every share: each iteration rewrites all
	// of it (version, x, and one y byte per secret byte) before it is read, so
	// no share ever sees another's bytes, and Destroy wipes the last share's
	// payload on the way out.
	raw := enclave.NewMemoryEnclave(rawShareOverhead + len(secret))
	defer raw.Destroy()

	rc, cf, rw := randCoeffs.Bytes(), coeffs.Bytes(), raw.Bytes()
	shares := make([]*ProtectedShare, 0, total)
	for i := 0; i < total; i++ {
		x := byte(i + 1) // non-zero x-coordinate
		rw[0] = shareFormatVersion
		rw[1] = x
		for b := range secret {
			cf[0] = secret[b]
			copy(cf[1:], rc[b*(threshold-1):(b+1)*(threshold-1)])
			rw[rawShareOverhead+b] = gfPolyEval(cf, x)
		}
		// The encoded value is written straight into the locked buffer the
		// share will own, then frozen: the share material exists in exactly
		// one place, and that place is wipeable.
		value := enclave.NewMemoryEnclave(base64.StdEncoding.EncodedLen(len(rw)))
		base64.StdEncoding.Encode(value.Bytes(), rw)
		value.Freeze()
		shares = append(shares, &ProtectedShare{
			Index:     i + 1,
			Threshold: threshold,
			Total:     total,
			Metadata:  make(map[string]string),
			value:     value,
		})
	}
	return shares, nil
}

// CombineProtected reconstructs the secret from M-or-more protected shares and
// returns it in protected memory. The caller MUST Destroy the returned enclave.
//
// Like Combine it performs NO integrity check and is NOT self-verifying: with
// fewer-than-threshold genuine shares, or a corrupted or mismatched share,
// reconstruction yields indistinguishable garbage rather than an error, and
// the caller must authenticate the result out-of-band (see Combine).
//
// Reconstruction is protected-NATIVE. Each share value is read IN PLACE out of
// its own locked buffer and base64-decoded into another locked buffer; the
// interpolated secret is written directly into the locked buffer that is
// returned. At no point does share material or the secret exist in an ordinary
// heap allocation, and the path deliberately does NOT route through
// CombineBytes, which by contract returns one. The decode buffer is destroyed
// on every return path, including the error paths; the input shares are left
// intact for the caller to Destroy.
//
// Exactly ONE decode buffer exists at a time, whatever the size of the share
// set. Holding a decoded copy of every share at once would make the peak
// locked footprint of a reconstruction proportional to the submission — 255
// shares of a MaxSecretBytes secret would be another ~16 MiB on top of the
// shares themselves — and locked memory is the one allocation this process
// cannot fail gracefully, since memguard answers a failed lock by terminating.
// So the shares are walked twice against a single reusable buffer: once to
// validate them and collect the x-coordinates, then, with the Lagrange basis
// those fix, once more to accumulate each share's contribution into the
// result. Decoding base64 twice is cheap; a peak that an untrusted share set
// gets to choose is not.
//
// Only the x-coordinates and the Lagrange basis derived from them live on the
// ordinary heap: a share's x-coordinate is its published index, not secret (see
// interpolateSecretInto).
func CombineProtected(shares []*ProtectedShare) (*enclave.MemoryEnclave, error) {
	if len(shares) == 0 {
		return nil, &CombineError{Reason: "no shares provided"}
	}
	// Same bound, applied for the same reason, as combine: the x-coordinate is
	// a non-zero byte, so no set larger than maxShares can hold distinct
	// shares, and rejecting up front bounds the work an unauthenticated
	// submission can force — here that work is locked-memory allocation, which
	// is a finite kernel resource, so the bound matters more on this path than
	// on the plain one.
	if len(shares) > maxShares {
		return nil, &CombineError{Reason: fmt.Sprintf("too many shares: %d exceeds the maximum of %d", len(shares), maxShares)}
	}
	for i, share := range shares {
		if share == nil {
			return nil, &CombineError{Reason: fmt.Sprintf("share %d is nil", i)}
		}
	}

	// The metadata checks below restate combine's, because they are checks on
	// this type's fields. The rules that actually decide what reconstructs —
	// the wire format and the duplicate/zero x-coordinate and payload-length
	// rules — are NOT restated: this path runs parseRawShare, the single copy
	// shared with combine and CombineBytes, so the string and protected paths
	// cannot diverge on which share sets they accept.
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

	// One buffer, sized for the largest value in the set and reused by every
	// decode. It is allocated BEFORE anything is written into it and destroyed
	// on every path out, so partially decoded and validation-rejected shares
	// are wiped too. The reconstructed secret is a separate enclave, and is the
	// only one that outlives this call.
	// Every read of a share's value below goes through WithValue, which holds
	// the share's borrow for the whole read. These slices alias caller-owned
	// protected memory that another goroutine may Destroy at any moment — a
	// cancelled or timed-out ceremony is exactly that shape — and a bare
	// Value() would release the borrow before the decode consumed the bytes.
	// Reconstruction obtains the slices itself, so a caller cannot make this
	// safe from outside.
	widest := 0
	for _, share := range shares {
		if err := share.WithValue(func(v []byte) error {
			if len(v) > widest {
				widest = len(v)
			}
			return nil
		}); err != nil {
			return nil, &CombineError{Reason: "share was destroyed during reconstruction"}
		}
	}
	buf := enclave.NewMemoryEnclave(base64.StdEncoding.DecodedLen(widest))
	defer buf.Destroy()

	// Pass one: validate every share and collect the x-coordinates. Nothing
	// secret is retained — a y-payload lives in buf only until the next share
	// overwrites it, and the stale tail of a shorter decode is unreachable
	// because parseRawShare has already required every payload to be the same
	// length as share 0's.
	xs := make([]byte, len(shares))
	seenX := make(map[byte]struct{}, len(shares))
	secretLen := -1
	for i, share := range shares {
		// Read in place: the share's own locked buffer is the source operand
		// of the decode, never a copy of it. The decode and the parse both run
		// inside the borrow, so the source cannot be unmapped mid-read.
		var (
			n   int
			x   byte
			y   []byte
			err error
		)
		if werr := share.WithValue(func(v []byte) error {
			n, err = base64.StdEncoding.Decode(buf.Bytes(), v)
			if err != nil {
				return &CombineError{Reason: fmt.Sprintf("failed to decode share %d: %v", i, err)}
			}
			x, y, err = parseRawShare(buf.Bytes()[:n], i, seenX, secretLen)
			return err
		}); werr != nil {
			return nil, werr
		}
		secretLen = len(y)
		xs[i] = x
	}

	// Pass two: accumulate. The basis depends only on the x-coordinates, which
	// pass one fixed, so each share's contribution can be added as it is
	// decoded and its payload immediately overwritten by the next.
	basis := gfLagrangeZero(xs)
	secret := enclave.NewMemoryEnclave(secretLen)
	for i, share := range shares {
		if werr := share.WithValue(func(v []byte) error {
			n, err := base64.StdEncoding.Decode(buf.Bytes(), v)
			if err != nil {
				// Unreachable: pass one decoded this same value, and a share's
				// value is frozen read-only from the moment it is constructed.
				// Handled rather than ignored so a future change to either of
				// those cannot turn it into a silently wrong secret.
				return &CombineError{Reason: fmt.Sprintf("failed to decode share %d: %v", i, err)}
			}
			accumulateShareInto(secret.Bytes(), buf.Bytes()[rawShareOverhead:n], basis[i])
			return nil
		}); werr != nil {
			secret.Destroy()
			return nil, werr
		}
	}
	// Frozen for the same reason a share value is: the reconstructed key is
	// final, and nothing should mutate it in place. Destroy still wipes it.
	secret.Freeze()
	return secret, nil
}

// Value returns the share's base64 value, or nil once the share has been
// destroyed.
//
// The slice ALIASES the protected buffer. It must not outlive the share
// (Destroy zeroes and unmaps that memory), and it must not be written to: the
// buffer is frozen read-only, so a write — including an attempt to wipe it —
// faults. Release share material with Destroy, never by zeroing this slice.
//
// CONCURRENCY: this returns a slice and then releases its lock, so it cannot
// guard the caller's USE of that slice against a concurrent Destroy — the
// memory is unmapped, so losing that race segfaults rather than reading stale
// bytes. Value is appropriate where the share is owned by one goroutine, which
// is the ordinary custody shape. Where a ceremony may tear shares down while a
// reader is still working — a cancelled or timed-out reconstruction — use
// WithValue, which holds the lock across the callback. See issue #15.
func (s *ProtectedShare) Value() []byte {
	if s == nil || s.value == nil {
		return nil
	}
	return s.value.Bytes()
}

// WithValue calls fn with the share's base64 value, holding the enclave's read
// lock for the whole call so the value cannot be destroyed underneath it. It
// returns fn's error, or an error without calling fn if the share is nil or
// already destroyed.
//
// This is the accessor to use anywhere teardown may be concurrent with the
// read. fn must not retain the slice beyond its own return, and must not
// RE-ENTER the same share by any method — not Value, WithValue or Validate,
// and not Destroy. Mutators take priority over readers in the underlying
// enclave, so a nested read blocks on any mutator that queued after fn started
// while that mutator waits on fn's own borrow; a nested Destroy waits on the
// borrow directly. Reading a DIFFERENT share, or a different enclave, is fine
// and is what CombineProtected's own callbacks do.
func (s *ProtectedShare) WithValue(fn func([]byte) error) error {
	if s == nil || s.value == nil {
		return enclave.ErrEnclaveDestroyed
	}
	return s.value.WithBytes(fn)
}

// Destroy wipes the share's value and releases its protected memory. It is
// safe to call on a nil receiver, more than once, and concurrently with
// itself, with Value and with WithValue — exactly one caller reaches memguard,
// and the enclave's lock keeps teardown from interleaving with a read in
// progress.
//
// What it cannot make safe is a slice a caller already took from Value and is
// still using; that span is outside any lock this type holds. WithValue exists
// for exactly that case.
//
// A ProtectedShare that is not destroyed holds an mlock'd, guard-paged
// allocation for the life of the process; nothing reclaims it, because the
// wipe is the whole point and a finalizer that ran at the collector's
// convenience would not be one.
func (s *ProtectedShare) Destroy() {
	if s == nil || s.value == nil {
		return
	}
	s.value.Destroy()
}

// Validate checks that the share has valid parameters and still holds a value.
// It is safe to call on a nil receiver: a nil share is a validation failure,
// not a panic (see Share.Validate — custody layers decode operator input, and
// a JSON null decodes to a nil *ProtectedShare).
func (s *ProtectedShare) Validate() error {
	if s == nil {
		return &ValidationError{Field: "share", Reason: "share is nil"}
	}
	if err := validateShareHeader(s.Index, s.Threshold, s.Total); err != nil {
		return err
	}
	if s.value == nil {
		return &ValidationError{Field: "value", Reason: "cannot be empty"}
	}
	// Through the borrow like every other read of the value. A bare
	// len(s.value.Bytes()) could not actually fault — it inspects the slice
	// header rather than the protected pages — but "no read of a share's value
	// outside a borrow" is only a useful rule if it is mechanical, and an
	// exception here is one a later change would copy without the reasoning.
	// Taken from the parallel work on PR #16, whose argument this is.
	empty := false
	if err := s.WithValue(func(v []byte) error {
		empty = len(v) == 0
		return nil
	}); err != nil {
		return &ValidationError{Field: "value", Reason: "share has been destroyed"}
	}
	if empty {
		return &ValidationError{Field: "value", Reason: "share has been destroyed"}
	}
	return nil
}

// String returns a string representation of the share. The share value is
// NEVER included, for the reason it is never included in Share.String: this is
// what reaches logs and %v/%s formatting, and even a prefix would leak share
// bytes.
func (s *ProtectedShare) String() string {
	if s == nil {
		return "ProtectedShare(nil)"
	}
	return fmt.Sprintf("ProtectedShare{Index: %d, Threshold: %d/%d, Value: [redacted]}",
		s.Index, s.Threshold, s.Total)
}

// LogValue implements slog.LogValuer so that a share reaching a structured
// logger is redacted whatever the handler is. String alone is not enough: slog
// resolves a LogValuer FIRST, but with none present a JSON handler encodes the
// value through its json.Marshaler — which for this type is the serialization
// path and is deliberately not redacted (see MarshalJSON).
func (s *ProtectedShare) LogValue() slog.Value {
	return slog.StringValue(s.String())
}

// MarshalJSON implements json.Marshaler, producing byte-for-byte the JSON that
// Share produces for the same share, so a ProtectedShare can be persisted or
// distributed anywhere a Share was.
//
// Like Share.MarshalJSON it is NOT redacted, and must not be: this is the
// SERIALIZATION path, and a custody layer that persists or distributes shares
// as JSON needs the real value back. Redaction belongs to String and LogValue.
//
// The encoding is written by hand rather than delegated to json.Marshal for
// the reason this type exists: marshalling a struct with a string value field
// would mint exactly the immortal share string a ProtectedShare is holding the
// value in an enclave to avoid. The returned slice IS share material — the
// caller owns it and should wipe it once it has been written out. The exact
// capacity is computed up front so that no append reallocates and strands an
// un-wiped copy of the value on the heap.
func (s *ProtectedShare) MarshalJSON() ([]byte, error) {
	if s == nil {
		return []byte("null"), nil
	}
	var meta []byte
	if len(s.Metadata) > 0 {
		var err error
		if meta, err = json.Marshal(s.Metadata); err != nil {
			return nil, &ValidationError{Field: "metadata", Reason: err.Error()}
		}
	}
	// Non-secret: the header carries only the unauthenticated metadata.
	head := fmt.Sprintf(`{"index":%d,"threshold":%d,"total":%d,"value":"`, s.Index, s.Threshold, s.Total)
	const metaKey = `,"metadata":`

	// The WHOLE encoding happens inside the borrow, not just the read of the
	// value. An earlier version took `val := s.Value()`, which ends the borrow
	// at the point it returns, and then appended `val` into the output — a read
	// of the aliasing slice after the enclave was free to be destroyed, which is
	// the same use-after-destroy this change exists to close (issue #15,
	// Codex round 3). Copying the value out first would be worse, not better:
	// it would mint exactly the unprotected copy of share material this type
	// holds an enclave to avoid. So the append runs where the bytes are still
	// guaranteed mapped.
	var out []byte
	if err := s.WithValue(func(val []byte) error {
		if len(val) == 0 {
			return &ValidationError{Field: "value", Reason: "share has been destroyed"}
		}
		// Sized exactly, before the first append, so that no append reallocates
		// and strands an un-wiped copy of the value on the heap.
		size := len(head) + len(val) + len(`"}`)
		if meta != nil {
			size += len(metaKey) + len(meta)
		}
		out = make([]byte, 0, size)
		out = append(out, head...)
		// val holds only standard-base64 characters (NewProtectedShare rejects
		// anything else and the buffer is immutable thereafter), none of which
		// JSON escapes, so it is written into the string literal verbatim.
		out = append(out, val...)
		out = append(out, '"')
		if meta != nil {
			out = append(out, metaKey...)
			out = append(out, meta...)
		}
		out = append(out, '}')
		return nil
	}); err != nil {
		// A destroyed share reports the same ValidationError it always did.
		// WithValue's own refusal for an already-destroyed share is
		// enclave.ErrEnclaveDestroyed, which is translated rather than
		// propagated so this method's error contract is unchanged.
		if errors.Is(err, enclave.ErrEnclaveDestroyed) {
			return nil, &ValidationError{Field: "value", Reason: "share has been destroyed"}
		}
		return nil, err
	}
	return out, nil
}

// UnmarshalJSON implements json.Unmarshaler for the JSON shape Share and
// ProtectedShare both produce.
//
// A share decoded this way holds protected memory and MUST be Destroyed like
// any other; decoding is a construction. Note that the caller's own JSON input
// buffer still holds the share material after this returns — this method
// cannot wipe a buffer it does not own.
//
// It decodes ONE share, and it is not the ingestion path for an untrusted share
// SET. Decoding a []*ProtectedShare through the generic decoder allocates one
// locked buffer per element as the elements are visited, so an unauthenticated
// submission chooses how much locked memory is taken and CombineProtected's
// 255-share bound arrives long after the allocations it would have limited.
// Use UnmarshalShareSet for anything arriving from outside the process: it
// bounds the set before it allocates any of it.
func (s *ProtectedShare) UnmarshalJSON(data []byte) error {
	var aux struct {
		Index     int               `json:"index"`
		Threshold int               `json:"threshold"`
		Total     int               `json:"total"`
		Value     json.RawMessage   `json:"value"`
		Metadata  map[string]string `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	// The value is decoded as a RAW token, not into a string field:
	// json.RawMessage is a []byte copy of the quoted literal that can be
	// wiped, whereas a string field would mint the immortal share string this
	// type exists to avoid.
	defer wipe(aux.Value)
	if len(aux.Value) < 2 || aux.Value[0] != '"' || aux.Value[len(aux.Value)-1] != '"' {
		return &ValidationError{Field: "value", Reason: "must be a JSON string"}
	}
	// NewProtectedShare wipes the value bytes as it copies them in; the quotes
	// and any remaining bytes are covered by the deferred wipe. An escape
	// sequence in the literal is not decoded — the base64 check rejects it,
	// which is correct: a share value that needs escaping is not a share value
	// this package ever produced.
	decoded, err := NewProtectedShare(aux.Index, aux.Threshold, aux.Total, aux.Value[1:len(aux.Value)-1])
	if err != nil {
		return err
	}
	if aux.Metadata != nil {
		decoded.Metadata = aux.Metadata
	}
	// Decoding into a share that already holds a value — json.Unmarshal reuses
	// the elements of a non-empty []*ProtectedShare — must not strand the old
	// locked buffer.
	s.Destroy()
	*s = *decoded
	return nil
}

// UnmarshalShareSet decodes a JSON array of shares — the shape json.Marshal
// produces for a []*Share or a []*ProtectedShare — bounding the set BEFORE it
// allocates any locked memory for it.
//
// This is the ingestion path for shares arriving from outside the process, and
// the reason it exists rather than json.Unmarshal into a []*ProtectedShare is
// resource custody, not convenience. Shares are unauthenticated by
// construction: nothing about a share can be checked until the set is
// combined, so a recovery endpoint necessarily decodes whatever it is handed.
// The generic decoder allocates one mlock'd buffer per element AS IT VISITS
// THEM, which lets the submission pick how much of a finite kernel resource to
// take, and memguard answers a failed lock by terminating the process — so the
// answer to an oversized submission would be a dead service rather than an
// error. CombineProtected's 255-share bound cannot help: it runs on shares
// that have already been allocated.
//
// Two checks run against the undecoded document, in this order and before the
// first MemoryEnclave exists:
//
//   - at most maxShares (255) elements — the scheme's own bound, since the
//     x-coordinate is a non-zero byte and no larger set can hold distinct
//     shares;
//   - at most MaxShareSetBytes of JSON, which bounds the sum of the values
//     because each is a substring of the document.
//
// Neither reads a value, and the second is measured on the raw elements the
// standard decoder has already copied, so bounding costs no additional copy of
// share material.
//
// Every returned share must be Destroyed. On any error nothing is returned and
// every share already built is destroyed, so a rejected set leaks no locked
// memory. As with UnmarshalJSON, the caller's own data buffer still holds the
// share material afterwards and is the caller's to wipe.
func UnmarshalShareSet(data []byte) ([]*ProtectedShare, error) {
	// Into raw elements, not shares: this decode allocates ordinary heap that
	// the collector reclaims, and it is what makes the bounds checkable before
	// anything is locked.
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	// The elements are copies of the document's share objects — share material
	// on the ordinary heap — so they are wiped on every path out.
	defer func() {
		for _, elem := range raw {
			wipe(elem)
		}
	}()

	if len(raw) == 0 {
		return nil, &ValidationError{Field: "shares", Reason: "share set is empty"}
	}
	if len(raw) > maxShares {
		return nil, &ValidationError{Field: "shares", Reason: fmt.Sprintf(
			"share set has %d shares, exceeding the maximum of %d", len(raw), maxShares)}
	}
	// Summed over the elements rather than taken from len(data): the same
	// bound, but it cannot be evaded by whitespace accounting, and each
	// element's length is an upper bound on the locked buffer it will produce.
	size := 0
	for _, elem := range raw {
		size += len(elem)
	}
	if size > MaxShareSetBytes {
		return nil, &ValidationError{Field: "shares", Reason: fmt.Sprintf(
			"share set is %d bytes of JSON, exceeding the %d-byte budget", size, MaxShareSetBytes)}
	}

	shares := make([]*ProtectedShare, 0, len(raw))
	for i, elem := range raw {
		share := &ProtectedShare{}
		if err := share.UnmarshalJSON(elem); err != nil {
			// Unwind: a set that is rejected half way through must not strand
			// the locked memory its accepted prefix already took.
			for _, built := range shares {
				built.Destroy()
			}
			return nil, &ValidationError{Field: "shares", Reason: fmt.Sprintf("share %d: %v", i, err)}
		}
		shares = append(shares, share)
	}
	return shares, nil
}

// Protect moves a plain Share's value into protected memory, returning the
// ProtectedShare equivalent. It is the migration path for callers that already
// hold Shares — from Split, or decoded from JSON.
//
// It bounds FURTHER exposure; it cannot undo what has happened. The Share's
// value is a Go string and stays readable in the heap until the GC reclaims
// it, whichever copies were made of it along the way. A caller that can choose
// where the share material is born should call SplitProtected instead and
// never create the string.
func (s *Share) Protect() (*ProtectedShare, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	// The conversion copies; NewProtectedShare wipes that copy into the
	// enclave. The string itself is the one copy nothing can take away.
	value := []byte(s.Value)
	protected, err := NewProtectedShare(s.Index, s.Threshold, s.Total, value)
	if err != nil {
		wipe(value)
		return nil, err
	}
	for k, v := range s.Metadata {
		protected.Metadata[k] = v
	}
	return protected, nil
}

// validateShareHeader checks the non-secret, unauthenticated metadata every
// share carries. It is the single copy of those rules, shared by
// Share.Validate and ProtectedShare.Validate so the deprecated string path and
// the protected path cannot drift into accepting different share sets — the
// same reason parseRawShare is the single copy of the wire-format rules.
func validateShareHeader(index, threshold, total int) error {
	if index < 1 {
		return &ValidationError{Field: "index", Reason: fmt.Sprintf("must be >= 1, got %d", index)}
	}
	if threshold < 2 {
		return &ValidationError{Field: "threshold", Reason: fmt.Sprintf("must be >= 2, got %d", threshold)}
	}
	if total < threshold {
		return &ValidationError{Field: "total", Reason: fmt.Sprintf("must be >= threshold %d, got %d", threshold, total)}
	}
	if index > total {
		return &ValidationError{Field: "index", Reason: fmt.Sprintf("must be <= total %d, got %d", total, index)}
	}
	return nil
}

// isStandardBase64 reports whether b is a syntactically valid standard-base64
// (RFC 4648 section 4) encoding: a non-empty length that is a multiple of four,
// characters drawn from the standard alphabet, and padding confined to the end
// of the final quantum as "xx==" or "xxx=". Alphabet membership alone is not
// enough — "A", "====" and "A=AA" are all made of legal characters and none of
// them decodes.
//
// It deliberately does NOT decode. Decoding to validate would materialize a
// second plaintext copy of the share payload, which is the one thing this type
// exists to avoid, and the grammar is decidable from the encoded form alone.
//
// The grammar checked is exactly the one base64.StdEncoding.Decode accepts, so
// a value that passes here is a value the combine path can decode. That
// includes non-canonical trailing bits in the final quantum (the low bits
// dropped by the padding), which the standard-library decoder tolerates and
// only StdEncoding.Strict rejects: refusing them here would reject shares the
// decoder — and therefore the deprecated Share path — accepts, splitting the
// two paths on which shares are valid.
func isStandardBase64(b []byte) bool {
	if len(b) == 0 || len(b)%4 != 0 {
		return false
	}
	padded := false
	for i, c := range b {
		switch {
		case c == '=':
			// Padding is legal only as the last one or two characters:
			// anything earlier cannot be the tail of the final quantum.
			if i < len(b)-2 {
				return false
			}
			padded = true
		case padded:
			// A data character after padding — "A=AA" and friends.
			return false
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '+', c == '/':
		default:
			return false
		}
	}
	return true
}
