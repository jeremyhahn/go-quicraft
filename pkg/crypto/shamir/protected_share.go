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
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// newProtectedMemory moves bytes into locked, frozen memory; replaceable only through testOnlyProtectedMemory.
var newProtectedMemory = enclave.NewMemory

// newSizedProtectedMemory allocates locked, zeroed, writable memory for output this package produces; every unbudgeted sized allocation goes through it.
var newSizedProtectedMemory = enclave.NewMemorySized

// lockValue moves value into locked, frozen memory, admitted against a when non-nil; value is wiped on every path.
func lockValue(a enclave.Allocator, value []byte) (enclave.ByteAccess, error) {
	if a == nil {
		return newProtectedMemory(value)
	}
	return a.New(value)
}

// lockSized allocates n bytes of locked, zeroed, writable memory, admitted
// against a when the caller supplied one and unbudgeted when a is nil. See
// lockValue.
func lockSized(a enclave.Allocator, n int) (enclave.ByteAccess, error) {
	if a == nil {
		return newSizedProtectedMemory(n)
	}
	return a.NewSized(n)
}

// release destroys an enclave on a decided cleanup path and drops a refused-release report; WipeAll at shutdown bounds a refused frozen region.
func release(e enclave.ByteAccess) {
	if e == nil {
		return
	}
	_ = e.Destroy()
}

// freeze makes e read-only through its Freezer capability. A handle without
// the capability is refused with a *enclave.CapabilityError rather than
// handed back writable.
func freeze(e enclave.ByteAccess) error {
	var f enclave.Freezer
	if !enclave.As(e, &f) {
		return &enclave.CapabilityError{Backend: e.Backend().Name(), Capability: enclave.CapabilityFreeze}
	}
	return f.Freeze()
}

// borrow calls fn with e's locked pages for the duration of fn; errors from fn pass through unchanged.
func borrow(e enclave.ByteAccess, op string, fn func([]byte) error) error {
	ran := false
	err := e.WithBytes(func(b []byte) error {
		ran = true
		return fn(b)
	})
	if err != nil && !ran {
		return &ProtectedMemoryError{Op: op, Err: err}
	}
	return err
}

// testOnlyProtectedMemory replaces the locked-memory constructor and returns a restore func; it panics outside a test binary.
func testOnlyProtectedMemory(fn func([]byte) (enclave.ByteAccess, error)) func() {
	if !testing.Testing() {
		panic("shamir.testOnlyProtectedMemory: test-only allocator override called outside a test binary")
	}
	prev := newProtectedMemory
	newProtectedMemory = fn
	return func() { newProtectedMemory = prev }
}

// testOnlySizedProtectedMemory is testOnlyProtectedMemory for the sized allocator.
func testOnlySizedProtectedMemory(fn func(int) (enclave.ByteAccess, error)) func() {
	if !testing.Testing() {
		panic("shamir.testOnlySizedProtectedMemory: test-only allocator override called outside a test binary")
	}
	prev := newSizedProtectedMemory
	newSizedProtectedMemory = fn
	return func() { newSizedProtectedMemory = prev }
}

// maxProtectedValueBytes is the largest base64 share value NewProtectedShare accepts: the encoding of a MaxSecretBytes share.
var maxProtectedValueBytes = base64.StdEncoding.EncodedLen(rawShareOverhead + MaxSecretBytes)

// lockedBytes reports the RLIMIT_MEMLOCK cost of one n-byte allocation (enclave.LockedSize); an unmappable size is priced past the budget.
func lockedBytes(n int) int {
	locked := enclave.LockedSize(n)
	if n > 0 && locked <= 0 {
		return MaxShareSetBytes + 1
	}
	return int(locked)
}

// MaxShareSetBytes is the locked memory one share set may use: maxShareSetPages pages, enforced before SplitProtected or UnmarshalShareSet allocates.
// enclave.LockedSize(1), not lockedBytes(1), avoids an initialisation cycle.
var MaxShareSetBytes = maxShareSetBytesFor(int(enclave.LockedSize(1)))

// maxShareSetPages is the budget itself: the page count every figure in
// MaxShareSetBytes's derivation is stated in.
const maxShareSetPages = 512

// maxShareSetBytesFor is the budget in bytes for page size pageSize.
func maxShareSetBytesFor(pageSize int) int { return maxShareSetPages * pageSize }

// splitLockedBytes is SplitProtected's peak locked memory, each enclave priced separately in whole pages; validateSplitParams bounds the factors.
func splitLockedBytes(secretLen, threshold, total int) int {
	encoded := base64.StdEncoding.EncodedLen(rawShareOverhead + secretLen)
	return lockedBytes(secretLen*(threshold-1)) + // random coefficients
		lockedBytes(threshold) + // per-share coefficient vector
		lockedBytes(rawShareOverhead+secretLen) + // reused raw payload
		total*lockedBytes(encoded) // one frozen value per returned share
}

// ProtectedShare is a Shamir share whose value lives in locked memory; every one, including unmarshalled ones, must be Destroyed. Metadata is unauthenticated.
type ProtectedShare struct {
	// Index is the share's 1-based position, equal to the x-coordinate
	// embedded in the value.
	Index int
	// Threshold is the M of M-of-N; unauthenticated caller metadata.
	Threshold int
	// Total is the N of the M-of-N scheme. Unauthenticated, like Threshold.
	Total int
	// Metadata is caller-defined and carries no cryptographic meaning. It is
	// NOT protected memory — do not put secret material here.
	Metadata map[string]string

	// value is the base64-encoded [version][x][y...] payload — exactly the
	// bytes of a SplitBytes value — in an mlock'd, guard-paged buffer.
	value enclave.ByteAccess
}

// NewProtectedShare copies base64 value into protected memory and wipes value; a *ValidationError leaves value untouched, a *ProtectedMemoryError has wiped it. Destroy the result.
func NewProtectedShare(index, threshold, total int, value []byte) (*ProtectedShare, error) {
	return newProtectedShare(nil, index, threshold, total, value)
}

// NewProtectedShareWith is NewProtectedShare admitted against a; over budget it returns a *ProtectedMemoryError matching enclave.ErrBudgetExhausted, and a nil a is refused.
func NewProtectedShareWith(a enclave.Allocator, index, threshold, total int, value []byte) (*ProtectedShare, error) {
	if a == nil {
		return nil, enclave.ErrNilAllocator
	}
	return newProtectedShare(a, index, threshold, total, value)
}

// newProtectedShare is the one implementation behind NewProtectedShare and
// NewProtectedShareWith; a nil a selects the unbudgeted constructor (see
// lockValue).
func newProtectedShare(a enclave.Allocator, index, threshold, total int, value []byte) (*ProtectedShare, error) {
	if err := validateShareHeader(index, threshold, total); err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, &ValidationError{Field: "value", Reason: "cannot be empty"}
	}
	// Only standard base64 may enter protected memory: MarshalJSON writes these bytes unescaped.
	if !isStandardBase64(value) {
		return nil, &ValidationError{Field: "value", Reason: "must be standard base64"}
	}
	// Bound the value before it reaches locked memory, a process-wide resource; the bound equals what parseRawShare would reject.
	if len(value) > maxProtectedValueBytes {
		return nil, &ValidationError{Field: "value", Reason: fmt.Sprintf(
			"is %d bytes, exceeding the %d-byte maximum for a %d-byte payload",
			len(value), maxProtectedValueBytes, MaxSecretBytes)}
	}
	// NewMemory (or Allocator.New) locks, wipes the source and freezes; it reports a refusal instead of terminating.
	held, err := lockValue(a, value)
	if err != nil {
		// NOT a ValidationError: value is wiped by now. See the contract above.
		return nil, &ProtectedMemoryError{Op: "locking share value", Err: err}
	}
	return &ProtectedShare{
		Index:     index,
		Threshold: threshold,
		Total:     total,
		Metadata:  make(map[string]string),
		value:     held,
	}, nil
}

// SplitProtected splits secret into total shares, any threshold of which reconstruct it, generating everything in locked memory; the footprint is priced against MaxShareSetBytes first. Destroy every share.
func SplitProtected(secret []byte, threshold, total int) ([]*ProtectedShare, error) {
	return splitProtected(nil, secret, threshold, total)
}

// SplitProtectedWith is SplitProtected with every locked allocation admitted against a; a budget refusal destroys what this call built, and a nil a is refused.
func SplitProtectedWith(a enclave.Allocator, secret []byte, threshold, total int) ([]*ProtectedShare, error) {
	if a == nil {
		return nil, enclave.ErrNilAllocator
	}
	return splitProtected(a, secret, threshold, total)
}

// splitProtected is the one implementation behind SplitProtected and
// SplitProtectedWith; a nil a selects the unbudgeted constructors (see
// lockSized).
func splitProtected(a enclave.Allocator, secret []byte, threshold, total int) ([]*ProtectedShare, error) {
	if err := validateSplitParams(secret, threshold, total); err != nil {
		return nil, err
	}
	// Refuse a footprint over MaxShareSetBytes before the first allocation.
	if need := splitLockedBytes(len(secret), threshold, total); need > MaxShareSetBytes {
		return nil, &SplitError{Reason: fmt.Sprintf(
			"%d-of-%d shares of a %d-byte secret need %d bytes of locked memory, exceeding the %d-byte budget",
			threshold, total, len(secret), need, MaxShareSetBytes)}
	}

	// One polynomial per secret byte from one crypto/rand read into locked memory; coefficients are as sensitive as the secret.
	randCoeffs, err := lockSized(a, len(secret)*(threshold-1))
	if err != nil {
		return nil, &ProtectedMemoryError{Op: "locking coefficient memory", Err: err}
	}
	defer release(randCoeffs)
	// Each scratch is borrowed for exactly the span that uses it; no view outlives its borrow.
	var shares []*ProtectedShare
	if err := borrow(randCoeffs, "opening coefficient memory", func(rc []byte) error {
		if _, err := io.ReadFull(randReader, rc); err != nil {
			return &SplitError{Reason: fmt.Sprintf("reading random coefficients: %v", err)}
		}
		// Raw coefficients are used verbatim, including a zero top coefficient,
		// and no secret digest is embedded in shares — see SplitBytes for why
		// both of those are deliberate and must not be "fixed".

		coeffs, err := lockSized(a, threshold) // [secret byte, c1..c_{t-1}]
		if err != nil {
			return &ProtectedMemoryError{Op: "locking coefficient vector", Err: err}
		}
		defer release(coeffs)
		return borrow(coeffs, "opening coefficient vector", func(cf []byte) error {
			// One payload buffer serves every share; each iteration rewrites all of it.
			raw, err := lockSized(a, rawShareOverhead+len(secret))
			if err != nil {
				return &ProtectedMemoryError{Op: "locking share payload", Err: err}
			}
			defer release(raw)
			return borrow(raw, "opening share payload", func(rw []byte) error {
				var err error
				shares, err = evalProtectedShares(a, secret, threshold, total, rc, cf, rw)
				return err
			})
		})
	}); err != nil {
		return nil, err
	}
	return shares, nil
}

// evalProtectedShares evaluates total shares into locked, frozen values using the caller's borrowed rc, cf and rw views.
func evalProtectedShares(a enclave.Allocator, secret []byte, threshold, total int, rc, cf, rw []byte) ([]*ProtectedShare, error) {
	encodedLen := base64.StdEncoding.EncodedLen(len(rw))

	shares := make([]*ProtectedShare, 0, total)
	// On error, destroy the shares already built so no locked memory leaks.
	failed := func(op string, err error) error {
		for _, s := range shares {
			s.Destroy()
		}
		return &ProtectedMemoryError{Op: op, Err: err}
	}
	for i := 0; i < total; i++ {
		x := byte(i + 1) // non-zero x-coordinate
		rw[0] = shareFormatVersion
		rw[1] = x
		for b := range secret {
			cf[0] = secret[b]
			copy(cf[1:], rc[b*(threshold-1):(b+1)*(threshold-1)])
			rw[rawShareOverhead+b] = gfPolyEval(cf, x)
		}
		// Encode directly into the share's own region, then freeze it.
		value, err := lockSized(a, encodedLen)
		if err != nil {
			return nil, failed("locking share value", err)
		}
		if err := value.WithBytes(func(sb []byte) error {
			base64.StdEncoding.Encode(sb, rw)
			return nil
		}); err != nil {
			release(value)
			return nil, failed("encoding share value", err)
		}
		// A refused freeze leaves the region writable, so the share is torn down rather than returned.
		if err := freeze(value); err != nil {
			release(value)
			return nil, failed("freezing share value", err)
		}
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

// CombineProtected reconstructs the secret into locked memory without integrity checking, using one reusable decode buffer; the caller must Destroy the result.
func CombineProtected(shares []*ProtectedShare) (enclave.ByteAccess, error) {
	return combineProtected(nil, shares)
}

// CombineProtectedWith is CombineProtected with both locked allocations admitted against a; a nil a is refused.
func CombineProtectedWith(a enclave.Allocator, shares []*ProtectedShare) (enclave.ByteAccess, error) {
	if a == nil {
		return nil, enclave.ErrNilAllocator
	}
	return combineProtected(a, shares)
}

// combineProtected is the one implementation behind CombineProtected and
// CombineProtectedWith; a nil a selects the unbudgeted constructors (see
// lockSized).
func combineProtected(a enclave.Allocator, shares []*ProtectedShare) (enclave.ByteAccess, error) {
	if len(shares) == 0 {
		return nil, &CombineError{Reason: "no shares provided"}
	}
	// At most maxShares shares, as in CombineBytes.
	if len(shares) > maxShares {
		return nil, &CombineError{Reason: fmt.Sprintf("too many shares: %d exceeds the maximum of %d", len(shares), maxShares)}
	}
	for i, share := range shares {
		if share == nil {
			return nil, &CombineError{Reason: fmt.Sprintf("share %d is nil", i)}
		}
	}

	// Metadata checks only; parseRawShare holds the wire-format rules shared with CombineBytes.
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

	// One decode buffer sized for the largest value, destroyed on every path; every share read goes through WithValue.
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
	buf, err := lockSized(a, base64.StdEncoding.DecodedLen(widest))
	if err != nil {
		return nil, &ProtectedMemoryError{Op: "locking decode buffer", Err: err}
	}
	defer release(buf)

	// Pass one: validate every share and collect the x-coordinates.
	xs := make([]byte, len(shares))
	seenX := make(map[byte]struct{}, len(shares))
	secretLen := -1
	if err := borrow(buf, "opening decode buffer", func(bufBytes []byte) error {
		for i, share := range shares {
			// Read in place: the share's own locked buffer is the source operand
			// of the decode, never a copy of it. The decode and the parse both
			// run inside the borrow, so the source cannot be unmapped mid-read.
			var (
				n   int
				x   byte
				y   []byte
				err error
			)
			if werr := share.WithValue(func(v []byte) error {
				n, err = base64.StdEncoding.Decode(bufBytes, v)
				if err != nil {
					return &CombineError{Reason: fmt.Sprintf("failed to decode share %d: %v", i, err)}
				}
				x, y, err = parseRawShare(bufBytes[:n], i, seenX, secretLen)
				return err
			}); werr != nil {
				return werr
			}
			secretLen = len(y)
			xs[i] = x
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Pass two: accumulate. The basis depends only on the x-coordinates, which
	// pass one fixed, so each share's contribution can be added as it is
	// decoded and its payload immediately overwritten by the next.
	basis := gfLagrangeZero(xs)
	// The returned enclave: destroyed on every error path below.
	secret, err := lockSized(a, secretLen)
	if err != nil {
		return nil, &ProtectedMemoryError{Op: "locking secret buffer", Err: err}
	}
	// The secret's borrow ends before Freeze below, which waits out in-flight
	// borrows and would otherwise deadlock on this one.
	if err := borrow(secret, "opening secret buffer", func(secretBytes []byte) error {
		return borrow(buf, "opening decode buffer", func(bufBytes []byte) error {
			for i, share := range shares {
				if werr := share.WithValue(func(v []byte) error {
					n, err := base64.StdEncoding.Decode(bufBytes, v)
					if err != nil {
						// Unreachable: pass one decoded this frozen value.
						return &CombineError{Reason: fmt.Sprintf("failed to decode share %d: %v", i, err)}
					}
					accumulateShareInto(secretBytes, bufBytes[rawShareOverhead:n], basis[i])
					return nil
				}); werr != nil {
					return werr
				}
			}
			return nil
		})
	}); err != nil {
		release(secret)
		return nil, err
	}
	// Freeze the reconstructed secret in place; a refused freeze destroys it.
	if ferr := freeze(secret); ferr != nil {
		release(secret)
		return nil, &ProtectedMemoryError{Op: "freezing reconstructed secret", Err: ferr}
	}
	return secret, nil
}

// WithValue calls fn with the base64 value under the enclave's read lock; fn must not retain, write or re-enter this share.
func (s *ProtectedShare) WithValue(fn func([]byte) error) error {
	if s == nil || s.value == nil {
		return enclave.ErrEnclaveDestroyed
	}
	return s.value.WithBytes(fn)
}

// Destroy wipes and releases the value; safe on nil, repeatedly and concurrently. A refused release stays on go-enclave's WipeAll ledger.
func (s *ProtectedShare) Destroy() {
	if s == nil || s.value == nil {
		return
	}
	release(s.value)
}

// Validate checks the share's parameters and that it still holds a value; a nil share fails validation.
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
	// Read the value through the borrow, like every other read.
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

// String returns a description of the share without its value.
func (s *ProtectedShare) String() string {
	if s == nil {
		return "ProtectedShare(nil)"
	}
	return fmt.Sprintf("ProtectedShare{Index: %d, Threshold: %d/%d, Value: [redacted]}",
		s.Index, s.Threshold, s.Total)
}

// LogValue implements slog.LogValuer so loggers never encode the value through MarshalJSON.
func (s *ProtectedShare) LogValue() slog.Value {
	return slog.StringValue(s.String())
}

// MarshalJSON implements json.Marshaler with the real value; the caller owns and should wipe the returned bytes.
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

	// Encode entirely inside the borrow so the value is never read after it.
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
		// Translate enclave.ErrEnclaveDestroyed to the existing ValidationError.
		if errors.Is(err, enclave.ErrEnclaveDestroyed) {
			return nil, &ValidationError{Field: "value", Reason: "share has been destroyed"}
		}
		return nil, err
	}
	return out, nil
}

// UnmarshalJSON decodes one share into locked memory (Destroy it); use UnmarshalShareSet for untrusted sets.
func (s *ProtectedShare) UnmarshalJSON(data []byte) error {
	decoded, err := decodeProtectedShare(nil, data)
	if err != nil {
		return err
	}
	// Decoding into a share that already holds a value — json.Unmarshal reuses
	// the elements of a non-empty []*ProtectedShare — must not strand the old
	// locked buffer.
	s.Destroy()
	*s = *decoded
	return nil
}

// decodeProtectedShare decodes one share object, locking its value against a when non-nil.
func decodeProtectedShare(a enclave.Allocator, data []byte) (*ProtectedShare, error) {
	var aux struct {
		Index     int               `json:"index"`
		Threshold int               `json:"threshold"`
		Total     int               `json:"total"`
		Value     json.RawMessage   `json:"value"`
		Metadata  map[string]string `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return nil, err
	}
	// Decode the value as a wipeable raw token, never a string.
	defer wipe(aux.Value)
	if len(aux.Value) < 2 || aux.Value[0] != '"' || aux.Value[len(aux.Value)-1] != '"' {
		return nil, &ValidationError{Field: "value", Reason: "must be a JSON string"}
	}
	// newProtectedShare wipes the value bytes; the deferred wipe covers the rest.
	decoded, err := newProtectedShare(a, aux.Index, aux.Threshold, aux.Total, aux.Value[1:len(aux.Value)-1])
	if err != nil {
		return nil, err
	}
	if aux.Metadata != nil {
		decoded.Metadata = aux.Metadata
	}
	return decoded, nil
}

// shareSetLockedBytes prices UnmarshalShareSet's locked memory in pages, stopping at the first element past the budget.
func shareSetLockedBytes(raw []json.RawMessage) int {
	size := 0
	for _, elem := range raw {
		size += elementLockedBytes(elem)
		if size > MaxShareSetBytes {
			return size
		}
	}
	return size
}

// elementLockedBytes prices the one enclave an element becomes, its value page-rounded, matching splitLockedBytes.
func elementLockedBytes(elem json.RawMessage) int {
	var aux struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(elem, &aux); err != nil || len(aux.Value) < 2 {
		return lockedBytes(len(elem))
	}
	defer wipe(aux.Value)
	// Less the two quotes: UnmarshalJSON passes aux.Value[1:len-1] to
	// NewProtectedShare, and that slice is what becomes the enclave.
	return lockedBytes(len(aux.Value) - 2)
}

// UnmarshalShareSet decodes a JSON array of shares, checking the count and MaxShareSetBytes before allocating; on error every built share is destroyed.
func UnmarshalShareSet(data []byte) ([]*ProtectedShare, error) {
	return unmarshalShareSet(nil, data)
}

// UnmarshalShareSetWith is UnmarshalShareSet with every value admitted against a; a nil a is refused.
func UnmarshalShareSetWith(a enclave.Allocator, data []byte) ([]*ProtectedShare, error) {
	if a == nil {
		return nil, enclave.ErrNilAllocator
	}
	return unmarshalShareSet(a, data)
}

// unmarshalShareSet is the one implementation behind UnmarshalShareSet and
// UnmarshalShareSetWith; a nil a selects the unbudgeted constructor (see
// lockValue).
func unmarshalShareSet(a enclave.Allocator, data []byte) ([]*ProtectedShare, error) {
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
	// Sum each element's length page-rounded, since each becomes one enclave.
	if size := shareSetLockedBytes(raw); size > MaxShareSetBytes {
		return nil, &ValidationError{Field: "shares", Reason: fmt.Sprintf(
			"share set needs at least %d bytes of locked memory, exceeding the %d-byte budget",
			size, MaxShareSetBytes)}
	}

	shares := make([]*ProtectedShare, 0, len(raw))
	for i, elem := range raw {
		share, err := decodeProtectedShare(a, elem)
		if err != nil {
			// Unwind: a set that is rejected half way through must not strand
			// the locked memory its accepted prefix already took — nor, under an
			// allocator, the reservation it holds against a.
			for _, built := range shares {
				built.Destroy()
			}
			// Report a refused lock as a *ProtectedMemoryError, not a validation error.
			var pme *ProtectedMemoryError
			if errors.As(err, &pme) {
				return nil, &ProtectedMemoryError{Op: fmt.Sprintf("share %d: %s", i, pme.Op), Err: pme.Err}
			}
			return nil, &ValidationError{Field: "shares", Reason: fmt.Sprintf("share %d: %v", i, err)}
		}
		shares = append(shares, share)
	}
	return shares, nil
}

// validateShareHeader checks a share's unauthenticated metadata; shared by Validate and the constructor.
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

// isStandardBase64 reports whether b is the grammar base64.StdEncoding.Decode accepts, without decoding.
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
