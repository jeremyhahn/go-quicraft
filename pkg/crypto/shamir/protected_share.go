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

// newProtectedMemory moves bytes into locked, frozen memory. It is
// package-private and swapped ONLY through testOnlyProtectedMemory, which
// panics outside a test binary — the same gate, and for the same reason, as
// randReader: this sits on the share-custody path, and a mutable global on that
// path must be provably unreachable from production code.
//
// It exists because the failure it stands in for cannot be produced otherwise.
// Reaching a refused mlock for real means driving RLIMIT_MEMLOCK to its ceiling
// across the whole process, which would fail allocations in every unrelated test
// sharing the binary rather than the one under test. The
// ownership contract on that path — the value is already wiped, and the error is
// a *ProtectedMemoryError rather than a *ValidationError — is exactly what a
// caller gets wrong silently, so it is worth being able to test.
var newProtectedMemory = enclave.NewMemoryEnclave

// newSizedProtectedMemory allocates locked, zeroed, still-WRITABLE memory for
// its caller to fill in place and then freeze. It is the sibling seam to
// newProtectedMemory, gated the same way and for the same reason.
//
// The two are not interchangeable and the split is the point. newProtectedMemory
// brings material that already exists OUTSIDE an enclave in — a caller's share
// value — and freezes it as it moves. This one is for material this package is
// about to PRODUCE, which must never touch the ordinary heap at all: the encoded
// share value is written straight into these pages and frozen there.
//
// EVERY sized allocation in this package goes through it, not only the per-share
// one in SplitProtected's loop: SplitProtected's three pre-loop buffers (the
// coefficient pool, the coefficient vector and the share payload) and both of
// CombineProtected's (the decode scratch and the returned secret) included. A
// call site that bypassed it would leave its refusal path — a kernel that
// denies the lock, and the *ProtectedMemoryError it maps to — out of reach of
// the errLockRefused injection, and so untested. A seam half the call sites
// bypass is not a seam.
//
// Both seams stand in for the UNBUDGETED constructors only. A call that was
// handed an [enclave.Allocator] — the ...With variants — allocates through it
// instead (see lockValue and lockSized), and needs no seam: its refusal is
// produced for real by an allocator sized to refuse, which, unlike exhausting
// RLIMIT_MEMLOCK, touches nothing outside the test that built it.
var newSizedProtectedMemory = enclave.NewMemoryEnclaveSized

// lockValue moves value into locked, frozen memory, admitted against a when the
// caller supplied one and unbudgeted when a is nil.
//
// It is the single place this package chooses between the two, so threading an
// allocator through a path is one parameter rather than a nil branch at every
// allocation site — the same shape as go-enclave's own allocSized. nil is not
// a refusal down here: it is how the original, allocator-less entry points ask
// for the unbudgeted behaviour they have always had. The refusal of a nil
// allocator belongs at the ...With entry points, where it is a statement about
// the call.
//
// Both constructors take ownership of value and wipe it on every path,
// including a budget refusal, so callers keep one ownership contract whichever
// is chosen.
func lockValue(a *enclave.Allocator, value []byte) (*enclave.MemoryEnclave, error) {
	if a == nil {
		return newProtectedMemory(value)
	}
	return a.New(value)
}

// lockSized allocates n bytes of locked, zeroed, writable memory, admitted
// against a when the caller supplied one and unbudgeted when a is nil. See
// lockValue.
func lockSized(a *enclave.Allocator, n int) (*enclave.MemoryEnclave, error) {
	if a == nil {
		return newSizedProtectedMemory(n)
	}
	return a.NewSized(n)
}

// release destroys an enclave on a cleanup path whose outcome is already
// decided, and deliberately drops go-enclave's report of a refused release.
//
// Every caller is either a scratch buffer of a call that has produced its
// result, or a partial result being torn down on the way to returning the
// error that caused the teardown. Neither can do anything useful with a second
// error: replacing the result would discard material the caller owns, and
// replacing the error would hide the failure that matters. Dropping the report
// changes nothing about the material; what that material's state is after a
// refusal depends on whether the region was frozen:
//
//   - A WRITABLE region is wiped before any step the kernel can refuse, so a
//     refused munlock or munmap holds pages that are already zero, on
//     go-enclave's WipeAll ledger for a later retry.
//   - A FROZEN region (every finished share value and recovered secret this
//     package hands out, and under go-enclave v0.7 every buffer
//     SealedEnclave.Open returns) must be made writable before it can be wiped. A denied mprotect falls back to a bare
//     unmap and ErrReleasedWithoutWipe — no in-process wipe, but the pages are
//     gone and the kernel zeroes them before reuse. If that unmap is refused
//     too, the mapping stays held UNWIPED — read-only and mlock'd, never
//     swapped, but still holding the material — on the WipeAll ledger until a
//     later Destroy or a shutdown WipeAll releases it.
//
// So a refused release of a frozen region is not a wipe, and the WipeAll at
// shutdown is what bounds it. This package is a primitive with no logger; a
// consumer that must observe a refused release holds the enclave itself and
// checks what Destroy returns.
//
// Detected canary corruption still panics inside Destroy, as go-enclave
// documents; that is not dropped here.
func release(e *enclave.MemoryEnclave) {
	_ = e.Destroy()
}

// testOnlyProtectedMemory replaces the locked-memory constructor and returns a
// function restoring the previous one. It PANICS outside a test binary.
func testOnlyProtectedMemory(fn func([]byte) (*enclave.MemoryEnclave, error)) func() {
	if !testing.Testing() {
		panic("shamir.testOnlyProtectedMemory: test-only allocator override called outside a test binary")
	}
	prev := newProtectedMemory
	newProtectedMemory = fn
	return func() { newProtectedMemory = prev }
}

// testOnlySizedProtectedMemory is testOnlyProtectedMemory for the sized
// allocator. Same gate, same restore contract, and separate because the two
// constructors have different signatures and different callers: overriding one
// must not silently change the behaviour of the other.
func testOnlySizedProtectedMemory(fn func(int) (*enclave.MemoryEnclave, error)) func() {
	if !testing.Testing() {
		panic("shamir.testOnlySizedProtectedMemory: test-only allocator override called outside a test binary")
	}
	prev := newSizedProtectedMemory
	newSizedProtectedMemory = fn
	return func() { newSizedProtectedMemory = prev }
}

// maxProtectedValueBytes is the largest base64 share value NewProtectedShare
// will move into locked memory: the encoding of a full
// [version:1][x:1][y:MaxSecretBytes] share. It is the locked-memory analogue
// of parseRawShare's MaxSecretBytes bound, applied one step earlier because
// the allocation it guards is a kernel resource rather than heap.
//
// It is a bound on the VALUE, not a budget, so it is stated in value bytes and
// is not page-rounded. NewProtectedShare makes exactly one allocation, whose
// real cost is lockedBytes of that value — at most one page more than this
// number, and no share count multiplies it. The under-counting that page
// rounding prevents is a property of the SET paths, where one page of
// slack per share is the dominant term; here it cannot compound. The paths that
// build a set from many NewProtectedShare calls — UnmarshalShareSet, and
// seal.ShamirInitResult.UnmarshalJSON through it — price the set page-rounded
// against MaxShareSetBytes before the first of those calls runs.
var maxProtectedValueBytes = base64.StdEncoding.EncodedLen(rawShareOverhead + MaxSecretBytes)

// lockedBytes reports what ONE allocation of n bytes actually costs against
// RLIMIT_MEMLOCK, which is n rounded UP to a whole page.
//
// It is go-enclave's own [enclave.LockedSize], not a local rounding rule, for
// the reason go-enclave exports it: the page size is the allocator's property,
// so a copy here would be a second answer to a question the dependency already
// answers, and the two would drift the first time either moved.
//
// The unit matters more than the arithmetic. A caller's bytes and the kernel's
// bytes are NOT the same unit — a 32-byte enclave locks a full 4 KiB page — so a
// budget summed from requested sizes cannot be compared to RLIMIT_MEMLOCK at
// all, which is the only comparison the budget exists to make. Every locked
// allocation this package prices goes through here.
//
// It returns 0 for n <= 0, matching LockedSize, and every caller below has
// already bounded n to a positive value.
//
// It does NOT pass LockedSize's other zero through. LockedSize also reports 0
// for a positive n too large to map at all, and a budget is the one place that
// answer must not be taken literally: pricing an impossible allocation at
// nothing ADMITS it, and the caller then meets the refusal deep inside a
// half-built set — which is the outcome pricing up front exists to avoid. So an
// unmappable size is priced past the budget, which refuses it. Unreachable from
// SplitProtected, whose factors validateSplitParams has already bounded; the
// path that can reach it in principle is UnmarshalShareSet, whose element
// lengths come from a document that arrives from outside the process.
func lockedBytes(n int) int {
	locked := enclave.LockedSize(n)
	if n > 0 && locked <= 0 {
		return MaxShareSetBytes + 1
	}
	return int(locked)
}

// MaxShareSetBytes is the locked memory this package will spend on ONE share
// set, counted in the PAGES the kernel charges rather than the bytes a caller
// asks for. It is maxShareSetPages pages, so its BYTE value depends on the
// host's page size. Both paths that can allocate a whole set enforce it before
// they allocate any of it: SplitProtected against the page-rounded footprint its
// parameters imply, and UnmarshalShareSet against the page-rounded sum of the
// undecoded elements (every share value is a substring of its element, so the
// element bounds the buffer it will produce).
//
// It is a POLICY bound, unlike maxProtectedValueBytes and maxShares which are
// properties of the scheme, and it exists because the per-value bounds are
// per value. Legal dimensions multiply: 255 shares that are each individually
// legal add up to ~22 MiB of locked payload, and a 64 KiB secret split 255
// ways needs ~16 MiB of polynomial coefficients on top. Both are comfortably
// past the 8 MiB RLIMIT_MEMLOCK a Linux host commonly defaults to — so a
// combination of legal arguments must produce an error rather than a refusal
// deep inside a half-built share set.
//
// A refused lock is a typed *ProtectedMemoryError that fails exactly the
// allocation that hit the ceiling. The bound is still worth enforcing up front:
// refusing before allocating is the difference between a clean error and a
// partially built set that has to be torn down.
//
// # Why it is a page count and not a byte count
//
// The budget is a PAGE budget — every figure the derivation below turns on is a
// page count — so stating it in the bytes of one particular host would make the
// primitive's headline case fail on every other. A fixed 2 << 20 is 512 pages
// on a 4 KiB-page host only: on a 16 KiB-page host the 255-share split of a
// 32-byte root key costs the same 260 pages, which is 4,259,840 bytes and would
// be refused; at 64 KiB it would miss by a factor of eight. 64 KiB pages are not
// exotic — RHEL and CentOS Stream arm64 kernels ship with them.
//
// The page size comes from enclave.LockedSize(1), the allocator's own answer,
// for the reason lockedBytes gives: the page size is the allocator's property,
// and a second source for it here would be a second answer that drifts.
//
// This is a var rather than a const because its value is a property of the
// host. Every consumer reads it; none uses it in a constant expression.
//
// One consequence worth stating plainly. Against the 8 MiB RLIMIT_MEMLOCK a
// Linux host commonly defaults to, a maximal set is a quarter of the ceiling at
// 4 KiB pages and FOUR TIMES it at 64 KiB. That is not this cap admitting more
// than it should: the kernel charges 512 pages either way, so the operator on a
// large-page host must raise RLIMIT_MEMLOCK. A byte-valued constant would not
// make that cost smaller, it would just refuse the operation instead of naming
// the cost.
//
// # Why 512 pages
//
// Page-rounded, a share set costs at least one page per share. Summing
// REQUESTED bytes instead under-counts a 255-share split by ~680x, so a cap
// stated that way looks generous while admitting sets that lock far more than
// it names.
//
// The floor is the headline case this primitive exists to serve — the full
// 255-share split of a 32-byte root key, which is 259 pages and does not fit
// 256. A cap that refuses the operation the package is for is not a cap, it is
// a bug; what follows is where the cap is put and why.
//
// 512 pages is set at the natural edge of the regime the primitive serves: a
// split whose ENCODED SHARE FITS ONE PAGE. That is every secret up to 3070
// bytes at the full 255 ways, whose peak is 448 pages — the largest footprint
// on that side of the line, and the number 512 is chosen to clear. It also
// admits the 5-of-5 split of the largest secret the package accepts, 192 pages.
// One byte past that edge every share needs a second page and the footprint
// jumps: 255 shares of a 3071-byte secret price at 703 pages and are refused,
// as are 255 shares of a 4 KiB secret at 767 — which is sharing a payload
// rather than a key. Envelope large payloads under a key that IS this size,
// exactly as MaxSecretBytes says.
//
// Every one of those figures is a page count and holds at any page size.
//
// This bounds ONE call: a process running several at once — or holding sets
// after they return — still has to bound itself.
// go-enclave's Allocator is the mechanism for that, and it belongs to whoever
// owns the process's locked-memory policy, not to this primitive. This package
// only accepts one: SplitProtectedWith, CombineProtectedWith,
// NewProtectedShareWith, UnmarshalShareSetWith and Share.ProtectWith charge
// every locked allocation they make to the allocator they are handed, so the
// caller's budget covers share material too.
//
// A caller that genuinely needs more does its own accounting and works share by
// share with SplitBytes and NewProtectedShare — the policy is then explicitly
// the caller's, which is where a number that cannot be derived from the scheme
// belongs.
// enclave.LockedSize(1) rather than lockedBytes(1): lockedBytes prices an
// unmappable size AGAINST this budget, so reaching for it here is an
// initialisation cycle. One byte is always mappable, so the guard it adds is
// not needed at this call site.
var MaxShareSetBytes = maxShareSetBytesFor(int(enclave.LockedSize(1)))

// maxShareSetPages is the budget itself: the page count every figure in
// MaxShareSetBytes's derivation is stated in.
const maxShareSetPages = 512

// maxShareSetBytesFor is the budget in bytes on a host whose page size is
// pageSize.
//
// The derivation is a function rather than an expression inlined above so a
// test can evaluate it at page sizes this host does not have. On a 4 KiB-page
// host the derived budget equals a fixed 2 << 20, so no assertion on
// MaxShareSetBytes can tell the two apart there; what a test can pin is that
// the budget scales with the page size, and that is this function.
func maxShareSetBytesFor(pageSize int) int { return maxShareSetPages * pageSize }

// splitLockedBytes reports the locked memory SplitProtected holds at its peak
// for these parameters: the random polynomial coefficients, the per-share
// coefficient vector and raw payload it reuses, and the encoded value of every
// share it returns.
//
// Every term is priced through lockedBytes, SEPARATELY, because each is its own
// enclave and the kernel charges each a whole page. Summing the requested sizes
// first and rounding once at the end would price 255 four-byte share values at
// 1020 bytes when the kernel charges 255 pages: measured against VmLck, a
// 255-share split of a one-byte secret priced that way comes out 680x over its
// own price — 1,044,480 charged against 1,536 priced. The per-share rounding,
// not the secret size, is what dominates a wide split.
//
// There is no encoding-scratch term. Each share is encoded directly into its
// own sized enclave and frozen in place with go-enclave's Freeze, so no second
// writable region is live alongside it. Were a reused scratch reintroduced, it
// would be live for the duration of every copy out of it and would have to be
// priced here, or MaxShareSetBytes would stop being an upper bound.
//
// The multiplication cannot overflow because validateSplitParams runs first and
// bounds each factor — the secret to MaxSecretBytes (64 KiB) and both threshold
// and total to maxShares (255) — so the largest product this can compute is 255
// page-rounded encodings of a 64 KiB secret, ~23 MB.
func splitLockedBytes(secretLen, threshold, total int) int {
	encoded := base64.StdEncoding.EncodedLen(rawShareOverhead + secretLen)
	return lockedBytes(secretLen*(threshold-1)) + // random coefficients
		lockedBytes(threshold) + // per-share coefficient vector
		lockedBytes(rawShareOverhead+secretLen) + // reused raw payload
		total*lockedBytes(encoded) // one frozen value per returned share
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
// un-wipeable copy in between.
//
// WHERE OWNERSHIP TRANSFERS, precisely, because the two failures differ:
//
//   - A *ValidationError means the value never reached protected memory and is
//     LEFT UNTOUCHED for the caller to wipe or retry. Every syntactic check runs
//     before any locked memory is allocated, which is also why an unauthenticated
//     submission cannot size that allocation.
//   - A *ProtectedMemoryError means the kernel refused the lock, and by then the
//     value has ALREADY BEEN WIPED — the locked-memory constructor wipes its
//     source before reporting a refusal. Do NOT retry with the same buffer; it
//     is zeroes. This is why the two are different types.
//
// The value must be syntactically valid standard base64 and no longer than the
// encoding of a MaxSecretBytes payload; both are checked BEFORE any locked
// memory is allocated, because that allocation is the resource an
// unauthenticated share submission would otherwise get to size.
//
// The returned share must be Destroyed.
//
// The value's locked memory is UNBUDGETED: it is charged to no
// [enclave.Allocator]. A caller that bounds its locked memory with one uses
// NewProtectedShareWith.
func NewProtectedShare(index, threshold, total int, value []byte) (*ProtectedShare, error) {
	return newProtectedShare(nil, index, threshold, total, value)
}

// NewProtectedShareWith is NewProtectedShare with the value's locked memory
// admitted against a, so a consumer's locked-memory budget covers it.
//
// Its contract is NewProtectedShare's, with one addition. When admitting the
// value would exceed a's budget it returns a *ProtectedMemoryError matching
// [enclave.ErrBudgetExhausted] under errors.Is — decided before any locked
// memory is requested, and, as for every *ProtectedMemoryError, with value
// ALREADY WIPED. The refusal fails this call alone; shares built earlier, from
// a or anywhere else, are untouched.
//
// A nil a is refused with [enclave.ErrNilAllocator] before value is touched,
// rather than falling back to the unbudgeted constructor: a caller that asked
// for a budget and supplied none has a bug, and silently locking outside the
// budget would hide it. Libraries obtain a from [enclave.ProcessAllocator].
func NewProtectedShareWith(a *enclave.Allocator, index, threshold, total int, value []byte) (*ProtectedShare, error) {
	if a == nil {
		return nil, enclave.ErrNilAllocator
	}
	return newProtectedShare(a, index, threshold, total, value)
}

// newProtectedShare is the one implementation behind NewProtectedShare and
// NewProtectedShareWith; a nil a selects the unbudgeted constructor (see
// lockValue).
func newProtectedShare(a *enclave.Allocator, index, threshold, total int, value []byte) (*ProtectedShare, error) {
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
	// mlock'd memory: a finite PROCESS-WIDE resource (RLIMIT_MEMLOCK), so what
	// one caller takes is denied to every other, and the next to reach the
	// ceiling is refused with ErrLockedMemoryUnavailable. UnmarshalJSON constructs from
	// operator-supplied JSON, which at a recovery ceremony is unauthenticated
	// by construction, so a value made of arbitrarily many legal base64
	// characters must not be able to reach the locking allocator at all.
	// parseRawShare's MaxSecretBytes check runs far too late to help:
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
	// NewMemoryEnclave (or Allocator.New, when a budget was supplied) moves the
	// bytes into locked memory, wipes the source and freezes the result. It
	// reports a refusal instead of terminating: locking pages is a syscall the
	// kernel can decline, a budget can be exhausted, and this constructor runs on
	// operator-supplied JSON at a recovery ceremony.
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
// against a PROCESS-WIDE RLIMIT_MEMLOCK, so a ceremony splitting into the full
// 255 shares should destroy them as they are distributed rather than holding
// the set: what it holds is denied to every other caller until it lets go.
//
// Because a refusal deep inside a half-built share set is a far worse answer
// than a refusal before it, the whole footprint — coefficients, scratch and
// every returned value — is priced against MaxShareSetBytes before the first
// allocation and refused with a *SplitError if it does not fit. Arguments that
// are individually legal can multiply into one that is not: see MaxShareSetBytes
// for what fits and what to do instead. Without that pricing the failure would
// be an ErrLockedMemoryUnavailable on the one allocation that hit the ceiling —
// recoverable, but not what this API should hand back for arguments it could
// have priced.
//
// Its locked memory is UNBUDGETED: it is charged to no [enclave.Allocator]. A
// caller that bounds its locked memory with one uses SplitProtectedWith.
func SplitProtected(secret []byte, threshold, total int) ([]*ProtectedShare, error) {
	return splitProtected(nil, secret, threshold, total)
}

// SplitProtectedWith is SplitProtected with EVERY locked allocation it makes —
// the coefficient pool, the coefficient vector, the share payload and each
// returned share's value — admitted against a, so a consumer's locked-memory
// budget covers the share set and the scratch that produced it.
//
// Its contract is SplitProtected's, with one addition. When an allocation would
// exceed a's budget it returns a *ProtectedMemoryError matching
// [enclave.ErrBudgetExhausted] under errors.Is, and that can happen partway
// through the set: the budget is shared with every other holder of a, so it
// cannot be priced up front the way MaxShareSetBytes is. Every region this call
// had already allocated — scratch and the shares built so far — is destroyed
// before it returns, so a refused split gives its whole reservation back to a.
// Shares returned by EARLIER calls are the caller's and are untouched.
//
// A nil a is refused with [enclave.ErrNilAllocator] before anything is
// allocated, rather than falling back to the unbudgeted path. Libraries obtain
// a from [enclave.ProcessAllocator].
func SplitProtectedWith(a *enclave.Allocator, secret []byte, threshold, total int) ([]*ProtectedShare, error) {
	if a == nil {
		return nil, enclave.ErrNilAllocator
	}
	return splitProtected(a, secret, threshold, total)
}

// splitProtected is the one implementation behind SplitProtected and
// SplitProtectedWith; a nil a selects the unbudgeted constructors (see
// lockSized).
func splitProtected(a *enclave.Allocator, secret []byte, threshold, total int) ([]*ProtectedShare, error) {
	if err := validateSplitParams(secret, threshold, total); err != nil {
		return nil, err
	}
	// The individual bounds are each satisfiable and still multiply into a
	// footprint no host can lock: a 64 KiB secret split 255 ways is ~16 MiB of
	// coefficients before a single share exists. Priced and refused BEFORE the
	// first allocation, so a caller that asks for too much gets a *SplitError
	// naming the arithmetic rather than an ErrLockedMemoryUnavailable partway
	// through a set it must then tear down — and rather than draining the
	// process-wide locked-memory budget on its way to failing.
	if need := splitLockedBytes(len(secret), threshold, total); need > MaxShareSetBytes {
		return nil, &SplitError{Reason: fmt.Sprintf(
			"%d-of-%d shares of a %d-byte secret need %d bytes of locked memory, exceeding the %d-byte budget",
			threshold, total, len(secret), need, MaxShareSetBytes)}
	}

	// One degree-(threshold-1) polynomial per secret byte, sharing a single
	// bulk read of crypto/rand — as in SplitBytes, but read directly into
	// locked memory. The coefficients are as sensitive as the secret: with
	// them and one share, the secret follows.
	randCoeffs, err := lockSized(a, len(secret)*(threshold-1))
	if err != nil {
		return nil, &ProtectedMemoryError{Op: "locking coefficient memory", Err: err}
	}
	defer release(randCoeffs)
	rc, err := randCoeffs.Open()
	if err != nil {
		return nil, &ProtectedMemoryError{Op: "opening coefficient memory", Err: err}
	}
	if _, err := io.ReadFull(randReader, rc); err != nil {
		return nil, &SplitError{Reason: fmt.Sprintf("reading random coefficients: %v", err)}
	}
	// Raw coefficients are used verbatim, including a zero top coefficient,
	// and no secret digest is embedded in shares — see SplitBytes for why
	// both of those are deliberate and must not be "fixed".

	coeffs, err := lockSized(a, threshold) // [secret byte, c1..c_{t-1}]
	if err != nil {
		return nil, &ProtectedMemoryError{Op: "locking coefficient vector", Err: err}
	}
	defer release(coeffs)
	cf, err := coeffs.Open()
	if err != nil {
		return nil, &ProtectedMemoryError{Op: "opening coefficient vector", Err: err}
	}
	// One raw payload buffer serves every share: each iteration rewrites all
	// of it (version, x, and one y byte per secret byte) before it is read, so
	// no share ever sees another's bytes, and Destroy wipes the last share's
	// payload on the way out.
	raw, err := lockSized(a, rawShareOverhead+len(secret))
	if err != nil {
		return nil, &ProtectedMemoryError{Op: "locking share payload", Err: err}
	}
	defer release(raw)
	rw, err := raw.Open()
	if err != nil {
		return nil, &ProtectedMemoryError{Op: "opening share payload", Err: err}
	}

	encodedLen := base64.StdEncoding.EncodedLen(len(rw))

	shares := make([]*ProtectedShare, 0, total)
	// A share built by an earlier iteration is locked memory NOTHING ELSE CAN
	// REACH once this function returns an error: the caller gets nil, and the
	// doc contract puts the Destroy obligation on the shares it never received.
	// So a mid-loop failure must tear down what it already built, or it leaks
	// mlock'd pages for the life of the process.
	//
	// That matters most on exactly the path that reaches it. Every failure below
	// is a locked-memory refusal — an allocation for a share value, or the
	// mprotect that freezes it — so they fire when locked memory is already
	// scarce, and a leak there makes the shortage permanent rather than
	// transient.
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
		// Encoded DIRECTLY into the region the share will own, then frozen. The
		// share material exists in exactly one place from the moment it exists at
		// all, and that place is locked and wipeable. Freezing after the fill
		// avoids a reused writable scratch and a copy out of it, which would add
		// a locked region to the peak footprint (see splitLockedBytes) and a
		// window in which two copies of a share value were live at once.
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
		// A refused freeze leaves the region alive and WRITABLE. A share whose
		// value can still be written is not the type this package hands out —
		// Value documents the buffer as read-only and callers rely on a stray
		// write faulting — so it is torn down rather than returned.
		if err := value.Freeze(); err != nil {
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
// shares themselves — and locked memory is PROCESS-WIDE, so a peak taken here
// is a peak denied to every other caller until this call returns, and its
// failure — an ErrLockedMemoryUnavailable — is not something an untrusted share
// set should get to provoke. So the shares are walked twice against a single reusable buffer: once to
// validate them and collect the x-coordinates, then, with the Lagrange basis
// those fix, once more to accumulate each share's contribution into the
// result. Decoding base64 twice is cheap; a peak that an untrusted share set
// gets to choose is not.
//
// That shape is also why this path needs no budget arithmetic and so has none
// to get wrong. It allocates TWO regions whatever the size of the set — the
// decode buffer and the secret scratch — each bounded by a share value the
// constructor already bounded, so page rounding costs at most two pages of
// slack in total and nothing multiplies by the share count. Per-share rounding
// summed 255 times, the under-count that matters on the split path, cannot
// arise here. The shares themselves were locked by whoever built them, against
// MaxShareSetBytes on that path.
//
// Only the x-coordinates and the Lagrange basis derived from them live on the
// ordinary heap: a share's x-coordinate is its published index, not secret (see
// interpolateSecretInto).
//
// Its locked memory is UNBUDGETED: it is charged to no [enclave.Allocator]. A
// caller that bounds its locked memory with one uses CombineProtectedWith.
func CombineProtected(shares []*ProtectedShare) (*enclave.MemoryEnclave, error) {
	return combineProtected(nil, shares)
}

// CombineProtectedWith is CombineProtected with both of its locked allocations
// — the decode buffer and the returned secret — admitted against a, so a
// consumer's locked-memory budget covers reconstruction too. The returned
// enclave holds its reservation on a until it is Destroyed.
//
// Its contract is CombineProtected's, with one addition. When an allocation
// would exceed a's budget it returns a *ProtectedMemoryError matching
// [enclave.ErrBudgetExhausted] under errors.Is, having destroyed whatever this
// call had already allocated. The input shares are left intact either way, as
// they are on every CombineProtected error path.
//
// A nil a is refused with [enclave.ErrNilAllocator] before anything is
// allocated, rather than falling back to the unbudgeted path. Libraries obtain
// a from [enclave.ProcessAllocator].
func CombineProtectedWith(a *enclave.Allocator, shares []*ProtectedShare) (*enclave.MemoryEnclave, error) {
	if a == nil {
		return nil, enclave.ErrNilAllocator
	}
	return combineProtected(a, shares)
}

// combineProtected is the one implementation behind CombineProtected and
// CombineProtectedWith; a nil a selects the unbudgeted constructors (see
// lockSized).
func combineProtected(a *enclave.Allocator, shares []*ProtectedShare) (*enclave.MemoryEnclave, error) {
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
	buf, err := lockSized(a, base64.StdEncoding.DecodedLen(widest))
	if err != nil {
		return nil, &ProtectedMemoryError{Op: "locking decode buffer", Err: err}
	}
	defer release(buf)

	// Materialized HERE, outside every WithValue below. buf is created in this
	// function and never escapes it, so no other goroutine can destroy it while
	// the loop runs — which is the condition go-enclave's Open documents as the
	// one that makes an aliasing view safe. go-enclave holds only the borrowed
	// enclave's own read lock, so opening buf inside a share's borrow would also
	// be safe; hoisting it is a clarity choice.
	bufBytes, err := buf.Open()
	if err != nil {
		return nil, &ProtectedMemoryError{Op: "opening decode buffer", Err: err}
	}

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
			n, err = base64.StdEncoding.Decode(bufBytes, v)
			if err != nil {
				return &CombineError{Reason: fmt.Sprintf("failed to decode share %d: %v", i, err)}
			}
			x, y, err = parseRawShare(bufBytes[:n], i, seenX, secretLen)
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
	// THE ENCLAVE THIS FUNCTION RETURNS, allocated before a single share is
	// accumulated and written into in place. It is not a scratch, so it is
	// destroyed on every error path below and on none of the success path.
	secret, err := lockSized(a, secretLen)
	if err != nil {
		return nil, &ProtectedMemoryError{Op: "locking secret buffer", Err: err}
	}
	// Materialized outside the borrow for the same reason as bufBytes above.
	secretBytes, err := secret.Open()
	if err != nil {
		release(secret)
		return nil, &ProtectedMemoryError{Op: "opening secret buffer", Err: err}
	}
	for i, share := range shares {
		if werr := share.WithValue(func(v []byte) error {
			n, err := base64.StdEncoding.Decode(bufBytes, v)
			if err != nil {
				// Unreachable: pass one decoded this same value, and a share's
				// value is frozen read-only from the moment it is constructed.
				// Handled rather than ignored so a future change to either of
				// those cannot turn it into a silently wrong secret.
				return &CombineError{Reason: fmt.Sprintf("failed to decode share %d: %v", i, err)}
			}
			accumulateShareInto(secretBytes, bufBytes[rawShareOverhead:n], basis[i])
			return nil
		}); werr != nil {
			release(secret)
			return nil, werr
		}
	}
	// FROZEN for the same reason a share value is: the reconstructed key is final,
	// and nothing should mutate it in place. Freeze mprotects the region the
	// accumulation already wrote into, so the secret exists in exactly one place
	// for its whole life and this function returns the enclave it built — no
	// locked-to-locked copy, and no second region holding the same secret.
	//
	// A refused freeze is destroyed rather than returned: the caller is promised
	// material it can read and destroy, not a buffer it can silently corrupt.
	if ferr := secret.Freeze(); ferr != nil {
		release(secret)
		return nil, &ProtectedMemoryError{Op: "freezing reconstructed secret", Err: ferr}
	}
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
// WithValue, which holds the lock across the callback.
func (s *ProtectedShare) Value() []byte {
	if s == nil || s.value == nil {
		return nil
	}
	// Open aliases the locked pages without holding a lock past this return,
	// which is Value's contract; an error here means the share was destroyed,
	// which Value reports as nil.
	b, err := s.value.Open()
	if err != nil {
		return nil
	}
	return b
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
// borrow directly.
//
// fn MAY touch a different share or enclave: go-enclave holds only the
// borrowed enclave's own read lock, so a nested borrow of a second share does
// not contend with anything but that share. Re-entering THIS share is
// forbidden, for the reason above.
func (s *ProtectedShare) WithValue(fn func([]byte) error) error {
	if s == nil || s.value == nil {
		return enclave.ErrEnclaveDestroyed
	}
	return s.value.WithBytes(fn)
}

// Destroy wipes the share's value and releases its protected memory. It is
// safe to call on a nil receiver, more than once, and concurrently with
// itself, with Value and with WithValue — exactly one caller performs the
// wipe, and the enclave's lock keeps teardown from interleaving with a read in
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
//
// A release the kernel refuses is not reported: Destroy returns nothing, and the
// refused mapping stays on go-enclave's WipeAll ledger for a later retry. See
// release for why dropping that report loses no material.
func (s *ProtectedShare) Destroy() {
	if s == nil || s.value == nil {
		return
	}
	release(s.value)
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
	// value. Taking `val := s.Value()` and appending it afterwards would end the
	// borrow at the point Value returns and then read the aliasing slice after
	// the enclave was free to be destroyed — a use-after-destroy. Copying the
	// value out first would be worse, not better:
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
//
// Its locked memory is UNBUDGETED: it is charged to no [enclave.Allocator], and
// it cannot be, because json.Unmarshaler's signature has no room for one. A
// caller that bounds its locked memory with an allocator decodes through
// UnmarshalShareSetWith instead — a single share is a one-element set, so the
// document is the share object wrapped in [ ] — or, when it already holds the
// value bytes outside JSON, builds the share with NewProtectedShareWith. Both
// run the same validation this method does.
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

// decodeProtectedShare is the one implementation behind UnmarshalJSON and the
// per-element decode of UnmarshalShareSet and UnmarshalShareSetWith: it decodes
// ONE share object into a new ProtectedShare whose value is locked through
// newProtectedShare, admitted against a when a is non-nil and unbudgeted when it
// is nil (see lockValue).
func decodeProtectedShare(a *enclave.Allocator, data []byte) (*ProtectedShare, error) {
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
	// The value is decoded as a RAW token, not into a string field:
	// json.RawMessage is a []byte copy of the quoted literal that can be
	// wiped, whereas a string field would mint the immortal share string this
	// type exists to avoid.
	defer wipe(aux.Value)
	if len(aux.Value) < 2 || aux.Value[0] != '"' || aux.Value[len(aux.Value)-1] != '"' {
		return nil, &ValidationError{Field: "value", Reason: "must be a JSON string"}
	}
	// newProtectedShare wipes the value bytes as it copies them in; the quotes
	// and any remaining bytes are covered by the deferred wipe. An escape
	// sequence in the literal is not decoded — the base64 check rejects it,
	// which is correct: a share value that needs escaping is not a share value
	// this package ever produced.
	decoded, err := newProtectedShare(a, aux.Index, aux.Threshold, aux.Total, aux.Value[1:len(aux.Value)-1])
	if err != nil {
		return nil, err
	}
	if aux.Metadata != nil {
		decoded.Metadata = aux.Metadata
	}
	return decoded, nil
}

// shareSetLockedBytes prices the locked memory UnmarshalShareSet will hold for
// these raw elements, in the PAGES the kernel charges.
//
// It is the SINGLE copy of that arithmetic, called by the guard and asserted
// against by its tests. A test that recomputed the sum for itself would track
// whatever the right answer is regardless of what the guard does — a witness
// derived from the code under test, which certifies an under-count as readily
// as a correct sum.
//
// The sum STOPS at the first element that carries it past the budget. That is
// what keeps it from overflowing: rounding adds up to a page per element on top
// of lengths already bounded only by a document that arrives from outside the
// process. The figure it returns on refusal is therefore a true LOWER bound on
// what the set would have taken, which is what the error says.
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

// elementLockedBytes prices the ONE enclave a share element becomes: its value,
// page-rounded.
//
// It prices the VALUE and not the whole element, and that is not a refinement —
// it is what keeps this bound and SplitProtected's comparable. A share's element
// carries ~48 bytes of JSON header on top of its value, and when the encoded
// value is an exact multiple of the page size that header pushes every element
// into an extra page. Priced per element, a 171-share set of 6142-byte secrets
// costs 2,101,248 here and 1,429,504 in splitLockedBytes: SplitProtected admits
// it and this refuses it, so a ceremony cannot read back a set the library
// itself produced. Priced per value the two agree by construction — this sum is
// exactly splitLockedBytes' total*lockedBytes(encoded) term, and that function
// adds four more positive terms on top — so whatever a split admits, this
// admits.
//
// The value is read out as a RAW token, so its length is the length of the
// literal NewProtectedShare will be handed, escapes and all; the enclave is
// built over exactly those bytes, which makes this an equality rather than a
// bound. The copy the decode makes is share material and is wiped here.
//
// An element that does not decode as a share object is priced at its whole
// length, which is an upper bound on any value inside it. Nothing is admitted on
// the strength of that estimate that would not also be admitted on a correct
// one, and UnmarshalJSON reports the real error a moment later.
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
// THEM, which lets the submission pick how much of a finite PROCESS-WIDE
// resource to take: an oversized one starves every other caller of locked
// memory, on a path that cannot authenticate the submitter, and the failure —
// an ErrLockedMemoryUnavailable on whichever allocation reaches the ceiling —
// is not one an unauthenticated caller should get to choose. CombineProtected's 255-share
// bound cannot help: it runs on shares that have already been allocated.
//
// Two checks run against the undecoded document, in this order and before the
// first MemoryEnclave exists:
//
//   - at most maxShares (255) elements — the scheme's own bound, since the
//     x-coordinate is a non-zero byte and no larger set can hold distinct
//     shares;
//   - at most MaxShareSetBytes of LOCKED MEMORY, priced by shareSetLockedBytes
//     as the sum over the elements of each element's VALUE rounded up to a whole
//     page. Each element becomes exactly one enclave over that value, and the
//     kernel charges it a page whatever it holds. Summing the raw lengths
//     instead under-counts a wide set of small shares by the page size;
//     pricing the element rather than the value over-counts by a
//     page and puts this bound out of step with SplitProtected's, so a set the
//     library produced could not be read back.
//
// Neither reads a value, and the second is measured on the raw elements the
// standard decoder has already copied, so bounding costs no additional copy of
// share material.
//
// Every returned share must be Destroyed. On any error nothing is returned and
// every share already built is destroyed, so a rejected set leaks no locked
// memory. As with UnmarshalJSON, the caller's own data buffer still holds the
// share material afterwards and is the caller's to wipe.
//
// A share whose value the locked-memory allocator refuses fails the set with a
// *ProtectedMemoryError naming the element, not a *ValidationError: the document
// was well formed and the host is out of locked memory, which is the distinction
// ProtectedMemoryError exists to draw.
//
// Its locked memory is UNBUDGETED: it is charged to no [enclave.Allocator]. A
// caller that bounds its locked memory with one uses UnmarshalShareSetWith.
func UnmarshalShareSet(data []byte) ([]*ProtectedShare, error) {
	return unmarshalShareSet(nil, data)
}

// UnmarshalShareSetWith is UnmarshalShareSet with every share's locked value
// admitted against a, so a consumer's locked-memory budget covers the recovery
// ceremony's ingestion path — the unauthenticated one, where a budget matters
// most.
//
// Its contract is UnmarshalShareSet's, with one addition. When admitting a
// share would exceed a's budget it returns a *ProtectedMemoryError matching
// [enclave.ErrBudgetExhausted] under errors.Is. That can happen partway through
// the set — the budget is shared with every other holder of a, so unlike
// MaxShareSetBytes it cannot be priced up front — and every share this call had
// already built is destroyed before it returns, so a refused set gives its whole
// reservation back to a. Shares returned by EARLIER calls are the caller's and
// are untouched. The share-count and MaxShareSetBytes bounds still apply, and
// still run before anything is admitted.
//
// A nil a is refused with [enclave.ErrNilAllocator] before data is read, rather
// than falling back to the unbudgeted path. Libraries obtain a from
// [enclave.ProcessAllocator].
func UnmarshalShareSetWith(a *enclave.Allocator, data []byte) ([]*ProtectedShare, error) {
	if a == nil {
		return nil, enclave.ErrNilAllocator
	}
	return unmarshalShareSet(a, data)
}

// unmarshalShareSet is the one implementation behind UnmarshalShareSet and
// UnmarshalShareSetWith; a nil a selects the unbudgeted constructor (see
// lockValue).
func unmarshalShareSet(a *enclave.Allocator, data []byte) ([]*ProtectedShare, error) {
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
	// element's length is an upper bound on the length of the value inside it.
	//
	// PAGE-ROUNDED per element, because that is what the element will cost. Each
	// one becomes exactly one enclave, and the kernel charges a whole page for
	// it however few bytes the value holds — so summing the raw lengths prices
	// 255 shares of a 32-byte key at ~25 KiB when they lock 255 pages, and the
	// bound stops bounding the resource it names.
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
			// A refused lock is reported as what it is. Folding it into a
			// *ValidationError would tell the operator the document was wrong
			// when the host is out of locked memory, and would hide
			// ErrBudgetExhausted and ErrLockedMemoryUnavailable from errors.Is.
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

// Protect moves a plain Share's value into protected memory, returning the
// ProtectedShare equivalent. It is the migration path for callers that already
// hold Shares — from Split, or decoded from JSON.
//
// It bounds FURTHER exposure; it cannot undo what has happened. The Share's
// value is a Go string and stays readable in the heap until the GC reclaims
// it, whichever copies were made of it along the way. A caller that can choose
// where the share material is born should call SplitProtected instead and
// never create the string.
//
// The value's locked memory is UNBUDGETED: it is charged to no
// [enclave.Allocator]. A caller that bounds its locked memory with one uses
// ProtectWith.
func (s *Share) Protect() (*ProtectedShare, error) {
	return s.protect(nil)
}

// ProtectWith is Protect with the value's locked memory admitted against a, so a
// consumer's locked-memory budget covers the migrated share.
//
// Its contract is Protect's, with one addition. When admitting the value would
// exceed a's budget it returns a *ProtectedMemoryError matching
// [enclave.ErrBudgetExhausted] under errors.Is, and nothing stays allocated.
// The Share is never modified — its value is a string, and the one copy this
// makes of it is wiped on every path — so a refused call can be retried against
// a larger budget. Shares built earlier, from a or anywhere else, are
// untouched.
//
// A nil a is refused with [enclave.ErrNilAllocator] before s is read, rather
// than falling back to the unbudgeted path. Libraries obtain a from
// [enclave.ProcessAllocator].
func (s *Share) ProtectWith(a *enclave.Allocator) (*ProtectedShare, error) {
	if a == nil {
		return nil, enclave.ErrNilAllocator
	}
	return s.protect(a)
}

// protect is the one implementation behind Protect and ProtectWith; a nil a
// selects the unbudgeted constructor (see lockValue).
func (s *Share) protect(a *enclave.Allocator) (*ProtectedShare, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	// The conversion copies; newProtectedShare wipes that copy into the
	// enclave. The string itself is the one copy nothing can take away.
	value := []byte(s.Value)
	protected, err := newProtectedShare(a, s.Index, s.Threshold, s.Total, value)
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
