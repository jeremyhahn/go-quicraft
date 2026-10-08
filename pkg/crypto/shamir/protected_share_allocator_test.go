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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// The tests in this file drive the ...With variants against REAL go-enclave
// allocators sized to refuse. That is the point: a budget is a
// refusal this package can be shown to surface without any seam, because an
// allocator's budget is local to the test that built it — unlike RLIMIT_MEMLOCK,
// which every test in the binary shares.

// pageBytes is one page as the allocator charges it.
func pageBytes() int64 { return enclave.LockedSize(1) }

// newBudget returns an allocator admitting exactly pages pages.
func newBudget(t *testing.T, pages int64) *enclave.Allocator {
	t.Helper()
	a, err := enclave.NewAllocator(pages * pageBytes())
	if err != nil {
		t.Fatalf("NewAllocator(%d pages): %v", pages, err)
	}
	return a
}

// assertBudgetExhausted checks that err is the documented refusal: a
// *ProtectedMemoryError that errors.Is reports as enclave.ErrBudgetExhausted.
func assertBudgetExhausted(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected a budget refusal, got nil")
	}
	if !errors.Is(err, enclave.ErrBudgetExhausted) {
		t.Fatalf("errors.Is(err, ErrBudgetExhausted) = false; err = %v", err)
	}
	var pme *ProtectedMemoryError
	if !errors.As(err, &pme) {
		t.Fatalf("expected *ProtectedMemoryError, got %T: %v", err, err)
	}
}

func TestNewProtectedShareWith_OnePageBudget(t *testing.T) {
	values, err := SplitBytes([]byte("issue-204 secret"), 2, 3)
	if err != nil {
		t.Fatalf("SplitBytes: %v", err)
	}
	want0 := append([]byte(nil), values[0]...)

	a := newBudget(t, 1)

	first, err := NewProtectedShareWith(a, 1, 2, 3, values[0])
	if err != nil {
		t.Fatalf("first share within budget: %v", err)
	}
	defer first.Destroy()
	if got := a.InUse(); got != pageBytes() {
		t.Fatalf("InUse after first share = %d, want one page (%d)", got, pageBytes())
	}

	second, err := NewProtectedShareWith(a, 2, 2, 3, values[1])
	if second != nil {
		second.Destroy()
		t.Fatal("second share beyond a one-page budget was admitted")
	}
	assertBudgetExhausted(t, err)
	// The ownership contract of a *ProtectedMemoryError: the value is wiped.
	if !bytes.Equal(values[1], make([]byte, len(values[1]))) {
		t.Fatal("refused value was not wiped; a *ProtectedMemoryError promises it is")
	}
	if got := a.InUse(); got != pageBytes() {
		t.Fatalf("InUse after refusal = %d, want it unchanged at %d", got, pageBytes())
	}

	// The earlier share is untouched by the refusal.
	if err := first.Validate(); err != nil {
		t.Fatalf("earlier share invalid after refusal: %v", err)
	}
	if !bytes.Equal(valueCopy(first), want0) {
		t.Fatal("earlier share's value changed after refusal")
	}

	first.Destroy()
	if got := a.InUse(); got != 0 {
		t.Fatalf("InUse after Destroy = %d, want 0", got)
	}
}

func TestSplitProtectedWith_OnePageBudgetIsRefusedWithoutLeaking(t *testing.T) {
	a := newBudget(t, 1)
	shares, err := SplitProtectedWith(a, []byte("0123456789abcdef0123456789abcdef"), 2, 3)
	if shares != nil {
		destroyAll(shares)
		t.Fatal("split admitted against a one-page budget")
	}
	assertBudgetExhausted(t, err)
	if got := a.InUse(); got != 0 {
		t.Fatalf("InUse after refused split = %d, want 0 (leaked reservation)", got)
	}
}

// TestSplitProtectedWith_BeyondBudgetLeavesEarlierSharesIntact pins partial
// refusal on the split path. The budget fits exactly one split's PEAK, so
// the first split succeeds and keeps its shares; the second gets its scratch
// admitted and is refused partway through the share loop — the hardest refusal
// to unwind — and must give back everything it took while the first set still
// reconstructs.
func TestSplitProtectedWith_BeyondBudgetLeavesEarlierSharesIntact(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	const threshold, total = 2, 3

	need := int64(splitLockedBytes(len(secret), threshold, total))
	a, err := enclave.NewAllocator(need)
	if err != nil {
		t.Fatalf("NewAllocator: %v", err)
	}

	first, err := SplitProtectedWith(a, secret, threshold, total)
	if err != nil {
		t.Fatalf("first split within budget: %v", err)
	}
	defer destroyAll(first)

	held := int64(total) * int64(lockedBytes(base64.StdEncoding.EncodedLen(rawShareOverhead+len(secret))))
	if got := a.InUse(); got != held {
		t.Fatalf("InUse after first split = %d, want the %d its shares hold (scratch released)", got, held)
	}
	snapshot := make([][]byte, len(first))
	for i, s := range first {
		snapshot[i] = append([]byte(nil), valueCopy(s)...)
	}

	second, err := SplitProtectedWith(a, secret, threshold, total)
	if second != nil {
		destroyAll(second)
		t.Fatal("second split beyond the budget was admitted")
	}
	assertBudgetExhausted(t, err)
	if got := a.InUse(); got != held {
		t.Fatalf("InUse after refused split = %d, want it back at %d (the refused call leaked)", got, held)
	}

	for i, s := range first {
		if !bytes.Equal(valueCopy(s), snapshot[i]) {
			t.Fatalf("earlier share %d changed after a refused split", i)
		}
	}
	got, err := CombineProtected(first[:threshold])
	if err != nil {
		t.Fatalf("earlier shares no longer combine: %v", err)
	}
	defer got.Destroy()
	if !bytes.Equal(encCopy(got), secret) {
		t.Fatal("earlier shares reconstruct the wrong secret after a refused split")
	}
}

func TestCombineProtectedWith_BeyondBudgetLeavesSharesIntact(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	shares, err := SplitProtected(secret, 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	defer destroyAll(shares)

	// Reconstruction takes two regions — the decode buffer and the secret — so
	// one page admits the first and refuses the second.
	tight := newBudget(t, 1)
	out, err := CombineProtectedWith(tight, shares[:2])
	if out != nil {
		out.Destroy()
		t.Fatal("combine admitted against a one-page budget")
	}
	assertBudgetExhausted(t, err)
	if got := tight.InUse(); got != 0 {
		t.Fatalf("InUse after refused combine = %d, want 0 (decode buffer leaked)", got)
	}

	// The shares were not consumed or damaged by the refusal.
	enough := newBudget(t, 2)
	out, err = CombineProtectedWith(enough, shares[:2])
	if err != nil {
		t.Fatalf("combine within budget: %v", err)
	}
	if !bytes.Equal(encCopy(out), secret) {
		out.Destroy()
		t.Fatal("combine within budget reconstructed the wrong secret")
	}
	// The returned secret keeps its reservation; the decode buffer does not.
	if got := enough.InUse(); got != pageBytes() {
		out.Destroy()
		t.Fatalf("InUse while holding the secret = %d, want one page (%d)", got, pageBytes())
	}
	out.Destroy()
	if got := enough.InUse(); got != 0 {
		t.Fatalf("InUse after destroying the secret = %d, want 0", got)
	}
}

// TestWithVariants_RefuseNilAllocator pins that a nil allocator is a refusal,
// never a silent fall back to unbudgeted locked memory, and that it is decided
// before NewProtectedShareWith touches the caller's value.
func TestWithVariants_RefuseNilAllocator(t *testing.T) {
	values, err := SplitBytes([]byte("nil allocator"), 2, 3)
	if err != nil {
		t.Fatalf("SplitBytes: %v", err)
	}
	want := append([]byte(nil), values[0]...)

	share, err := NewProtectedShareWith(nil, 1, 2, 3, values[0])
	if share != nil || !errors.Is(err, enclave.ErrNilAllocator) {
		share.Destroy()
		t.Fatalf("NewProtectedShareWith(nil) = (%v, %v), want ErrNilAllocator", share, err)
	}
	if !bytes.Equal(values[0], want) {
		t.Fatal("NewProtectedShareWith(nil) touched the caller's value")
	}

	shares, err := SplitProtectedWith(nil, []byte("secret"), 2, 3)
	if shares != nil || !errors.Is(err, enclave.ErrNilAllocator) {
		destroyAll(shares)
		t.Fatalf("SplitProtectedWith(nil) err = %v, want ErrNilAllocator", err)
	}

	built, err := SplitProtected([]byte("secret"), 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	defer destroyAll(built)
	out, err := CombineProtectedWith(nil, built)
	if out != nil || !errors.Is(err, enclave.ErrNilAllocator) {
		out.Destroy()
		t.Fatalf("CombineProtectedWith(nil) err = %v, want ErrNilAllocator", err)
	}
}

// marshalShareSet is the document UnmarshalShareSet(With) reads: exactly what
// json.Marshal writes for a share set.
func marshalShareSet(t *testing.T, shares []*ProtectedShare) []byte {
	t.Helper()
	doc, err := json.Marshal(shares)
	if err != nil {
		t.Fatalf("json.Marshal share set: %v", err)
	}
	return doc
}

func TestUnmarshalShareSetWith_ChargesAndDestroyRefunds(t *testing.T) {
	secret := []byte("issue-211 secret")
	shares, err := SplitProtected(secret, 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	defer destroyAll(shares)
	doc := marshalShareSet(t, shares)

	a := newBudget(t, 3)
	decoded, err := UnmarshalShareSetWith(a, doc)
	if err != nil {
		t.Fatalf("UnmarshalShareSetWith within budget: %v", err)
	}
	if got, want := a.InUse(), 3*pageBytes(); got != want {
		destroyAll(decoded)
		t.Fatalf("InUse after decoding 3 shares = %d, want %d (one page each)", got, want)
	}
	got, err := CombineProtected(decoded[:2])
	if err != nil {
		destroyAll(decoded)
		t.Fatalf("decoded shares do not combine: %v", err)
	}
	ok := bytes.Equal(encCopy(got), secret)
	got.Destroy()
	if !ok {
		destroyAll(decoded)
		t.Fatal("decoded shares reconstruct the wrong secret")
	}

	destroyAll(decoded)
	if got := a.InUse(); got != 0 {
		t.Fatalf("InUse after Destroy = %d, want 0", got)
	}
}

// TestUnmarshalShareSetWith_BeyondBudgetLeavesEarlierSharesIntact pins partial
// refusal on the recovery-ceremony ingestion path. An
// earlier decode holds one page; the second set fits two of its three shares in
// what is left and is refused on the third — partway through the set, the
// refusal that has to be unwound — and must give back what it took while the
// earlier share stays intact. The earlier decode is also the budgeted route for
// ONE share that UnmarshalJSON documents: a one-element set.
func TestUnmarshalShareSetWith_BeyondBudgetLeavesEarlierSharesIntact(t *testing.T) {
	shares, err := SplitProtected([]byte("0123456789abcdef0123456789abcdef"), 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	defer destroyAll(shares)

	a := newBudget(t, 3)
	earlier, err := UnmarshalShareSetWith(a, marshalShareSet(t, shares[:1]))
	if err != nil {
		t.Fatalf("one-element set within budget: %v", err)
	}
	defer destroyAll(earlier)
	held := a.InUse()
	if held != pageBytes() {
		t.Fatalf("InUse after one-element set = %d, want one page (%d)", held, pageBytes())
	}
	snapshot := append([]byte(nil), valueCopy(earlier[0])...)

	second, err := UnmarshalShareSetWith(a, marshalShareSet(t, shares))
	if second != nil {
		destroyAll(second)
		t.Fatal("set beyond the budget was admitted")
	}
	assertBudgetExhausted(t, err)
	var vErr *ValidationError
	if errors.As(err, &vErr) {
		t.Fatal("a budget refusal must NOT be reported as a *ValidationError")
	}
	if got := a.InUse(); got != held {
		t.Fatalf("InUse after refused set = %d, want it back at %d (the refused call leaked)", got, held)
	}

	if err := earlier[0].Validate(); err != nil {
		t.Fatalf("earlier share invalid after refusal: %v", err)
	}
	if !bytes.Equal(valueCopy(earlier[0]), snapshot) {
		t.Fatal("earlier share's value changed after a refused set")
	}
	if earlier[0].Index != shares[0].Index {
		t.Fatalf("earlier share index = %d, want %d", earlier[0].Index, shares[0].Index)
	}
}

// TestUnmarshalShareSetWith_SetBoundsRunBeforeTheAllocator pins that the
// allocator is an addition to the set's own bounds, not a replacement: an
// oversized set is refused as a *ValidationError before anything is admitted,
// however large the budget.
func TestUnmarshalShareSetWith_SetBoundsRunBeforeTheAllocator(t *testing.T) {
	a, err := enclave.NewAllocator(int64(4 * MaxShareSetBytes))
	if err != nil {
		t.Fatalf("NewAllocator: %v", err)
	}
	doc := []byte("[" + strings.Repeat(`{},`, maxShares) + `{}]`)
	shares, err := UnmarshalShareSetWith(a, doc)
	if shares != nil {
		destroyAll(shares)
		t.Fatal("a set of more than maxShares elements was admitted")
	}
	var vErr *ValidationError
	if !errors.As(err, &vErr) {
		t.Fatalf("err = %T (%v), want *ValidationError", err, err)
	}
	if got := a.InUse(); got != 0 {
		t.Fatalf("InUse after a bounds refusal = %d, want 0", got)
	}
}

// TestUnmarshalShareSet_LockRefusalIsProtectedMemoryError pins the error type
// the shared implementation reports on the unbudgeted path when the kernel
// refuses a share's lock: the same *ProtectedMemoryError the budgeted path
// reports, not a *ValidationError that would tell the operator the document was
// wrong.
func TestUnmarshalShareSet_LockRefusalIsProtectedMemoryError(t *testing.T) {
	shares, err := SplitProtected([]byte("lock refusal"), 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	defer destroyAll(shares)
	doc := marshalShareSet(t, shares)

	restore := testOnlyProtectedMemory(refuseProtectedMemory)
	defer restore()

	decoded, err := UnmarshalShareSet(doc)
	if decoded != nil {
		destroyAll(decoded)
		t.Fatal("a refused lock yielded a share set")
	}
	var pme *ProtectedMemoryError
	if !errors.As(err, &pme) {
		t.Fatalf("err = %T (%v), want *ProtectedMemoryError", err, err)
	}
	if !errors.Is(err, errLockRefused) {
		t.Fatal("the allocator's own error must remain reachable through Unwrap")
	}
	var vErr *ValidationError
	if errors.As(err, &vErr) {
		t.Fatal("a locked-memory refusal must NOT be reported as a *ValidationError")
	}
}

func TestShareProtectWith_OnePageBudget(t *testing.T) {
	plain, err := Split([]byte("issue-211 protect"), 2, 3)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	plain[0].Metadata = map[string]string{"custodian": "a"}

	a := newBudget(t, 1)
	first, err := plain[0].ProtectWith(a)
	if err != nil {
		t.Fatalf("first ProtectWith within budget: %v", err)
	}
	defer first.Destroy()
	if got := a.InUse(); got != pageBytes() {
		t.Fatalf("InUse after first share = %d, want one page (%d)", got, pageBytes())
	}
	if !bytes.Equal(valueCopy(first), []byte(plain[0].Value)) {
		t.Fatal("protected value differs from the plain share's")
	}
	if first.Metadata["custodian"] != "a" {
		t.Fatal("ProtectWith dropped the share's metadata")
	}
	want0 := append([]byte(nil), valueCopy(first)...)

	before := plain[1].Value
	second, err := plain[1].ProtectWith(a)
	if second != nil {
		second.Destroy()
		t.Fatal("second share beyond a one-page budget was admitted")
	}
	assertBudgetExhausted(t, err)
	if got := a.InUse(); got != pageBytes() {
		t.Fatalf("InUse after refusal = %d, want it unchanged at %d", got, pageBytes())
	}
	if plain[1].Value != before {
		t.Fatal("a refused ProtectWith changed the plain share")
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("earlier share invalid after refusal: %v", err)
	}
	if !bytes.Equal(valueCopy(first), want0) {
		t.Fatal("earlier share's value changed after refusal")
	}

	// The plain share is intact, so the refused call can be retried against a
	// budget that admits it.
	retry, err := plain[1].ProtectWith(newBudget(t, 1))
	if err != nil {
		t.Fatalf("retry against a fresh budget: %v", err)
	}
	retry.Destroy()

	first.Destroy()
	if got := a.InUse(); got != 0 {
		t.Fatalf("InUse after Destroy = %d, want 0", got)
	}
}

// TestUnmarshalAndProtectWith_RefuseNilAllocator pins the nil-allocator refusal
// for the UnmarshalShareSetWith and ProtectWith variants, and that it is
// decided before the input is read: a document that does not parse and a nil Share both report
// ErrNilAllocator rather than their own errors.
func TestUnmarshalAndProtectWith_RefuseNilAllocator(t *testing.T) {
	shares, err := UnmarshalShareSetWith(nil, []byte("not json"))
	if shares != nil || !errors.Is(err, enclave.ErrNilAllocator) {
		destroyAll(shares)
		t.Fatalf("UnmarshalShareSetWith(nil) err = %v, want ErrNilAllocator", err)
	}

	var nilShare *Share
	p, err := nilShare.ProtectWith(nil)
	if p != nil || !errors.Is(err, enclave.ErrNilAllocator) {
		p.Destroy()
		t.Fatalf("(*Share)(nil).ProtectWith(nil) err = %v, want ErrNilAllocator", err)
	}

	plain, err := Split([]byte("nil allocator"), 2, 3)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	p, err = plain[0].ProtectWith(nil)
	if p != nil || !errors.Is(err, enclave.ErrNilAllocator) {
		p.Destroy()
		t.Fatalf("ProtectWith(nil) err = %v, want ErrNilAllocator", err)
	}
}
