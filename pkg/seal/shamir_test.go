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

package seal

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir"
)

// --- ShamirConfig ---

func TestShamirConfig_Fields(t *testing.T) {
	cfg := ShamirConfig{
		Threshold:   3,
		TotalShares: 5,
		QuorumTTL:   10 * time.Minute,
	}
	if cfg.Threshold != 3 {
		t.Fatalf("expected threshold=3, got %d", cfg.Threshold)
	}
	if cfg.TotalShares != 5 {
		t.Fatalf("expected total=5, got %d", cfg.TotalShares)
	}
	if cfg.QuorumTTL != 10*time.Minute {
		t.Fatalf("expected TTL=10m, got %v", cfg.QuorumTTL)
	}
}

func TestShamirConfig_ZeroValueDefaults(t *testing.T) {
	cfg := ShamirConfig{}
	if cfg.Threshold != 0 {
		t.Fatalf("expected zero threshold, got %d", cfg.Threshold)
	}
	if cfg.TotalShares != 0 {
		t.Fatalf("expected zero total, got %d", cfg.TotalShares)
	}
	if cfg.QuorumTTL != 0 {
		t.Fatalf("expected zero TTL, got %v", cfg.QuorumTTL)
	}
}

// --- ShamirInitResult ---

// splitForResult builds the share set an initialization would hold. Every test
// below owns what it gets back and destroys it, directly or through the result.
func splitForResult(t *testing.T, threshold, total int) []*shamir.ProtectedShare {
	t.Helper()
	shares, err := shamir.SplitProtected([]byte("a 32-byte root key for testing.."), threshold, total)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	return shares
}

func TestShamirInitResult_Fields(t *testing.T) {
	result := ShamirInitResult{
		Shares:      splitForResult(t, 2, 3),
		Threshold:   2,
		TotalShares: 3,
	}
	defer result.Destroy()

	if len(result.Shares) != 3 {
		t.Fatalf("expected 3 shares, got %d", len(result.Shares))
	}
	if result.Threshold != 2 {
		t.Fatalf("expected threshold=2, got %d", result.Threshold)
	}
	if result.TotalShares != 3 {
		t.Fatalf("expected total=3, got %d", result.TotalShares)
	}
}

func TestShamirInitResult_EmptyShares(t *testing.T) {
	result := ShamirInitResult{}
	if result.Shares != nil {
		t.Fatal("expected nil shares for zero value")
	}
}

// Destroy is the whole reason the field was retyped: a result that dropped its
// references without wiping would leave all N shares — the point of widest
// exposure in the scheme — readable until the GC ran, which is exactly what the
// []string it replaced did. The assertion is on the SHARES, deliberately, and
// not on result.Shares being nil: clearing the slice is the cheap half, and a
// Destroy that only did that would pass a nil check while wiping nothing.
func TestShamirInitResult_DestroyWipesEveryShare(t *testing.T) {
	shares := splitForResult(t, 2, 3)
	result := ShamirInitResult{Shares: shares, Threshold: 2, TotalShares: 3}

	// Held separately: after Destroy the result no longer references them, so
	// the check has to be able to reach the shares it wiped.
	held := make([]*shamir.ProtectedShare, len(shares))
	copy(held, shares)
	for i, sh := range held {
		if v := sh.Value(); len(v) == 0 {
			t.Fatalf("share %d must be readable before Destroy", i)
		}
	}

	result.Destroy()

	for i, sh := range held {
		if v := sh.Value(); v != nil {
			t.Fatalf("share %d survived Destroy: the result wiped nothing", i)
		}
	}
	if result.Shares != nil {
		t.Fatal("Destroy must clear the slice so a later marshal cannot reach freed memory")
	}
}

// A ceremony carries `defer result.Destroy()` from the line the result exists,
// which means Destroy runs on paths where it has already run, and on a result
// that was never built. Neither may panic.
func TestShamirInitResult_DestroyIsSafeOnNilAndRepeated(t *testing.T) {
	var nilResult *ShamirInitResult
	nilResult.Destroy()

	result := &ShamirInitResult{Shares: splitForResult(t, 2, 3), Threshold: 2, TotalShares: 3}
	result.Destroy()
	result.Destroy()

	empty := &ShamirInitResult{}
	empty.Destroy()
}

// The retype changes what "shares" is on the wire: an array of share OBJECTS
// rather than an array of bare values. This pins that it is the SAME object
// encoding shamir.Share produces — the shape the rest of the stack already
// persists and distributes shares in — so the protected types introduce no
// third format for a consumer to special-case.
func TestShamirInitResult_JSONMatchesTheShareObjectShape(t *testing.T) {
	plain := []*shamir.Share{
		{Index: 1, Threshold: 2, Total: 3, Value: "AQEC"},
		{Index: 2, Threshold: 2, Total: 3, Value: "AQID"},
	}

	protected := make([]*shamir.ProtectedShare, 0, len(plain))
	for _, sh := range plain {
		p, err := sh.Protect()
		if err != nil {
			t.Fatalf("Protect: %v", err)
		}
		protected = append(protected, p)
	}
	result := ShamirInitResult{Shares: protected, Threshold: 2, TotalShares: 3}
	defer result.Destroy()

	got, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal protected result: %v", err)
	}

	// The yardstick is built from the plain shares and the same two scalar
	// fields, encoded by encoding/json itself — not by reusing the type under
	// test, which would move with any drift instead of catching it.
	want, err := json.Marshal(struct {
		Shares      []*shamir.Share `json:"shares"`
		Threshold   int             `json:"threshold"`
		TotalShares int             `json:"total_shares"`
	}{Shares: plain, Threshold: 2, TotalShares: 3})
	if err != nil {
		t.Fatalf("marshal reference shape: %v", err)
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("init-result JSON drifted from the share-object shape:\n got: %s\nwant: %s", got, want)
	}
}

// The retype also made the DEFAULT decoder a denial-of-service path: decoding a
// []*shamir.ProtectedShare through encoding/json locks a page per element as the
// elements are visited, so an oversized "shares" array chooses how much locked
// memory to take and memguard terminates the process on a failed lock. This
// pins that ShamirInitResult.UnmarshalJSON routes the set through the bounded
// shamir.UnmarshalShareSet, which refuses the set BEFORE it locks anything.
//
// The mutant this kills is a reversion to `json.Unmarshal(data, &plainStruct)`:
// the generic decoder accepts 256 well-formed elements (nothing bounds the
// count during decode), so an oversized document would decode "successfully"
// having allocated 256 locked buffers. The bounded path rejects it with no
// error swallowed and no share retained.
func TestShamirInitResult_UnmarshalRejectsOversizedShareSetBeforeAllocation(t *testing.T) {
	// maxShares is 255; 256 well-formed elements exceed the count bound, which
	// UnmarshalShareSet checks on the undecoded document before the first lock.
	const over = 256
	elems := make([]string, over)
	for i := range elems {
		elems[i] = `{"index":1,"threshold":2,"total":3,"value":"AQEC"}`
	}
	doc := []byte(`{"shares":[` + strings.Join(elems, ",") + `],"threshold":2,"total_shares":3}`)

	var result ShamirInitResult
	err := json.Unmarshal(doc, &result)
	if err == nil {
		result.Destroy()
		t.Fatal("expected an oversized share set to be rejected, got nil error")
	}
	if len(result.Shares) != 0 {
		result.Destroy()
		t.Fatalf("a rejected decode must retain no shares, got %d", len(result.Shares))
	}
}

// A set whose count and byte size are within bounds but that turns invalid part
// way through must leave the receiver holding nothing: UnmarshalShareSet
// destroys the shares its accepted prefix already locked, and UnmarshalJSON
// returns that error without assigning a partial set to the result.
func TestShamirInitResult_UnmarshalPartialFailureRetainsNothing(t *testing.T) {
	// Element 0 is a well-formed share that locks memory; element 1's value is
	// not base64, so NewProtectedShare rejects it and the prefix is unwound.
	doc := []byte(`{"shares":[` +
		`{"index":1,"threshold":2,"total":3,"value":"AQEC"},` +
		`{"index":2,"threshold":2,"total":3,"value":"!!!!"}` +
		`],"threshold":2,"total_shares":3}`)

	var result ShamirInitResult
	err := json.Unmarshal(doc, &result)
	if err == nil {
		result.Destroy()
		t.Fatal("expected a partial decode failure to return an error, got nil")
	}
	if result.Shares != nil {
		result.Destroy()
		t.Fatalf("a failed decode must retain no shares, got %d", len(result.Shares))
	}
}

// A valid set decodes back to the same shares it was marshalled from. The
// yardstick is the marshalled bytes themselves, re-marshalled from the decoded
// result: if decode dropped, reordered, or corrupted a share the second
// encoding would differ.
func TestShamirInitResult_UnmarshalRoundTrip(t *testing.T) {
	original := ShamirInitResult{Shares: splitForResult(t, 2, 3), Threshold: 2, TotalShares: 3}
	defer original.Destroy()

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded ShamirInitResult
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	defer decoded.Destroy()

	if decoded.Threshold != 2 || decoded.TotalShares != 3 {
		t.Fatalf("scalars drifted: threshold=%d total=%d", decoded.Threshold, decoded.TotalShares)
	}
	if len(decoded.Shares) != len(original.Shares) {
		t.Fatalf("expected %d shares, got %d", len(original.Shares), len(decoded.Shares))
	}

	reencoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatalf("round trip drifted:\n first: %s\nsecond: %s", encoded, reencoded)
	}
}

// A nil share set marshals to `"shares":null`; decoding that — or a document
// with no "shares" key at all — yields a result with no shares rather than an
// "empty set" rejection, so a zero-value result round-trips.
func TestShamirInitResult_UnmarshalNullShares(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  string
	}{
		{"explicit null", `{"shares":null,"threshold":0,"total_shares":0}`},
		{"absent", `{"threshold":4,"total_shares":7}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result ShamirInitResult
			if err := json.Unmarshal([]byte(tc.doc), &result); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			defer result.Destroy()
			if result.Shares != nil {
				t.Fatalf("expected nil shares, got %d", len(result.Shares))
			}
		})
	}

	// An empty array, by contrast, is a malformed set and is rejected.
	var empty ShamirInitResult
	if err := json.Unmarshal([]byte(`{"shares":[],"threshold":2,"total_shares":3}`), &empty); err == nil {
		empty.Destroy()
		t.Fatal("expected an empty share array to be rejected")
	}
}

// Decoding into a result that already holds shares must wipe the old set rather
// than strand its locked buffers — json reuse of a non-empty target is a real
// path, and the old shares are the widest-exposure material the type exists to
// protect.
func TestShamirInitResult_UnmarshalIntoReusedResultWipesOldShares(t *testing.T) {
	old := splitForResult(t, 2, 3)
	result := ShamirInitResult{Shares: old, Threshold: 2, TotalShares: 3}

	// Hold the old shares separately so the wipe is observable after decode
	// replaces the slice.
	held := make([]*shamir.ProtectedShare, len(old))
	copy(held, old)

	doc := []byte(`{"shares":[` +
		`{"index":1,"threshold":2,"total":3,"value":"AQEC"},` +
		`{"index":2,"threshold":2,"total":3,"value":"AQID"}` +
		`],"threshold":2,"total_shares":3}`)
	if err := json.Unmarshal(doc, &result); err != nil {
		t.Fatalf("unmarshal into reused result: %v", err)
	}
	defer result.Destroy()

	for i, sh := range held {
		if v := sh.Value(); v != nil {
			t.Fatalf("old share %d survived the decode: its locked buffer was stranded", i)
		}
	}
	if len(result.Shares) != 2 {
		t.Fatalf("expected 2 decoded shares, got %d", len(result.Shares))
	}
}

// --- QuorumProgress ---

func TestQuorumProgress_Complete(t *testing.T) {
	progress := QuorumProgress{
		Required:  3,
		Submitted: 3,
		Complete:  true,
	}
	if !progress.Complete {
		t.Fatal("expected complete=true")
	}
	if progress.Required != 3 {
		t.Fatalf("expected required=3, got %d", progress.Required)
	}
}

func TestQuorumProgress_Incomplete(t *testing.T) {
	progress := QuorumProgress{
		Required:  3,
		Submitted: 1,
		Complete:  false,
	}
	if progress.Complete {
		t.Fatal("expected complete=false")
	}
	if progress.Submitted != 1 {
		t.Fatalf("expected submitted=1, got %d", progress.Submitted)
	}
}

// --- DefaultQuorumTTL ---

func TestDefaultQuorumTTL(t *testing.T) {
	if DefaultQuorumTTL != 5*time.Minute {
		t.Fatalf("expected 5m, got %v", DefaultQuorumTTL)
	}
}
