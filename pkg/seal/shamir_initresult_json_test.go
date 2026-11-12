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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir"
)

// resultJSONSecret is the secret splitForResult splits.
var resultJSONSecret = []byte("a 32-byte root key for testing..")

// combineEquals reports whether CombineProtected of shares returns want.
func combineEquals(t *testing.T, shares []*shamir.ProtectedShare, want []byte) bool {
	t.Helper()
	secret, err := shamir.CombineProtected(shares)
	if err != nil {
		t.Fatalf("CombineProtected: %v", err)
	}
	defer secret.Destroy()
	var equal bool
	if err := secret.WithBytes(func(view []byte) error {
		equal = bytes.Equal(view, want)
		return nil
	}); err != nil {
		t.Fatalf("WithBytes: %v", err)
	}
	return equal
}

// (a) A 2-of-3 result that goes through JSON keeps its scalars, and every pair
// of decoded shares still reconstructs the secret. Comparing the encoded bytes
// alone would not show that the decoded shares are usable key material.
func TestShamirInitResultJSON_RoundTripSharesCombineToSecret(t *testing.T) {
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
		t.Fatalf("scalars drifted: threshold=%d total_shares=%d", decoded.Threshold, decoded.TotalShares)
	}
	if len(decoded.Shares) != 3 {
		t.Fatalf("expected 3 decoded shares, got %d", len(decoded.Shares))
	}
	for i := 0; i < 3; i++ {
		for j := i + 1; j < 3; j++ {
			pair := []*shamir.ProtectedShare{decoded.Shares[i], decoded.Shares[j]}
			if !combineEquals(t, pair, resultJSONSecret) {
				t.Fatalf("decoded shares %d and %d do not combine to the original secret", i, j)
			}
		}
	}
}

// (b) A document with "shares" absent, or set to null, decodes to no shares
// and no error. The scalars are still decoded.
func TestShamirInitResultJSON_AbsentOrNullSharesDecodeToNone(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  string
	}{
		{"absent", `{"threshold":2,"total_shares":3}`},
		{"null", `{"shares":null,"threshold":2,"total_shares":3}`},
		{"null with whitespace", `{"shares": null ,"threshold":2,"total_shares":3}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result ShamirInitResult
			if err := json.Unmarshal([]byte(tc.doc), &result); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			defer result.Destroy()
			if len(result.Shares) != 0 {
				t.Fatalf("expected no shares, got %d", len(result.Shares))
			}
			if result.Threshold != 2 || result.TotalShares != 3 {
				t.Fatalf("scalars not decoded: threshold=%d total_shares=%d", result.Threshold, result.TotalShares)
			}
		})
	}
}

// (c) An empty share array is a malformed set and is rejected.
func TestShamirInitResultJSON_EmptyShareArrayRejected(t *testing.T) {
	var result ShamirInitResult
	if err := json.Unmarshal([]byte(`{"shares":[],"threshold":2,"total_shares":3}`), &result); err == nil {
		result.Destroy()
		t.Fatal("expected an empty share array to be rejected")
	}
	if result.Shares != nil {
		result.Destroy()
		t.Fatalf("a rejected decode must retain no shares, got %d", len(result.Shares))
	}
}

// (d) Decoding into a receiver that already held shares destroys every one of
// them, and the receiver then holds the new set, which combines to its secret.
func TestShamirInitResultJSON_ReusedReceiverDestroysOldShares(t *testing.T) {
	source := ShamirInitResult{Shares: splitForResult(t, 2, 3), Threshold: 2, TotalShares: 3}
	defer source.Destroy()
	encoded, err := json.Marshal(source)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	old := splitForResult(t, 3, 5)
	held := append([]*shamir.ProtectedShare(nil), old...)
	result := ShamirInitResult{Shares: old, Threshold: 3, TotalShares: 5}
	if err := json.Unmarshal(encoded, &result); err != nil {
		result.Destroy()
		t.Fatalf("unmarshal into reused receiver: %v", err)
	}
	defer result.Destroy()

	for i, sh := range held {
		if sh.Validate() == nil {
			t.Fatalf("old share %d survived the decode into a reused receiver", i)
		}
	}
	if result.Threshold != 2 || result.TotalShares != 3 || len(result.Shares) != 3 {
		t.Fatalf("receiver not replaced: threshold=%d total_shares=%d shares=%d",
			result.Threshold, result.TotalShares, len(result.Shares))
	}
	if !combineEquals(t, result.Shares[:2], resultJSONSecret) {
		t.Fatal("the new shares in the reused receiver do not combine to the secret")
	}
}

// (e) A failed decode leaves the receiver unchanged: its shares are still live
// and still combine, and its scalars are untouched. Both a syntax error and a
// well-formed document that is rejected are checked.
func TestShamirInitResultJSON_FailedDecodeLeavesReceiverUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  string
	}{
		{"malformed JSON", `{"shares":[{"index":1,`},
		{"wrong shares type", `{"shares":"AQEC","threshold":9,"total_shares":9}`},
		{"empty share array", `{"shares":[],"threshold":9,"total_shares":9}`},
		{"invalid share value", `{"shares":[{"index":1,"threshold":2,"total":3,"value":"!!!!"}],"threshold":9,"total_shares":9}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shares := splitForResult(t, 2, 3)
			held := append([]*shamir.ProtectedShare(nil), shares...)
			result := ShamirInitResult{Shares: shares, Threshold: 2, TotalShares: 3}
			defer result.Destroy()

			if err := json.Unmarshal([]byte(tc.doc), &result); err == nil {
				t.Fatal("expected the decode to fail")
			}
			for i, sh := range held {
				if err := sh.Validate(); err != nil {
					t.Fatalf("held share %d was damaged by a failed decode: %v", i, err)
				}
			}
			if len(result.Shares) != 3 {
				t.Fatalf("receiver shares changed: got %d, want 3", len(result.Shares))
			}
			for i := range held {
				if result.Shares[i] != held[i] {
					t.Fatalf("receiver share %d was replaced by a failed decode", i)
				}
			}
			if result.Threshold != 2 || result.TotalShares != 3 {
				t.Fatalf("receiver scalars changed: threshold=%d total_shares=%d", result.Threshold, result.TotalShares)
			}
			if !combineEquals(t, result.Shares[1:], resultJSONSecret) {
				t.Fatal("receiver shares no longer combine to the secret after a failed decode")
			}
		})
	}
}
