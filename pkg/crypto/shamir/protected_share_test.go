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
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// destroyAll releases every share in a set. Locked memory is a finite kernel
// resource, and the test binary holds a lot of it if a set is abandoned.
func destroyAll(shares []*ProtectedShare) {
	for _, s := range shares {
		s.Destroy()
	}
}

func TestSplitProtected_RoundTrip(t *testing.T) {
	secret := []byte("root key material, 3-of-5")

	shares, err := SplitProtected(secret, 3, 5)
	if err != nil {
		t.Fatalf("SplitProtected returned unexpected error: %v", err)
	}
	defer destroyAll(shares)

	if len(shares) != 5 {
		t.Fatalf("expected 5 shares, got %d", len(shares))
	}
	for i, s := range shares {
		if s.Index != i+1 {
			t.Errorf("share %d: expected index %d, got %d", i, i+1, s.Index)
		}
		if s.Threshold != 3 || s.Total != 5 {
			t.Errorf("share %d: expected 3/5, got %d/%d", i, s.Threshold, s.Total)
		}
		if len(s.Value()) == 0 {
			t.Errorf("share %d: value is empty", i)
		}
		if err := s.Validate(); err != nil {
			t.Errorf("share %d: validation failed: %v", i, err)
		}
	}

	// Any threshold-sized subset reconstructs, and no subset copies the
	// secret out of protected memory.
	for _, subset := range [][]int{{0, 1, 2}, {1, 3, 4}, {0, 2, 4}, {0, 1, 2, 3, 4}} {
		picked := make([]*ProtectedShare, 0, len(subset))
		for _, i := range subset {
			picked = append(picked, shares[i])
		}
		buf, err := CombineProtected(picked)
		if err != nil {
			t.Fatalf("CombineProtected(%v) returned unexpected error: %v", subset, err)
		}
		if !bytes.Equal(encBytes(buf), secret) {
			t.Errorf("CombineProtected(%v) = %q, want %q", subset, encBytes(buf), secret)
		}
		buf.Destroy()
		if encBytes(buf) != nil {
			t.Errorf("subset %v: reconstructed secret survived Destroy", subset)
		}
	}
}

func TestSplitProtected_ValuesMatchSplit(t *testing.T) {
	// The two paths must produce the same share bytes for the same
	// randomness: only the custody of the value differs.
	secret := []byte("identical share material")
	restore := testOnlyRandReader(bytes.NewReader(bytes.Repeat([]byte{0x5a}, 4096)))
	plain, err := Split(secret, 2, 3)
	restore()
	if err != nil {
		t.Fatalf("Split returned unexpected error: %v", err)
	}

	restore = testOnlyRandReader(bytes.NewReader(bytes.Repeat([]byte{0x5a}, 4096)))
	protected, err := SplitProtected(secret, 2, 3)
	restore()
	if err != nil {
		t.Fatalf("SplitProtected returned unexpected error: %v", err)
	}
	defer destroyAll(protected)

	for i := range plain {
		if got, want := string(protected[i].Value()), plain[i].Value; got != want {
			t.Errorf("share %d: protected value %q != plain value %q", i, got, want)
		}
	}
}

func TestSplitProtected_InvalidArguments(t *testing.T) {
	tests := []struct {
		name             string
		secret           []byte
		threshold, total int
	}{
		{"threshold below two", []byte("secret"), 1, 3},
		{"total below threshold", []byte("secret"), 3, 2},
		{"empty secret", nil, 2, 3},
		{"total above 255", []byte("secret"), 2, 256},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shares, err := SplitProtected(tt.secret, tt.threshold, tt.total)
			if err == nil {
				destroyAll(shares)
				t.Fatalf("expected an error, got %d shares", len(shares))
			}
			var splitErr *SplitError
			if !errors.As(err, &splitErr) {
				t.Errorf("expected *SplitError, got %T: %v", err, err)
			}
			if shares != nil {
				t.Errorf("expected nil shares alongside the error, got %d", len(shares))
			}
		})
	}
}

func TestNewProtectedShare_WipesSource(t *testing.T) {
	value := []byte(base64.StdEncoding.EncodeToString([]byte{shareFormatVersion, 0x01, 0xab, 0xcd}))
	want := append([]byte(nil), value...)

	share, err := NewProtectedShare(1, 2, 3, value)
	if err != nil {
		t.Fatalf("NewProtectedShare returned unexpected error: %v", err)
	}
	defer share.Destroy()

	for i, b := range value {
		if b != 0 {
			t.Fatalf("source byte %d not wiped: got %d", i, b)
		}
	}
	if !bytes.Equal(share.Value(), want) {
		t.Errorf("protected value = %q, want %q", share.Value(), want)
	}
}

func TestNewProtectedShare_Invalid(t *testing.T) {
	valid := base64.StdEncoding.EncodeToString([]byte{shareFormatVersion, 0x01, 0xab})
	tests := []struct {
		name                    string
		index, threshold, total int
		value                   string
		field                   string
	}{
		{"index below one", 0, 2, 3, valid, "index"},
		{"index above total", 4, 2, 3, valid, "index"},
		{"threshold below two", 1, 1, 3, valid, "threshold"},
		{"total below threshold", 1, 3, 2, valid, "total"},
		{"empty value", 1, 2, 3, "", "value"},
		{"value not base64", 1, 2, 3, "not base64!", "value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := []byte(tt.value)
			share, err := NewProtectedShare(tt.index, tt.threshold, tt.total, source)
			if err == nil {
				share.Destroy()
				t.Fatal("expected a validation error, got nil")
			}
			if share != nil {
				t.Error("expected a nil share alongside the error")
			}
			var valErr *ValidationError
			if !errors.As(err, &valErr) {
				t.Fatalf("expected *ValidationError, got %T: %v", err, err)
			}
			if valErr.Field != tt.field {
				t.Errorf("error field = %q, want %q", valErr.Field, tt.field)
			}
			// A rejected value is still the caller's to wipe, so it must come
			// back untouched rather than half-consumed.
			if !bytes.Equal(source, []byte(tt.value)) {
				t.Errorf("source mutated on the error path: %q", source)
			}
		})
	}
}

func TestProtectedShare_JSONMatchesShare(t *testing.T) {
	value := base64.StdEncoding.EncodeToString([]byte{shareFormatVersion, 0x02, 0x11, 0x22})
	for _, metadata := range []map[string]string{nil, {"custodian": "sre-2", "issued": "2026-08-18"}} {
		plain := &Share{Index: 2, Threshold: 3, Total: 5, Value: value, Metadata: metadata}
		protected, err := NewProtectedShare(2, 3, 5, []byte(value))
		if err != nil {
			t.Fatalf("NewProtectedShare returned unexpected error: %v", err)
		}
		protected.Metadata = metadata

		wantJSON, err := json.Marshal(plain)
		if err != nil {
			t.Fatalf("marshalling Share: %v", err)
		}
		gotJSON, err := json.Marshal(protected)
		if err != nil {
			t.Fatalf("marshalling ProtectedShare: %v", err)
		}
		if !bytes.Equal(gotJSON, wantJSON) {
			t.Errorf("ProtectedShare JSON = %s, want %s", gotJSON, wantJSON)
		}
		// The encoder sizes its buffer exactly so that no append reallocates
		// and strands an un-wiped copy of the value on the heap. json.Marshal
		// compacts into a buffer of its own, so the property is asserted on
		// the slice MarshalJSON actually returns.
		raw, err := protected.MarshalJSON()
		if err != nil {
			t.Fatalf("MarshalJSON: %v", err)
		}
		if cap(raw) != len(raw) {
			t.Errorf("marshal buffer grew: len %d, cap %d", len(raw), cap(raw))
		}
		protected.Destroy()
	}
}

func TestProtectedShare_JSONRoundTrip(t *testing.T) {
	secret := []byte("a secret that survives a JSON round trip")
	shares, err := SplitProtected(secret, 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected returned unexpected error: %v", err)
	}
	shares[0].Metadata["custodian"] = "sre-1"

	encoded, err := json.Marshal(shares)
	destroyAll(shares)
	if err != nil {
		t.Fatalf("marshalling shares: %v", err)
	}

	var decoded []*ProtectedShare
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshalling shares: %v", err)
	}
	defer destroyAll(decoded)

	if len(decoded) != 3 {
		t.Fatalf("expected 3 shares, got %d", len(decoded))
	}
	if decoded[0].Metadata["custodian"] != "sre-1" {
		t.Errorf("metadata lost: %v", decoded[0].Metadata)
	}
	buf, err := CombineProtected(decoded[:2])
	if err != nil {
		t.Fatalf("CombineProtected returned unexpected error: %v", err)
	}
	defer buf.Destroy()
	if !bytes.Equal(encBytes(buf), secret) {
		t.Errorf("reconstructed %q, want %q", encBytes(buf), secret)
	}
}

func TestProtectedShare_UnmarshalDestroysPreviousValue(t *testing.T) {
	// json.Unmarshal reuses the elements of a non-empty slice, so decoding
	// over a share that already holds one must not strand its locked buffer.
	share, err := NewProtectedShare(1, 2, 3, []byte(base64.StdEncoding.EncodeToString([]byte{shareFormatVersion, 0x01, 0x77})))
	if err != nil {
		t.Fatalf("NewProtectedShare returned unexpected error: %v", err)
	}
	previous := share.value

	replacement := base64.StdEncoding.EncodeToString([]byte{shareFormatVersion, 0x02, 0x88})
	if err := json.Unmarshal([]byte(`{"index":2,"threshold":2,"total":3,"value":"`+replacement+`"}`), share); err != nil {
		t.Fatalf("UnmarshalJSON returned unexpected error: %v", err)
	}
	defer share.Destroy()

	if encBytes(previous) != nil {
		t.Error("the replaced value was not destroyed")
	}
	if share.Index != 2 {
		t.Errorf("index = %d, want 2", share.Index)
	}
	if string(share.Value()) != replacement {
		t.Errorf("value = %q, want %q", share.Value(), replacement)
	}
}

func TestProtectedShare_UnmarshalJSON_Invalid(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{"not an object", `["nope"]`},
		{"value is not a string", `{"index":1,"threshold":2,"total":3,"value":123}`},
		{"value is empty", `{"index":1,"threshold":2,"total":3,"value":""}`},
		{"value is escaped", `{"index":1,"threshold":2,"total":3,"value":"\u0041QEC"}`},
		{"index out of range", `{"index":9,"threshold":2,"total":3,"value":"AQEC"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var share ProtectedShare
			if err := json.Unmarshal([]byte(tt.json), &share); err == nil {
				share.Destroy()
				t.Fatal("expected an error, got nil")
			}
			if share.value != nil && encBytes(share.value) != nil {
				share.Destroy()
				t.Error("a rejected share kept protected memory")
			}
		})
	}
}

func TestProtectedShare_Destroy(t *testing.T) {
	share, err := NewProtectedShare(1, 2, 3, []byte(base64.StdEncoding.EncodeToString([]byte{shareFormatVersion, 0x01, 0x42})))
	if err != nil {
		t.Fatalf("NewProtectedShare returned unexpected error: %v", err)
	}

	share.Destroy()
	share.Destroy() // idempotent

	if share.Value() != nil {
		t.Error("Value survived Destroy")
	}
	var valErr *ValidationError
	if err := share.Validate(); !errors.As(err, &valErr) || valErr.Field != "value" {
		t.Errorf("Validate after Destroy = %v, want a value ValidationError", err)
	}
	if _, err := json.Marshal(share); err == nil {
		t.Error("marshalling a destroyed share succeeded")
	}
	if _, err := CombineProtected([]*ProtectedShare{share, share}); err == nil {
		t.Error("combining a destroyed share succeeded")
	}
}

func TestProtectedShare_NilReceiver(t *testing.T) {
	var share *ProtectedShare

	share.Destroy() // must not panic
	if share.Value() != nil {
		t.Error("Value on a nil share returned bytes")
	}
	var valErr *ValidationError
	if err := share.Validate(); !errors.As(err, &valErr) || valErr.Field != "share" {
		t.Errorf("Validate on a nil share = %v, want a share ValidationError", err)
	}
	if got := share.String(); got != "ProtectedShare(nil)" {
		t.Errorf("String on a nil share = %q", got)
	}
	if got := share.LogValue().String(); got != "ProtectedShare(nil)" {
		t.Errorf("LogValue on a nil share = %q", got)
	}
	encoded, err := json.Marshal(share)
	if err != nil {
		t.Fatalf("marshalling a nil share: %v", err)
	}
	if string(encoded) != "null" {
		t.Errorf("marshalling a nil share = %s, want null", encoded)
	}
}

func TestProtectedShare_Redaction(t *testing.T) {
	value := base64.StdEncoding.EncodeToString([]byte{shareFormatVersion, 0x01, 0x99, 0x88, 0x77})
	share, err := NewProtectedShare(1, 2, 3, []byte(value))
	if err != nil {
		t.Fatalf("NewProtectedShare returned unexpected error: %v", err)
	}
	defer share.Destroy()

	if got := share.String(); !strings.Contains(got, "[redacted]") || strings.Contains(got, value) {
		t.Errorf("String leaked share material: %q", got)
	}

	// A structured logger resolves LogValue first, whatever the handler.
	var sink bytes.Buffer
	slog.New(slog.NewJSONHandler(&sink, nil)).Info("share", "share", share)
	if strings.Contains(sink.String(), value) {
		t.Errorf("JSON handler logged share material: %s", sink.String())
	}
	if !strings.Contains(sink.String(), "[redacted]") {
		t.Errorf("JSON handler did not log the redacted form: %s", sink.String())
	}
}

func TestCombineProtected_Invalid(t *testing.T) {
	secret := []byte("shares for the error paths")
	shares, err := SplitProtected(secret, 3, 5)
	if err != nil {
		t.Fatalf("SplitProtected returned unexpected error: %v", err)
	}
	defer destroyAll(shares)

	relabelled, err := NewProtectedShare(1, 3, 9, append([]byte(nil), shares[0].Value()...))
	if err != nil {
		t.Fatalf("NewProtectedShare returned unexpected error: %v", err)
	}
	defer relabelled.Destroy()

	rethresholded, err := NewProtectedShare(1, 4, 5, append([]byte(nil), shares[0].Value()...))
	if err != nil {
		t.Fatalf("NewProtectedShare returned unexpected error: %v", err)
	}
	defer rethresholded.Destroy()

	tests := []struct {
		name   string
		shares []*ProtectedShare
		want   string
	}{
		{"no shares", nil, "no shares provided"},
		// The 255-share bound is checked before any share is touched, so a
		// nil set is enough to exercise it and costs no locked memory.
		{"too many shares", make([]*ProtectedShare, maxShares+1), "too many shares"},
		{"nil element", []*ProtectedShare{shares[0], nil, shares[2]}, "share 1 is nil"},
		{"below threshold", shares[:2], "need at least 3 shares"},
		{"mismatched total", []*ProtectedShare{shares[0], relabelled, shares[2]}, "different total"},
		{"mismatched threshold", []*ProtectedShare{shares[0], rethresholded, shares[2]}, "different threshold"},
		{"duplicate share", []*ProtectedShare{shares[0], shares[0], shares[1]}, "duplicate share"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, err := CombineProtected(tt.shares)
			if err == nil {
				buf.Destroy()
				t.Fatal("expected an error, got nil")
			}
			if buf != nil {
				t.Error("expected a nil buffer alongside the error")
			}
			var combineErr *CombineError
			if !errors.As(err, &combineErr) {
				t.Fatalf("expected *CombineError, got %T: %v", err, err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestCombineProtected_WireFormatRulesAreShared(t *testing.T) {
	// CombineProtected decodes natively into protected memory, but the
	// wire-format rules it enforces are still parseRawShare's — the same copy
	// combine and CombineBytes use. A foreign version byte is the cheapest
	// observable proof.
	foreign := base64.StdEncoding.EncodeToString([]byte{shareFormatVersion + 1, 0x01, 0x33})
	first, err := NewProtectedShare(1, 2, 2, []byte(foreign))
	if err != nil {
		t.Fatalf("NewProtectedShare returned unexpected error: %v", err)
	}
	defer first.Destroy()
	second, err := NewProtectedShare(2, 2, 2, []byte(base64.StdEncoding.EncodeToString([]byte{shareFormatVersion, 0x02, 0x44})))
	if err != nil {
		t.Fatalf("NewProtectedShare returned unexpected error: %v", err)
	}
	defer second.Destroy()

	if _, err := CombineProtected([]*ProtectedShare{first, second}); err == nil ||
		!strings.Contains(err.Error(), "unsupported format version") {
		t.Errorf("CombineProtected error = %v, want an unsupported-format-version refusal", err)
	}
}

func TestShare_Protect(t *testing.T) {
	secret := []byte("migrated from the string path")
	plain, err := Split(secret, 2, 3)
	if err != nil {
		t.Fatalf("Split returned unexpected error: %v", err)
	}
	plain[0].Metadata["custodian"] = "sre-3"

	protected := make([]*ProtectedShare, 0, len(plain))
	for i, p := range plain {
		ps, err := p.Protect()
		if err != nil {
			t.Fatalf("share %d: Protect returned unexpected error: %v", i, err)
		}
		protected = append(protected, ps)
		if string(ps.Value()) != p.Value {
			t.Errorf("share %d: protected value %q != %q", i, ps.Value(), p.Value)
		}
	}
	defer destroyAll(protected)

	if protected[0].Metadata["custodian"] != "sre-3" {
		t.Errorf("metadata not carried over: %v", protected[0].Metadata)
	}
	buf, err := CombineProtected(protected[1:])
	if err != nil {
		t.Fatalf("CombineProtected returned unexpected error: %v", err)
	}
	defer buf.Destroy()
	if !bytes.Equal(encBytes(buf), secret) {
		t.Errorf("reconstructed %q, want %q", encBytes(buf), secret)
	}
}

func TestShare_Protect_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		share *Share
		field string
	}{
		{"nil share", nil, "share"},
		{"empty value", &Share{Index: 1, Threshold: 2, Total: 3}, "value"},
		{"index out of range", &Share{Index: 4, Threshold: 2, Total: 3, Value: "AQEC"}, "index"},
		{"value not base64", &Share{Index: 1, Threshold: 2, Total: 3, Value: "not base64!"}, "value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protected, err := tt.share.Protect()
			if err == nil {
				protected.Destroy()
				t.Fatal("expected an error, got nil")
			}
			if protected != nil {
				t.Error("expected a nil share alongside the error")
			}
			var valErr *ValidationError
			if !errors.As(err, &valErr) {
				t.Fatalf("expected *ValidationError, got %T: %v", err, err)
			}
			if valErr.Field != tt.field {
				t.Errorf("error field = %q, want %q", valErr.Field, tt.field)
			}
		})
	}
}

func TestProtectedShare_InteropWithPlainShare(t *testing.T) {
	// A protected share serialized as JSON is a plain share on the wire: a
	// custody layer can persist one and reconstruct with the other.
	secret := []byte("interoperable share material")
	protected, err := SplitProtected(secret, 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected returned unexpected error: %v", err)
	}
	encoded, err := json.Marshal(protected)
	destroyAll(protected)
	if err != nil {
		t.Fatalf("marshalling protected shares: %v", err)
	}

	var plain []*Share
	if err := json.Unmarshal(encoded, &plain); err != nil {
		t.Fatalf("unmarshalling as plain shares: %v", err)
	}
	reconstructed, err := Combine(plain[:2])
	if err != nil {
		t.Fatalf("Combine returned unexpected error: %v", err)
	}
	if !bytes.Equal(reconstructed, secret) {
		t.Errorf("Combine reconstructed %q, want %q", reconstructed, secret)
	}
}

func TestIsStandardBase64(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		// Well-formed.
		{"AQEC", true},
		{"AQ==", true},
		{"AQE=", true},
		{"az09+/==", true},
		{"AQECAwQF", true},
		// Alphabet violations.
		{"AQ EC", false},
		{`AQ"C`, false},
		{"AQ\\C", false},
		{"AQ-C", false},
		{"AQ_C", false},
		{"AQ\nC", false},
		// Length violations: standard base64 comes in quanta of four.
		{"", false},
		{"A", false},
		{"AQ", false},
		{"AQE", false},
		{"AQECA", false},
		{"az09+/=", false},
		// Padding violations: legal characters, undecodable strings.
		{"====", false},
		{"A===", false},
		{"A=AA", false},
		{"=AAA", false},
		{"AA=A", false},
		{"AQ==AQEC", false},
		{"AQECAQ==AQEC", false},
	}
	for _, tt := range tests {
		if got := isStandardBase64([]byte(tt.value)); got != tt.want {
			t.Errorf("isStandardBase64(%q) = %v, want %v", tt.value, got, tt.want)
		}
		// The grammar this enforces must be exactly the one the decoder
		// accepts: a value it admits must decode, and one it rejects must not
		// be silently accepted anywhere downstream. (The empty string is the
		// one deliberate divergence — it decodes to nothing, and an empty
		// share value is rejected as empty before this check runs.)
		if tt.value != "" {
			_, err := base64.StdEncoding.DecodeString(tt.value)
			if decodes := err == nil; decodes != tt.want {
				t.Errorf("isStandardBase64(%q) = %v but base64 decode success = %v",
					tt.value, tt.want, decodes)
			}
		}
	}
}

func TestNewProtectedShare_RejectsUndecodableBase64(t *testing.T) {
	// Every one of these is built from standard-alphabet characters, so an
	// alphabet-membership check admits them; none of them decodes, and a
	// share that cannot decode must never reach protected memory or a
	// serialized share set.
	for _, value := range []string{"A", "AQE", "====", "A===", "A=AA", "AA=A", "AQ==AQEC"} {
		t.Run(value, func(t *testing.T) {
			share, err := NewProtectedShare(1, 2, 3, []byte(value))
			if err == nil {
				share.Destroy()
				t.Fatalf("NewProtectedShare(%q) was accepted; want a validation error", value)
			}
			var valErr *ValidationError
			if !errors.As(err, &valErr) {
				t.Fatalf("expected *ValidationError, got %T: %v", err, err)
			}
			if valErr.Field != "value" {
				t.Errorf("error field = %q, want %q", valErr.Field, "value")
			}

			// The same value must be refused when it arrives as JSON.
			doc := fmt.Sprintf(`{"index":1,"threshold":2,"total":3,"value":%q}`, value)
			var decoded ProtectedShare
			if err := json.Unmarshal([]byte(doc), &decoded); err == nil {
				decoded.Destroy()
				t.Fatalf("json.Unmarshal accepted value %q; want a validation error", value)
			}
			if decoded.value != nil && encBytes(decoded.value) != nil {
				decoded.Destroy()
				t.Error("a rejected share kept protected memory")
			}
		})
	}
}

// TestSplitProtected_LeavesSecretToTheCaller pins the documented contract that
// the secret argument is neither copied nor wiped: it is the caller's buffer,
// and a caller that passed protected memory would be very surprised to find it
// zeroed — or to find the split had quietly worked from a copy.
func TestSplitProtected_LeavesSecretToTheCaller(t *testing.T) {
	secret := []byte("root key the caller still owns")
	want := append([]byte(nil), secret...)

	shares, err := SplitProtected(secret, 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected returned unexpected error: %v", err)
	}
	defer destroyAll(shares)

	if !bytes.Equal(secret, want) {
		t.Errorf("SplitProtected mutated the caller's secret: %q, want %q", secret, want)
	}
}

// TestSplitProtected_ProtectedSecretRoundTrip runs the end-to-end shape a
// custody caller should use: the secret is in locked memory before the split,
// every share value is in locked memory, and the reconstruction comes back in
// locked memory. Nothing in the chain is an ordinary heap buffer the caller
// has to remember to wipe.
func TestSplitProtected_ProtectedSecretRoundTrip(t *testing.T) {
	want := []byte("thirty-two bytes of key material")
	held := mustFromBytes(t, append([]byte(nil), want...))
	defer held.Destroy()

	shares, err := SplitProtected(encBytes(held), 3, 5)
	if err != nil {
		t.Fatalf("SplitProtected returned unexpected error: %v", err)
	}
	defer destroyAll(shares)

	got, err := CombineProtected(shares[:3])
	if err != nil {
		t.Fatalf("CombineProtected returned unexpected error: %v", err)
	}
	defer got.Destroy()

	if !bytes.Equal(encBytes(got), want) {
		t.Errorf("reconstructed %q, want %q", encBytes(got), want)
	}
	// The source enclave is untouched by the split.
	if !bytes.Equal(encBytes(held), want) {
		t.Errorf("the caller's protected secret changed: %q", encBytes(held))
	}
}

// TestCombineProtected_LeavesSharesUsable verifies the input shares survive a
// reconstruction: CombineProtected reads their values in place, so consuming a
// share set must remain the caller's decision (a ceremony reconstructs from
// one subset and may need another).
func TestCombineProtected_LeavesSharesUsable(t *testing.T) {
	secret := []byte("reconstructed twice from one set")
	shares, err := SplitProtected(secret, 2, 4)
	if err != nil {
		t.Fatalf("SplitProtected returned unexpected error: %v", err)
	}
	defer destroyAll(shares)

	values := make([]string, len(shares))
	for i, s := range shares {
		values[i] = string(s.Value())
	}

	first, err := CombineProtected(shares[:2])
	if err != nil {
		t.Fatalf("first CombineProtected returned unexpected error: %v", err)
	}
	first.Destroy()

	for i, s := range shares {
		if got := string(s.Value()); got != values[i] {
			t.Errorf("share %d changed across a reconstruction", i)
		}
	}
	second, err := CombineProtected(shares[2:])
	if err != nil {
		t.Fatalf("second CombineProtected returned unexpected error: %v", err)
	}
	defer second.Destroy()
	if !bytes.Equal(encBytes(second), secret) {
		t.Errorf("second reconstruction = %q, want %q", encBytes(second), secret)
	}
}

// TestSplitProtected_RandomnessFailure verifies the generation path reports a
// randomness failure as a typed SplitError and yields no shares — a partial
// share set from a short read would be share material nobody owns.
func TestSplitProtected_RandomnessFailure(t *testing.T) {
	restore := testOnlyRandReader(bytes.NewReader([]byte{0x01, 0x02}))
	shares, err := SplitProtected([]byte("a secret longer than the entropy"), 3, 5)
	restore()

	if err == nil {
		destroyAll(shares)
		t.Fatal("expected an error from a short randomness source, got nil")
	}
	if shares != nil {
		destroyAll(shares)
		t.Error("expected no shares alongside the error")
	}
	var splitErr *SplitError
	if !errors.As(err, &splitErr) {
		t.Fatalf("expected *SplitError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "random coefficients") {
		t.Errorf("error = %q, want it to name the randomness failure", err)
	}
}

// TestNewProtectedShare_RejectsOversizedValue pins the locked-memory bound.
//
// A value made of arbitrarily many legal base64 characters is syntactically
// perfect, so the alphabet check passes it: only the length bound stands
// between an unauthenticated submission and an mlock'd allocation of the
// submitter's choosing. That matters more here than on the string path —
// RLIMIT_MEMLOCK is a finite PROCESS-WIDE resource, so what the submission takes
// is denied to every other caller — and parseRawShare's MaxSecretBytes check is far too
// late, since it only sees the payload after CombineProtected has already
// locked it.
func TestNewProtectedShare_RejectsOversizedValue(t *testing.T) {
	// One base64 quantum past the largest share this package can produce.
	oversized := bytes.Repeat([]byte("A"), maxProtectedValueBytes+4)
	if !isStandardBase64(oversized) {
		t.Fatal("test input is not valid base64, so the length bound is not what would reject it")
	}

	share, err := NewProtectedShare(1, 2, 3, oversized)
	if err == nil {
		share.Destroy()
		t.Fatal("NewProtectedShare accepted an oversized value; want a validation error")
	}
	if share != nil {
		share.Destroy()
		t.Error("a rejected share was returned; its locked memory would leak")
	}
	var valErr *ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if valErr.Field != "value" {
		t.Errorf("error field = %q, want %q", valErr.Field, "value")
	}
	// Documented contract: a validation failure leaves the source for the
	// caller to wipe, rather than half-consuming it.
	if !bytes.Equal(oversized, bytes.Repeat([]byte("A"), len(oversized))) {
		t.Error("a rejected value was mutated; the caller can no longer wipe what it passed")
	}

	// The same value must be refused when it arrives as operator-supplied
	// JSON, which is the path that actually faces unauthenticated input.
	doc := fmt.Sprintf(`{"index":1,"threshold":2,"total":3,"value":%q}`, oversized)
	var decoded ProtectedShare
	if err := json.Unmarshal([]byte(doc), &decoded); err == nil {
		decoded.Destroy()
		t.Fatal("json.Unmarshal accepted an oversized value; want a validation error")
	}
	if decoded.value != nil {
		decoded.Destroy()
		t.Error("a rejected share kept protected memory")
	}
}

// TestNewProtectedShare_AcceptsMaxSizeValue is the other half of the bound:
// it must reject exactly what parseRawShare would have rejected and nothing
// more, so the largest share this package can actually produce still round
// trips through the protected path.
func TestNewProtectedShare_AcceptsMaxSizeValue(t *testing.T) {
	secret := bytes.Repeat([]byte{0x5a}, MaxSecretBytes)
	shares, err := SplitProtected(secret, 2, 2)
	if err != nil {
		t.Fatalf("SplitProtected returned unexpected error: %v", err)
	}
	defer destroyAll(shares)

	if got := len(shares[0].Value()); got != maxProtectedValueBytes {
		t.Fatalf("largest producible share value is %d bytes, but the bound is %d", got, maxProtectedValueBytes)
	}

	// Re-entering that value through the untrusted constructor must work: it
	// is the boundary case, one quantum below the rejection above.
	value := append([]byte(nil), shares[0].Value()...)
	reborn, err := NewProtectedShare(shares[0].Index, shares[0].Threshold, shares[0].Total, value)
	if err != nil {
		t.Fatalf("NewProtectedShare rejected a legitimate max-size share: %v", err)
	}
	defer reborn.Destroy()

	got, err := CombineProtected([]*ProtectedShare{reborn, shares[1]})
	if err != nil {
		t.Fatalf("CombineProtected returned unexpected error: %v", err)
	}
	defer got.Destroy()
	if !bytes.Equal(encBytes(got), secret) {
		t.Error("max-size secret did not round trip through the protected path")
	}
}

// marshalShareSet renders a share set as the JSON array a custody caller would
// submit, without going through the shares themselves — the oversized cases
// below have to build documents no legitimate share set could produce.
func shareSetJSON(values []string) []byte {
	elems := make([]string, len(values))
	for i, v := range values {
		elems[i] = fmt.Sprintf(`{"index":%d,"threshold":2,"total":%d,"value":%q}`, i+1, len(values), v)
	}
	return []byte("[" + strings.Join(elems, ",") + "]")
}

// TestUnmarshalShareSet_RoundTrip pins the ordinary path: what json.Marshal
// writes for a share set is what UnmarshalShareSet reads back, and the shares
// that come out reconstruct the secret.
func TestUnmarshalShareSet_RoundTrip(t *testing.T) {
	secret := []byte("root key material, 3-of-5")
	shares, err := SplitProtected(secret, 3, 5)
	if err != nil {
		t.Fatalf("SplitProtected returned unexpected error: %v", err)
	}
	defer destroyAll(shares)

	doc, err := json.Marshal(shares)
	if err != nil {
		t.Fatalf("marshalling the share set: %v", err)
	}
	decoded, err := UnmarshalShareSet(doc)
	if err != nil {
		t.Fatalf("UnmarshalShareSet returned unexpected error: %v", err)
	}
	defer destroyAll(decoded)

	if len(decoded) != len(shares) {
		t.Fatalf("decoded %d shares, want %d", len(decoded), len(shares))
	}
	for i := range shares {
		if !bytes.Equal(decoded[i].Value(), shares[i].Value()) {
			t.Errorf("share %d: value %q, want %q", i, decoded[i].Value(), shares[i].Value())
		}
	}

	got, err := CombineProtected(decoded[:3])
	if err != nil {
		t.Fatalf("CombineProtected returned unexpected error: %v", err)
	}
	defer got.Destroy()
	if !bytes.Equal(encBytes(got), secret) {
		t.Errorf("reconstructed %q, want %q", encBytes(got), secret)
	}
}

// TestUnmarshalShareSet_BoundsTheSetBeforeAllocating covers the collection-level
// resource bound. Each element below is individually legal — the per-value
// bound has nothing to say about any of them — and it is the SET that must be
// refused, before the first locked buffer exists: locked memory is a finite
// process-wide resource, so an unauthenticated submission must not get to choose how much of
// it to take.
func TestUnmarshalShareSet_BoundsTheSetBeforeAllocating(t *testing.T) {
	small := base64.StdEncoding.EncodeToString([]byte{shareFormatVersion, 0x01, 0xab})
	// Individually legal at exactly the per-share maximum: it takes only 13 of
	// these to pass the 1 MiB set budget, which is the point — 255 of them
	// would lock ~22 MiB.
	large := strings.Repeat("A", maxProtectedValueBytes)

	tests := []struct {
		name  string
		doc   []byte
		field string
	}{
		{"empty set", []byte(`[]`), "shares"},
		{"too many shares", shareSetJSON(repeatValue(small, maxShares+1)), "shares"},
		{"over the byte budget", shareSetJSON(repeatValue(large, 1+MaxShareSetBytes/len(large))), "shares"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shares, err := UnmarshalShareSet(tt.doc)
			if err == nil {
				destroyAll(shares)
				t.Fatalf("UnmarshalShareSet accepted the set; want a validation error")
			}
			if shares != nil {
				destroyAll(shares)
				t.Error("a rejected set returned shares; their locked memory would leak")
			}
			var valErr *ValidationError
			if !errors.As(err, &valErr) {
				t.Fatalf("expected *ValidationError, got %T: %v", err, err)
			}
			if valErr.Field != tt.field {
				t.Errorf("error field = %q, want %q", valErr.Field, tt.field)
			}
		})
	}
}

// TestUnmarshalShareSet_UnwindsOnBadElement pins that a set rejected part way
// through leaves nothing behind: the shares built from the accepted prefix are
// destroyed rather than stranded, since nothing outside can reach them to do it.
func TestUnmarshalShareSet_UnwindsOnBadElement(t *testing.T) {
	valid := base64.StdEncoding.EncodeToString([]byte{shareFormatVersion, 0x01, 0xab})
	doc := shareSetJSON([]string{valid, valid, "not base64!"})

	shares, err := UnmarshalShareSet(doc)
	if err == nil {
		destroyAll(shares)
		t.Fatal("UnmarshalShareSet accepted a set with an invalid share; want an error")
	}
	if shares != nil {
		destroyAll(shares)
		t.Fatal("a rejected set returned shares; the accepted prefix must be destroyed, not returned")
	}
	var valErr *ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if !strings.Contains(valErr.Reason, "share 2") {
		t.Errorf("error does not name the offending element: %v", err)
	}
}

// TestUnmarshalShareSet_AcceptsAFullCeremony is the other half of the budget:
// the largest set this primitive actually exists to serve — every one of the
// 255 shares of a root key — must fit comfortably inside it.
func TestUnmarshalShareSet_AcceptsAFullCeremony(t *testing.T) {
	secret := bytes.Repeat([]byte{0x5a}, 32)
	shares, err := SplitProtected(secret, 3, maxShares)
	if err != nil {
		t.Fatalf("SplitProtected returned unexpected error: %v", err)
	}
	defer destroyAll(shares)

	doc, err := json.Marshal(shares)
	if err != nil {
		t.Fatalf("marshalling the share set: %v", err)
	}
	if len(doc) > MaxShareSetBytes/10 {
		t.Errorf("a full 255-share root-key ceremony is %d bytes, uncomfortably close to the %d-byte budget",
			len(doc), MaxShareSetBytes)
	}
	decoded, err := UnmarshalShareSet(doc)
	if err != nil {
		t.Fatalf("UnmarshalShareSet rejected a full root-key ceremony: %v", err)
	}
	defer destroyAll(decoded)
	if len(decoded) != maxShares {
		t.Fatalf("decoded %d shares, want %d", len(decoded), maxShares)
	}
}

// repeatValue builds n copies of a share value.
func repeatValue(value string, n int) []string {
	values := make([]string, n)
	for i := range values {
		values[i] = value
	}
	return values
}

// TestCombineProtected_MatchesCombineBytes pins the two-pass reconstruction
// against the reference path. CombineProtected folds each share in as it
// decodes it, reusing one buffer, rather than holding a decoded copy of every
// share — the peak locked footprint of a reconstruction must not be
// proportional to the size of the submitted set. The arithmetic must be
// identical to the all-at-once interpolation regardless.
func TestCombineProtected_MatchesCombineBytes(t *testing.T) {
	for _, secretLen := range []int{1, 2, 31, 32, 33, 1024} {
		secret := make([]byte, secretLen)
		for i := range secret {
			secret[i] = byte(i*7 + 3)
		}
		for _, scheme := range [][2]int{{2, 3}, {3, 5}, {5, 5}, {2, 16}} {
			threshold, total := scheme[0], scheme[1]
			shares, err := SplitProtected(secret, threshold, total)
			if err != nil {
				t.Fatalf("SplitProtected(%d bytes, %d/%d): %v", secretLen, threshold, total, err)
			}

			// The same share set through the wipeable-buffer path, which
			// interpolates from every decoded payload at once.
			values := make([][]byte, len(shares))
			for i, s := range shares {
				values[i] = append([]byte(nil), s.Value()...)
			}
			want, err := CombineBytes(values[:threshold], threshold)
			if err != nil {
				t.Fatalf("CombineBytes(%d bytes, %d/%d): %v", secretLen, threshold, total, err)
			}

			got, err := CombineProtected(shares[:threshold])
			if err != nil {
				t.Fatalf("CombineProtected(%d bytes, %d/%d): %v", secretLen, threshold, total, err)
			}
			if !bytes.Equal(encBytes(got), want) {
				t.Errorf("%d bytes, %d/%d: CombineProtected = %x, CombineBytes = %x",
					secretLen, threshold, total, encBytes(got), want)
			}
			if !bytes.Equal(encBytes(got), secret) {
				t.Errorf("%d bytes, %d/%d: reconstruction does not match the secret", secretLen, threshold, total)
			}
			got.Destroy()
			destroyAll(shares)
		}
	}
}

// TestCombineProtected_ReusedBufferDoesNotLeakBetweenShares guards the one risk
// a reused decode buffer introduces: a share's payload sitting in the buffer
// when the next share is decoded into it. Reconstruction is order-independent,
// so any bleed between shares shows up as a different secret for a different
// ordering of the same set.
func TestCombineProtected_ReusedBufferDoesNotLeakBetweenShares(t *testing.T) {
	secret := []byte("order must not change the outcome")
	shares, err := SplitProtected(secret, 3, 5)
	if err != nil {
		t.Fatalf("SplitProtected returned unexpected error: %v", err)
	}
	defer destroyAll(shares)

	for _, order := range [][]int{{0, 1, 2}, {2, 1, 0}, {4, 0, 3}, {3, 4, 0}} {
		picked := make([]*ProtectedShare, 0, len(order))
		for _, i := range order {
			picked = append(picked, shares[i])
		}
		got, err := CombineProtected(picked)
		if err != nil {
			t.Fatalf("CombineProtected(%v): %v", order, err)
		}
		if !bytes.Equal(encBytes(got), secret) {
			t.Errorf("CombineProtected(%v) = %q, want %q", order, encBytes(got), secret)
		}
		got.Destroy()
	}
}

// TestSplitProtected_RefusesUnlockableCombinations covers the cross product the
// per-dimension bounds miss: a secret size and a share count that are each
// legal on their own and multiply into a footprint no host can lock. The check
// is arithmetic and runs before the first allocation, so this test proves the
// rejection without depending on the host's RLIMIT_MEMLOCK — nothing is
// allocated on the way to the error.
func TestSplitProtected_RefusesUnlockableCombinations(t *testing.T) {
	tests := []struct {
		name             string
		secretLen        int
		threshold, total int
	}{
		// ~16 MiB of coefficients plus ~22 MiB of share values.
		{"max secret split every way", MaxSecretBytes, maxShares, maxShares},
		// The coefficients alone are over budget: 64 KiB x 19.
		{"max secret, modest threshold", MaxSecretBytes, 20, 20},
		// The values alone are over budget: 255 x ~5.5 KiB.
		{"payload-sized secret, full ceremony", 4 << 10, 2, maxShares},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shares, err := SplitProtected(make([]byte, tt.secretLen), tt.threshold, tt.total)
			if err == nil {
				destroyAll(shares)
				t.Fatal("SplitProtected accepted a combination it cannot lock; want a *SplitError")
			}
			if shares != nil {
				destroyAll(shares)
				t.Error("a rejected split returned shares")
			}
			var splitErr *SplitError
			if !errors.As(err, &splitErr) {
				t.Fatalf("expected *SplitError, got %T: %v", err, err)
			}
			if need := splitLockedBytes(tt.secretLen, tt.threshold, tt.total); need <= MaxShareSetBytes {
				t.Errorf("case is not over budget: needs %d of %d bytes", need, MaxShareSetBytes)
			}
		})
	}
}

// TestSplitProtected_AcceptsRealCeremonies is the other half of that bound: the
// shapes this primitive actually exists to serve must all fit, including the
// largest secret the package accepts at a realistic share count.
func TestSplitProtected_AcceptsRealCeremonies(t *testing.T) {
	tests := []struct {
		name             string
		secretLen        int
		threshold, total int
	}{
		{"root key, full 255-share ceremony", 32, 3, maxShares},
		{"root key, 3-of-5", 32, 3, 5},
		{"max secret, 5-of-5", MaxSecretBytes, 5, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret := bytes.Repeat([]byte{0x5a}, tt.secretLen)
			shares, err := SplitProtected(secret, tt.threshold, tt.total)
			if err != nil {
				t.Fatalf("SplitProtected refused a legitimate ceremony (%d bytes of budget): %v",
					splitLockedBytes(tt.secretLen, tt.threshold, tt.total), err)
			}
			defer destroyAll(shares)

			got, err := CombineProtected(shares[:tt.threshold])
			if err != nil {
				t.Fatalf("CombineProtected returned unexpected error: %v", err)
			}
			defer got.Destroy()
			if !bytes.Equal(encBytes(got), secret) {
				t.Error("the secret did not round trip")
			}
		})
	}
}

// TestMaxShareSetBytesIsAPageBudgetAtEveryPageSize pins MaxShareSetBytes as a
// page budget.
//
// A fixed 2 << 20 is 512 pages on a 4 KiB-page host and on no other. Every
// figure in the derivation is a page count, so stating it in the bytes of one
// particular host would make the primitive's headline case — the 255-share
// split of a 32-byte root key — fail everywhere else: the same 260 pages is
// 4,259,840 bytes at 16 KiB and 17,039,360 at 64 KiB, both refused by a 2 MiB
// cap. RHEL and CentOS Stream arm64 ship 64 KiB pages, and CI is amd64-only,
// so no gate would see it.
//
// The test does not need a big-page host, because the quantity that matters is
// page-size independent. It takes the footprint the REAL implementation prices
// — no second copy of the formula here — converts it to pages using this host's
// page size, and then asserts the budget holds at 4 KiB, 16 KiB and 64 KiB.
func TestMaxShareSetBytesIsAPageBudgetAtEveryPageSize(t *testing.T) {
	page := lockedBytes(1)
	if page <= 0 {
		t.Fatalf("lockedBytes(1) = %d, want this host's page size", page)
	}

	// The exported value must BE the page budget on this host, whatever its
	// page size. This is what stops the constant drifting back into bytes.
	if want := maxShareSetBytesFor(page); MaxShareSetBytes != want {
		t.Fatalf("MaxShareSetBytes = %d, want %d (%d pages x %d bytes)",
			MaxShareSetBytes, want, maxShareSetPages, page)
	}

	// The headline case, priced by the implementation rather than restated.
	const rootKeyBytes = 32
	footprint := splitLockedBytes(rootKeyBytes, 255, 255)
	if footprint%page != 0 {
		t.Fatalf("splitLockedBytes returned %d, which is not a whole number of "+
			"%d-byte pages; the budget's page arithmetic assumes it is",
			footprint, page)
	}
	pages := footprint / page

	for _, ps := range []int{4096, 16384, 65536} {
		budget := maxShareSetBytesFor(ps)
		cost := pages * ps
		if cost > budget {
			t.Errorf("on a %d-byte-page host the 255-share split of a %d-byte root "+
				"key costs %d pages (%d bytes) against a budget of %d pages (%d bytes): "+
				"the primitive's headline case is refused",
				ps, rootKeyBytes, pages, cost, maxShareSetPages, budget)
		}
	}

	// And a fixed byte-valued cap, to show this is a real difference rather
	// than a restatement: such a cap refuses that same set on both larger page
	// sizes.
	const fixedByteCap = 2 << 20
	for _, ps := range []int{16384, 65536} {
		if pages*ps <= fixedByteCap {
			t.Errorf("the 255-share split costs %d bytes at a %d-byte page, which "+
				"a fixed %d-byte cap would admit; this test is not exercising "+
				"the page-size dependence it guards", pages*ps, ps, fixedByteCap)
		}
	}
}
