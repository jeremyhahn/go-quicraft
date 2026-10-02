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
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// TestShare_JSONLoggingIsRedacted is the regression for the share-material leak
// through structured logging. Share.String redacted the value, but slog does
// not call String when a json.Marshaler is available: a *Share logged through
// slog.JSONHandler was marshalled by Share.MarshalJSON and its full base64
// share value written to the log. A single leaked share is a permanent,
// unrevocable disclosure of root-key custody material.
//
// The fix is slog.LogValuer, which slog resolves BEFORE consulting any
// marshaller — so the redaction holds on every handler rather than only on the
// one that happens to use fmt.
func TestShare_JSONLoggingIsRedacted(t *testing.T) {
	secret := []byte("root-key-material-32-bytes-long!")
	shares, err := Split(secret, 2, 3)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	share := shares[0]
	if share.Value == "" {
		t.Fatal("share has no value to leak")
	}

	for _, tc := range []struct {
		name    string
		handler func(*bytes.Buffer) slog.Handler
	}{
		{"json", func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) }},
		{"text", func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			slog.New(tc.handler(&buf)).Info("custody event", "share", share)
			out := buf.String()
			if strings.Contains(out, share.Value) {
				t.Fatalf("%s handler logged the raw share value: %s", tc.name, out)
			}
			if !strings.Contains(out, "redacted") {
				t.Fatalf("%s handler did not log the redacted form: %s", tc.name, out)
			}
		})
	}

	// Also cover the value used as an attr value inside a group, which takes a
	// different path through the handler.
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("custody event",
		slog.Group("ceremony", slog.Any("share", share)))
	if strings.Contains(buf.String(), share.Value) {
		t.Fatalf("grouped attr logged the raw share value: %s", buf.String())
	}

	// SERIALIZATION is deliberately NOT redacted: a custody layer that persists
	// or distributes shares as JSON must get the real value back, and conflating
	// the two would silently destroy custody material instead of protecting it.
	encoded, err := json.Marshal(share)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var round Share
	if err := json.Unmarshal(encoded, &round); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if round.Value != share.Value {
		t.Fatal("json.Marshal must round-trip the share value for persistence")
	}
}

// TestCombine_BoundsPayloadAndShareCount pins the memory/CPU bound on the
// combine path. Shares arriving at a recovery ceremony cannot be authenticated
// before they are combined, so an unauthenticated submission could otherwise
// force a 255 x 1 MB reconstruction (~340 MB of input, ~0.75x of it copied,
// seconds of CPU) from a caller with no credentials at all.
func TestCombine_BoundsPayloadAndShareCount(t *testing.T) {
	if _, err := SplitBytes(make([]byte, MaxSecretBytes+1), 2, 3); err == nil {
		t.Fatal("SplitBytes accepted a secret above MaxSecretBytes")
	}
	// The bound is not so tight that a legitimate key is refused.
	if _, err := SplitBytes(make([]byte, 32), 2, 3); err != nil {
		t.Fatalf("SplitBytes(32-byte key): %v", err)
	}

	// A hand-built oversized share must be rejected at parse time, before the
	// rest of the set is decoded.
	big := make([]byte, rawShareOverhead+MaxSecretBytes+1)
	big[0] = shareFormatVersion
	big[1] = 1
	if _, _, err := parseRawShare(big, 0, map[byte]struct{}{}, -1); err == nil {
		t.Fatal("parseRawShare accepted a payload above MaxSecretBytes")
	}

	// More shares than GF(2^8) has non-zero x-coordinates is rejected up front,
	// on both reconstruction paths.
	tooMany := make([]*Share, maxShares+1)
	for i := range tooMany {
		tooMany[i] = &Share{Index: 1, Threshold: 2, Total: 3, Value: "AQE="}
	}
	if _, err := Combine(tooMany); err == nil {
		t.Fatal("Combine accepted more than maxShares shares")
	}
	tooManyValues := make([][]byte, maxShares+1)
	for i := range tooManyValues {
		tooManyValues[i] = []byte("AQE=")
	}
	if _, err := CombineBytes(tooManyValues, 2); err == nil {
		t.Fatal("CombineBytes accepted more than maxShares shares")
	}
}
