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

// Package shamir implements Shamir's Secret Sharing scheme for splitting secrets
// into N shares where any M shares can reconstruct the original secret.
package shamir

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
)

// Share represents a single piece of a secret split using Shamir's Secret Sharing.
//
// NOTE: shares carry NO digest of the secret. An earlier design embedded
// sha256(secret) in every share for a self-check, but that is an offline
// brute-force ORACLE — a single shareholder (or anyone who sees one persisted
// share) can confirm guessed secrets without ever reaching the threshold,
// defeating Shamir's information-theoretic secrecy for low-entropy secrets.
// Integrity is therefore the caller's responsibility and MUST be established
// out-of-band — e.g. by using the reconstructed secret in an authenticated
// decrypt that fails on a wrong key (see Combine / CombineUnverified).
type Share struct {
	// Index is the share's 1-based position, equal to the x-coordinate
	// embedded in Value.
	Index int `json:"index"`
	// Threshold is the M of the M-of-N scheme. It is UNAUTHENTICATED
	// caller-supplied metadata, not a property Combine can verify: Combine
	// takes the threshold from shares[0].Threshold, so relabelling two
	// shares of a real 5-of-9 split as Threshold:2 makes Combine return
	// garbage rather than an error. Callers must obtain the true threshold
	// from a trusted source and authenticate the reconstructed secret
	// out-of-band.
	Threshold int `json:"threshold"`
	// Total is the N of the M-of-N scheme. Unauthenticated, like Threshold.
	Total int `json:"total"`
	// Value is the base64 encoding of [version:1][x:1][y...].
	//
	// Deprecated: a Go string is immortal — it cannot be zeroed, so every
	// share stays readable in the heap until the GC reclaims it, and a
	// ceremony holding all N shares is the point of widest exposure in the
	// whole scheme. Use ProtectedShare, whose value lives in an mlock'd,
	// guard-paged enclave.Memory that Destroy genuinely wipes;
	// SplitProtected and CombineProtected are the Split/Combine equivalents,
	// and Share.Protect moves an existing Share onto that path. The field
	// itself remains: this type is still the JSON shape shares are persisted
	// and distributed in, and removing it would break every consumer at once.
	Value string `json:"value"`
	// Metadata is caller-defined and carries no cryptographic meaning.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// MarshalJSON implements json.Marshaler for Share.
//
// It is NOT redacted, and must not be: this is the SERIALIZATION path, and a
// custody layer that persists or distributes shares as JSON needs the real
// value back. Redaction belongs to the paths that exist to be read by humans —
// String, and LogValue below, which is what a structured logger resolves first.
func (s *Share) MarshalJSON() ([]byte, error) {
	type Alias Share
	return json.Marshal(&struct{ *Alias }{Alias: (*Alias)(s)})
}

// LogValue implements slog.LogValuer so that a Share reaching a structured
// logger is redacted whatever the handler is.
//
// String alone was not enough. slog resolves a LogValuer FIRST, but with none
// present slog.JSONHandler encodes an arbitrary value through its
// json.Marshaler — so logging a *Share through a JSON handler wrote the full
// share material and never called String at all. Shares are root-key custody
// material and a single leaked share is a permanent, unrevocable disclosure, so
// the redaction has to hold on every handler rather than on the one that
// happens to use fmt.
func (s *Share) LogValue() slog.Value {
	if s == nil {
		return slog.StringValue("Share(nil)")
	}
	return slog.StringValue(s.String())
}

// UnmarshalJSON implements json.Unmarshaler for Share.
func (s *Share) UnmarshalJSON(data []byte) error {
	type Alias Share
	aux := &struct{ *Alias }{Alias: (*Alias)(s)}
	return json.Unmarshal(data, aux)
}

// Bytes returns the raw share value as bytes.
func (s *Share) Bytes() ([]byte, error) {
	return base64.StdEncoding.DecodeString(s.Value)
}

// String returns a string representation of the share. The share value
// is NEVER included: shares are root-key custody material, and String is
// exactly what reaches logs and %v/%s formatting, so even a prefix would
// leak share bytes.
func (s *Share) String() string {
	return fmt.Sprintf("Share{Index: %d, Threshold: %d/%d, Value: [redacted]}",
		s.Index, s.Threshold, s.Total)
}

// Validate checks if the share has valid parameters. It is safe to call on a
// nil receiver: a nil share is a validation failure, not a panic. Callers
// deserialize shares from operator input (a JSON null in a submitted share list
// decodes to a nil *Share), and that must not take down the process.
func (s *Share) Validate() error {
	if s == nil {
		return &ValidationError{Field: "share", Reason: "share is nil"}
	}
	if err := validateShareHeader(s.Index, s.Threshold, s.Total); err != nil {
		return err
	}
	if s.Value == "" {
		return &ValidationError{Field: "value", Reason: "cannot be empty"}
	}
	return nil
}
