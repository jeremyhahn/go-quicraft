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
)

// Share represents a single piece of a secret split using Shamir's Secret Sharing.
type Share struct {
	Index     int               `json:"index"`
	Threshold int               `json:"threshold"`
	Total     int               `json:"total"`
	Value     string            `json:"value"`
	Digest    [32]byte          `json:"digest"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// MarshalJSON implements json.Marshaler for Share.
func (s *Share) MarshalJSON() ([]byte, error) {
	type Alias Share
	return json.Marshal(&struct{ *Alias }{Alias: (*Alias)(s)})
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

// String returns a string representation of the share.
func (s *Share) String() string {
	truncLen := min(len(s.Value), 16)
	return fmt.Sprintf("Share{Index: %d, Threshold: %d/%d, Value: %s...}",
		s.Index, s.Threshold, s.Total, s.Value[:truncLen])
}

// Validate checks if the share has valid parameters.
func (s *Share) Validate() error {
	if s.Index < 1 {
		return &ValidationError{Field: "index", Reason: fmt.Sprintf("must be >= 1, got %d", s.Index)}
	}
	if s.Threshold < 2 {
		return &ValidationError{Field: "threshold", Reason: fmt.Sprintf("must be >= 2, got %d", s.Threshold)}
	}
	if s.Total < s.Threshold {
		return &ValidationError{Field: "total", Reason: fmt.Sprintf("must be >= threshold %d, got %d", s.Threshold, s.Total)}
	}
	if s.Index > s.Total {
		return &ValidationError{Field: "index", Reason: fmt.Sprintf("must be <= total %d, got %d", s.Total, s.Index)}
	}
	if s.Value == "" {
		return &ValidationError{Field: "value", Reason: "cannot be empty"}
	}
	return nil
}
