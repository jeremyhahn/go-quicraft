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

package config

import (
	"context"
	"errors"
	"testing"
)

// validateLimitsBase returns a Config that passes every Validate check
// before the MaxInMemLogSize and compression type checks.
func validateLimitsBase() Config {
	return Config{ShardID: 1, ReplicaID: 1, HeartbeatRTT: 1, ElectionRTT: 10}
}

// assertValidateLimitsField asserts that c.Validate returns a
// *ValidationError naming wantField, or nil when wantField is empty.
func assertValidateLimitsField(ctx context.Context, t *testing.T, c Config, wantField string) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	err := c.Validate()
	if wantField == "" {
		if err != nil {
			t.Fatalf("Validate: got %v, want nil", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("Validate: got nil, want a *ValidationError for %s", wantField)
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("Validate: got %T (%v), want *ValidationError", err, err)
	}
	if ve.Field != wantField {
		t.Fatalf("Field: got %q, want %q", ve.Field, wantField)
	}
}

func TestConfig_ValidateLimits_MaxInMemLogSize(t *testing.T) {
	tests := []struct {
		name      string
		size      uint64
		wantField string
	}{
		{"ZeroDisablesLimit", 0, ""},
		{"One", 1, "MaxInMemLogSize"},
		{"EqualToNonCmdFieldsSize", entryNonCmdFieldsSize, "MaxInMemLogSize"},
		{"Exactly128", 128, "MaxInMemLogSize"},
		{"OneEntryWithOneByteCmd", entryNonCmdFieldsSize + 1, ""},
		{"Exactly129", 129, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := validateLimitsBase()
			c.MaxInMemLogSize = tc.size
			assertValidateLimitsField(context.Background(), t, c, tc.wantField)
		})
	}
}

func TestConfig_ValidateLimits_EntryNonCmdFieldsSizeIs128(t *testing.T) {
	if entryNonCmdFieldsSize != 128 {
		t.Fatalf("entryNonCmdFieldsSize: got %d, want 128", entryNonCmdFieldsSize)
	}
}

func TestConfig_ValidateLimits_UnknownSnapshotCompressionType(t *testing.T) {
	c := validateLimitsBase()
	c.SnapshotCompressionType = CompressionType(99)
	assertValidateLimitsField(context.Background(), t, c, "SnapshotCompressionType")
}

func TestConfig_ValidateLimits_UnknownEntryCompressionType(t *testing.T) {
	for _, snap := range []CompressionType{NoCompression, Snappy} {
		c := validateLimitsBase()
		c.SnapshotCompressionType = snap
		c.EntryCompressionType = CompressionType(99)
		assertValidateLimitsField(context.Background(), t, c, "EntryCompressionType")
	}
}

func TestConfig_ValidateLimits_KnownCompressionTypes(t *testing.T) {
	tests := []struct {
		name  string
		snap  CompressionType
		entry CompressionType
	}{
		{"NoneNone", NoCompression, NoCompression},
		{"SnappyNone", Snappy, NoCompression},
		{"NoneSnappy", NoCompression, Snappy},
		{"SnappySnappy", Snappy, Snappy},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := validateLimitsBase()
			c.SnapshotCompressionType = tc.snap
			c.EntryCompressionType = tc.entry
			assertValidateLimitsField(context.Background(), t, c, "")
		})
	}
}
