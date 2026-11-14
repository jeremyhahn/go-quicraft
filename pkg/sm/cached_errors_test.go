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

package sm

import (
	"errors"
	"math"
	"testing"
)

// TestNewCachedDiskStateMachine_TypedErrors pins the typed errors documented
// in docs/state-machines/api.md for NewCachedDiskStateMachine: a
// *CacheMaxEntriesError carrying the rejected value when MaxEntries is not
// positive, an *InvalidCacheConfigError with its Reason when ExtractKV is nil,
// and the MaxEntries check taking precedence when both fields are bad.
func TestNewCachedDiskStateMachine_TypedErrors(t *testing.T) {
	tests := []struct {
		name       string
		cfg        CacheConfig
		wantMax    bool
		wantValue  int
		wantReason string
	}{
		{name: "max entries zero", cfg: CacheConfig{MaxEntries: 0, ExtractKV: simpleExtractKV}, wantMax: true, wantValue: 0},
		{name: "max entries negative", cfg: CacheConfig{MaxEntries: -1, ExtractKV: simpleExtractKV}, wantMax: true, wantValue: -1},
		{name: "max entries min int", cfg: CacheConfig{MaxEntries: math.MinInt, ExtractKV: simpleExtractKV}, wantMax: true, wantValue: math.MinInt},
		{name: "extract kv nil", cfg: CacheConfig{MaxEntries: 1}, wantReason: "ExtractKV must not be nil"},
		{name: "both bad max entries wins", cfg: CacheConfig{MaxEntries: 0}, wantMax: true, wantValue: 0},
		{name: "valid", cfg: CacheConfig{MaxEntries: 1, ExtractKV: simpleExtractKV}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cached, err := NewCachedDiskStateMachine(newTrackingDiskStateMachine(), tt.cfg)
			switch {
			case tt.wantMax:
				var maxErr *CacheMaxEntriesError
				if !errors.As(err, &maxErr) {
					t.Fatalf("err = %T(%v), want *CacheMaxEntriesError", err, err)
				}
				if maxErr.Value != tt.wantValue {
					t.Errorf("Value = %d, want %d", maxErr.Value, tt.wantValue)
				}
				var cfgErr *InvalidCacheConfigError
				if errors.As(err, &cfgErr) {
					t.Errorf("err also matches *InvalidCacheConfigError: %v", err)
				}
			case tt.wantReason != "":
				var cfgErr *InvalidCacheConfigError
				if !errors.As(err, &cfgErr) {
					t.Fatalf("err = %T(%v), want *InvalidCacheConfigError", err, err)
				}
				if cfgErr.Reason != tt.wantReason {
					t.Errorf("Reason = %q, want %q", cfgErr.Reason, tt.wantReason)
				}
			default:
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				if cached == nil {
					t.Fatal("cached = nil, want a CachedDiskStateMachine")
				}
				return
			}
			if cached != nil {
				t.Errorf("cached = %v, want nil on error", cached)
			}
		})
	}
}

// TestCachedDiskStateMachine_GetHashNonHasherTypedError pins that GetHash on
// an inner store that does not implement Hasher returns hash 0 and an
// *InvalidCacheConfigError, as docs/state-machines/api.md documents.
func TestCachedDiskStateMachine_GetHashNonHasherTypedError(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	if _, ok := any(inner).(Hasher); ok {
		t.Fatal("test precondition: trackingDiskStateMachine must not implement Hasher")
	}
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 1,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine: %v", err)
	}
	hash, err := cached.GetHash()
	var cfgErr *InvalidCacheConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("err = %T(%v), want *InvalidCacheConfigError", err, err)
	}
	if cfgErr.Reason == "" {
		t.Error("Reason is empty")
	}
	if hash != 0 {
		t.Errorf("hash = %d, want 0", hash)
	}
}
