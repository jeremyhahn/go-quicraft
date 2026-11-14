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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// fixedResultDiskSM is a DiskStateMachine without NALookup whose Lookup
// answers with a fixed result and error, so BytesLookup takes the Lookup
// fallback path.
type fixedResultDiskSM struct {
	testOnDiskStateMachine
	result interface{}
	err    error
}

// Lookup returns the configured result and error.
func (sm *fixedResultDiskSM) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	return sm.result, sm.err
}

// lookupResultPayload is a struct type the byte-oriented path cannot represent.
type lookupResultPayload struct {
	Value int
}

// newFixedResultCached wraps a fixedResultDiskSM in a CachedDiskStateMachine.
func newFixedResultCached(ctx context.Context, t *testing.T, result interface{}, err error) *CachedDiskStateMachine {
	t.Helper()
	_ = ctx
	inner := &fixedResultDiskSM{result: result, err: err}
	if _, ok := interface{}(inner).(NALookup); ok {
		t.Fatal("fixedResultDiskSM must not implement NALookup")
	}
	cached, cerr := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 16,
		ExtractKV:  simpleExtractKV,
	})
	if cerr != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", cerr)
	}
	return cached
}

func TestUnsupportedLookupResult_NonByteResultNamesType(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		result interface{}
	}{
		{name: "string", result: "a string value"},
		{name: "struct pointer", result: &lookupResultPayload{Value: 7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cached := newFixedResultCached(ctx, t, tc.result, nil)
			val, err := cached.BytesLookup([]byte("k"))
			if val != nil {
				t.Errorf("BytesLookup value = %v, want nil", val)
			}
			want := fmt.Sprintf("%T", tc.result)
			var ue *UnsupportedLookupResultError
			if !errors.As(err, &ue) {
				t.Fatalf("BytesLookup error = %v (%T), want *UnsupportedLookupResultError", err, err)
			}
			if ue.Got != want {
				t.Errorf("Got = %q, want %q", ue.Got, want)
			}
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error text %q does not name type %q", err.Error(), want)
			}
		})
	}
}

func TestUnsupportedLookupResult_NilAndBytesResults(t *testing.T) {
	ctx := context.Background()
	t.Run("nil", func(t *testing.T) {
		cached := newFixedResultCached(ctx, t, nil, nil)
		val, err := cached.BytesLookup([]byte("k"))
		if val != nil || err != nil {
			t.Fatalf("BytesLookup = (%v, %v), want (nil, nil)", val, err)
		}
	})
	t.Run("bytes", func(t *testing.T) {
		want := []byte("byte value")
		cached := newFixedResultCached(ctx, t, want, nil)
		val, err := cached.BytesLookup([]byte("k"))
		if err != nil {
			t.Fatalf("BytesLookup error = %v, want nil", err)
		}
		if string(val) != string(want) {
			t.Errorf("BytesLookup = %q, want %q", val, want)
		}
	})
}

func TestUnsupportedLookupResult_InnerErrorReturnedUnchanged(t *testing.T) {
	ctx := context.Background()
	innerErr := errors.New("inner lookup failed")
	cached := newFixedResultCached(ctx, t, "ignored", innerErr)
	val, err := cached.BytesLookup([]byte("k"))
	if val != nil {
		t.Errorf("BytesLookup value = %v, want nil", val)
	}
	if !errors.Is(err, innerErr) {
		t.Fatalf("BytesLookup error = %v, want %v", err, innerErr)
	}
	var ue *UnsupportedLookupResultError
	if errors.As(err, &ue) {
		t.Errorf("inner error must not be wrapped as UnsupportedLookupResultError")
	}
}
