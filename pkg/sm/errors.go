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
	"fmt"
	"strconv"
)

// UnsupportedLookupResultError is returned by CachedDiskStateMachine.BytesLookup when Lookup answers with a type the byte path cannot represent.
type UnsupportedLookupResultError struct {
	// Got is the concrete type the inner Lookup returned.
	Got string
}

// Error returns a human-readable description of the unusable result type.
func (e *UnsupportedLookupResultError) Error() string {
	return "sm: BytesLookup cannot represent a Lookup result of type " + e.Got +
		"; implement NALookup on the state machine or return []byte"
}

// unsupportedLookupResult builds the error for v.
func unsupportedLookupResult(v interface{}) error {
	return &UnsupportedLookupResultError{Got: fmt.Sprintf("%T", v)}
}

// InvalidCacheConfigError is returned when CacheConfig validation fails.
type InvalidCacheConfigError struct {
	Reason string
}

// Error returns a human-readable description of the invalid configuration.
func (e *InvalidCacheConfigError) Error() string {
	return "sm: invalid cache config: " + e.Reason
}

// CacheMaxEntriesError is returned when MaxEntries is not positive.
type CacheMaxEntriesError struct {
	Value int
}

// Error returns a human-readable description of the invalid MaxEntries.
func (e *CacheMaxEntriesError) Error() string {
	return "sm: cache max entries must be > 0, got " + strconv.Itoa(e.Value)
}
