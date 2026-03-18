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

import "strconv"

// InvalidCacheConfigError is returned when CacheConfig validation fails.
type InvalidCacheConfigError struct {
	Reason string
}

func (e *InvalidCacheConfigError) Error() string {
	return "sm: invalid cache config: " + e.Reason
}

// CacheMaxEntriesError is returned when MaxEntries is not positive.
type CacheMaxEntriesError struct {
	Value int
}

func (e *CacheMaxEntriesError) Error() string {
	return "sm: cache max entries must be > 0, got " + strconv.Itoa(e.Value)
}
