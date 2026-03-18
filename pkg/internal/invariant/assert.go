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

// Package invariant provides assertion helpers for programmer errors.
// These are intended for impossible states and violated invariants,
// never for runtime error handling.
package invariant

// Assert panics with a descriptive message if the condition is false.
// Used ONLY for programmer errors / impossible states, never for runtime errors.
//
// Example:
//
//	invariant.Assert(index >= 0, "index must be non-negative")
func Assert(condition bool, msg string) {
	if !condition {
		panic("invariant violated: " + msg)
	}
}
