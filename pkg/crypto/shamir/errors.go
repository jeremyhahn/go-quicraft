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

import "fmt"

// SplitError is returned when splitting a secret fails.
type SplitError struct {
	Reason string
}

// Error implements the error interface.
func (e *SplitError) Error() string {
	return fmt.Sprintf("quicraft/shamir: split failed: %s", e.Reason)
}

// CombineError is returned when combining shares fails.
type CombineError struct {
	Reason string
}

// Error implements the error interface.
func (e *CombineError) Error() string {
	return fmt.Sprintf("quicraft/shamir: combine failed: %s", e.Reason)
}

// ProtectedMemoryError reports a refused mlock (RLIMIT_MEMLOCK); unlike a ValidationError, the caller's value has already been wiped.
type ProtectedMemoryError struct {
	// Op names the allocation that was refused, for diagnosis.
	Op string
	// Err is the underlying refusal from the locked-memory allocator.
	Err error
}

// Error implements the error interface.
func (e *ProtectedMemoryError) Error() string {
	return fmt.Sprintf("quicraft/shamir: %s: locked memory unavailable: %v", e.Op, e.Err)
}

// Unwrap exposes the allocator's own error, so errors.Is reaches go-enclave's
// sentinels.
func (e *ProtectedMemoryError) Unwrap() error { return e.Err }

// ValidationError is returned when share validation fails.
type ValidationError struct {
	Field  string
	Reason string
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("quicraft/shamir: validation failed for %s: %s", e.Field, e.Reason)
}
