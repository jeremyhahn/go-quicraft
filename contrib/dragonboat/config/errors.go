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

import "fmt"

// ValidationError is returned when configuration validation fails. It
// identifies the invalid field and provides a human-readable explanation
// of the constraint that was violated.
type ValidationError struct {
	Field   string
	Message string
}

// Error returns a formatted validation error message.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("dragonboat/config: invalid %s: %s", e.Field, e.Message)
}

// FileReadError is returned when a TLS certificate or key file cannot be
// read from disk during NodeHostConfig conversion.
type FileReadError struct {
	Path string
	Err  error
}

// Error returns a formatted file read error message.
func (e *FileReadError) Error() string {
	return fmt.Sprintf("dragonboat/config: failed to read file %q: %s", e.Path, e.Err)
}

// Unwrap returns the underlying error.
func (e *FileReadError) Unwrap() error {
	return e.Err
}
