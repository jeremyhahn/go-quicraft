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

package kv

import "fmt"

// KeyTooLongError is returned when a key exceeds MaxKeyLength bytes.
type KeyTooLongError struct {
	Length int
}

// Error implements the error interface.
func (e *KeyTooLongError) Error() string {
	return fmt.Sprintf("kv: key length %d exceeds maximum %d", e.Length, MaxKeyLength)
}

// KeyNotFoundError is returned when a lookup key does not exist.
type KeyNotFoundError struct {
	Key string
}

func (e *KeyNotFoundError) Error() string {
	return "kv: key not found: " + e.Key
}

// InvalidCommandError is returned when a command cannot be decoded.
type InvalidCommandError struct {
	Reason string
}

func (e *InvalidCommandError) Error() string {
	return "kv: invalid command: " + e.Reason
}

// HashWriteError is returned when writing to a hash fails during GetHash.
type HashWriteError struct {
	Err error
}

func (e *HashWriteError) Error() string {
	return "kv: hash write failed: " + e.Err.Error()
}

func (e *HashWriteError) Unwrap() error {
	return e.Err
}
