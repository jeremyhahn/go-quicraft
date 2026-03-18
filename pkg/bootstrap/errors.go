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

package bootstrap

import (
	"strconv"
)

// QuorumError is returned when quorum cannot be established.
type QuorumError struct {
	Have int
	Need int
}

// Error returns a human-readable description of the quorum failure.
func (e *QuorumError) Error() string {
	return "bootstrap: insufficient peers for quorum (have " +
		strconv.Itoa(e.Have) + ", need " + strconv.Itoa(e.Need) + ")"
}

// Error is returned for general bootstrap failures.
type Error struct {
	Phase string
	Err   error
}

// Error returns a human-readable description of the bootstrap failure.
func (e *Error) Error() string {
	return "bootstrap: " + e.Phase + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *Error) Unwrap() error {
	return e.Err
}

// InvalidAddressError is returned when a peer address fails host:port validation.
type InvalidAddressError struct {
	Address string
	Reason  string
}

// Error returns a human-readable description of the invalid address.
func (e *InvalidAddressError) Error() string {
	return "bootstrap: invalid peer address " + e.Address + ": " + e.Reason
}

// DuplicateAddressError is returned when two different NodeIDs share
// the same address.
type DuplicateAddressError struct {
	Address string
	NodeID1 uint64
	NodeID2 uint64
}

// Error returns a human-readable description of the duplicate address.
func (e *DuplicateAddressError) Error() string {
	return "bootstrap: duplicate address " + e.Address +
		" for NodeIDs " + strconv.FormatUint(e.NodeID1, 10) +
		" and " + strconv.FormatUint(e.NodeID2, 10)
}
