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
	"errors"
	"testing"
)

func TestQuorumError_Error(t *testing.T) {
	err := &QuorumError{Have: 2, Need: 3}
	expected := "bootstrap: insufficient peers for quorum (have 2, need 3)"
	if got := err.Error(); got != expected {
		t.Errorf("QuorumError.Error() = %q, want %q", got, expected)
	}
}

func TestQuorumError_ZeroValues(t *testing.T) {
	err := &QuorumError{Have: 0, Need: 0}
	expected := "bootstrap: insufficient peers for quorum (have 0, need 0)"
	if got := err.Error(); got != expected {
		t.Errorf("QuorumError.Error() = %q, want %q", got, expected)
	}
}

func TestError_Error(t *testing.T) {
	inner := errors.New("connection refused")
	err := &Error{Phase: "discovery", Err: inner}
	expected := "bootstrap: discovery: connection refused"
	if got := err.Error(); got != expected {
		t.Errorf("Error.Error() = %q, want %q", got, expected)
	}
}

func TestError_Unwrap(t *testing.T) {
	inner := errors.New("inner error")
	err := &Error{Phase: "test", Err: inner}
	if unwrapped := err.Unwrap(); unwrapped != inner {
		t.Errorf("Error.Unwrap() = %v, want %v", unwrapped, inner)
	}
}

func TestError_UnwrapWithErrorsIs(t *testing.T) {
	inner := errors.New("specific")
	err := &Error{Phase: "test", Err: inner}
	if !errors.Is(err, inner) {
		t.Error("errors.Is should match the wrapped inner error")
	}
}

func TestErrorTypes_AreDistinct(t *testing.T) {
	qe := &QuorumError{Have: 1, Need: 2}
	be := &Error{Phase: "p", Err: errors.New("e")}

	var q *QuorumError
	var b *Error

	if !errors.As(qe, &q) {
		t.Error("QuorumError should match *QuorumError")
	}
	if !errors.As(be, &b) {
		t.Error("Error should match *Error")
	}
}

func TestError_NestedUnwrap(t *testing.T) {
	root := errors.New("root cause")
	inner := &Error{Phase: "inner", Err: root}
	outer := &Error{Phase: "outer", Err: inner}

	if !errors.Is(outer, root) {
		t.Error("errors.Is should find root cause through nested Errors")
	}
}
