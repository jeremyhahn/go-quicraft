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

package batch

import (
	"errors"
	"testing"
)

func TestClosedError_Error(t *testing.T) {
	t.Parallel()
	err := &ClosedError{}
	want := "quicraft/batch: aggregator closed"
	if got := err.Error(); got != want {
		t.Errorf("ClosedError.Error() = %q, want %q", got, want)
	}
}

func TestClosedError_Is(t *testing.T) {
	t.Parallel()
	err := &ClosedError{}

	if !errors.Is(err, ErrClosed) {
		t.Error("ClosedError should match ErrClosed sentinel via errors.Is")
	}
	if errors.Is(err, ErrQueueFull) {
		t.Error("ClosedError should not match ErrQueueFull")
	}
}

func TestClosedError_As(t *testing.T) {
	t.Parallel()
	var target *ClosedError
	err := &ClosedError{}

	if !errors.As(err, &target) {
		t.Error("errors.As should match *ClosedError")
	}
}

func TestClosedError_AsNonMatching(t *testing.T) {
	t.Parallel()
	var target *QueueFullError
	err := &ClosedError{}

	if errors.As(err, &target) {
		t.Error("errors.As should not match *QueueFullError for a ClosedError")
	}
}

func TestQueueFullError_Error(t *testing.T) {
	t.Parallel()
	err := &QueueFullError{QueueSize: 5000}
	want := "quicraft/batch: queue full (capacity 5000)"
	if got := err.Error(); got != want {
		t.Errorf("QueueFullError.Error() = %q, want %q", got, want)
	}
}

func TestQueueFullError_ErrorZeroCapacity(t *testing.T) {
	t.Parallel()
	err := &QueueFullError{QueueSize: 0}
	want := "quicraft/batch: queue full (capacity 0)"
	if got := err.Error(); got != want {
		t.Errorf("QueueFullError.Error() = %q, want %q", got, want)
	}
}

func TestQueueFullError_Is(t *testing.T) {
	t.Parallel()
	err := &QueueFullError{QueueSize: 100}

	if !errors.Is(err, ErrQueueFull) {
		t.Error("QueueFullError should match ErrQueueFull sentinel via errors.Is")
	}
	if errors.Is(err, ErrClosed) {
		t.Error("QueueFullError should not match ErrClosed")
	}
}

func TestQueueFullError_As(t *testing.T) {
	t.Parallel()
	var target *QueueFullError
	err := &QueueFullError{QueueSize: 256}

	if !errors.As(err, &target) {
		t.Fatal("errors.As should match *QueueFullError")
	}
	if target.QueueSize != 256 {
		t.Errorf("QueueSize = %d, want 256", target.QueueSize)
	}
}

func TestNilProposerError_Error(t *testing.T) {
	t.Parallel()
	err := &NilProposerError{}
	want := "quicraft/batch: proposer is nil"
	if got := err.Error(); got != want {
		t.Errorf("NilProposerError.Error() = %q, want %q", got, want)
	}
}

func TestNilProposerError_Is(t *testing.T) {
	t.Parallel()
	err := &NilProposerError{}

	if !errors.Is(err, ErrNilProposer) {
		t.Error("NilProposerError should match ErrNilProposer sentinel via errors.Is")
	}
	if errors.Is(err, ErrFlush) {
		t.Error("NilProposerError should not match ErrFlush")
	}
}

func TestNilProposerError_As(t *testing.T) {
	t.Parallel()
	var target *NilProposerError
	err := &NilProposerError{}

	if !errors.As(err, &target) {
		t.Error("errors.As should match *NilProposerError")
	}
}

func TestFlushError_Error(t *testing.T) {
	t.Parallel()
	inner := errors.New("proposer unavailable")
	err := &FlushError{ShardID: 42, Err: inner}
	want := "quicraft/batch: flush failed for shard 42: proposer unavailable"
	if got := err.Error(); got != want {
		t.Errorf("FlushError.Error() = %q, want %q", got, want)
	}
}

func TestFlushError_ErrorZeroShard(t *testing.T) {
	t.Parallel()
	inner := errors.New("timeout")
	err := &FlushError{ShardID: 0, Err: inner}
	want := "quicraft/batch: flush failed for shard 0: timeout"
	if got := err.Error(); got != want {
		t.Errorf("FlushError.Error() = %q, want %q", got, want)
	}
}

func TestFlushError_Is(t *testing.T) {
	t.Parallel()
	inner := errors.New("some error")
	err := &FlushError{ShardID: 1, Err: inner}

	if !errors.Is(err, ErrFlush) {
		t.Error("FlushError should match ErrFlush sentinel via errors.Is")
	}
	if errors.Is(err, ErrDecode) {
		t.Error("FlushError should not match ErrDecode")
	}
}

func TestFlushError_Unwrap(t *testing.T) {
	t.Parallel()
	inner := errors.New("wrapped cause")
	err := &FlushError{ShardID: 7, Err: inner}

	if unwrapped := err.Unwrap(); unwrapped != inner {
		t.Errorf("Unwrap() = %v, want %v", unwrapped, inner)
	}
}

func TestFlushError_UnwrapWithErrorsIs(t *testing.T) {
	t.Parallel()
	inner := errors.New("specific cause")
	err := &FlushError{ShardID: 3, Err: inner}

	if !errors.Is(err, inner) {
		t.Error("errors.Is should find the wrapped inner error through Unwrap")
	}
}

func TestFlushError_As(t *testing.T) {
	t.Parallel()
	var target *FlushError
	inner := errors.New("test")
	err := &FlushError{ShardID: 99, Err: inner}

	if !errors.As(err, &target) {
		t.Fatal("errors.As should match *FlushError")
	}
	if target.ShardID != 99 {
		t.Errorf("ShardID = %d, want 99", target.ShardID)
	}
}

func TestFlushError_UnwrapNilErr(t *testing.T) {
	t.Parallel()
	err := &FlushError{ShardID: 1, Err: nil}

	if unwrapped := err.Unwrap(); unwrapped != nil {
		t.Errorf("Unwrap() = %v, want nil", unwrapped)
	}
}

func TestDecodeError_Error(t *testing.T) {
	t.Parallel()
	err := &DecodeError{Reason: "truncated length prefix"}
	want := "quicraft/batch: decode failed: truncated length prefix"
	if got := err.Error(); got != want {
		t.Errorf("DecodeError.Error() = %q, want %q", got, want)
	}
}

func TestDecodeError_ErrorEmptyReason(t *testing.T) {
	t.Parallel()
	err := &DecodeError{Reason: ""}
	want := "quicraft/batch: decode failed: "
	if got := err.Error(); got != want {
		t.Errorf("DecodeError.Error() = %q, want %q", got, want)
	}
}

func TestDecodeError_Is(t *testing.T) {
	t.Parallel()
	err := &DecodeError{Reason: "bad data"}

	if !errors.Is(err, ErrDecode) {
		t.Error("DecodeError should match ErrDecode sentinel via errors.Is")
	}
	if errors.Is(err, ErrFlush) {
		t.Error("DecodeError should not match ErrFlush")
	}
}

func TestDecodeError_As(t *testing.T) {
	t.Parallel()
	var target *DecodeError
	err := &DecodeError{Reason: "command data exceeds payload boundary"}

	if !errors.As(err, &target) {
		t.Fatal("errors.As should match *DecodeError")
	}
	if target.Reason != "command data exceeds payload boundary" {
		t.Errorf("Reason = %q, want %q", target.Reason, "command data exceeds payload boundary")
	}
}

// TestErrorTypes_AreDistinct pins that each concrete error type is
// distinguishable from every OTHER one, which is what a caller switching on
// errors.As depends on.
//
// Its previous shape asserted only errors.As(X, &targetOfXsOwnType) for each
// type in turn — five tautologies that no implementation of Is or Unwrap could
// falsify. Nothing in it tested distinctness, which is the name of the test and
// the only interesting property: a type that started matching a sibling (an
// over-broad Is, or an Unwrap chain that reached one) would have passed.
//
// The full cross-product is checked here, both for errors.As (type identity)
// and errors.Is against each type's own sentinel. ErrEncode/EncodeError is
// included; it was absent from every distinctness check in this file.
func TestErrorTypes_AreDistinct(t *testing.T) {
	t.Parallel()

	// asOf reports whether err matches the concrete type this entry names.
	// Each closure allocates its own target so a match cannot leak between
	// table rows.
	cases := []struct {
		name     string
		err      error
		sentinel error
		matches  func(error) bool
	}{
		{"ClosedError", &ClosedError{}, ErrClosed,
			func(e error) bool { var t *ClosedError; return errors.As(e, &t) }},
		{"QueueFullError", &QueueFullError{QueueSize: 10}, ErrQueueFull,
			func(e error) bool { var t *QueueFullError; return errors.As(e, &t) }},
		{"NilProposerError", &NilProposerError{}, ErrNilProposer,
			func(e error) bool { var t *NilProposerError; return errors.As(e, &t) }},
		{"FlushError", &FlushError{ShardID: 1, Err: errors.New("e")}, ErrFlush,
			func(e error) bool { var t *FlushError; return errors.As(e, &t) }},
		{"DecodeError", &DecodeError{Reason: "r"}, ErrDecode,
			func(e error) bool { var t *DecodeError; return errors.As(e, &t) }},
		{"EncodeError", &EncodeError{Reason: "r"}, ErrEncode,
			func(e error) bool { var t *EncodeError; return errors.As(e, &t) }},
	}

	for i, subject := range cases {
		for j, probe := range cases {
			gotAs := probe.matches(subject.err)
			if wantAs := i == j; gotAs != wantAs {
				t.Errorf("errors.As(%s, *%s) = %v, want %v",
					subject.name, probe.name, gotAs, wantAs)
			}
			gotIs := errors.Is(subject.err, probe.sentinel)
			if wantIs := i == j; gotIs != wantIs {
				t.Errorf("errors.Is(%s, %v) = %v, want %v",
					subject.name, probe.sentinel, gotIs, wantIs)
			}
		}
	}
}

func TestSentinelErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	// ErrEncode is included: it was the one sentinel this check never covered,
	// so nothing prevented it from being defined as an alias of another.
	sentinels := []error{ErrClosed, ErrQueueFull, ErrNilProposer, ErrFlush, ErrDecode, ErrEncode}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if i != j && errors.Is(a, b) {
				t.Errorf("sentinel %q should not match %q", a, b)
			}
		}
	}
}

// TestFlushError_ErrorIndeterminate covers FlushError.Error's indeterminate
// branch, which no test reached. The wording is the caller's only signal that a
// retry may apply the whole batch a second time — the aggregator has no
// at-most-once path — so it must not read like the definite-failure message.
func TestFlushError_ErrorIndeterminate(t *testing.T) {
	t.Parallel()
	inner := errors.New("context deadline exceeded")
	err := &FlushError{ShardID: 5, Err: inner, Indeterminate: true}

	want := "quicraft/batch: flush outcome indeterminate for shard 5: context deadline exceeded"
	if got := err.Error(); got != want {
		t.Errorf("FlushError.Error() = %q, want %q", got, want)
	}

	definite := &FlushError{ShardID: 5, Err: inner}
	if definite.Error() == err.Error() {
		t.Error("an indeterminate flush must not report the same message as a definite failure")
	}
}

func TestEncodeError_Error(t *testing.T) {
	t.Parallel()
	err := &EncodeError{Reason: "command length exceeds MaxInt32"}
	want := "quicraft/batch: encode failed: command length exceeds MaxInt32"
	if got := err.Error(); got != want {
		t.Errorf("EncodeError.Error() = %q, want %q", got, want)
	}
}

func TestEncodeError_ErrorEmptyReason(t *testing.T) {
	t.Parallel()
	err := &EncodeError{Reason: ""}
	want := "quicraft/batch: encode failed: "
	if got := err.Error(); got != want {
		t.Errorf("EncodeError.Error() = %q, want %q", got, want)
	}
}

func TestEncodeError_Is(t *testing.T) {
	t.Parallel()
	err := &EncodeError{Reason: "bad command"}

	if !errors.Is(err, ErrEncode) {
		t.Error("EncodeError should match ErrEncode sentinel via errors.Is")
	}
	if errors.Is(err, ErrDecode) {
		t.Error("EncodeError should not match ErrDecode")
	}
}

func TestEncodeError_As(t *testing.T) {
	t.Parallel()
	var target *EncodeError
	err := &EncodeError{Reason: "command length exceeds MaxInt32"}

	if !errors.As(err, &target) {
		t.Fatal("errors.As should match *EncodeError")
	}
	if target.Reason != "command length exceeds MaxInt32" {
		t.Errorf("Reason = %q, want %q", target.Reason, "command length exceeds MaxInt32")
	}
}
