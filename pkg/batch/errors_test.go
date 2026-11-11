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

func TestErrors_Error(t *testing.T) {
	t.Parallel()
	deadline := errors.New("context deadline exceeded")
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"ClosedError", &ClosedError{}, "quicraft/batch: aggregator closed"},
		{"QueueFullError", &QueueFullError{QueueSize: 5000},
			"quicraft/batch: queue full (capacity 5000)"},
		{"NilProposerError", &NilProposerError{}, "quicraft/batch: proposer is nil"},
		{"FlushError", &FlushError{ShardID: 42, Err: errors.New("proposer unavailable")},
			"quicraft/batch: flush failed for shard 42: proposer unavailable"},
		// An indeterminate flush must not read like a definite failure.
		{"FlushError indeterminate", &FlushError{ShardID: 5, Err: deadline, Indeterminate: true},
			"quicraft/batch: flush outcome indeterminate for shard 5: context deadline exceeded"},
		{"DecodeError", &DecodeError{Reason: "truncated length prefix"},
			"quicraft/batch: decode failed: truncated length prefix"},
		{"EncodeError", &EncodeError{Reason: "command length exceeds MaxInt32"},
			"quicraft/batch: encode failed: command length exceeds MaxInt32"},
	}
	for _, tc := range cases {
		if got := tc.err.Error(); got != tc.want {
			t.Errorf("%s.Error() = %q, want %q", tc.name, got, tc.want)
		}
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
// included; it was absent from every distinctness check in this file. Two
// sentinels defined as the same value would also fail the errors.Is half.
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

// TestFlushError_ErrorIndeterminate covers FlushError.Error's indeterminate
// branch, which no test reached. The wording is the caller's only signal that a
// retry may apply the whole batch a second time — the aggregator has no
// at-most-once path — so it must not read like the definite-failure message.
