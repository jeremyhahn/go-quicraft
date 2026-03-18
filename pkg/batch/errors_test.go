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

func TestErrorTypes_AreDistinct(t *testing.T) {
	t.Parallel()

	closed := &ClosedError{}
	queueFull := &QueueFullError{QueueSize: 10}
	nilProposer := &NilProposerError{}
	flush := &FlushError{ShardID: 1, Err: errors.New("e")}
	decode := &DecodeError{Reason: "r"}

	var ce *ClosedError
	var qfe *QueueFullError
	var npe *NilProposerError
	var fe *FlushError
	var de *DecodeError

	if !errors.As(closed, &ce) {
		t.Error("ClosedError should match *ClosedError")
	}
	if !errors.As(queueFull, &qfe) {
		t.Error("QueueFullError should match *QueueFullError")
	}
	if !errors.As(nilProposer, &npe) {
		t.Error("NilProposerError should match *NilProposerError")
	}
	if !errors.As(flush, &fe) {
		t.Error("FlushError should match *FlushError")
	}
	if !errors.As(decode, &de) {
		t.Error("DecodeError should match *DecodeError")
	}
}

func TestSentinelErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	sentinels := []error{ErrClosed, ErrQueueFull, ErrNilProposer, ErrFlush, ErrDecode}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if i != j && errors.Is(a, b) {
				t.Errorf("sentinel %q should not match %q", a, b)
			}
		}
	}
}
