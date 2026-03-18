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

// Package batch provides a write-batching aggregator that groups multiple
// Raft proposals into a single flushed batch for improved throughput.
package batch

import (
	"errors"
	"fmt"
)

// Sentinel errors are pre-allocated for zero-alloc hot path returns and
// use with errors.Is(). Each error uses the "quicraft/batch:" prefix for
// clear provenance in error chains.
var (
	// ErrClosed is returned when Submit is called on a closed aggregator.
	ErrClosed = errors.New("quicraft/batch: aggregator closed")

	// ErrQueueFull is returned when the proposal queue has reached its
	// configured capacity and cannot accept new submissions.
	ErrQueueFull = errors.New("quicraft/batch: queue full")

	// ErrNilProposer is returned when NewAggregator is called with a
	// nil Proposer.
	ErrNilProposer = errors.New("quicraft/batch: proposer is nil")

	// ErrFlush is returned when a flush operation fails.
	ErrFlush = errors.New("quicraft/batch: flush failed")

	// ErrDecode is returned when batch decoding encounters malformed data.
	ErrDecode = errors.New("quicraft/batch: decode failed")
)

// ClosedError is returned when an operation is attempted on a closed
// aggregator. It carries no additional context beyond the sentinel.
type ClosedError struct{}

// Error returns a human-readable description of the closed condition.
func (e *ClosedError) Error() string {
	return ErrClosed.Error()
}

// Is reports whether target matches the sentinel ErrClosed error.
func (e *ClosedError) Is(target error) bool {
	return target == ErrClosed
}

// QueueFullError is returned when the proposal queue has reached capacity.
// It carries the current queue size for diagnostics.
type QueueFullError struct {
	QueueSize int
}

// Error returns a human-readable description of the queue-full condition.
func (e *QueueFullError) Error() string {
	return fmt.Sprintf("quicraft/batch: queue full (capacity %d)", e.QueueSize)
}

// Is reports whether target matches the sentinel ErrQueueFull error.
func (e *QueueFullError) Is(target error) bool {
	return target == ErrQueueFull
}

// NilProposerError is returned when a nil Proposer is passed to
// NewAggregator. The aggregator cannot function without a valid proposer.
type NilProposerError struct{}

// Error returns a human-readable description of the nil proposer condition.
func (e *NilProposerError) Error() string {
	return ErrNilProposer.Error()
}

// Is reports whether target matches the sentinel ErrNilProposer error.
func (e *NilProposerError) Is(target error) bool {
	return target == ErrNilProposer
}

// FlushError wraps a flush failure with shard context for diagnostics.
// The ShardID identifies which shard's batch failed to flush, and the
// underlying Err contains the proposer's error.
type FlushError struct {
	ShardID uint64
	Err     error
}

// Error returns a human-readable description of the flush failure.
func (e *FlushError) Error() string {
	return fmt.Sprintf("quicraft/batch: flush failed for shard %d: %v", e.ShardID, e.Err)
}

// Is reports whether target matches the sentinel ErrFlush error.
func (e *FlushError) Is(target error) bool {
	return target == ErrFlush
}

// Unwrap returns the underlying error.
func (e *FlushError) Unwrap() error {
	return e.Err
}

// DecodeError is returned when batch decoding encounters malformed data.
// It carries the specific reason for the decode failure.
type DecodeError struct {
	Reason string
}

// Error returns a human-readable description of the decode failure.
func (e *DecodeError) Error() string {
	return fmt.Sprintf("quicraft/batch: decode failed: %s", e.Reason)
}

// Is reports whether target matches the sentinel ErrDecode error.
func (e *DecodeError) Is(target error) bool {
	return target == ErrDecode
}
