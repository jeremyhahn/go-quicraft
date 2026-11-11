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

package transport

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Typed error contracts: message, sentinel and Unwrap
// ---------------------------------------------------------------------------

// TestTypedErrorContracts pins, per typed error, the text Error() carries,
// the sentinel errors.Is reports (and the unrelated ones it must not), and
// the cause Unwrap exposes.
func TestTypedErrorContracts(t *testing.T) {
	asn1Err := errors.New("asn1: structure error")
	x509Err := errors.New("x509: malformed certificate")
	ioTimeout := errors.New("i/o timeout")
	refused := errors.New("connection refused")
	corrupt := errors.New("snappy: corrupt input")

	tests := []struct {
		name     string
		err      error
		exact    string   // full Error() text, when pinned
		wants    []string // substrings Error() must carry
		notWants []string // substrings Error() must not carry
		is       error    // sentinel errors.Is must report
		notIs    []error  // sentinels errors.Is must not report
		unwrap   error    // cause Unwrap must return
	}{
		{
			name:  "CA parse error with cause",
			err:   &CACertificateParseError{Reason: "x509.ParseCertificate failed", Err: asn1Err},
			exact: "transport: CA certificate parse failed: x509.ParseCertificate failed: asn1: structure error",
		},
		{
			// Must not carry a trailing ": <nil>" artifact.
			name:  "CA parse error without cause",
			err:   &CACertificateParseError{Reason: "PEM decode returned nil block"},
			exact: "transport: CA certificate parse failed: PEM decode returned nil block",
		},
		{
			name:   "CA parse error unwraps",
			err:    &CACertificateParseError{Reason: "x509.ParseCertificate failed", Err: x509Err},
			notIs:  []error{ErrStreamClosed, ErrSnapshotChunkWriteTimeout, ErrInvalidMagic},
			unwrap: x509Err,
		},
		{
			name:  "invalid shard ID",
			err:   &InvalidShardIDError{From: 5, MsgType: 12},
			wants: []string{"ShardID=0", "replica 5", "type 12"},
			is:    ErrInvalidShardID,
			notIs: []error{ErrStreamClosed},
		},
		{
			name:  "chunk filepath traversal",
			err:   &ChunkFilepathTraversalError{ShardID: 7, Filepath: "../etc/passwd"},
			wants: []string{"path traversal", "shard 7"},
			is:    ErrChunkFilepathTraversal,
			notIs: []error{ErrChunkFilepathTooLong},
		},
		{
			name:  "type assertion",
			err:   &TypeAssertionError{Context: "Start UDP listener", ExpectedType: "*net.UDPConn", ActualValue: "not-a-conn"},
			wants: []string{"type assertion failed", "Start UDP listener", "*net.UDPConn", "string"},
		},
		{
			name:  "snapshot memory budget exceeded",
			err:   &SnapshotMemoryBudgetExceededError{Accumulated: 1024 * 1024, Budget: 512 * 1024},
			wants: []string{"memory budget exceeded", "1048576", "524288"},
			is:    ErrSnapshotMemoryBudgetExceeded,
			notIs: []error{ErrStreamClosed},
		},
		{
			name:  "worker panic with target",
			err:   &WorkerPanicError{WorkerName: "sendQueueWorker", Target: "127.0.0.1:9999", Value: "index out of range", Stack: []byte("goroutine 1...")},
			wants: []string{"sendQueueWorker", "127.0.0.1:9999", "index out of range"},
		},
		{
			// Global workers like connectionCleanup have no target, so no brackets.
			name:     "worker panic without target",
			err:      &WorkerPanicError{WorkerName: "connectionCleanup", Value: "nil pointer dereference", Stack: []byte("goroutine 1...")},
			wants:    []string{"connectionCleanup", "nil pointer dereference"},
			notWants: []string{"["},
		},
		{
			name:   "snapshot chunk write timeout",
			err:    &SnapshotChunkWriteTimeoutError{ShardID: 5, ReplicaID: 3, ChunkID: 17, Err: ioTimeout},
			wants:  []string{"shard 5", "replica 3", "chunk 17", "i/o timeout"},
			is:     ErrSnapshotChunkWriteTimeout,
			notIs:  []error{ErrStreamClosed},
			unwrap: ioTimeout,
		},
		{
			name:   "dial error",
			err:    &DialError{Target: "192.168.1.1:9999", Err: refused},
			wants:  []string{"dial", "192.168.1.1:9999", "connection refused"},
			unwrap: refused,
		},
		{
			name:   "frame decompress error",
			err:    &FrameDecompressError{Err: corrupt},
			wants:  []string{"frame decompression failed", "corrupt input"},
			unwrap: corrupt,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.err.Error()
			if tc.exact != "" && msg != tc.exact {
				t.Fatalf("Error() = %q, want %q", msg, tc.exact)
			}
			for _, want := range tc.wants {
				if !strings.Contains(msg, want) {
					t.Errorf("error message %q missing %q", msg, want)
				}
			}
			for _, bad := range tc.notWants {
				if strings.Contains(msg, bad) {
					t.Errorf("error message %q must not contain %q", msg, bad)
				}
			}
			if tc.is != nil && !errors.Is(tc.err, tc.is) {
				t.Errorf("errors.Is(%T, %v) = false, want true", tc.err, tc.is)
			}
			for _, other := range tc.notIs {
				if errors.Is(tc.err, other) {
					t.Errorf("%T matches the unrelated sentinel %v", tc.err, other)
				}
			}
			if tc.unwrap != nil {
				if got := errors.Unwrap(tc.err); got != tc.unwrap {
					t.Errorf("Unwrap() = %v, want %v", got, tc.unwrap)
				}
				if !errors.Is(tc.err, tc.unwrap) {
					t.Errorf("errors.Is should match the cause through Unwrap")
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Sentinel identity of the snapshot/frame abort errors
// ---------------------------------------------------------------------------

// TestSnapshotAbortErrors_SentinelIdentity pins the errors.Is contract for the
// typed abort errors. Callers (and the receive-side rejection accounting)
// distinguish an abort reason by matching the sentinel, not by string: an
// error whose Is does not report its own sentinel would be silently
// misclassified, and one that matched a FOREIGN sentinel would send the caller
// down the wrong recovery path.
func TestSnapshotAbortErrors_SentinelIdentity(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		sentinel error
		wants    []string
	}{
		{
			name:     "frame drain incomplete",
			err:      &FrameDrainError{Declared: 4096, Drained: 100, Err: errors.New("eof")},
			sentinel: ErrFrameDrainIncomplete,
			wants:    []string{"100", "4096", "desynchronised", "eof"},
		},
		{
			name:     "transfer timeout",
			err:      &SnapshotTransferTimeoutError{Elapsed: time.Minute, Limit: time.Second, ReceivedBytes: 17},
			sentinel: ErrSnapshotTransferTimeout,
			wants:    []string{"1m0s", "1s", "17"},
		},
		{
			name:     "progress stalled",
			err:      &SnapshotProgressStalledError{Elapsed: time.Second, ReceivedBytes: 3, MinBytesPerSec: 999},
			sentinel: ErrSnapshotProgressStalled,
			wants:    []string{"3", "1s", "999"},
		},
		{
			name:     "zero-length frame flood",
			err:      &SnapshotZeroLengthFrameFloodError{Count: 51, Max: 50},
			sentinel: ErrSnapshotZeroLengthFrameFlood,
			wants:    []string{"51", "50"},
		},
		{
			name:     "header length out of range",
			err:      &SnapshotHeaderLengthError{Declared: 9, Min: 32, Max: 4096},
			sentinel: ErrSnapshotHeaderLength,
			wants:    []string{"9", "32", "4096"},
		},
		{
			name:     "chunk count out of range",
			err:      &SnapshotChunkCountError{Declared: 1 << 40, Max: 1024},
			sentinel: ErrSnapshotChunkCount,
			wants:    []string{"1099511627776", "1024"},
		},
	}

	allSentinels := []error{
		ErrFrameDrainIncomplete,
		ErrSnapshotTransferTimeout,
		ErrSnapshotProgressStalled,
		ErrSnapshotZeroLengthFrameFlood,
		ErrSnapshotHeaderLength,
		ErrSnapshotChunkCount,
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, tc.sentinel) {
				t.Fatalf("errors.Is(%T, %v) = false, want true", tc.err, tc.sentinel)
			}
			// Must not answer for any OTHER sentinel.
			for _, other := range allSentinels {
				if other == tc.sentinel {
					continue
				}
				if errors.Is(tc.err, other) {
					t.Errorf("%T also matches the unrelated sentinel %v", tc.err, other)
				}
			}
			msg := tc.err.Error()
			for _, want := range tc.wants {
				if !strings.Contains(msg, want) {
					t.Errorf("error message %q missing %q", msg, want)
				}
			}
		})
	}
}
