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
	"fmt"

	"github.com/quic-go/quic-go"
)

// Sentinel errors for transport operations. Each uses the "transport:"
// prefix for clear provenance in error chains. These are pre-allocated
// for zero-alloc hot path returns and use with errors.Is().
var (
	// ErrInvalidMagic is returned when the frame header magic bytes
	// do not match the expected 0x51 0x43 ("QC").
	ErrInvalidMagic = errors.New("transport: invalid magic bytes")

	// ErrUnsupportedVersion is returned when the wire version in the
	// frame header exceeds the supported WireVersion.
	ErrUnsupportedVersion = errors.New("transport: unsupported wire version")

	// ErrFrameTooLarge is returned when the frame payload length exceeds
	// MaxFrameSize (16MB).
	ErrFrameTooLarge = errors.New("transport: frame exceeds max size")

	// ErrPeerIdentityMismatch is returned when the peer certificate SAN
	// does not match the expected node address from the registry.
	ErrPeerIdentityMismatch = errors.New("transport: peer identity mismatch")

	// ErrDecompressedSizeLimitExceeded is returned when the declared
	// decompressed size exceeds MaxDecompressedSize, preventing
	// decompression bomb attacks.
	ErrDecompressedSizeLimitExceeded = errors.New("transport: decompressed size exceeds limit")

	// ErrDecompressionMismatch is returned when the actual decompressed
	// length differs from the declared length in the Snappy header.
	ErrDecompressionMismatch = errors.New("transport: decompression length mismatch")

	// ErrStreamClosed is returned when an operation is attempted on a
	// stream that has been closed.
	ErrStreamClosed = errors.New("transport: stream closed")

	// ErrBufferTooSmall is returned when a buffer is too small to hold
	// the data being marshaled or unmarshaled.
	ErrBufferTooSmall = errors.New("transport: buffer too small")

	// ErrPayloadTooLarge is returned when WriteFrame or MarshalFrame
	// receives a payload larger than MaxFrameSize.
	ErrPayloadTooLarge = errors.New("transport: payload too large")

	// ErrInsecure0RTT is returned when Enable0RTT is true but MTLSConfig
	// is nil. QUIC 0-RTT data is replayable and must only be used with
	// mutual TLS authentication to prevent unauthenticated replay attacks.
	ErrInsecure0RTT = errors.New("transport: 0-RTT requires mTLS (MTLSConfig must be set)")

	// ErrMTLSConfigRequired is returned when MTLSConfig is nil. QUIC
	// transport requires mutual TLS authentication for all connections.
	// Self-signed and unauthenticated modes are not supported.
	ErrMTLSConfigRequired = errors.New("transport: MTLSConfig is required (mutual TLS is mandatory)")

	// ErrInvalidMaxStreamsPerConnection is returned when MaxStreamsPerConnection < 1.
	ErrInvalidMaxStreamsPerConnection = errors.New("transport: max streams per connection must be >= 1")

	// ErrCACertificateParse is returned when the CA certificate PEM cannot
	// be decoded or parsed. Used as a sentinel for errors.Is() matching
	// against CACertificateParseError.
	ErrCACertificateParse = errors.New("transport: CA certificate parse failed")

	// ErrSnapshotChunkWriteTimeout is the sentinel for
	// SnapshotChunkWriteTimeoutError. Returned when a snapshot chunk
	// write exceeds the per-chunk write deadline, indicating a slow
	// receiver or network stall.
	ErrSnapshotChunkWriteTimeout = errors.New("transport: snapshot chunk write deadline exceeded")
)

// Application-level QUIC error codes. QUIC reserves 0x00 for
// no-error and framework codes; application codes start at 0x01.
// Using named constants instead of raw 0 values makes CloseWithError
// calls self-documenting and enables protocol-level error diagnosis.
const (
	// QUICErrShutdown indicates a graceful transport shutdown.
	QUICErrShutdown quic.ApplicationErrorCode = 0x1

	// QUICErrConnLimitReached indicates the global connection limit was reached.
	QUICErrConnLimitReached quic.ApplicationErrorCode = 0x2

	// QUICErrIPLimitReached indicates the per-IP connection limit was reached.
	QUICErrIPLimitReached quic.ApplicationErrorCode = 0x3
)

// FrameDecompressError wraps an underlying Snappy decode failure that
// occurred during transport frame decompression.
type FrameDecompressError struct {
	Err error
}

// Error returns a human-readable description of the frame decompression failure.
func (e *FrameDecompressError) Error() string {
	return fmt.Sprintf("transport: frame decompression failed: %v", e.Err)
}

// Unwrap returns the underlying error for errors.Is/As chain traversal.
func (e *FrameDecompressError) Unwrap() error {
	return e.Err
}

// Snapshot chunk sentinel errors for errors.Is() matching.
var (
	// ErrChunkFilepathTooLong is returned when a snapshot chunk's Filepath
	// exceeds the POSIX PATH_MAX limit.
	ErrChunkFilepathTooLong = errors.New("transport: snapshot chunk filepath too long")

	// ErrChunkDataTooLarge is returned when a snapshot chunk's Data payload
	// exceeds the maximum allowed size.
	ErrChunkDataTooLarge = errors.New("transport: snapshot chunk data too large")

	// ErrChunkDeploymentIDMismatch is returned when a snapshot chunk's
	// DeploymentID does not match the configured deployment.
	ErrChunkDeploymentIDMismatch = errors.New("transport: snapshot chunk deployment ID mismatch")

	// ErrChunkFilepathTraversal is returned when a snapshot chunk's Filepath
	// contains path traversal sequences.
	ErrChunkFilepathTraversal = errors.New("transport: snapshot chunk filepath contains path traversal")
)

// ChunkFilepathTooLongError is returned when a deserialized snapshot chunk
// has a Filepath exceeding the POSIX PATH_MAX (4096 bytes).
type ChunkFilepathTooLongError struct {
	ShardID uint64
	Length  int
	Max     int
}

// Error returns a human-readable description of the filepath violation.
func (e *ChunkFilepathTooLongError) Error() string {
	return fmt.Sprintf(
		"transport: snapshot chunk filepath too long for shard %d (%d bytes, max %d)",
		e.ShardID, e.Length, e.Max)
}

// Is reports whether target matches the sentinel ErrChunkFilepathTooLong.
func (e *ChunkFilepathTooLongError) Is(target error) bool {
	return target == ErrChunkFilepathTooLong
}

// ChunkDataTooLargeError is returned when a deserialized snapshot chunk's
// Data payload exceeds the maximum allowed size (64MB).
type ChunkDataTooLargeError struct {
	ShardID uint64
	Length  int
	Max     int
}

// Error returns a human-readable description of the data size violation.
func (e *ChunkDataTooLargeError) Error() string {
	return fmt.Sprintf(
		"transport: snapshot chunk data too large for shard %d (%d bytes, max %d)",
		e.ShardID, e.Length, e.Max)
}

// Is reports whether target matches the sentinel ErrChunkDataTooLarge.
func (e *ChunkDataTooLargeError) Is(target error) bool {
	return target == ErrChunkDataTooLarge
}

// ChunkDeploymentIDMismatchError is returned when a snapshot chunk's
// DeploymentID does not match the receiver's configured deployment ID.
type ChunkDeploymentIDMismatchError struct {
	ShardID  uint64
	Expected uint64
	Got      uint64
}

// Error returns a human-readable description of the mismatch.
func (e *ChunkDeploymentIDMismatchError) Error() string {
	return fmt.Sprintf(
		"transport: snapshot chunk deployment ID mismatch for shard %d (expected %d, got %d)",
		e.ShardID, e.Expected, e.Got)
}

// Is reports whether target matches the sentinel ErrChunkDeploymentIDMismatch.
func (e *ChunkDeploymentIDMismatchError) Is(target error) bool {
	return target == ErrChunkDeploymentIDMismatch
}

// ChunkFilepathTraversalError is returned when a deserialized snapshot chunk
// has a Filepath containing path traversal sequences (e.g. "..").
type ChunkFilepathTraversalError struct {
	ShardID  uint64
	Filepath string
}

// Error returns a human-readable description of the path traversal violation.
func (e *ChunkFilepathTraversalError) Error() string {
	return fmt.Sprintf(
		"transport: snapshot chunk filepath contains path traversal for shard %d",
		e.ShardID)
}

// Is reports whether target matches the sentinel ErrChunkFilepathTraversal.
func (e *ChunkFilepathTraversalError) Is(target error) bool {
	return target == ErrChunkFilepathTraversal
}

// DialError is returned when a connection dial fails due to an invalid
// target address. It wraps the underlying parse error and includes the
// target address for diagnostic context.
type DialError struct {
	Target string
	Err    error
}

// Error returns a human-readable description of the dial failure.
func (e *DialError) Error() string {
	return fmt.Sprintf("transport: dial %s failed: %v", e.Target, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As chain traversal.
func (e *DialError) Unwrap() error {
	return e.Err
}

// CACertificateParseError is returned when the CA certificate PEM provided
// in MTLSConfig.CACert cannot be decoded or the DER block cannot be parsed
// as an X.509 certificate. This is a fail-fast error at startup when
// RevocationConfig is non-nil, because OCSP/CRL checking requires a valid
// issuer certificate.
type CACertificateParseError struct {
	// Reason describes the failure mode (e.g., "PEM decode returned nil block").
	Reason string
	// Err is the underlying parse error, if any. Nil when the PEM block
	// itself could not be decoded.
	Err error
}

// Error returns a human-readable description of the CA certificate parse failure.
func (e *CACertificateParseError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("transport: CA certificate parse failed: %s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("transport: CA certificate parse failed: %s", e.Reason)
}

// Is reports whether target matches the sentinel ErrCACertificateParse.
func (e *CACertificateParseError) Is(target error) bool {
	return target == ErrCACertificateParse
}

// Unwrap returns the underlying error for errors.Is/As chain traversal.
func (e *CACertificateParseError) Unwrap() error {
	return e.Err
}

// SnapshotChunkWriteTimeoutError is returned when writing a snapshot chunk
// to a QUIC stream exceeds the per-chunk write deadline. This indicates the
// receiver is slow or the network is stalled. The snapshot send is aborted
// and the snapshotTracker slot is released so Stop() can proceed.
type SnapshotChunkWriteTimeoutError struct {
	ShardID   uint64
	ReplicaID uint64
	ChunkID   uint64
	Err       error
}

// Error returns a human-readable description of the timeout.
func (e *SnapshotChunkWriteTimeoutError) Error() string {
	return fmt.Sprintf(
		"transport: snapshot chunk write deadline exceeded for shard %d replica %d chunk %d: %v",
		e.ShardID, e.ReplicaID, e.ChunkID, e.Err)
}

// Is reports whether target matches the sentinel ErrSnapshotChunkWriteTimeout.
func (e *SnapshotChunkWriteTimeoutError) Is(target error) bool {
	return target == ErrSnapshotChunkWriteTimeout
}

// Unwrap returns the underlying error for errors.Is/As chain traversal.
func (e *SnapshotChunkWriteTimeoutError) Unwrap() error {
	return e.Err
}

// ErrInvalidShardID is the sentinel for InvalidShardIDError. Returned
// when an inbound Raft message has ShardID == 0, which is never valid
// for a real shard.
var ErrInvalidShardID = errors.New("transport: invalid shard ID (zero)")

// InvalidShardIDError is returned when an inbound Raft message has a
// zero ShardID. Zero is reserved and never assigned to real shards, so
// a message with ShardID == 0 indicates corruption or a malicious peer.
// The message is logged and dropped rather than closing the connection.
type InvalidShardIDError struct {
	From    uint64
	MsgType uint64
}

// Error returns a human-readable description of the invalid shard ID.
func (e *InvalidShardIDError) Error() string {
	return fmt.Sprintf(
		"transport: received message with ShardID=0 from replica %d (type %d)",
		e.From, e.MsgType,
	)
}

// Is reports whether target matches the sentinel ErrInvalidShardID.
func (e *InvalidShardIDError) Is(target error) bool {
	return target == ErrInvalidShardID
}

// TypeAssertionError is returned when a type assertion fails on data
// retrieved from interface{} sources. This indicates an internal
// programming error where the actual type does not match the expected type.
type TypeAssertionError struct {
	Context      string // description of where the assertion occurred
	ExpectedType string // the type that was expected
	ActualValue  any    // the actual value retrieved (may be nil)
}

// Error returns a human-readable description of the type assertion failure.
func (e *TypeAssertionError) Error() string {
	return fmt.Sprintf(
		"transport: type assertion failed in %s: expected %s, got %T",
		e.Context, e.ExpectedType, e.ActualValue,
	)
}

// WorkerPanicError records a panic recovered from a transport background
// goroutine. It captures the worker name, the recovered value, and the
// stack trace at the point of panic for diagnostic logging.
type WorkerPanicError struct {
	WorkerName string // identifies the goroutine (e.g., "sendQueueWorker", "connectionCleanup")
	Target     string // target address for per-target workers, empty otherwise
	Value      any    // the value passed to panic()
	Stack      []byte // runtime stack trace captured via debug.Stack()
}

// Error returns a human-readable description of the worker panic.
func (e *WorkerPanicError) Error() string {
	if e.Target != "" {
		return fmt.Sprintf(
			"transport: %s[%s] panicked: %v",
			e.WorkerName, e.Target, e.Value,
		)
	}
	return fmt.Sprintf(
		"transport: %s panicked: %v",
		e.WorkerName, e.Value,
	)
}

// ErrSnapshotMemoryBudgetExceeded is the sentinel for
// SnapshotMemoryBudgetExceededError. Returned when a single snapshot
// receive accumulates more chunk data than the configured memory budget.
var ErrSnapshotMemoryBudgetExceeded = errors.New("transport: snapshot memory budget exceeded")

// SnapshotMemoryBudgetExceededError is returned when the accumulated chunk
// data for a single snapshot receive exceeds the configured memory budget.
// This prevents a malicious or misconfigured peer from causing unbounded
// memory growth on the receiver.
type SnapshotMemoryBudgetExceededError struct {
	Accumulated int64 // total bytes accumulated so far
	Budget      int64 // configured maximum memory budget
}

// Error returns a human-readable description of the budget violation.
func (e *SnapshotMemoryBudgetExceededError) Error() string {
	return fmt.Sprintf(
		"transport: snapshot memory budget exceeded: accumulated %d bytes, budget %d bytes",
		e.Accumulated, e.Budget,
	)
}

// Is reports whether target matches the sentinel ErrSnapshotMemoryBudgetExceeded.
func (e *SnapshotMemoryBudgetExceededError) Is(target error) bool {
	return target == ErrSnapshotMemoryBudgetExceeded
}
