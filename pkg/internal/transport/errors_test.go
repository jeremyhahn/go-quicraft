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
)

// ---------------------------------------------------------------------------
// CACertificateParseError
// ---------------------------------------------------------------------------

// TestCACertificateParseErrorErrorWithUnderlyingError verifies that the
// Error() method includes both the reason and the underlying error when
// Err is non-nil. This is the common case when x509.ParseCertificate
// returns a parse failure.
func TestCACertificateParseErrorErrorWithUnderlyingError(t *testing.T) {
	underlying := errors.New("asn1: structure error")
	err := &CACertificateParseError{
		Reason: "x509.ParseCertificate failed",
		Err:    underlying,
	}

	msg := err.Error()
	if !strings.Contains(msg, "CA certificate parse failed") {
		t.Fatalf("error message missing prefix: %s", msg)
	}
	if !strings.Contains(msg, "x509.ParseCertificate failed") {
		t.Fatalf("error message missing reason: %s", msg)
	}
	if !strings.Contains(msg, "asn1: structure error") {
		t.Fatalf("error message missing underlying error: %s", msg)
	}

	expected := "transport: CA certificate parse failed: x509.ParseCertificate failed: asn1: structure error"
	if msg != expected {
		t.Fatalf("Error() = %q, want %q", msg, expected)
	}
}

// TestCACertificateParseErrorErrorWithoutUnderlyingError verifies that the
// Error() method omits the underlying error suffix when Err is nil. This
// is the case when PEM decoding returns a nil block (no parse error to wrap).
func TestCACertificateParseErrorErrorWithoutUnderlyingError(t *testing.T) {
	err := &CACertificateParseError{
		Reason: "PEM decode returned nil block",
		Err:    nil,
	}

	msg := err.Error()
	expected := "transport: CA certificate parse failed: PEM decode returned nil block"
	if msg != expected {
		t.Fatalf("Error() = %q, want %q", msg, expected)
	}

	// Must not contain a trailing ": <nil>" artifact.
	if strings.Contains(msg, "<nil>") {
		t.Fatalf("error message should not contain <nil>: %s", msg)
	}
}

// TestCACertificateParseErrorUnwrapReturnsUnderlyingError verifies that
// Unwrap() returns the original error for errors.Is/As chain traversal.
func TestCACertificateParseErrorUnwrapReturnsUnderlyingError(t *testing.T) {
	underlying := errors.New("x509: malformed certificate")
	err := &CACertificateParseError{
		Reason: "x509.ParseCertificate failed",
		Err:    underlying,
	}

	unwrapped := err.Unwrap()
	if unwrapped != underlying {
		t.Fatalf("Unwrap() = %v, want %v", unwrapped, underlying)
	}

	// errors.Is must traverse the chain.
	if !errors.Is(err, underlying) {
		t.Fatal("errors.Is should match the underlying error through Unwrap")
	}
}

// TestCACertificateParseErrorUnwrapReturnsNilWhenNoUnderlyingError verifies
// that Unwrap() returns nil when Err is nil, correctly terminating the
// errors.Is/As chain. This happens for PEM decode failures that have no
// underlying parse error.
func TestCACertificateParseErrorUnwrapReturnsNilWhenNoUnderlyingError(t *testing.T) {
	err := &CACertificateParseError{
		Reason: "PEM decode returned nil block",
		Err:    nil,
	}

	unwrapped := err.Unwrap()
	if unwrapped != nil {
		t.Fatalf("Unwrap() = %v, want nil", unwrapped)
	}
}

// TestCACertificateParseErrorIsSentinelMatch verifies that the Is()
// method matches ErrCACertificateParse sentinel and that errors.As
// extracts the structured fields.
func TestCACertificateParseErrorIsSentinelMatch(t *testing.T) {
	err := &CACertificateParseError{
		Reason: "test reason",
		Err:    errors.New("test underlying"),
	}

	if !errors.Is(err, ErrCACertificateParse) {
		t.Fatal("CACertificateParseError should match ErrCACertificateParse")
	}

	var parseErr *CACertificateParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected *CACertificateParseError, got %T", err)
	}
	if parseErr.Reason != "test reason" {
		t.Fatalf("Reason = %q, want %q", parseErr.Reason, "test reason")
	}
}

// TestCACertificateParseErrorDoesNotMatchOtherSentinels verifies that the
// error does not falsely match unrelated sentinel errors.
func TestCACertificateParseErrorDoesNotMatchOtherSentinels(t *testing.T) {
	err := &CACertificateParseError{
		Reason: "test",
		Err:    errors.New("inner"),
	}

	if errors.Is(err, ErrStreamClosed) {
		t.Fatal("CACertificateParseError should not match ErrStreamClosed")
	}
	if errors.Is(err, ErrSnapshotChunkWriteTimeout) {
		t.Fatal("CACertificateParseError should not match ErrSnapshotChunkWriteTimeout")
	}
	if errors.Is(err, ErrInvalidMagic) {
		t.Fatal("CACertificateParseError should not match ErrInvalidMagic")
	}
}

// ---------------------------------------------------------------------------
// InvalidShardIDError
// ---------------------------------------------------------------------------

// TestInvalidShardIDErrorMessage verifies the error message format includes
// the replica ID and message type for diagnostic purposes.
func TestInvalidShardIDErrorMessage(t *testing.T) {
	err := &InvalidShardIDError{
		From:    5,
		MsgType: 12,
	}
	msg := err.Error()
	if !strings.Contains(msg, "ShardID=0") {
		t.Fatalf("error message missing ShardID=0: %s", msg)
	}
	if !strings.Contains(msg, "replica 5") {
		t.Fatalf("error message missing replica ID: %s", msg)
	}
	if !strings.Contains(msg, "type 12") {
		t.Fatalf("error message missing message type: %s", msg)
	}
}

// TestInvalidShardIDErrorIsSentinel verifies that the typed error matches
// the ErrInvalidShardID sentinel via errors.Is().
func TestInvalidShardIDErrorIsSentinel(t *testing.T) {
	err := &InvalidShardIDError{From: 1, MsgType: 7}

	if !errors.Is(err, ErrInvalidShardID) {
		t.Fatal("InvalidShardIDError should match ErrInvalidShardID sentinel")
	}

	// Should not match unrelated sentinels.
	if errors.Is(err, ErrStreamClosed) {
		t.Fatal("InvalidShardIDError should not match ErrStreamClosed")
	}
}

// TestInvalidShardIDErrorAs verifies that errors.As extracts the
// structured fields from the typed error.
func TestInvalidShardIDErrorAs(t *testing.T) {
	var target error = &InvalidShardIDError{From: 42, MsgType: 17}

	var shardErr *InvalidShardIDError
	if !errors.As(target, &shardErr) {
		t.Fatal("errors.As should extract *InvalidShardIDError")
	}
	if shardErr.From != 42 {
		t.Fatalf("From = %d, want 42", shardErr.From)
	}
	if shardErr.MsgType != 17 {
		t.Fatalf("MsgType = %d, want 17", shardErr.MsgType)
	}
}

// ---------------------------------------------------------------------------
// ChunkFilepathTraversalError
// ---------------------------------------------------------------------------

// TestChunkFilepathTraversalErrorMessage verifies the error message includes
// the shard ID for diagnostic purposes.
func TestChunkFilepathTraversalErrorMessage(t *testing.T) {
	err := &ChunkFilepathTraversalError{
		ShardID:  7,
		Filepath: "../etc/passwd",
	}
	msg := err.Error()
	if !strings.Contains(msg, "path traversal") {
		t.Fatalf("error message missing 'path traversal': %s", msg)
	}
	if !strings.Contains(msg, "shard 7") {
		t.Fatalf("error message missing shard ID: %s", msg)
	}
}

// TestChunkFilepathTraversalErrorIs verifies the sentinel matching.
func TestChunkFilepathTraversalErrorIs(t *testing.T) {
	err := &ChunkFilepathTraversalError{ShardID: 1, Filepath: "../hack"}
	if !errors.Is(err, ErrChunkFilepathTraversal) {
		t.Fatal("should match ErrChunkFilepathTraversal sentinel")
	}
	if errors.Is(err, ErrChunkFilepathTooLong) {
		t.Fatal("should not match ErrChunkFilepathTooLong")
	}
}

// ---------------------------------------------------------------------------
// TypeAssertionError
// ---------------------------------------------------------------------------

// TestTypeAssertionErrorMessage verifies the error message includes context,
// expected type, and actual value type.
func TestTypeAssertionErrorMessage(t *testing.T) {
	err := &TypeAssertionError{
		Context:      "Start UDP listener",
		ExpectedType: "*net.UDPConn",
		ActualValue:  "not-a-conn",
	}
	msg := err.Error()
	if !strings.Contains(msg, "type assertion failed") {
		t.Fatalf("error message missing 'type assertion failed': %s", msg)
	}
	if !strings.Contains(msg, "Start UDP listener") {
		t.Fatalf("error message missing context: %s", msg)
	}
	if !strings.Contains(msg, "*net.UDPConn") {
		t.Fatalf("error message missing expected type: %s", msg)
	}
	if !strings.Contains(msg, "string") {
		t.Fatalf("error message missing actual type: %s", msg)
	}
}

// TestTypeAssertionErrorWithNilValue verifies the error message when
// ActualValue is nil.
func TestTypeAssertionErrorWithNilValue(t *testing.T) {
	err := &TypeAssertionError{
		Context:      "test context",
		ExpectedType: "*quic.Conn",
		ActualValue:  nil,
	}
	msg := err.Error()
	if !strings.Contains(msg, "<nil>") {
		t.Fatalf("error message should contain <nil> for nil value: %s", msg)
	}
}

// ---------------------------------------------------------------------------
// SnapshotMemoryBudgetExceededError
// ---------------------------------------------------------------------------

// TestSnapshotMemoryBudgetExceededErrorMessage verifies the error message
// includes accumulated bytes and budget.
func TestSnapshotMemoryBudgetExceededErrorMessage(t *testing.T) {
	err := &SnapshotMemoryBudgetExceededError{
		Accumulated: 1024 * 1024,
		Budget:      512 * 1024,
	}
	msg := err.Error()
	if !strings.Contains(msg, "memory budget exceeded") {
		t.Fatalf("error message missing 'memory budget exceeded': %s", msg)
	}
	if !strings.Contains(msg, "1048576") {
		t.Fatalf("error message missing accumulated bytes: %s", msg)
	}
	if !strings.Contains(msg, "524288") {
		t.Fatalf("error message missing budget bytes: %s", msg)
	}
}

// TestSnapshotMemoryBudgetExceededErrorIs verifies sentinel matching.
func TestSnapshotMemoryBudgetExceededErrorIs(t *testing.T) {
	err := &SnapshotMemoryBudgetExceededError{
		Accumulated: 100,
		Budget:      50,
	}
	if !errors.Is(err, ErrSnapshotMemoryBudgetExceeded) {
		t.Fatal("should match ErrSnapshotMemoryBudgetExceeded")
	}
	if errors.Is(err, ErrStreamClosed) {
		t.Fatal("should not match ErrStreamClosed")
	}
}

// ---------------------------------------------------------------------------
// WorkerPanicError
// ---------------------------------------------------------------------------

// TestWorkerPanicErrorWithTarget verifies the error message format when
// Target is set.
func TestWorkerPanicErrorWithTarget(t *testing.T) {
	err := &WorkerPanicError{
		WorkerName: "sendQueueWorker",
		Target:     "127.0.0.1:9999",
		Value:      "index out of range",
		Stack:      []byte("goroutine 1..."),
	}
	msg := err.Error()
	if !strings.Contains(msg, "sendQueueWorker") {
		t.Fatalf("error message missing worker name: %s", msg)
	}
	if !strings.Contains(msg, "127.0.0.1:9999") {
		t.Fatalf("error message missing target: %s", msg)
	}
	if !strings.Contains(msg, "index out of range") {
		t.Fatalf("error message missing panic value: %s", msg)
	}
}

// TestWorkerPanicErrorWithoutTarget verifies the error message format when
// Target is empty (global workers like connectionCleanup).
func TestWorkerPanicErrorWithoutTarget(t *testing.T) {
	err := &WorkerPanicError{
		WorkerName: "connectionCleanup",
		Target:     "",
		Value:      "nil pointer dereference",
		Stack:      []byte("goroutine 1..."),
	}
	msg := err.Error()
	if !strings.Contains(msg, "connectionCleanup") {
		t.Fatalf("error message missing worker name: %s", msg)
	}
	if strings.Contains(msg, "[") {
		t.Fatalf("error message should not contain brackets when target is empty: %s", msg)
	}
	if !strings.Contains(msg, "nil pointer dereference") {
		t.Fatalf("error message missing panic value: %s", msg)
	}
}

// ---------------------------------------------------------------------------
// SnapshotChunkWriteTimeoutError
// ---------------------------------------------------------------------------

// TestSnapshotChunkWriteTimeoutErrorMessage verifies the error message
// includes shard, replica, chunk, and underlying error.
func TestSnapshotChunkWriteTimeoutErrorMessage(t *testing.T) {
	underlying := errors.New("i/o timeout")
	err := &SnapshotChunkWriteTimeoutError{
		ShardID:   5,
		ReplicaID: 3,
		ChunkID:   17,
		Err:       underlying,
	}
	msg := err.Error()
	if !strings.Contains(msg, "shard 5") {
		t.Fatalf("error message missing shard ID: %s", msg)
	}
	if !strings.Contains(msg, "replica 3") {
		t.Fatalf("error message missing replica ID: %s", msg)
	}
	if !strings.Contains(msg, "chunk 17") {
		t.Fatalf("error message missing chunk ID: %s", msg)
	}
	if !strings.Contains(msg, "i/o timeout") {
		t.Fatalf("error message missing underlying error: %s", msg)
	}
}

// TestSnapshotChunkWriteTimeoutErrorIs verifies sentinel matching.
func TestSnapshotChunkWriteTimeoutErrorIs(t *testing.T) {
	err := &SnapshotChunkWriteTimeoutError{
		ShardID: 1, ReplicaID: 2, ChunkID: 3,
		Err: errors.New("timeout"),
	}
	if !errors.Is(err, ErrSnapshotChunkWriteTimeout) {
		t.Fatal("should match ErrSnapshotChunkWriteTimeout")
	}
	if errors.Is(err, ErrStreamClosed) {
		t.Fatal("should not match ErrStreamClosed")
	}
}

// TestSnapshotChunkWriteTimeoutErrorUnwrap verifies Unwrap returns the
// underlying error.
func TestSnapshotChunkWriteTimeoutErrorUnwrap(t *testing.T) {
	underlying := errors.New("network unreachable")
	err := &SnapshotChunkWriteTimeoutError{
		ShardID: 1, ReplicaID: 2, ChunkID: 3,
		Err: underlying,
	}
	if err.Unwrap() != underlying {
		t.Fatalf("Unwrap() = %v, want %v", err.Unwrap(), underlying)
	}
	if !errors.Is(err, underlying) {
		t.Fatal("errors.Is should traverse through Unwrap")
	}
}

// ---------------------------------------------------------------------------
// DialError
// ---------------------------------------------------------------------------

// TestDialErrorMessage verifies the error message includes the target
// and the underlying error.
func TestDialErrorMessage(t *testing.T) {
	err := &DialError{
		Target: "192.168.1.1:9999",
		Err:    errors.New("connection refused"),
	}
	msg := err.Error()
	if !strings.Contains(msg, "dial") {
		t.Fatalf("error message missing 'dial': %s", msg)
	}
	if !strings.Contains(msg, "192.168.1.1:9999") {
		t.Fatalf("error message missing target: %s", msg)
	}
	if !strings.Contains(msg, "connection refused") {
		t.Fatalf("error message missing underlying error: %s", msg)
	}
}

// TestDialErrorUnwrap verifies Unwrap returns the underlying error.
func TestDialErrorUnwrap(t *testing.T) {
	underlying := errors.New("host not found")
	err := &DialError{Target: "bad-host:1234", Err: underlying}
	if err.Unwrap() != underlying {
		t.Fatalf("Unwrap() = %v, want %v", err.Unwrap(), underlying)
	}
}

// ---------------------------------------------------------------------------
// FrameDecompressError
// ---------------------------------------------------------------------------

// TestFrameDecompressErrorMessage verifies the error message format.
func TestFrameDecompressErrorMessage(t *testing.T) {
	err := &FrameDecompressError{Err: errors.New("corrupt input")}
	msg := err.Error()
	if !strings.Contains(msg, "frame decompression failed") {
		t.Fatalf("error message missing expected prefix: %s", msg)
	}
	if !strings.Contains(msg, "corrupt input") {
		t.Fatalf("error message missing underlying error: %s", msg)
	}
}

// TestFrameDecompressErrorUnwrap verifies chain traversal.
func TestFrameDecompressErrorUnwrap(t *testing.T) {
	underlying := errors.New("snappy: corrupt input")
	err := &FrameDecompressError{Err: underlying}
	if err.Unwrap() != underlying {
		t.Fatalf("Unwrap() = %v, want %v", err.Unwrap(), underlying)
	}
	if !errors.Is(err, underlying) {
		t.Fatal("errors.Is should match through Unwrap")
	}
}
