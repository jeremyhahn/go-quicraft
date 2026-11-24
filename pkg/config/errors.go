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

package config

import (
	"fmt"
	"time"
)

// ValidationError is a typed error returned when configuration
// validation fails. It identifies the invalid field and provides a
// human-readable explanation of the constraint that was violated.
type ValidationError struct {
	Field   string
	Message string
}

// Error returns a formatted validation error message.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("config: invalid %s: %s", e.Field, e.Message)
}

// newValidationError constructs a ValidationError for the given
// field and message.
func newValidationError(field, message string) *ValidationError {
	return &ValidationError{
		Field:   field,
		Message: message,
	}
}

// LogValidationError is returned when log configuration validation
// fails. It identifies the invalid field, provides a human-readable
// explanation, and includes the rejected value.
type LogValidationError struct {
	Field   string
	Message string
	Value   string
}

// Error returns a formatted log config validation error message.
func (e *LogValidationError) Error() string {
	return fmt.Sprintf("config: invalid LogConfig.%s: %s (got %q)", e.Field, e.Message, e.Value)
}

// MaxProposalPayloadSizeExceededError is returned when MaxProposalPayloadSize exceeds the protocol's MaxEntrySize.
type MaxProposalPayloadSizeExceededError struct {
	Configured      uint64
	ProtocolMaximum uint64
}

// Error returns a formatted error message describing the violation.
func (e *MaxProposalPayloadSizeExceededError) Error() string {
	return fmt.Sprintf(
		"config: MaxProposalPayloadSize %d exceeds wire protocol maximum %d",
		e.Configured, e.ProtocolMaximum,
	)
}

// MaxApplyEntrySizeExceededError is returned when MaxApplyEntrySize exceeds what the transport can frame.
type MaxApplyEntrySizeExceededError struct {
	Configured   uint64
	FrameMaximum uint64
}

// Error returns a human-readable description.
func (e *MaxApplyEntrySizeExceededError) Error() string {
	return fmt.Sprintf(
		"config: MaxApplyEntrySize %d exceeds the maximum replication payload %d "+
			"that the transport can frame",
		e.Configured, e.FrameMaximum,
	)
}

// MaxEntrySizeExceededError is returned when MaxEntrySize exceeds the protocol's MaxEntrySize.
type MaxEntrySizeExceededError struct {
	Configured      uint64
	ProtocolMaximum uint64
}

// Error returns a formatted error message describing the violation.
func (e *MaxEntrySizeExceededError) Error() string {
	return fmt.Sprintf(
		"config: MaxEntrySize %d exceeds wire protocol maximum %d",
		e.Configured, e.ProtocolMaximum,
	)
}

// KeyRotationIntervalTooSmallError is returned when KeyRotationInterval is non-zero but under 1 second.
type KeyRotationIntervalTooSmallError struct {
	Configured time.Duration
	Minimum    time.Duration
}

// Error returns a formatted error message describing the violation.
func (e *KeyRotationIntervalTooSmallError) Error() string {
	return fmt.Sprintf(
		"config: KeyRotationInterval %v is below minimum %v",
		e.Configured, e.Minimum,
	)
}

// SessionExpiryEntriesTooSmallError is returned when SessionExpiryEntries is non-zero but under 100.
type SessionExpiryEntriesTooSmallError struct {
	Configured uint64
	Minimum    uint64
}

// Error returns a formatted error message describing the violation.
func (e *SessionExpiryEntriesTooSmallError) Error() string {
	return fmt.Sprintf(
		"config: SessionExpiryEntries %d is below minimum %d",
		e.Configured, e.Minimum,
	)
}

// MaxMessageReceiveMemoryTooSmallError is returned when MaxMessageReceiveMemory cannot hold one maximum-size frame's charge.
type MaxMessageReceiveMemoryTooSmallError struct {
	Configured int64
	Minimum    int64
}

// Error returns a formatted error message describing the violation.
func (e *MaxMessageReceiveMemoryTooSmallError) Error() string {
	return fmt.Sprintf(
		"config: TransportConfig.MaxMessageReceiveMemory %d is below minimum %d (one maximum-size frame)",
		e.Configured, e.Minimum,
	)
}

// ErrMTLSConfigRequired is the sentinel for MTLSConfigRequiredError.
// Use errors.Is to match.
var ErrMTLSConfigRequired = &MTLSConfigRequiredError{}

// MTLSConfigRequiredError is returned when transport is enabled but
// MTLSConfig is nil. All QUIC transport connections require mTLS;
// there is no insecure fallback.
type MTLSConfigRequiredError struct{}

// Error returns a human-readable description of the missing mTLS config.
func (e *MTLSConfigRequiredError) Error() string {
	return "config: MTLSConfig is required when transport is enabled"
}

// Is reports whether target matches this error type.
func (e *MTLSConfigRequiredError) Is(target error) bool {
	_, ok := target.(*MTLSConfigRequiredError)
	return ok
}

// ErrNoSyncWALOutsideTest is the sentinel for NoSyncWALOutsideTestError.
// Use errors.Is to match.
var ErrNoSyncWALOutsideTest = &NoSyncWALOutsideTestError{}

// NoSyncWALOutsideTestError is returned when NoSyncWAL is set outside a test binary.
type NoSyncWALOutsideTestError struct{}

// Error returns a human-readable description naming the NoSyncWAL field.
func (e *NoSyncWALOutsideTestError) Error() string {
	return "config: NoSyncWAL is allowed only in test binaries; it disables the WAL fdatasync that makes an acknowledged entry durable"
}

// Is reports whether target matches this error type.
func (e *NoSyncWALOutsideTestError) Is(target error) bool {
	_, ok := target.(*NoSyncWALOutsideTestError)
	return ok
}

// ErrLeaseReadRequiresCheckQuorum is the sentinel for
// LeaseReadRequiresCheckQuorumError. Use errors.Is to match.
var ErrLeaseReadRequiresCheckQuorum = &LeaseReadRequiresCheckQuorumError{}

// LeaseReadRequiresCheckQuorumError is returned when LeaseRead is enabled without CheckQuorum (Raft thesis 6.4).
type LeaseReadRequiresCheckQuorumError struct{}

// Error returns a human-readable description of the safety violation.
func (e *LeaseReadRequiresCheckQuorumError) Error() string {
	return "config: LeaseRead requires CheckQuorum to be enabled; " +
		"without CheckQuorum a partitioned leader serves stale reads indefinitely"
}

// Is reports whether target matches this error type.
func (e *LeaseReadRequiresCheckQuorumError) Is(target error) bool {
	_, ok := target.(*LeaseReadRequiresCheckQuorumError)
	return ok
}
