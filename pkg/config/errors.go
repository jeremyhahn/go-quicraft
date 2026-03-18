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

// MaxProposalPayloadSizeExceededError is returned when the configured
// MaxProposalPayloadSize exceeds the wire protocol's compile-time
// MaxEntrySize. The protocol maximum is the hard ceiling enforced
// during deserialization; the config value must be at or below it.
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

// KeyRotationIntervalTooSmallError is returned when the configured
// KeyRotationInterval is non-zero but less than the minimum of 1 second.
// Sub-second rotation intervals would cause excessive key churn and
// associated storage overhead.
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

// SessionExpiryEntriesTooSmallError is returned when the configured
// SessionExpiryEntries is non-zero but less than the minimum of 100.
// Values below 100 would aggressively expire sessions and cause
// excessive re-registrations.
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

// ErrLeaseReadRequiresCheckQuorum is the sentinel for
// LeaseReadRequiresCheckQuorumError. Use errors.Is to match.
var ErrLeaseReadRequiresCheckQuorum = &LeaseReadRequiresCheckQuorumError{}

// LeaseReadRequiresCheckQuorumError is returned when LeaseRead is enabled
// but CheckQuorum is disabled. Without CheckQuorum, a partitioned leader
// cannot detect quorum loss and will continue serving lease-based reads
// indefinitely, violating linearizability. Per Raft PhD thesis Section 6.4,
// lease reads require the leader to maintain quorum contact.
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
