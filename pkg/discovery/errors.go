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

package discovery

import "strconv"

// Error is returned when a discovery operation fails.
type Error struct {
	Method string
	Err    error
}

// Error returns a human-readable description of the discovery failure.
func (e *Error) Error() string {
	return "discovery: " + e.Method + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *Error) Unwrap() error {
	return e.Err
}

// NoPeersError is returned when no peers are found.
type NoPeersError struct {
	Method string
}

// Error returns a human-readable description of the no-peers condition.
func (e *NoPeersError) Error() string {
	return "discovery: " + e.Method + ": no peers found"
}

// TimeoutError is returned when a discovery operation times out.
type TimeoutError struct {
	Method string
}

// Error returns a human-readable description of the timeout.
func (e *TimeoutError) Error() string {
	return "discovery: " + e.Method + ": timed out"
}

// AuthenticationError is returned when HMAC verification fails.
type AuthenticationError struct{}

// Error returns a human-readable description of the authentication failure.
func (e *AuthenticationError) Error() string {
	return "discovery: authentication failed"
}

// EndpointError is returned when an HTTP request to a discovery endpoint fails.
type EndpointError struct {
	// Method is the HTTP method that failed (e.g., "PUT", "GET").
	Method string

	// Endpoint is the URL that was being contacted.
	Endpoint string

	// Err is the underlying error.
	Err error
}

// Error returns a human-readable description of the endpoint failure.
func (e *EndpointError) Error() string {
	return "discovery: endpoint " + e.Method + " " + e.Endpoint + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *EndpointError) Unwrap() error {
	return e.Err
}

// RegistrationError is returned when a node fails to register with the
// discovery service.
type RegistrationError struct {
	// Method is the discovery method that failed registration.
	Method string

	// Err is the underlying error.
	Err error
}

// Error returns a human-readable description of the registration failure.
func (e *RegistrationError) Error() string {
	return "discovery: " + e.Method + ": registration failed: " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *RegistrationError) Unwrap() error {
	return e.Err
}

// ResponseTooLargeError is returned when an HTTP response body exceeds
// the maximum allowed size.
type ResponseTooLargeError struct {
	// Endpoint is the URL that returned the oversized response.
	Endpoint string

	// MaxBytes is the maximum number of bytes allowed.
	MaxBytes int64
}

// Error returns a human-readable description of the oversized response.
func (e *ResponseTooLargeError) Error() string {
	return "discovery: response from " + e.Endpoint + " exceeds maximum size of " + strconv.FormatInt(e.MaxBytes, 10) + " bytes"
}

// ValidationError is returned when discovery configuration is invalid.
type ValidationError struct {
	// Field is the configuration field that failed validation.
	Field string

	// Reason describes why the value is invalid.
	Reason string
}

// Error returns a human-readable description of the validation failure.
func (e *ValidationError) Error() string {
	return "discovery: invalid " + e.Field + ": " + e.Reason
}

// CancelledError is returned when a discovery operation is cancelled
// via context cancellation or deadline expiry.
type CancelledError struct {
	// Method is the discovery method that was cancelled.
	Method string

	// Err is the underlying context error (context.Canceled or
	// context.DeadlineExceeded).
	Err error
}

// Error returns a human-readable description of the cancellation.
func (e *CancelledError) Error() string {
	return "discovery: " + e.Method + ": cancelled: " + e.Err.Error()
}

// Unwrap returns the underlying context error.
func (e *CancelledError) Unwrap() error {
	return e.Err
}

// ReplayError is returned when a multicast message has an expired or
// future timestamp, indicating a potential replay attack.
type ReplayError struct{}

// Error returns a human-readable description of the replay rejection.
func (e *ReplayError) Error() string {
	return "discovery: multicast message rejected: timestamp outside acceptable window"
}

// InsecureDiscoveryEndpointError is returned when the discovery endpoint
// uses an insecure scheme (e.g., http://) and AllowInsecureHTTP is false.
// Discovery tokens are sensitive credentials; transmitting them over
// plaintext HTTP exposes them to network-level interception.
type InsecureDiscoveryEndpointError struct {
	// Endpoint is the URL that failed the scheme check.
	Endpoint string
}

// Error returns a human-readable description of the insecure endpoint.
func (e *InsecureDiscoveryEndpointError) Error() string {
	return "discovery: endpoint " + e.Endpoint + " must use https:// scheme (set AllowInsecureHTTP for dev/testing)"
}
