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

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestError_Error(t *testing.T) {
	err := &Error{
		Method: "test-method",
		Err:    errors.New("connection refused"),
	}
	expected := "discovery: test-method: connection refused"
	if got := err.Error(); got != expected {
		t.Errorf("Error.Error() = %q, want %q", got, expected)
	}
}

func TestError_Unwrap(t *testing.T) {
	inner := errors.New("inner error")
	err := &Error{Method: "test", Err: inner}
	if unwrapped := err.Unwrap(); unwrapped != inner {
		t.Errorf("Error.Unwrap() = %v, want %v", unwrapped, inner)
	}
}

func TestError_UnwrapWithErrorsIs(t *testing.T) {
	inner := errors.New("specific error")
	err := &Error{Method: "test", Err: inner}
	if !errors.Is(err, inner) {
		t.Error("errors.Is should match the wrapped inner error")
	}
}

func TestNoPeersError_Error(t *testing.T) {
	err := &NoPeersError{Method: "static"}
	expected := "discovery: static: no peers found"
	if got := err.Error(); got != expected {
		t.Errorf("NoPeersError.Error() = %q, want %q", got, expected)
	}
}

func TestNoPeersError_EmptyMethod(t *testing.T) {
	err := &NoPeersError{Method: ""}
	expected := "discovery: : no peers found"
	if got := err.Error(); got != expected {
		t.Errorf("NoPeersError.Error() = %q, want %q", got, expected)
	}
}

func TestTimeoutError_Error(t *testing.T) {
	err := &TimeoutError{Method: "multicast"}
	expected := "discovery: multicast: timed out"
	if got := err.Error(); got != expected {
		t.Errorf("TimeoutError.Error() = %q, want %q", got, expected)
	}
}

func TestTimeoutError_EmptyMethod(t *testing.T) {
	err := &TimeoutError{Method: ""}
	expected := "discovery: : timed out"
	if got := err.Error(); got != expected {
		t.Errorf("TimeoutError.Error() = %q, want %q", got, expected)
	}
}

func TestAuthenticationError_Error(t *testing.T) {
	err := &AuthenticationError{}
	expected := "discovery: authentication failed"
	if got := err.Error(); got != expected {
		t.Errorf("AuthenticationError.Error() = %q, want %q", got, expected)
	}
}

func TestEndpointError_Error(t *testing.T) {
	err := &EndpointError{
		Method:   "PUT",
		Endpoint: "https://discovery.example.com/v1/nodes",
		Err:      errors.New("connection refused"),
	}
	expected := "discovery: endpoint PUT https://discovery.example.com/v1/nodes: connection refused"
	if got := err.Error(); got != expected {
		t.Errorf("EndpointError.Error() = %q, want %q", got, expected)
	}
}

func TestEndpointError_Unwrap(t *testing.T) {
	inner := errors.New("inner endpoint error")
	err := &EndpointError{Method: "GET", Endpoint: "https://example.com", Err: inner}
	if unwrapped := err.Unwrap(); unwrapped != inner {
		t.Errorf("EndpointError.Unwrap() = %v, want %v", unwrapped, inner)
	}
}

func TestEndpointError_UnwrapWithErrorsIs(t *testing.T) {
	inner := errors.New("specific endpoint error")
	err := &EndpointError{Method: "GET", Endpoint: "https://example.com", Err: inner}
	if !errors.Is(err, inner) {
		t.Error("errors.Is should match the wrapped inner error")
	}
}

func TestEndpointError_TypeAssertion(t *testing.T) {
	err := &EndpointError{Method: "PUT", Endpoint: "https://example.com", Err: errors.New("fail")}
	var ee *EndpointError
	if !errors.As(err, &ee) {
		t.Error("errors.As should match *EndpointError")
	}
	if ee.Method != "PUT" {
		t.Errorf("Method = %q, want %q", ee.Method, "PUT")
	}
	if ee.Endpoint != "https://example.com" {
		t.Errorf("Endpoint = %q, want %q", ee.Endpoint, "https://example.com")
	}
}

func TestRegistrationError_Error(t *testing.T) {
	err := &RegistrationError{
		Method: "token",
		Err:    errors.New("service unavailable"),
	}
	expected := "discovery: token: registration failed: service unavailable"
	if got := err.Error(); got != expected {
		t.Errorf("RegistrationError.Error() = %q, want %q", got, expected)
	}
}

func TestRegistrationError_Unwrap(t *testing.T) {
	inner := errors.New("inner registration error")
	err := &RegistrationError{Method: "token", Err: inner}
	if unwrapped := err.Unwrap(); unwrapped != inner {
		t.Errorf("RegistrationError.Unwrap() = %v, want %v", unwrapped, inner)
	}
}

func TestRegistrationError_UnwrapWithErrorsIs(t *testing.T) {
	inner := errors.New("specific registration error")
	err := &RegistrationError{Method: "token", Err: inner}
	if !errors.Is(err, inner) {
		t.Error("errors.Is should match the wrapped inner error")
	}
}

func TestRegistrationError_TypeAssertion(t *testing.T) {
	err := &RegistrationError{Method: "token", Err: errors.New("fail")}
	var re *RegistrationError
	if !errors.As(err, &re) {
		t.Error("errors.As should match *RegistrationError")
	}
	if re.Method != "token" {
		t.Errorf("Method = %q, want %q", re.Method, "token")
	}
}

func TestValidationError_Error(t *testing.T) {
	err := &ValidationError{
		Field:  "Endpoint",
		Reason: "must not be empty",
	}
	expected := "discovery: invalid Endpoint: must not be empty"
	if got := err.Error(); got != expected {
		t.Errorf("ValidationError.Error() = %q, want %q", got, expected)
	}
}

func TestValidationError_TypeAssertion(t *testing.T) {
	err := &ValidationError{Field: "Token", Reason: "must not be empty"}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Error("errors.As should match *ValidationError")
	}
	if ve.Field != "Token" {
		t.Errorf("Field = %q, want %q", ve.Field, "Token")
	}
}

func TestCancelledError_Error(t *testing.T) {
	err := &CancelledError{
		Method: "manager",
		Err:    context.Canceled,
	}
	expected := "discovery: manager: cancelled: context canceled"
	if got := err.Error(); got != expected {
		t.Errorf("CancelledError.Error() = %q, want %q", got, expected)
	}
}

func TestCancelledError_Unwrap(t *testing.T) {
	err := &CancelledError{
		Method: "manager",
		Err:    context.DeadlineExceeded,
	}
	if unwrapped := err.Unwrap(); unwrapped != context.DeadlineExceeded {
		t.Errorf("CancelledError.Unwrap() = %v, want %v", unwrapped, context.DeadlineExceeded)
	}
}

func TestCancelledError_UnwrapWithErrorsIs(t *testing.T) {
	err := &CancelledError{Method: "manager", Err: context.Canceled}
	if !errors.Is(err, context.Canceled) {
		t.Error("errors.Is should match context.Canceled through CancelledError")
	}
}

func TestCancelledError_TypeAssertion(t *testing.T) {
	err := &CancelledError{Method: "manager", Err: context.Canceled}
	var ce *CancelledError
	if !errors.As(err, &ce) {
		t.Error("errors.As should match *CancelledError")
	}
	if ce.Method != "manager" {
		t.Errorf("Method = %q, want %q", ce.Method, "manager")
	}
}

func TestResponseTooLargeError_Error(t *testing.T) {
	err := &ResponseTooLargeError{
		Endpoint: "https://example.com/api",
		MaxBytes: 1048576,
	}
	msg := err.Error()
	if msg == "" {
		t.Fatal("ResponseTooLargeError.Error() returned empty string")
	}
	if !strings.Contains(msg, "https://example.com/api") {
		t.Errorf("ResponseTooLargeError.Error() = %q, want to contain endpoint", msg)
	}
	if !strings.Contains(msg, "1048576") {
		t.Errorf("ResponseTooLargeError.Error() = %q, want to contain max bytes", msg)
	}
}

func TestResponseTooLargeError_TypeAssertion(t *testing.T) {
	var err error = &ResponseTooLargeError{
		Endpoint: "https://example.com",
		MaxBytes: 100,
	}
	var rtl *ResponseTooLargeError
	if !errors.As(err, &rtl) {
		t.Fatal("errors.As should match *ResponseTooLargeError")
	}
	if rtl.Endpoint != "https://example.com" {
		t.Errorf("Endpoint = %q, want %q", rtl.Endpoint, "https://example.com")
	}
}

func TestInsecureDiscoveryEndpointError_Error(t *testing.T) {
	err := &InsecureDiscoveryEndpointError{
		Endpoint: "http://insecure.example.com",
	}
	msg := err.Error()
	if msg == "" {
		t.Fatal("InsecureDiscoveryEndpointError.Error() returned empty string")
	}
	if !strings.Contains(msg, "http://insecure.example.com") {
		t.Errorf("InsecureDiscoveryEndpointError.Error() = %q, want to contain endpoint", msg)
	}
	if !strings.Contains(msg, "https://") {
		t.Errorf("InsecureDiscoveryEndpointError.Error() = %q, want to contain 'https://'", msg)
	}
}

func TestInsecureDiscoveryEndpointError_TypeAssertion(t *testing.T) {
	var err error = &InsecureDiscoveryEndpointError{
		Endpoint: "http://example.com",
	}
	var ie *InsecureDiscoveryEndpointError
	if !errors.As(err, &ie) {
		t.Fatal("errors.As should match *InsecureDiscoveryEndpointError")
	}
}

func TestErrorTypes_AreDistinct(t *testing.T) {
	disc := &Error{Method: "m", Err: errors.New("e")}
	noPeers := &NoPeersError{Method: "m"}
	timeout := &TimeoutError{Method: "m"}
	auth := &AuthenticationError{}

	var de *Error
	var npe *NoPeersError
	var te *TimeoutError
	var ae *AuthenticationError

	if !errors.As(disc, &de) {
		t.Error("Error should match *Error")
	}
	if !errors.As(noPeers, &npe) {
		t.Error("NoPeersError should match *NoPeersError")
	}
	if !errors.As(timeout, &te) {
		t.Error("TimeoutError should match *TimeoutError")
	}
	if !errors.As(auth, &ae) {
		t.Error("AuthenticationError should match *AuthenticationError")
	}
}
