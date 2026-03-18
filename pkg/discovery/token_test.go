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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// mockHTTPClient implements HTTPClient for testing.
type mockHTTPClient struct {
	doFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.doFunc(req)
}

// newMockResponse creates an *http.Response with the given status code and body.
func newMockResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     make(http.Header),
	}
}

func TestTokenDiscovery_Discover(t *testing.T) {
	var callCount atomic.Int32

	nodesResp := tokenNodesResponse{
		Nodes: []tokenNode{
			{NodeID: 1, Address: "10.0.0.1:5000"},
			{NodeID: 2, Address: "10.0.0.2:5000"},
			{NodeID: 3, Address: "10.0.0.3:5000"},
		},
	}
	nodesJSON, err := json.Marshal(nodesResp)
	if err != nil {
		t.Fatalf("failed to marshal nodes: %v", err)
	}

	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			callCount.Add(1)
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			return newMockResponse(http.StatusOK, string(nodesJSON)), nil
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
		Timeout:     5 * time.Second,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	peers, err := td.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover() error: %v", err)
	}

	// Self (NodeID=1) should be excluded.
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers (self excluded), got %d", len(peers))
	}

	found := map[uint64]bool{}
	for _, p := range peers {
		found[p.NodeID] = true
	}
	if found[1] {
		t.Error("self (NodeID=1) should be excluded from results")
	}
	if !found[2] || !found[3] {
		t.Errorf("expected NodeID 2 and 3 in results, got %v", found)
	}
}

func TestTokenDiscovery_EmptyEndpoint(t *testing.T) {
	_, err := NewTokenDiscovery(TokenConfig{
		Token:       "test-token",
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
	})
	if err == nil {
		t.Fatal("expected error for empty endpoint")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if ve.Field != "Endpoint" {
		t.Errorf("Field = %q, want %q", ve.Field, "Endpoint")
	}
}

func TestTokenDiscovery_EmptyToken(t *testing.T) {
	_, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
	})
	if err == nil {
		t.Fatal("expected error for empty token")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if ve.Field != "Token" {
		t.Errorf("Field = %q, want %q", ve.Field, "Token")
	}
}

func TestTokenDiscovery_EmptyAddress(t *testing.T) {
	_, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		ClusterSize: 3,
	})
	if err == nil {
		t.Fatal("expected error for empty address")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if ve.Field != "Address" {
		t.Errorf("Field = %q, want %q", ve.Field, "Address")
	}
}

func TestTokenDiscovery_InvalidClusterSize(t *testing.T) {
	tests := []struct {
		name        string
		clusterSize int
	}{
		{"zero", 0},
		{"negative", -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTokenDiscovery(TokenConfig{
				Endpoint:    "https://discovery.example.com",
				Token:       "test-token",
				Address:     "10.0.0.1:5000",
				ClusterSize: tt.clusterSize,
			})
			if err == nil {
				t.Fatal("expected error for invalid cluster size")
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("expected *ValidationError, got %T: %v", err, err)
			}
			if ve.Field != "ClusterSize" {
				t.Errorf("Field = %q, want %q", ve.Field, "ClusterSize")
			}
		})
	}
}

func TestTokenDiscovery_Timeout(t *testing.T) {
	// Return only 1 node when cluster needs 3.
	nodesResp := tokenNodesResponse{
		Nodes: []tokenNode{
			{NodeID: 1, Address: "10.0.0.1:5000"},
		},
	}
	nodesJSON, _ := json.Marshal(nodesResp)

	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			return newMockResponse(http.StatusOK, string(nodesJSON)), nil
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
		Timeout:     100 * time.Millisecond,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	_, err = td.Discover(context.Background())
	if err == nil {
		t.Fatal("expected timeout error")
	}
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("expected *TimeoutError, got %T: %v", err, err)
	}
}

func TestTokenDiscovery_Stop(t *testing.T) {
	// Return only 1 node so discovery keeps polling.
	nodesResp := tokenNodesResponse{
		Nodes: []tokenNode{
			{NodeID: 1, Address: "10.0.0.1:5000"},
		},
	}
	nodesJSON, _ := json.Marshal(nodesResp)

	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			return newMockResponse(http.StatusOK, string(nodesJSON)), nil
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
		Timeout:     10 * time.Second,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := td.Discover(context.Background())
		done <- err
	}()

	// Give the goroutine time to start polling.
	<-time.After(50 * time.Millisecond)

	if err := td.Stop(); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after Stop()")
		}
		var te *TimeoutError
		if !errors.As(err, &te) {
			t.Fatalf("expected *TimeoutError, got %T: %v", err, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Discover() did not return after Stop()")
	}
}

func TestTokenDiscovery_Name(t *testing.T) {
	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		Address:     "10.0.0.1:5000",
		ClusterSize: 1,
		HTTPClient:  &mockHTTPClient{},
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}
	if name := td.Name(); name != "token" {
		t.Errorf("Name() = %q, want %q", name, "token")
	}
}

func TestTokenDiscovery_ImplementsMethod(t *testing.T) {
	var _ Method = (*TokenDiscovery)(nil)
}

func TestTokenDiscovery_RegistrationFailure(t *testing.T) {
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("connection refused")
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	_, err = td.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error for registration failure")
	}
	var ee *EndpointError
	if !errors.As(err, &ee) {
		t.Fatalf("expected *EndpointError, got %T: %v", err, err)
	}
	if ee.Method != http.MethodPut {
		t.Errorf("Method = %q, want %q", ee.Method, http.MethodPut)
	}
}

func TestTokenDiscovery_RegistrationBadStatus(t *testing.T) {
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusInternalServerError, "server error"), nil
			}
			return newMockResponse(http.StatusOK, `{"nodes":[]}`), nil
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	_, err = td.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error for bad registration status")
	}
	var re *RegistrationError
	if !errors.As(err, &re) {
		t.Fatalf("expected *RegistrationError, got %T: %v", err, err)
	}
}

func TestTokenDiscovery_PollFailure(t *testing.T) {
	var callCount atomic.Int32

	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			callCount.Add(1)
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			// GET fails.
			return nil, errors.New("network error")
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
		Timeout:     5 * time.Second,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	_, err = td.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error for poll failure")
	}
	var ee *EndpointError
	if !errors.As(err, &ee) {
		t.Fatalf("expected *EndpointError, got %T: %v", err, err)
	}
	if ee.Method != http.MethodGet {
		t.Errorf("Method = %q, want %q", ee.Method, http.MethodGet)
	}
}

func TestTokenDiscovery_PollBadStatus(t *testing.T) {
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			return newMockResponse(http.StatusServiceUnavailable, "unavailable"), nil
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	_, err = td.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error for bad poll status")
	}
	var ee *EndpointError
	if !errors.As(err, &ee) {
		t.Fatalf("expected *EndpointError, got %T: %v", err, err)
	}
}

func TestTokenDiscovery_ExcludesSelf(t *testing.T) {
	nodesResp := tokenNodesResponse{
		Nodes: []tokenNode{
			{NodeID: 42, Address: "10.0.0.42:5000"},
			{NodeID: 99, Address: "10.0.0.99:5000"},
		},
	}
	nodesJSON, _ := json.Marshal(nodesResp)

	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			return newMockResponse(http.StatusOK, string(nodesJSON)), nil
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      42,
		Address:     "10.0.0.42:5000",
		ClusterSize: 2,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	peers, err := td.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover() error: %v", err)
	}

	if len(peers) != 1 {
		t.Fatalf("expected 1 peer (self excluded), got %d", len(peers))
	}
	if peers[0].NodeID != 99 {
		t.Errorf("NodeID = %d, want 99", peers[0].NodeID)
	}
	if peers[0].Address != "10.0.0.99:5000" {
		t.Errorf("Address = %q, want %q", peers[0].Address, "10.0.0.99:5000")
	}
}

func TestTokenDiscovery_DefaultTimeout(t *testing.T) {
	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		Address:     "10.0.0.1:5000",
		ClusterSize: 1,
		HTTPClient:  &mockHTTPClient{},
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}
	if td.cfg.Timeout != 5*time.Minute {
		t.Errorf("Timeout = %v, want %v", td.cfg.Timeout, 5*time.Minute)
	}
}

func TestTokenDiscovery_DefaultHTTPClient(t *testing.T) {
	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		Address:     "10.0.0.1:5000",
		ClusterSize: 1,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}
	if td.client != http.DefaultClient {
		t.Error("expected http.DefaultClient as default")
	}
}

func TestTokenDiscovery_StopIdempotent(t *testing.T) {
	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		Address:     "10.0.0.1:5000",
		ClusterSize: 1,
		HTTPClient:  &mockHTTPClient{},
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	if err := td.Stop(); err != nil {
		t.Fatalf("first Stop() error: %v", err)
	}
	if err := td.Stop(); err != nil {
		t.Fatalf("second Stop() error: %v", err)
	}
}

func TestTokenDiscovery_PollInvalidJSON(t *testing.T) {
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			return newMockResponse(http.StatusOK, "not-json"), nil
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	_, err = td.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	var ee *EndpointError
	if !errors.As(err, &ee) {
		t.Fatalf("expected *EndpointError, got %T: %v", err, err)
	}
}

func TestTokenDiscovery_ClusterSizeOne(t *testing.T) {
	nodesResp := tokenNodesResponse{
		Nodes: []tokenNode{
			{NodeID: 1, Address: "10.0.0.1:5000"},
		},
	}
	nodesJSON, _ := json.Marshal(nodesResp)

	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			return newMockResponse(http.StatusOK, string(nodesJSON)), nil
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 1,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	peers, err := td.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover() error: %v", err)
	}

	// Self is excluded, so no peers returned.
	if len(peers) != 0 {
		t.Fatalf("expected 0 peers (self excluded), got %d", len(peers))
	}
}

func TestTokenDiscovery_URLConstruction(t *testing.T) {
	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "abc-123",
		NodeID:      42,
		Address:     "10.0.0.1:5000",
		ClusterSize: 1,
		HTTPClient:  &mockHTTPClient{},
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	nodeURL := td.buildNodeURL()
	expected := "https://discovery.example.com/v1/discovery/nodes/42"
	if nodeURL != expected {
		t.Errorf("buildNodeURL() = %q, want %q", nodeURL, expected)
	}

	nodesURL := td.buildNodesURL()
	expected = "https://discovery.example.com/v1/discovery/nodes"
	if nodesURL != expected {
		t.Errorf("buildNodesURL() = %q, want %q", nodesURL, expected)
	}
}

func TestTokenDiscovery_InsecureHTTPRejected(t *testing.T) {
	_, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "http://discovery.example.com",
		Token:       "test-token",
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
	})
	if err == nil {
		t.Fatal("expected error for http:// endpoint")
	}
	var ie *InsecureDiscoveryEndpointError
	if !errors.As(err, &ie) {
		t.Fatalf("expected *InsecureDiscoveryEndpointError, got %T: %v", err, err)
	}
	if ie.Endpoint != "http://discovery.example.com" {
		t.Errorf("Endpoint = %q, want %q", ie.Endpoint, "http://discovery.example.com")
	}
}

func TestTokenDiscovery_InsecureHTTPAllowed(t *testing.T) {
	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:          "http://discovery.example.com",
		Token:             "test-token",
		Address:           "10.0.0.1:5000",
		ClusterSize:       1,
		AllowInsecureHTTP: true,
		HTTPClient:        &mockHTTPClient{},
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}
	if td == nil {
		t.Fatal("expected non-nil TokenDiscovery")
	}
}

func TestTokenDiscovery_HTTPSAccepted(t *testing.T) {
	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		Address:     "10.0.0.1:5000",
		ClusterSize: 1,
		HTTPClient:  &mockHTTPClient{},
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}
	if td == nil {
		t.Fatal("expected non-nil TokenDiscovery")
	}
}

func TestTokenDiscovery_AuthorizationHeader(t *testing.T) {
	var capturedHeaders []http.Header

	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			capturedHeaders = append(capturedHeaders, req.Header.Clone())
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			nodesResp := tokenNodesResponse{
				Nodes: []tokenNode{
					{NodeID: 1, Address: "10.0.0.1:5000"},
				},
			}
			nodesJSON, _ := json.Marshal(nodesResp)
			return newMockResponse(http.StatusOK, string(nodesJSON)), nil
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "secret-token-123",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 1,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	_, err = td.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover() error: %v", err)
	}

	if len(capturedHeaders) < 2 {
		t.Fatalf("expected at least 2 requests (register + poll), got %d", len(capturedHeaders))
	}

	for i, h := range capturedHeaders {
		auth := h.Get("Authorization")
		expected := "Bearer secret-token-123"
		if auth != expected {
			t.Errorf("request[%d] Authorization = %q, want %q", i, auth, expected)
		}
	}
}

func TestTokenDiscovery_TokenNotInURL(t *testing.T) {
	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "secret-token-456",
		NodeID:      42,
		Address:     "10.0.0.1:5000",
		ClusterSize: 1,
		HTTPClient:  &mockHTTPClient{},
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	nodeURL := td.buildNodeURL()
	if strings.Contains(nodeURL, "secret-token-456") {
		t.Errorf("buildNodeURL() contains token in URL: %q", nodeURL)
	}

	nodesURL := td.buildNodesURL()
	if strings.Contains(nodesURL, "secret-token-456") {
		t.Errorf("buildNodesURL() contains token in URL: %q", nodesURL)
	}
}

// BenchmarkTokenDiscoveryDiscover measures the performance of token-based
// discovery with a mock HTTP client returning 3 nodes.
func BenchmarkTokenDiscoveryDiscover(b *testing.B) {
	b.ReportAllocs()

	nodesResp := tokenNodesResponse{
		Nodes: []tokenNode{
			{NodeID: 1, Address: "10.0.0.1:5000"},
			{NodeID: 2, Address: "10.0.0.2:5000"},
			{NodeID: 3, Address: "10.0.0.3:5000"},
		},
	}
	nodesJSON, err := json.Marshal(nodesResp)
	if err != nil {
		b.Fatalf("failed to marshal nodes: %v", err)
	}

	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			return newMockResponse(http.StatusOK, string(nodesJSON)), nil
		},
	}

	for b.Loop() {
		td, err := NewTokenDiscovery(TokenConfig{
			Endpoint:    "https://discovery.example.com",
			Token:       "bench-token",
			NodeID:      1,
			Address:     "10.0.0.1:5000",
			ClusterSize: 3,
			Timeout:     5 * time.Second,
			HTTPClient:  client,
		})
		if err != nil {
			b.Fatalf("NewTokenDiscovery() error: %v", err)
		}
		_, _ = td.Discover(context.Background())
	}
}

// ---------------------------------------------------------------------------
// Coverage: Discover context cancellation path
// ---------------------------------------------------------------------------

func TestTokenDiscovery_ContextCanceled(t *testing.T) {
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			// Return insufficient nodes to force polling.
			nodesResp := tokenNodesResponse{
				Nodes: []tokenNode{{NodeID: 1, Address: "10.0.0.1:5000"}},
			}
			data, _ := json.Marshal(nodesResp)
			return newMockResponse(http.StatusOK, string(data)), nil
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
		Timeout:     5 * time.Second,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err = td.Discover(ctx)
	if err == nil {
		t.Fatal("expected error for canceled context")
	}
	var ce *CancelledError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CancelledError, got %T: %v", err, err)
	}
}

// ---------------------------------------------------------------------------
// Coverage: poll ResponseTooLargeError
// ---------------------------------------------------------------------------

func TestTokenDiscovery_PollResponseTooLarge(t *testing.T) {
	// Create a response body larger than maxTokenResponseBytes (1 MiB).
	largeBody := strings.Repeat("x", 1<<20+100)

	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			return newMockResponse(http.StatusOK, largeBody), nil
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	_, err = td.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error for oversized response")
	}
	var rtl *ResponseTooLargeError
	if !errors.As(err, &rtl) {
		t.Fatalf("expected *ResponseTooLargeError, got %T: %v", err, err)
	}
	if rtl.MaxBytes != maxTokenResponseBytes {
		t.Errorf("ResponseTooLargeError.MaxBytes = %d, want %d", rtl.MaxBytes, maxTokenResponseBytes)
	}
}

// ---------------------------------------------------------------------------
// Coverage: register HTTP request creation error
// ---------------------------------------------------------------------------

func TestTokenDiscovery_RegisterRequestCreationError(t *testing.T) {
	// Use an endpoint with an invalid URL to trigger NewRequestWithContext error.
	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 1,
		HTTPClient: &mockHTTPClient{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return newMockResponse(http.StatusOK, ""), nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	// Override the endpoint to an invalid URL that will cause request creation to fail.
	td.cfg.Endpoint = "://invalid-url"
	err = td.register(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
	var re *RegistrationError
	if !errors.As(err, &re) {
		t.Fatalf("expected *RegistrationError, got %T: %v", err, err)
	}
}

// ---------------------------------------------------------------------------
// Coverage: poll HTTP request creation error
// ---------------------------------------------------------------------------

func TestTokenDiscovery_PollRequestCreationError(t *testing.T) {
	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 1,
		HTTPClient: &mockHTTPClient{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return newMockResponse(http.StatusOK, ""), nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	// Override endpoint to cause request creation failure in poll.
	td.cfg.Endpoint = "://invalid"
	_, err = td.poll(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid URL in poll")
	}
	var ee *EndpointError
	if !errors.As(err, &ee) {
		t.Fatalf("expected *EndpointError, got %T: %v", err, err)
	}
}

// ---------------------------------------------------------------------------
// Coverage: poll read error
// ---------------------------------------------------------------------------

func TestTokenDiscovery_PollReadError(t *testing.T) {
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut {
				return newMockResponse(http.StatusOK, ""), nil
			}
			// Return a response with a reader that fails.
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(&failingReader{err: errors.New("read error")}),
				Header:     make(http.Header),
			}, nil
		},
	}

	td, err := NewTokenDiscovery(TokenConfig{
		Endpoint:    "https://discovery.example.com",
		Token:       "test-token",
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		ClusterSize: 3,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("NewTokenDiscovery() error: %v", err)
	}

	_, err = td.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error for read failure")
	}
	var ee *EndpointError
	if !errors.As(err, &ee) {
		t.Fatalf("expected *EndpointError, got %T: %v", err, err)
	}
}

// failingReader is a reader that always returns an error.
type failingReader struct {
	err error
}

func (r *failingReader) Read(p []byte) (int, error) {
	return 0, r.err
}
