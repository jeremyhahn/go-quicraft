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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// defaultTokenTimeout is the default timeout for token discovery.
	defaultTokenTimeout = 5 * time.Minute

	// tokenPollInterval is how often to poll for new nodes.
	tokenPollInterval = time.Second

	// tokenMethodName is the name returned by Name().
	tokenMethodName = "token"

	// maxTokenResponseBytes is the maximum allowed size for HTTP response
	// bodies from the discovery service. 1 MiB is generous for a JSON
	// list of node registrations and protects against memory exhaustion
	// from a compromised or malicious endpoint.
	maxTokenResponseBytes = 1 << 20 // 1 MiB
)

// HTTPClient is the interface for making HTTP requests. It is satisfied
// by *http.Client and enables mock injection for testing.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// TokenConfig configures token-based discovery. Nodes register with a
// discovery endpoint using a shared token and the service coordinates
// peer exchange, similar to etcd's discovery token protocol.
type TokenConfig struct {
	// Endpoint is the URL of the discovery service (e.g., "https://discovery.example.com").
	// Must use the https:// scheme unless AllowInsecureHTTP is set.
	Endpoint string

	// Token is the shared bootstrap token that identifies which cluster
	// to join. Sent as an Authorization: Bearer header, never in the URL.
	Token string

	// NodeID is this node's unique identifier.
	NodeID uint64

	// Address is this node's Raft address to announce.
	Address string

	// ClusterSize is the expected number of nodes in the cluster.
	// Discovery waits until this many nodes have registered.
	ClusterSize int

	// Timeout is how long to wait for all nodes to register.
	// Defaults to 5 minutes if zero.
	Timeout time.Duration

	// HTTPClient overrides the default HTTP client (for testing).
	HTTPClient HTTPClient

	// AllowInsecureHTTP permits http:// endpoints for development and
	// testing. In production, the discovery endpoint must use https://
	// to protect the bearer token in transit.
	AllowInsecureHTTP bool
}

// tokenNode represents a node in the discovery service response.
type tokenNode struct {
	NodeID  uint64 `json:"node_id"`
	Address string `json:"address"`
}

// tokenNodesResponse is the JSON response from the discovery service
// when listing registered nodes.
type tokenNodesResponse struct {
	Nodes []tokenNode `json:"nodes"`
}

// tokenRegisterBody is the JSON body sent when registering a node.
type tokenRegisterBody struct {
	Address string `json:"address"`
}

// TokenDiscovery implements token-based peer discovery by registering
// with a central discovery service and polling for other nodes.
type TokenDiscovery struct {
	cfg     TokenConfig
	client  HTTPClient
	stopped atomic.Bool
	stopCh  chan struct{}
}

// NewTokenDiscovery creates a TokenDiscovery that registers with and
// polls a discovery endpoint. It validates the configuration and applies
// defaults for Timeout and HTTPClient when not set.
func NewTokenDiscovery(cfg TokenConfig) (*TokenDiscovery, error) {
	if cfg.Endpoint == "" {
		return nil, &ValidationError{Field: "Endpoint", Reason: "must not be empty"}
	}
	if !cfg.AllowInsecureHTTP && !strings.HasPrefix(cfg.Endpoint, "https://") {
		return nil, &InsecureDiscoveryEndpointError{Endpoint: cfg.Endpoint}
	}
	if cfg.Token == "" {
		return nil, &ValidationError{Field: "Token", Reason: "must not be empty"}
	}
	if cfg.Address == "" {
		return nil, &ValidationError{Field: "Address", Reason: "must not be empty"}
	}
	if cfg.ClusterSize < 1 {
		return nil, &ValidationError{Field: "ClusterSize", Reason: "must be at least 1"}
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTokenTimeout
	}

	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	return &TokenDiscovery{
		cfg:    cfg,
		client: client,
		stopCh: make(chan struct{}),
	}, nil
}

// Discover registers this node with the discovery service and polls
// until ClusterSize nodes have registered, then returns all peers
// excluding self. It respects the configured Timeout and can be
// cancelled via Stop() or by cancelling the provided context. The
// context is propagated to all HTTP requests, enabling the caller
// to cancel in-flight I/O.
func (td *TokenDiscovery) Discover(ctx context.Context) ([]Peer, error) {
	if err := td.register(ctx); err != nil {
		return nil, err
	}

	deadline := time.NewTimer(td.cfg.Timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(tokenPollInterval)
	defer ticker.Stop()

	for {
		peers, err := td.poll(ctx)
		if err != nil {
			return nil, err
		}
		if len(peers) >= td.cfg.ClusterSize {
			return td.excludeSelf(peers), nil
		}

		select {
		case <-ctx.Done():
			return nil, &CancelledError{
				Method: tokenMethodName,
				Err:    ctx.Err(),
			}
		case <-deadline.C:
			return nil, &TimeoutError{Method: tokenMethodName}
		case <-td.stopCh:
			return nil, &TimeoutError{Method: tokenMethodName}
		case <-ticker.C:
			// Continue polling.
		}
	}
}

// Name returns "token".
func (td *TokenDiscovery) Name() string {
	return tokenMethodName
}

// Stop halts discovery polling by signalling the stop channel.
func (td *TokenDiscovery) Stop() error {
	if td.stopped.CompareAndSwap(false, true) {
		close(td.stopCh)
	}
	return nil
}

// register sends a PUT request to register this node with the discovery service.
// The provided context is propagated to the HTTP request.
func (td *TokenDiscovery) register(ctx context.Context) error {
	body := tokenRegisterBody{Address: td.cfg.Address}
	data, err := json.Marshal(body)
	if err != nil {
		return &RegistrationError{
			Method: tokenMethodName,
			Err:    err,
		}
	}

	url := td.buildNodeURL()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, strings.NewReader(string(data)))
	if err != nil {
		return &RegistrationError{
			Method: tokenMethodName,
			Err:    err,
		}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+td.cfg.Token)

	resp, err := td.client.Do(req)
	if err != nil {
		return &EndpointError{
			Method:   http.MethodPut,
			Endpoint: url,
			Err:      err,
		}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &RegistrationError{
			Method: tokenMethodName,
			Err:    errors.New("unexpected status: " + resp.Status),
		}
	}

	return nil
}

// poll fetches the list of registered nodes from the discovery service.
// The provided context is propagated to the HTTP request.
func (td *TokenDiscovery) poll(ctx context.Context) ([]tokenNode, error) {
	url := td.buildNodesURL()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, &EndpointError{
			Method:   http.MethodGet,
			Endpoint: url,
			Err:      err,
		}
	}
	req.Header.Set("Authorization", "Bearer "+td.cfg.Token)

	resp, err := td.client.Do(req)
	if err != nil {
		return nil, &EndpointError{
			Method:   http.MethodGet,
			Endpoint: url,
			Err:      err,
		}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &EndpointError{
			Method:   http.MethodGet,
			Endpoint: url,
			Err:      errors.New("unexpected status: " + resp.Status),
		}
	}

	limited := io.LimitReader(resp.Body, maxTokenResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, &EndpointError{
			Method:   http.MethodGet,
			Endpoint: url,
			Err:      err,
		}
	}
	if int64(len(data)) > maxTokenResponseBytes {
		return nil, &ResponseTooLargeError{
			Endpoint: url,
			MaxBytes: maxTokenResponseBytes,
		}
	}

	var nodesResp tokenNodesResponse
	if err := json.Unmarshal(data, &nodesResp); err != nil {
		return nil, &EndpointError{
			Method:   http.MethodGet,
			Endpoint: url,
			Err:      err,
		}
	}

	return nodesResp.Nodes, nil
}

// excludeSelf filters out this node from the discovered nodes and
// converts them to Peer values.
func (td *TokenDiscovery) excludeSelf(nodes []tokenNode) []Peer {
	peers := make([]Peer, 0, len(nodes))
	for _, n := range nodes {
		if n.NodeID == td.cfg.NodeID {
			continue
		}
		peers = append(peers, Peer(n))
	}
	return peers
}

// buildNodeURL constructs the URL for registering a specific node.
// The token is sent via Authorization header, not in the URL path.
func (td *TokenDiscovery) buildNodeURL() string {
	return td.cfg.Endpoint + "/v1/discovery/nodes/" + strconv.FormatUint(td.cfg.NodeID, 10)
}

// buildNodesURL constructs the URL for listing all registered nodes.
// The token is sent via Authorization header, not in the URL path.
func (td *TokenDiscovery) buildNodesURL() string {
	return td.cfg.Endpoint + "/v1/discovery/nodes"
}
