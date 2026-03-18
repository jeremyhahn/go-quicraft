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
	"sync"
	"time"
)

// Manager tries discovery methods in order with configurable retry logic.
// It returns the first successful result from any method.
type Manager struct {
	cfg   ManagerConfig
	peers []Peer
	mu    sync.RWMutex
}

// NewManager creates a discovery manager with the given configuration.
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = time.Second
	}
	return &Manager{cfg: cfg}
}

// Discover tries each configured method in order and returns the first
// successful result. If all methods fail, it retries up to MaxRetries times
// with RetryInterval between attempts. The provided context allows the
// caller to cancel discovery mid-retry, preventing the caller from being
// blocked for up to MaxRetries * RetryInterval during shutdown.
// Returns NoPeersError if all attempts are exhausted, or CancelledError
// if the context is cancelled.
func (m *Manager) Discover(ctx context.Context) ([]Peer, error) {
	if len(m.cfg.Methods) == 0 {
		return nil, &NoPeersError{Method: "manager"}
	}

	attempts := 1 + m.cfg.MaxRetries
	var lastErr error

	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			retryTimer := time.NewTimer(m.cfg.RetryInterval)
			select {
			case <-ctx.Done():
				retryTimer.Stop()
				return nil, &CancelledError{
					Method: "manager",
					Err:    ctx.Err(),
				}
			case <-retryTimer.C:
			}
		}

		// Check context before trying methods to avoid unnecessary work
		// after cancellation.
		if err := ctx.Err(); err != nil {
			return nil, &CancelledError{
				Method: "manager",
				Err:    err,
			}
		}

		for _, method := range m.cfg.Methods {
			peers, err := method.Discover(ctx)
			if err != nil {
				lastErr = err
				continue
			}
			if len(peers) > 0 {
				m.mu.Lock()
				m.peers = peers
				m.mu.Unlock()
				return peers, nil
			}
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, &NoPeersError{Method: "manager"}
}

// Peers returns the most recently discovered peers.
func (m *Manager) Peers() []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.peers) == 0 {
		return nil
	}
	result := make([]Peer, len(m.peers))
	copy(result, m.peers)
	return result
}

// Stop releases resources held by all configured methods.
func (m *Manager) Stop() error {
	var firstErr error
	for _, method := range m.cfg.Methods {
		if err := method.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
