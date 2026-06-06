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

// Package stopper provides a goroutine lifecycle helper for managing
// graceful shutdown of worker goroutines.
package stopper

import (
	"sync"
	"sync/atomic"
)

// Stopper manages graceful goroutine shutdown. Workers launched via
// RunWorker should select on ShouldStop() to know when to exit.
type Stopper struct {
	wg      sync.WaitGroup
	stopCh  chan struct{}
	stopped atomic.Bool
	// mu serializes RunWorker's wg.Add with Stop setting stopped, so a
	// wg.Add can never race a wg.Wait that has already returned (the
	// "WaitGroup misuse: Add called concurrently with Wait" panic).
	mu sync.Mutex
}

// New creates a new Stopper ready to track worker goroutines.
func New() *Stopper {
	return &Stopper{
		stopCh: make(chan struct{}),
	}
}

// RunWorker launches a goroutine tracked by the Stopper. The goroutine
// function should select on ShouldStop() to detect when shutdown has
// been requested.
//
// Example:
//
//	s.RunWorker(func() {
//	    for {
//	        select {
//	        case <-s.ShouldStop():
//	            return
//	        default:
//	            // do work
//	        }
//	    }
//	})
func (s *Stopper) RunWorker(f func()) {
	// Hold mu across the stopped-check and wg.Add so they are atomic with
	// respect to Stop (which sets stopped under the same mu before Wait).
	// This closes the TOCTOU window where a check-then-Add could race a
	// Wait that already returned. After stop, the worker is not launched.
	s.mu.Lock()
	if s.stopped.Load() {
		s.mu.Unlock()
		return
	}
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		f()
	}()
}

// ShouldStop returns a channel that is closed when Stop is called.
// Workers should select on this channel to detect shutdown.
func (s *Stopper) ShouldStop() <-chan struct{} {
	return s.stopCh
}

// Stop signals all workers to stop and waits for them to finish.
// Stop is idempotent and safe to call multiple times.
func (s *Stopper) Stop() {
	// Set stopped under mu so it is ordered before any concurrent RunWorker's
	// wg.Add (which is also under mu). Once we release mu with stopped=true,
	// no further RunWorker will Add, so the wg.Wait below cannot race a
	// late Add. mu is NOT held across Wait (workers don't take mu).
	s.mu.Lock()
	first := s.stopped.CompareAndSwap(false, true)
	s.mu.Unlock()
	if first {
		close(s.stopCh)
	}
	s.wg.Wait()
}

// Close is an alias for Stop that implements io.Closer.
// Always returns nil.
func (s *Stopper) Close() error {
	s.Stop()
	return nil
}
