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

package engine

import (
	"sync"
	"testing"
	"time"
)

// TestEngine_StopIsIdempotent checks a second Engine.Stop does not panic.
func TestEngine_StopIsIdempotent(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)
	e.Start()

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.Stop()
		e.Stop()
		e.Stop()
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("repeated Engine.Stop did not complete; the second call is " +
			"either panicking or deadlocking")
	}
}

// TestEngine_StopConcurrentCallersAllReturn checks concurrent Stop callers all return only after shutdown finishes.
func TestEngine_StopConcurrentCallersAllReturn(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)
	e.Start()

	const callers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			<-start
			e.Stop()
		}()
	}
	close(start)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent Engine.Stop callers did not all return")
	}

	// Every tier must actually be stopped once the calls have returned; a
	// stopOnce that swallowed the shutdown entirely would also pass the
	// timeout above.
	for _, tier := range []struct {
		name string
		c    chan struct{}
	}{
		{"step", e.stepStopC},
		{"commit", e.commitStopC},
		{"apply", e.applyStopC},
		{"snapshot", e.snapshotStopC},
	} {
		select {
		case <-tier.c:
		default:
			t.Errorf("%s stop channel is still open after Stop returned; the "+
				"shutdown did not run", tier.name)
		}
	}
}
