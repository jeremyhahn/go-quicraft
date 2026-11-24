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
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
)

// listenerQueueSize bounds the cold-path notification queue; it fills only when a listener blocks.
const listenerQueueSize = 1024

// listenerDispatcher runs listener notifications in FIFO order on one goroutine holding no engine lock, so a listener may call any Host API.
// It never blocks a worker: leadership changes are retried, campaign launches and shard failures are dropped, counted and logged.
type listenerDispatcher struct {
	q chan func()

	// running gates queueing; while false there is no consumer and dispatch invokes inline.
	running atomic.Bool

	// dropped counts notifications refused because the queue was full.
	dropped atomic.Uint64

	// started records whether a goroutine was ever launched, so stop knows whether to join.
	started atomic.Bool

	// stopOnce keeps stop idempotent: Engine.Stop is documented as safe to call
	// twice, and a second close of stopC would panic.
	stopOnce sync.Once

	stopC chan struct{}
	doneC chan struct{}
	// doneOnce closes doneC once, from whichever exit path reaches it first.
	doneOnce sync.Once
}

// newListenerDispatcher creates a dispatcher with the standard queue size. The
// goroutine is not started until start is called.
func newListenerDispatcher() *listenerDispatcher {
	return &listenerDispatcher{
		q:     make(chan func(), listenerQueueSize),
		stopC: make(chan struct{}),
		doneC: make(chan struct{}),
	}
}

// start launches the dispatcher goroutine; it must run before the worker tiers.
func (d *listenerDispatcher) start(wg *sync.WaitGroup) {
	d.started.Store(true)
	d.running.Store(true)
	// Supervised: a listener that calls runtime.Goexit ends the goroutine,
	// which invoke's recover cannot prevent, and the dispatcher is restarted
	// on a fresh one. doneC is closed only on the final exit.
	supervise.Supervise(wg, d.stopC,
		func() {
			d.run()
			d.closeDone()
		},
		func() {
			// The stopped path runs user listeners too: the drain is
			// resilient, and doneC is closed whatever they do.
			defer d.closeDone()
			supervise.RunResilient(d.drain)
		})
}

// closeDone closes doneC exactly once, from whichever exit path reaches it:
// the dispatcher's own return, or its stopped path after a Goexit.
func (d *listenerDispatcher) closeDone() {
	d.doneOnce.Do(func() { close(d.doneC) })
}

// run consumes notifications until stopC closes, then drains whatever the
// workers enqueued before they exited.
func (d *listenerDispatcher) run() {
	for {
		select {
		case fn := <-d.q:
			d.invoke(fn)
		case <-d.stopC:
			supervise.RunResilient(d.drain)
			return
		}
	}
}

// drain delivers every queued notification without blocking. Called on the way
// out so an event produced during shutdown still reaches the listener.
func (d *listenerDispatcher) drain() {
	for {
		select {
		case fn := <-d.q:
			d.invoke(fn)
		default:
			return
		}
	}
}

// invoke runs one notification, recovering a panic in listener code.
func (d *listenerDispatcher) invoke(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			// Contained: a slog handler that panicked here would escape this
			// recover and take down the process.
			supervise.LogContained(func() {
				slog.Error("event listener panic recovered",
					"panic", fmt.Sprint(r),
					"stack", string(stack),
				)
			})
		}
	}()
	fn()
}

// dispatch queues fn for the dispatcher goroutine and reports acceptance without blocking; with no goroutine running it invokes fn inline.
func (d *listenerDispatcher) dispatch(fn func()) bool {
	if d == nil || !d.running.Load() {
		fn()
		return true
	}
	select {
	case d.q <- fn:
		return true
	default:
		d.dropped.Add(1)
		return false
	}
}

// stop shuts the dispatcher down after every worker tier has exited and drains the queue; idempotent and safe if never started.
func (d *listenerDispatcher) stop() {
	d.running.Store(false)
	d.stopOnce.Do(func() { close(d.stopC) })
	if d.started.Load() {
		<-d.doneC
	}
	// Runs user listeners: resilient, so a listener that Goexits cannot end
	// the goroutine stopping the engine (Engine.Stop, Host.Close).
	supervise.RunResilient(d.drain)
}

// droppedCount returns the number of notifications refused because the queue
// was full.
func (d *listenerDispatcher) droppedCount() uint64 {
	if d == nil {
		return 0
	}
	return d.dropped.Load()
}
