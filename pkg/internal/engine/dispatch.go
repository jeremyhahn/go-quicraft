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

// listenerQueueSize bounds the notification queue. Only COLD-PATH
// notifications travel on it — leadership transitions, campaign launches and
// terminal shard failures — so a thousand slots is several orders of magnitude
// more than a healthy host produces between dispatcher wake-ups. The queue
// fills only when a listener violates the documented non-blocking contract, and
// what happens then is spelled out on dispatch.
const listenerQueueSize = 1024

// listenerDispatcher runs user-visible notifications on ONE dedicated goroutine
// instead of on the engine worker that produced them.
//
// # Why this exists
//
// Every callback the step worker fired ran synchronously on the step-worker
// goroutine WHILE that goroutine held stepWorker.nodesMu.RLock — tick() and
// processReady() each hold it for their whole body. Host.StopShard re-enters the
// same goroutine's nodesMu.Lock() via Engine.UnloadNode -> stepWorker.unloadNode,
// and Go's RWMutex is not reentrant, so an operator handler that reacted to an
// event by stopping the shard did not stall, it DEADLOCKED — permanently, and
// not only for that shard: every shard mapped to the same worker
// (shardID % numWorkers) stopped ticking, so no heartbeats, no elections and no
// inbox drain, and a later Host.Close never returned from Engine.Stop's
// stepWg.Wait(). "React to a failed shard by stopping it" is the obvious handler
// to write, which is what made it reachable.
//
// Moving the notification off the worker goroutine removes the class rather than
// the instance: the dispatcher goroutine holds no engine lock, so a listener may
// call any Host API, including the synchronous StopShard.
//
// # Ordering guarantee
//
// Notifications are consumed by exactly one goroutine from a FIFO channel, so
// they are delivered in the order the engine produced them — a TOTAL order, and
// therefore per-shard order as well. A listener that watched a shard go
// leader -> follower -> leader still sees those three events in that sequence.
//
// There is deliberately NO ordering guarantee between dispatched notifications
// and the callbacks that remain synchronous (OnApplied, OnCommitted,
// OnReadyToRead, OnProposalFailed, OnReadIndexBatched, OnReadIndexFailed). Those
// resolve pending client requests and have documented happens-before
// relationships with raft.Peer.Handle — OnReadIndexBatched must register its
// aliases before the ReadIndex reaches raft, or a single-node shard completes
// the batch key before the aliases exist — so they cannot be queued behind user
// code. They are also unordered with respect to each other (they run on three
// different worker tiers).
//
// # Queue-full policy
//
// dispatch NEVER blocks the producing worker. Blocking would reintroduce the
// deadlock it exists to remove: a worker parked on a full queue still holds
// nodesMu.RLock, so a listener calling StopShard from the dispatcher goroutine
// would wait on nodesMu.Lock() behind that parked reader, and neither would
// move again.
//
// It also never drops silently. dispatch reports acceptance, and each caller
// chooses a policy that suits its event:
//
//   - Leadership changes are RETRIED, not dropped. publishLeaderChange commits
//     its prevLeaders/prevLeaderTerms bookkeeping only on acceptance, so a
//     rejected publish is re-attempted on the next tick and the notification
//     cannot be lost. This matters: OnLeaderUpdated is what maintains the
//     Host's isLeader mirror, and losing one would leave ShardInfo
//     permanently wrong. Read readiness (OnLeaderReady) is retried the same
//     way, and only once its leader change was accepted: a consumer waiting
//     on it has nothing else to wake it.
//   - Campaign launches and shard failures are dropped, counted in
//     ListenerNotificationsDropped, and logged at ERROR with the shard, the
//     reason and the running total, so the event still reaches the operator's
//     log even though it does not reach their listener. Retaining them instead
//     would mean holding engine liveness hostage to a listener that is, by
//     construction, not consuming.
type listenerDispatcher struct {
	q chan func()

	// running gates the queueing path. While false — before Engine.Start and
	// after Engine.Stop, and in unit tests that build workers without an engine
	// — there is no goroutine to consume the queue, so dispatch invokes inline.
	// That is the correct behaviour rather than a fallback: with no worker
	// goroutines running there is no lock to be reentrant against.
	running atomic.Bool

	// dropped counts notifications refused because the queue was full.
	dropped atomic.Uint64

	// started records whether a goroutine was ever launched, so stop can tell
	// "the dispatcher is draining" from "there is nothing to join". An engine
	// that is constructed and stopped without ever being started is ordinary —
	// unit tests do it constantly — and stop must not wait on a doneC nobody
	// will ever close.
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

// start launches the dispatcher goroutine and tracks it on wg. It must run
// BEFORE the worker tiers so no worker can enqueue into a queue nothing is
// draining.
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

// invoke runs one notification, containing a panic in user listener code. The
// synchronous call sites this replaced were covered by the worker's own
// restart-on-panic recovery; without an equivalent here a panicking listener
// would take down the whole process, which is strictly worse than the behaviour
// it replaced.
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

// dispatch queues fn for delivery on the dispatcher goroutine and reports
// whether it was accepted. It never blocks. See the type documentation for the
// ordering guarantee and the queue-full policy.
//
// A nil receiver, or a dispatcher whose goroutine is not running, invokes fn
// inline and reports acceptance: there is no worker goroutine holding a lock in
// that state, so there is nothing to be reentrant against.
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

// stop shuts the dispatcher down. It must run AFTER every worker tier has
// exited, so no producer remains that could strand a notification in the queue.
// The final non-blocking drain covers a notification enqueued between the
// running flag going false and the goroutine's own drain completing.
//
// It is idempotent, and safe on a dispatcher that was never started: an engine
// constructed and stopped without Start has no goroutine to join, so waiting on
// doneC would hang Stop forever.
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
