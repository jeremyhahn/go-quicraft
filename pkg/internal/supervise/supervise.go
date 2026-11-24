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

// Package supervise keeps goroutines that run user code alive through panics and runtime.Goexit (ClassifyExit, Supervise, RunResilient, RunEach) and logs diagnostics off the caller (LogContained); standard library only.
package supervise

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ErrGoexit stands in for the panic value of a worker run that
// ended in runtime.Goexit, for which recover returns nil (ClassifyExit).
var ErrGoexit = errors.New("supervise: goroutine exited via runtime.Goexit")

// RestartDelay is the delay before restarting a panicked
// worker. This prevents tight panic-restart loops from consuming CPU
// and flooding logs when a worker hits a persistent panic trigger.
const RestartDelay = 100 * time.Millisecond

// AwaitRestart waits out RestartDelay before a worker that
// ended abnormally is run again, and reports false, without waiting, once
// stopC is closed: the worker is then not restarted.
func AwaitRestart(stopC <-chan struct{}) bool {
	select {
	case <-stopC:
		return false
	default:
	}
	t := time.NewTimer(RestartDelay)
	defer t.Stop()
	select {
	case <-stopC:
		return false
	case <-t.C:
		return true
	}
}

// ClassifyExit classifies how a run ended from its deferred check: normal, a panic (value returned) or a runtime.Goexit (ErrGoexit).
func ClassifyExit(recovered any, normalReturn bool) (value any, abnormal bool) {
	if recovered != nil {
		return recovered, true
	}
	if !normalReturn {
		return ErrGoexit, true
	}
	return nil, false
}

// Supervise runs loop on a goroutine counted by wg and restarts it on a fresh goroutine after a runtime.Goexit; stopped runs instead if stopC closes. wg may be nil.
func Supervise(wg *sync.WaitGroup, stopC <-chan struct{}, loop, stopped func()) {
	add, done := func() {}, func() {}
	if wg != nil {
		add, done = func() { wg.Add(1) }, wg.Done
	}
	add()
	go func() {
		normalReturn := false
		defer func() {
			if !normalReturn {
				add()
				go func() {
					defer done()
					if !AwaitRestart(stopC) {
						if stopped != nil {
							stopped()
						}
						return
					}
					Supervise(wg, stopC, loop, stopped)
				}()
			}
			done()
		}()
		loop()
		normalReturn = true
	}()
}

// RunResilient runs fn to completion on helper goroutines, restarting after a panic or Goexit; fn must consume work before running user code on it.
func RunResilient(fn func()) {
	for {
		normal := make(chan bool, 1)
		go func() {
			ok := false
			defer func() {
				r := recover()
				// The outcome is published first: nothing below may keep the
				// caller waiting.
				normal <- ok
				if r != nil {
					LogContained(func() {
						slog.Error("panic recovered in a resilient run; resuming it",
							"panic", fmt.Sprint(r))
					})
				}
			}()
			fn()
			ok = true
		}()
		if <-normal {
			return
		}
	}
}

// RunEach calls call(0) through call(n-1) in order on a helper goroutine; a call that panics or exits ends only itself and none runs twice.
func RunEach(n int, call func(i int)) {
	if n <= 0 {
		return
	}
	next := 0
	RunResilient(func() {
		for next < n {
			i := next
			next++
			call(i)
		}
	})
}

// DiagnosticQueueCapacity is how many contained diagnostics may wait for the
// slog handler at once (LogContained). A record submitted while the queue is
// full is dropped and counted in DiagnosticsDropped.
const DiagnosticQueueCapacity = 256

// contained is the process-wide contained-diagnostics worker, one bounded queue in front of the slog default handler.
var contained = newDiagnosticWorker(DiagnosticQueueCapacity)

// LogContained queues emit for the contained-diagnostics worker without blocking, or drops and counts it when the queue is full; emit must capture everything it logs.
func LogContained(emit func()) {
	contained.submit(emit)
}

// DiagnosticsDropped returns how many contained diagnostics were dropped because the queue was full; a rising count means a slow slog handler.
func DiagnosticsDropped() uint64 {
	return contained.dropped.Load()
}

// diagnosticWorker runs contained diagnostics on at most one goroutine, which
// exists only while its queue is not empty.
type diagnosticWorker struct {
	queue   chan func()
	running atomic.Bool
	dropped atomic.Uint64
}

// newDiagnosticWorker returns an idle worker whose queue holds capacity
// records.
func newDiagnosticWorker(capacity int) *diagnosticWorker {
	return &diagnosticWorker{queue: make(chan func(), capacity)}
}

// submit queues emit without blocking, dropping and counting it when the
// queue is full, and makes sure a worker goroutine will run what is queued.
func (w *diagnosticWorker) submit(emit func()) {
	select {
	case w.queue <- emit:
	default:
		w.dropped.Add(1)
	}
	if w.running.CompareAndSwap(false, true) {
		Supervise(nil, nil, w.drain, nil)
	}
}

// drain runs queued records until the queue is empty, then releases the worker, retaking it for a record queued in between.
func (w *diagnosticWorker) drain() {
	for {
		select {
		case emit := <-w.queue:
			runContained(emit)
		default:
			w.running.Store(false)
			if len(w.queue) == 0 || !w.running.CompareAndSwap(false, true) {
				return
			}
		}
	}
}

// runContained runs emit, recovering a panic in it so the worker goes on with
// the next record. A runtime.Goexit cannot be recovered; it ends the worker
// goroutine, and Supervise starts a fresh one.
func runContained(emit func()) {
	defer func() { _ = recover() }()
	emit()
}
