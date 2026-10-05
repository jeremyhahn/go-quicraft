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

// Package supervise keeps long-lived goroutines running when code they call
// ends them abnormally, and keeps the diagnostics of that from doing harm.
//
// A goroutine that runs user code — a state machine, a callback, a metrics
// collector, a slog handler, a user LogDB — or a library that may panic can
// be ended two ways besides a normal return: a panic, which recover stops, and
// runtime.Goexit, which nothing stops and for which recover returns nil. The
// package gives every recovery and supervision path in the library the same
// four tools for both:
//
//   - ClassifyExit tells a deferred check how a run ended, reporting a Goexit
//     as ErrGoexit so the recovery treats it exactly as a panic.
//   - Supervise runs a worker's restart loop and starts it again on a fresh
//     goroutine when a Goexit ends it, with the caller's WaitGroup accounting
//     and the restart wait (AwaitRestart, RestartDelay) a panic restart takes.
//   - RunResilient runs a shutdown or cleanup path to completion on helper
//     goroutines, so no abnormal exit inside it can leave its caller waiting.
//   - LogContained emits a diagnostic on a goroutine of its own.
//
// The rule they serve is settle first, log last: a recovery settles
// everything structural, and publishes every completion signal, from defers
// user code cannot skip, before any diagnostic is emitted, and the diagnostic
// goes through LogContained, because the slog handler and the String or Error
// method of a recovered panic value are user code too.
//
// The package depends on the standard library only, so every layer that runs
// such code — the engine, the transport and the host — can import it.
package supervise

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
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

// ClassifyExit classifies how a worker run ended, from its deferred check.
// recovered must be recover()'s result, called directly by that deferred
// function; normalReturn is set by the run's last statement. abnormal is false
// for a normal return. Otherwise the run ended in a panic, whose value is
// returned, or in runtime.Goexit — the run did not return normally, yet
// recover returns nil — reported as ErrGoexit so the recovery treats it
// exactly as a panic. A Goexit cannot be stopped: it ends the goroutine once
// the deferred calls have run, and Supervise restarts the loop on a
// fresh one.
func ClassifyExit(recovered any, normalReturn bool) (value any, abnormal bool) {
	if recovered != nil {
		return recovered, true
	}
	if !normalReturn {
		return ErrGoexit, true
	}
	return nil, false
}

// Supervise runs loop — a worker's restart-on-panic loop — on a
// goroutine counted by wg. A panic in the worker is recovered inside loop,
// which restarts it; a runtime.Goexit cannot be recovered and ends the
// goroutine, loop included, so this restarts loop on a fresh goroutine, after
// the same wait a panic restart takes and with the same wg accounting: the
// replacement is counted before the exiting goroutine is released. stopped,
// when not nil, runs instead of the restart if stopC closes meanwhile; a loop
// that signals its own completion (a done channel, say) must signal it there
// too, and only on its normal return otherwise — never from a deferred call,
// which also runs on the Goexit being restarted. wg may be nil.
//
// It supervises every long-lived goroutine that runs user code or callbacks:
// the engine's apply, commit and step workers, each per-shard apply
// goroutine, the snapshot pool workers and the listener dispatcher, the
// host's key-rotation loop, and the transport's send-queue workers,
// connection cleanup and accept loop.
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

// RunResilient runs fn to completion on helper goroutines, starting a fresh
// one whenever user code inside fn ends one with a panic or runtime.Goexit,
// and returns once fn has returned normally. The calling goroutine never runs
// fn itself, so no user code can end it. fn must consume what it has handled
// before running user code on it, so that a rerun resumes where the last run
// stopped instead of repeating it. It is for shutdown and cleanup paths that
// must finish — a drain whose callers then close a done channel.
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
						slog.Error("panic recovered in a shutdown drain; resuming it",
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

// LogContained emits a diagnostic on a goroutine of its own, so the slog
// handler — user code, through slog.SetDefault — and the String or Error
// method of a recovered panic value cannot reach the caller: a panic is
// recovered there, and a runtime.Goexit ends only that goroutine. It is
// fire-and-forget, never delaying or blocking its caller.
//
// Every recovery and supervision path uses it, and only after it has settled
// everything structural and published every completion signal: logging is
// never allowed to stand between an abnormal exit and its settlement.
func LogContained(emit func()) {
	go func() {
		defer func() { _ = recover() }()
		emit()
	}()
}
