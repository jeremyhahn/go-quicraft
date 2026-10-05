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

package supervise

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// hostileHandler is a slog handler — user-configurable through slog.SetDefault
// — that runs act on every record: a panic or runtime.Goexit.
type hostileHandler struct{ act func() }

func (h hostileHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h hostileHandler) Handle(context.Context, slog.Record) error {
	h.act()
	return nil
}
func (h hostileHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h hostileHandler) WithGroup(string) slog.Handler      { return h }

// installHostileLogger makes the default slog handler run act until the test
// ends.
func installHostileLogger(t *testing.T, act func()) {
	t.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(hostileHandler{act: act}))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

// hostileExits are the two ways a slog handler can leave its caller.
var hostileExits = map[string]func(){
	"panic":  func() { panic("slog handler panicked") },
	"Goexit": runtime.Goexit,
}

// await fails the test unless c closes within five seconds.
func await(t *testing.T, c <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(5 * time.Second):
		t.Fatal(what)
	}
}

func TestClassifyExit(t *testing.T) {
	cases := map[string]struct {
		recovered    any
		normalReturn bool
		wantValue    any
		wantAbnormal bool
	}{
		"normal return": {nil, true, nil, false},
		"panic":         {"boom", false, "boom", true},
		"Goexit":        {nil, false, ErrGoexit, true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			v, abnormal := ClassifyExit(c.recovered, c.normalReturn)
			if v != c.wantValue || abnormal != c.wantAbnormal {
				t.Fatalf("ClassifyExit = (%v, %v), want (%v, %v)", v, abnormal, c.wantValue, c.wantAbnormal)
			}
		})
	}
}

func TestAwaitRestart(t *testing.T) {
	stopC := make(chan struct{})
	if !AwaitRestart(stopC) {
		t.Fatal("AwaitRestart refused a restart with stopC open")
	}
	close(stopC)
	if AwaitRestart(stopC) {
		t.Fatal("AwaitRestart allowed a restart after stopC closed")
	}
}

// TestSupervise_RestartsAfterGoexit: a loop ended by runtime.Goexit is run
// again on a fresh goroutine, and wg counts it throughout.
func TestSupervise_RestartsAfterGoexit(t *testing.T) {
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	var runs atomic.Int32
	finished := make(chan struct{})
	Supervise(&wg, stopC, func() {
		if runs.Add(1) == 1 {
			runtime.Goexit()
		}
		close(finished)
	}, nil)
	await(t, finished, "the loop was not restarted after a Goexit")
	wg.Wait()
	if got := runs.Load(); got != 2 {
		t.Fatalf("loop ran %d times, want 2", got)
	}
}

// TestSupervise_StoppedRunsInsteadOfRestart: once stopC is closed, a loop
// ended by runtime.Goexit is not restarted; stopped runs instead.
func TestSupervise_StoppedRunsInsteadOfRestart(t *testing.T) {
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	close(stopC)
	var runs atomic.Int32
	stopped := make(chan struct{})
	Supervise(&wg, stopC, func() {
		runs.Add(1)
		runtime.Goexit()
	}, func() { close(stopped) })
	await(t, stopped, "stopped did not run")
	wg.Wait()
	if got := runs.Load(); got != 1 {
		t.Fatalf("loop ran %d times, want 1", got)
	}
}

// TestRunResilient_PublishesBeforeLogging: RunResilient reports a run's
// outcome before it logs anything, so a slog handler that panics or Goexits
// cannot leave its caller waiting forever.
func TestRunResilient_PublishesBeforeLogging(t *testing.T) {
	for exitName, exit := range hostileExits {
		t.Run(exitName, func(t *testing.T) {
			installHostileLogger(t, exit)
			done := make(chan struct{})
			go func() {
				defer close(done)
				first := true
				RunResilient(func() {
					if first {
						first = false
						panic("fn panicked")
					}
				})
			}()
			await(t, done, "RunResilient never returned: its diagnostic ran before the outcome was published")
		})
	}
}

// TestRunResilient_ResumesAfterGoexit: a run ended by runtime.Goexit is run
// again until it returns normally.
func TestRunResilient_ResumesAfterGoexit(t *testing.T) {
	var runs atomic.Int32
	RunResilient(func() {
		if runs.Add(1) == 1 {
			runtime.Goexit()
		}
	})
	if got := runs.Load(); got != 2 {
		t.Fatalf("fn ran %d times, want 2", got)
	}
}

// TestLogContained_HostileHandler: a handler that panics or Goexits ends only
// the diagnostic's own goroutine.
func TestLogContained_HostileHandler(t *testing.T) {
	for exitName, exit := range hostileExits {
		t.Run(exitName, func(t *testing.T) {
			installHostileLogger(t, exit)
			emitted := make(chan struct{})
			LogContained(func() {
				defer close(emitted)
				slog.Error("diagnostic")
			})
			await(t, emitted, "the diagnostic never ran")
		})
	}
}
