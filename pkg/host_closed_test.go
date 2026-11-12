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

package quicraft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// closedKind says what an exported *Host method owes its caller once
// Close has returned.
type closedKind int

const (
	// closedErr: the method returns an error matching ErrClosed.
	closedErr closedKind = iota + 1
	// closedGetter: the method has no error to return and is documented
	// as safe after Close; it must only return without panicking.
	closedGetter
	// closedSelf: Close itself, covered by TestHostClosedCloseIdempotent.
	closedSelf
)

// hostClosedContract classifies every exported *Host method. A method
// missing from this table fails TestHostClosedEveryMethod, so a new
// method is covered by default: whoever adds it states its contract here.
var hostClosedContract = map[string]closedKind{
	"Close": closedSelf,

	// Getters with no error result, safe after Close.
	"HasNodeInfo":             closedGetter,
	"HealthCheck":             closedGetter,
	"ID":                      closedGetter,
	"IsDraining":              closedGetter,
	"IsSealed":                closedGetter,
	"IsShardJoining":          closedGetter,
	"NoOPSession":             closedGetter,
	"NodeHostInfo":            closedGetter,
	"NodeIdentity":            closedGetter,
	"RaftAddress":             closedGetter,
	"ShardCount":              closedGetter,
	"UnregisterLeaderHandler": closedGetter,

	// Drain returns ErrClosed once Close has completed; Close's own call to
	// Drain, made while shutdown runs, is unaffected.
	"Drain": closedErr,

	// Everything else returns ErrClosed.
	"CloseSession":                       closedErr,
	"DiagPipelineState":                  closedErr,
	"EmergencySeal":                      closedErr,
	"DiagRaftState":                      closedErr,
	"DiagRemotes":                        closedErr,
	"DiagShardGates":                     closedErr,
	"ExecuteOnLeader":                    closedErr,
	"ExportSnapshot":                     closedErr,
	"FollowerRead":                       closedErr,
	"ImportSnapshot":                     closedErr,
	"LeaderID":                           closedErr,
	"LeaderIDAndTerm":                    closedErr,
	"LogRange":                           closedErr,
	"LogReader":                          closedErr,
	"NewSession":                         closedErr,
	"NewSessionWithOwner":                closedErr,
	"PauseShard":                         closedErr,
	"Propose":                            closedErr,
	"ProposeWithSession":                 closedErr,
	"QueryLocalNode":                     closedErr,
	"QueryRaftLog":                       closedErr,
	"ReadIndex":                          closedErr,
	"RegisterLeaderHandler":              closedErr,
	"RegisterSession":                    closedErr,
	"ReloadMTLS":                         closedErr,
	"RemoveData":                         closedErr,
	"RequestAddNode":                     closedErr,
	"RequestAddNodeWithIdentity":         closedErr,
	"RequestAddObserver":                 closedErr,
	"RequestAddObserverWithIdentity":     closedErr,
	"RequestAddWitness":                  closedErr,
	"RequestAddWitnessWithIdentity":      closedErr,
	"RequestCompaction":                  closedErr,
	"RequestLeaderTransfer":              closedErr,
	"RequestRemoveNode":                  closedErr,
	"RequestSnapshot":                    closedErr,
	"RequestUpdateWitnessPolicy":         closedErr,
	"ResumeShard":                        closedErr,
	"ShardInfo":                          closedErr,
	"ShardMembership":                    closedErr,
	"ShardReadReady":                     closedErr,
	"StaleRead":                          closedErr,
	"StaleReadBuf":                       closedErr,
	"StartShard":                         closedErr,
	"StateMachineHash":                   closedErr,
	"StopReplica":                        closedErr,
	"StopShard":                          closedErr,
	"SyncGetShardMembership":             closedErr,
	"SyncPropose":                        closedErr,
	"SyncProposeWithSession":             closedErr,
	"SyncRead":                           closedErr,
	"SyncReadBuf":                        closedErr,
	"SyncRemoveData":                     closedErr,
	"SyncRequestAddNode":                 closedErr,
	"SyncRequestAddNodeWithIdentity":     closedErr,
	"SyncRequestAddObserver":             closedErr,
	"SyncRequestAddObserverWithIdentity": closedErr,
	"SyncRequestAddWitness":              closedErr,
	"SyncRequestAddWitnessWithIdentity":  closedErr,
	"SyncRequestRemoveNode":              closedErr,
	"SyncRequestSnapshot":                closedErr,
	"SyncRequestUpdateWitnessPolicy":     closedErr,
}

// closedCallDeadline bounds each call on the closed host: a method that
// blocks after Close fails the test instead of hanging it.
const closedCallDeadline = 5 * time.Second

// closedArgs builds the arguments for a method from its parameter types.
// The cases are the explicit table of what each parameter type needs: a
// real context, a shard and replica ID, an address, a key, a session, a
// reader and a writer. Any other type gets its zero value, which is the
// least valid input: a method must fail with ErrClosed before it looks at
// its arguments.
func closedArgs(ctx context.Context, mt reflect.Type, session *Session) []reflect.Value {
	n := mt.NumIn()
	if mt.IsVariadic() {
		n-- // call with no variadic options
	}
	args := make([]reflect.Value, n)
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	writerType := reflect.TypeOf((*io.Writer)(nil)).Elem()
	readerType := reflect.TypeOf((*io.Reader)(nil)).Elem()
	for i := 0; i < n; i++ {
		pt := mt.In(i)
		switch {
		case pt == ctxType:
			args[i] = reflect.ValueOf(ctx)
		case pt == writerType:
			args[i] = reflect.ValueOf(io.Discard)
		case pt == readerType:
			args[i] = reflect.ValueOf(io.Reader(strings.NewReader("")))
		case pt == reflect.TypeOf(session):
			args[i] = reflect.ValueOf(session)
		case pt.Kind() == reflect.Uint64:
			args[i] = reflect.ValueOf(uint64(1)).Convert(pt)
		case pt.Kind() == reflect.String:
			args[i] = reflect.ValueOf("127.0.0.1:1").Convert(pt)
		case pt.Kind() == reflect.Slice && pt.Elem().Kind() == reflect.Uint8:
			args[i] = reflect.ValueOf([]byte("k")).Convert(pt)
		case pt.Kind() == reflect.Map:
			m := reflect.MakeMap(pt)
			if pt.Key().Kind() == reflect.Uint64 && pt.Elem().Kind() == reflect.String {
				m.SetMapIndex(reflect.ValueOf(uint64(1)).Convert(pt.Key()),
					reflect.ValueOf("127.0.0.1:1").Convert(pt.Elem()))
			}
			args[i] = m
		default:
			args[i] = reflect.Zero(pt)
		}
	}
	return args
}

// callClosed calls one method, recovering a panic and giving up at the
// deadline. It returns the results, or a non-empty failure.
func callClosed(m reflect.Value, args []reflect.Value) (out []reflect.Value, failure string) {
	type result struct {
		out   []reflect.Value
		panic any
	}
	done := make(chan result, 1)
	go func() {
		var r result
		defer func() {
			r.panic = recover()
			done <- r
		}()
		r.out = m.Call(args)
	}()
	select {
	case r := <-done:
		if r.panic != nil {
			return nil, fmt.Sprintf("panicked: %v", r.panic)
		}
		return r.out, ""
	case <-time.After(closedCallDeadline):
		return nil, fmt.Sprintf("blocked past %v", closedCallDeadline)
	}
}

func TestHostClosedEveryMethod(t *testing.T) {
	h, closeH := newBoundedHost(t)
	session := h.NoOPSession(1)
	if !closeH("Close") || t.Failed() {
		return
	}

	errType := reflect.TypeOf((*error)(nil)).Elem()
	hv := reflect.ValueOf(h)
	ht := hv.Type()
	seen := make(map[string]bool, ht.NumMethod())
	for i := 0; i < ht.NumMethod(); i++ {
		name := ht.Method(i).Name
		seen[name] = true
		kind, ok := hostClosedContract[name]
		if !ok {
			t.Errorf("%s: not in hostClosedContract; state its after-Close contract there", name)
			continue
		}
		if kind == closedSelf {
			continue
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), closedCallDeadline)
			defer cancel()
			m := hv.Method(i)
			out, failure := callClosed(m, closedArgs(ctx, m.Type(), session))
			if failure != "" {
				t.Fatalf("%s after Close %s", name, failure)
			}
			var err error
			hasErr := false
			for _, o := range out {
				if o.Type() == errType {
					hasErr = true
					if !o.IsNil() {
						err = o.Interface().(error)
					}
				}
				// Free a buffer a read method handed out.
				if release, ok := o.Interface().(func()); ok && release != nil {
					release()
				}
			}
			switch kind {
			case closedErr:
				if !hasErr {
					t.Fatalf("%s is classified closedErr but returns no error", name)
				}
				if !errors.Is(err, ErrClosed) {
					t.Fatalf("%s after Close = %v, want ErrClosed", name, err)
				}
			case closedGetter:
				if hasErr {
					t.Fatalf("%s returns an error; classify it closedErr", name)
				}
			}
		})
	}
	for name := range hostClosedContract {
		if !seen[name] {
			t.Errorf("hostClosedContract lists %s, which *Host no longer exports", name)
		}
	}
}

func TestHostClosedCloseIdempotent(t *testing.T) {
	_, closeH := newBoundedHost(t)
	const callers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			closeH("concurrent Close")
		}()
	}
	close(start)
	// Each closeH returns within closeDeadline, so Wait is bounded.
	wg.Wait()
	if t.Failed() {
		return
	}
	for i := 0; i < 2; i++ {
		if !closeH(fmt.Sprintf("repeat Close %d", i)) {
			return
		}
	}
}

// TestHostClosedEmergencySealDuringClose holds Close open after it has
// admitted shutdown (h.closed is set) but before it has sealed the barrier,
// and calls EmergencySeal in that window. An operator asking for immediate
// key destruction must get it while shutdown is still draining: the barrier
// is sealed at once and the call does not wait for Close.
func TestHostClosedEmergencySealDuringClose(t *testing.T) {
	cfg := testHostConfig(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	cfg.EventListener = &config.EventListener{
		OnNodeHostShuttingDown: func() {
			close(entered)
			<-release
		},
	}
	b := &mockBarrier{}
	h, err := NewHost(cfg, WithMemoryLogDB(), WithBarrier(b), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	closeErr := make(chan error, 1)
	go func() { closeErr <- h.Close() }()
	defer func() {
		unblock()
		select {
		case <-closeErr:
		case <-time.After(closeDeadline):
			t.Errorf("Close blocked past %v after release", closeDeadline)
		}
	}()

	select {
	case <-entered:
	case <-time.After(closeDeadline):
		t.Fatalf("Close did not reach shutdown within %v", closeDeadline)
	}
	if b.IsSealed() {
		t.Fatal("barrier sealed before EmergencySeal")
	}

	sealErr := make(chan error, 1)
	go func() { sealErr <- h.EmergencySeal() }()
	select {
	case err := <-sealErr:
		if err != nil {
			t.Errorf("EmergencySeal during Close: got %v, want nil", err)
		}
	case <-time.After(closedCallDeadline):
		t.Fatalf("EmergencySeal blocked past %v while Close was running", closedCallDeadline)
	}
	if !b.IsSealed() {
		t.Error("EmergencySeal during Close left the barrier unsealed")
	}

	unblock()
	select {
	case err := <-closeErr:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(closeDeadline):
		t.Fatalf("Close blocked past %v after release", closeDeadline)
	}
	closeErr <- nil // the deferred wait reads it
	if err := h.EmergencySeal(); !errors.Is(err, ErrClosed) {
		t.Errorf("EmergencySeal after Close: got %v, want ErrClosed", err)
	}
}

// flakySealBarrier is a mockBarrier whose first Seal fails without sealing;
// every later Seal behaves as mockBarrier's.
type flakySealBarrier struct {
	mockBarrier
	failOnce sync.Once
	failErr  error
}

func (b *flakySealBarrier) Seal() error {
	failed := false
	b.failOnce.Do(func() { failed = true })
	if failed {
		return b.failErr
	}
	return b.mockBarrier.Seal()
}

// TestHostClosedEmergencySealRetriesFailedCloseSeal checks that a Close whose
// barrier seal failed does not turn EmergencySeal into a no-op: the operator's
// key-destruction path must still seal the barrier, and only once it has
// does EmergencySeal report ErrClosed.
func TestHostClosedEmergencySealRetriesFailedCloseSeal(t *testing.T) {
	sealFailure := errors.New("seal failure")
	b := &flakySealBarrier{failErr: sealFailure}
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithBarrier(b), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	if err := h.Close(); !errors.Is(err, sealFailure) {
		t.Fatalf("Close: got %v, want the seal failure", err)
	}
	if b.IsSealed() {
		t.Fatal("barrier sealed although Close's seal failed")
	}
	if err := h.EmergencySeal(); err != nil {
		t.Fatalf("EmergencySeal after a failed Close seal: got %v, want nil", err)
	}
	if !b.IsSealed() {
		t.Fatal("EmergencySeal after a failed Close seal left the barrier unsealed")
	}
	if err := h.EmergencySeal(); !errors.Is(err, ErrClosed) {
		t.Errorf("EmergencySeal after a successful retry: got %v, want ErrClosed", err)
	}
}

// closeDeadline bounds every Close call in the closed-host tests.
const closeDeadline = 30 * time.Second

// newBoundedHost builds a memory-backed Host without newTestHost's
// auto-close cleanup, which calls Close synchronously and so would hang on
// the same stuck stopOnce until the suite timeout. It returns the host and a
// closer that runs Close in a goroutine under closeDeadline, recovers a
// panic, and reports a failure with what. The closer returns false when
// Close blocked; the registered cleanup then skips its own Close.
func newBoundedHost(t *testing.T) (*Host, func(what string) bool) {
	t.Helper()
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	var mu sync.Mutex
	stuck := false
	closeH := func(what string) bool {
		errc := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					errc <- fmt.Errorf("panicked: %v", r)
				}
			}()
			errc <- h.Close()
		}()
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("%s: %v", what, err)
			}
			return true
		case <-time.After(closeDeadline):
			mu.Lock()
			stuck = true
			mu.Unlock()
			t.Errorf("%s blocked past %v", what, closeDeadline)
			return false
		}
	}
	t.Cleanup(func() {
		mu.Lock()
		skip := stuck
		mu.Unlock()
		if !skip {
			closeH("cleanup Close")
		}
	})
	return h, closeH
}
