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
	// closedExempt: documented to keep its own behaviour after Close; it
	// must only return without panicking or blocking. Each entry says why.
	closedExempt
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

	// Drain is idempotent and returns its one recorded result to every
	// caller; Close itself calls it after setting the closed flag.
	"Drain": closedExempt,

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
	h := newTestHost(t)
	session := h.NoOPSession(1)
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
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
	h := newTestHost(t)
	const callers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	panics := make(chan any, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panics <- r
				}
			}()
			<-start
			if err := h.Close(); err != nil {
				t.Errorf("concurrent Close: %v", err)
			}
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent Close blocked")
	}
	close(panics)
	for r := range panics {
		t.Errorf("concurrent Close panicked: %v", r)
	}
	for i := 0; i < 2; i++ {
		if err := h.Close(); err != nil {
			t.Errorf("repeat Close %d: %v", i, err)
		}
	}
}
