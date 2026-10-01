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

package rsm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// parkTimeout bounds how long a test waits for a call that must NOT be
// blocked. A regression parks the call forever, so the bound turns a deadlock
// into a reported failure.
const parkTimeout = 5 * time.Second

// park is a one-shot rendezvous a test arms on one method of gateSM: the
// method closes entered when it starts and then waits for release.
type park struct {
	entered chan struct{}
	release chan struct{}
}

func newPark() *park {
	return &park{entered: make(chan struct{}), release: make(chan struct{})}
}

func (p *park) wait() {
	if p == nil {
		return
	}
	close(p.entered)
	<-p.release
}

// gateSM is a ConcurrentStateMachine whose Update, Lookup and
// RecoverFromSnapshot can each be parked inside the user state machine. It is
// what lets a test hold one call open in the user SM and assert whether a
// second one is excluded by the adapter. Each park is armed before the
// machine is shared with another goroutine and fires once.
type gateSM struct {
	lookup  *park
	update  *park
	recover *park
}

func (g *gateSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	p := g.update
	g.update = nil
	p.wait()
	for i, e := range entries {
		results[i] = sm.Result{Value: e.Index}
	}
	return nil
}

func (g *gateSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	p := g.lookup
	g.lookup = nil
	p.wait()
	return uint64(0), nil
}

func (g *gateSM) PrepareSnapshot() (interface{}, error) { return struct{}{}, nil }

func (g *gateSM) SaveSnapshot(_ context.Context, _ interface{}, w io.Writer, _ <-chan struct{}) error {
	_, err := w.Write([]byte{1})
	return err
}

func (g *gateSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	p := g.recover
	g.recover = nil
	p.wait()
	_, err := io.ReadAll(r)
	return err
}

func (g *gateSM) Close(_ context.Context) error { return nil }

// gateDiskSM is gateSM on the on-disk tier.
type gateDiskSM struct{ gateSM }

func (g *gateDiskSM) Open(_ context.Context, _ string, _ <-chan struct{}) (uint64, error) {
	return 0, nil
}

func (g *gateDiskSM) Sync() error { return nil }

var (
	_ sm.ConcurrentStateMachine = (*gateSM)(nil)
	_ sm.DiskStateMachine       = (*gateDiskSM)(nil)
)

// concurrentTiers runs fn once per tier whose contract permits Lookup
// concurrently with Update, against a fresh machine wrapping g.
func concurrentTiers(t *testing.T, fn func(t *testing.T, s *StateMachine, g *gateSM)) {
	t.Helper()
	tiers := []struct {
		name string
		want SMType
		make func() (interface{}, *gateSM)
	}{
		{"concurrent", SMTypeConcurrent, func() (interface{}, *gateSM) {
			g := &gateSM{}
			return g, g
		}},
		{"ondisk", SMTypeOnDisk, func() (interface{}, *gateSM) {
			g := &gateDiskSM{}
			return g, &g.gateSM
		}},
	}
	for _, tier := range tiers {
		t.Run(tier.name, func(t *testing.T) {
			user, g := tier.make()
			createFn := func(uint64, uint64) interface{} { return user }
			s, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, true)
			if err != nil {
				t.Fatalf("NewStateMachine = %v", err)
			}
			if s.SMType() != tier.want {
				t.Fatalf("SMType = %v, want %v", s.SMType(), tier.want)
			}
			fn(t, s, g)
		})
	}
}

func applyIndex(s *StateMachine, index uint64) error {
	entries := []proto.Entry{{Index: index, Term: 1, Type: proto.EntryNormal, Cmd: []byte("v")}}
	return s.Apply(entries, make([]sm.Result, 1))
}

// await reports whether done delivers within parkTimeout, and what it
// delivered.
func await(done <-chan error) (bool, error) {
	select {
	case err := <-done:
		return true, err
	case <-time.After(parkTimeout):
		return false, nil
	}
}

// TestLookupDoesNotBlockApplyOnConcurrentTiers is the regression test for
// #209. ConcurrentStateMachine and DiskStateMachine both document Lookup as
// "May be called concurrently with Update", yet Apply took readGuard for
// writing on every tier, so one read held open inside the user SM parked the
// shard's Apply — and, through sync.RWMutex's writer preference, every read
// that arrived after it.
//
// Fail-before-fix, observed: with Apply holding readGuard.Lock on every tier,
// both the Apply and the second Lookup below time out.
func TestLookupDoesNotBlockApplyOnConcurrentTiers(t *testing.T) {
	concurrentTiers(t, func(t *testing.T, s *StateMachine, g *gateSM) {
		slow := newPark()
		g.lookup = slow
		slowDone := make(chan error, 1)
		go func() {
			_, err := s.Lookup(context.Background(), "scan")
			slowDone <- err
		}()
		<-slow.entered
		defer func() {
			// Unwind the parked read whatever the outcome, so a failure is
			// reported rather than leaking a goroutine into Close.
			select {
			case <-slow.release:
			default:
				close(slow.release)
			}
			<-slowDone
			if err := s.Close(); err != nil {
				t.Errorf("Close = %v", err)
			}
		}()

		applied := make(chan error, 1)
		go func() { applied <- applyIndex(s, 1) }()
		ok, err := await(applied)
		if !ok {
			t.Fatal("Apply blocked behind a Lookup parked in the user state machine")
		}
		if err != nil {
			t.Fatalf("Apply = %v", err)
		}

		read := make(chan error, 1)
		go func() {
			_, err := s.Lookup(context.Background(), "point")
			read <- err
		}()
		ok, err = await(read)
		if !ok {
			t.Fatal("a second Lookup blocked behind the parked Lookup")
		}
		if err != nil {
			t.Fatalf("second Lookup = %v", err)
		}
	})
}

// TestApplyDoesNotBlockLookupOnConcurrentTiers is the converse: an Update in
// progress inside the user SM must not hold local reads out on the tiers that
// promise to synchronise those reads themselves.
func TestApplyDoesNotBlockLookupOnConcurrentTiers(t *testing.T) {
	concurrentTiers(t, func(t *testing.T, s *StateMachine, g *gateSM) {
		slow := newPark()
		g.update = slow
		applied := make(chan error, 1)
		go func() { applied <- applyIndex(s, 1) }()
		<-slow.entered
		defer func() {
			select {
			case <-slow.release:
			default:
				close(slow.release)
			}
			if err := <-applied; err != nil {
				t.Errorf("Apply = %v", err)
			}
			if err := s.Close(); err != nil {
				t.Errorf("Close = %v", err)
			}
		}()

		read := make(chan error, 1)
		go func() {
			_, err := s.Lookup(context.Background(), "point")
			read <- err
		}()
		ok, err := await(read)
		if !ok {
			t.Fatal("Lookup blocked behind an Apply parked in the user state machine")
		}
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
	})
}

// TestRecoverExcludesApplyOnConcurrentTiers pins the exclusion that applyMu
// took over from readGuard. Recovery replaces the session table and policy
// that Apply works against, so the two must still not overlap once Apply
// holds readGuard only for reading.
func TestRecoverExcludesApplyOnConcurrentTiers(t *testing.T) {
	concurrentTiers(t, func(t *testing.T, s *StateMachine, g *gateSM) {
		defer func() {
			if err := s.Close(); err != nil {
				t.Errorf("Close = %v", err)
			}
		}()
		var snap bytes.Buffer
		if err := s.SaveSnapshot(context.Background(), &snap, nil); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		slow := newPark()
		g.recover = slow
		recovered := make(chan error, 1)
		go func() {
			recovered <- s.RecoverFromSnapshot(context.Background(), &snap, nil)
		}()
		<-slow.entered

		applied := make(chan error, 1)
		go func() { applied <- applyIndex(s, 1) }()
		select {
		case err := <-applied:
			close(slow.release)
			<-recovered
			t.Fatalf("Apply ran during RecoverFromSnapshot (err=%v)", err)
		case <-time.After(50 * time.Millisecond):
		}

		close(slow.release)
		if ok, err := await(recovered); !ok || err != nil {
			t.Fatalf("RecoverFromSnapshot = %v (returned=%v)", err, ok)
		}
		if ok, err := await(applied); !ok || err != nil {
			t.Fatalf("Apply after recovery = %v (returned=%v)", err, ok)
		}
	})
}

// TestCloseWaitsForLookupOnConcurrentTiers keeps the Close half of
// readGuard's contract on the tiers whose Apply is now a reader: Close waits
// out a read in progress, and a read that follows observes ErrClosed.
func TestCloseWaitsForLookupOnConcurrentTiers(t *testing.T) {
	concurrentTiers(t, func(t *testing.T, s *StateMachine, g *gateSM) {
		slow := newPark()
		g.lookup = slow
		slowDone := make(chan error, 1)
		go func() {
			_, err := s.Lookup(context.Background(), "scan")
			slowDone <- err
		}()
		<-slow.entered

		closed := make(chan error, 1)
		go func() { closed <- s.Close() }()
		select {
		case err := <-closed:
			close(slow.release)
			<-slowDone
			t.Fatalf("Close returned during an in-flight Lookup (err=%v)", err)
		case <-time.After(50 * time.Millisecond):
		}

		close(slow.release)
		if ok, err := await(slowDone); !ok || err != nil {
			t.Fatalf("in-flight Lookup = %v (returned=%v)", err, ok)
		}
		if ok, err := await(closed); !ok || err != nil {
			t.Fatalf("Close = %v (returned=%v)", err, ok)
		}
		if _, err := s.Lookup(context.Background(), "after"); !errors.Is(err, ErrClosed) {
			t.Fatalf("Lookup after Close = %v, want ErrClosed", err)
		}
		if err := applyIndex(s, 1); !errors.Is(err, ErrClosed) {
			t.Fatalf("Apply after Close = %v, want ErrClosed", err)
		}
	})
}

// scanSM is a state machine whose Lookup takes a fixed span of CPU time, like
// a range scan or an export, and whose Update is trivial. It holds no lock of
// its own: Update touches nothing Lookup reads.
type scanSM struct{ scan time.Duration }

func (s *scanSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i, e := range entries {
		results[i] = sm.Result{Value: e.Index}
	}
	return nil
}

func (s *scanSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	for start := time.Now(); time.Since(start) < s.scan; {
	}
	return uint64(0), nil
}

func (s *scanSM) GetHash() (uint64, error)                                              { return 0, nil }
func (s *scanSM) PrepareSnapshot() (interface{}, error)                                 { return struct{}{}, nil }
func (s *scanSM) RecoverFromSnapshot(context.Context, io.Reader, <-chan struct{}) error { return nil }
func (s *scanSM) Close(context.Context) error                                           { return nil }

// scanRegularSM is scanSM on the basic tier.
type scanRegularSM struct{ scanSM }

func (s *scanRegularSM) SaveSnapshot(context.Context, io.Writer, <-chan struct{}) error { return nil }

// scanConcurrentSM is scanSM on the concurrent tier.
type scanConcurrentSM struct{ scanSM }

func (s *scanConcurrentSM) SaveSnapshot(context.Context, interface{}, io.Writer, <-chan struct{}) error {
	return nil
}

// scanDiskSM is scanSM on the on-disk tier.
type scanDiskSM struct{ scanConcurrentSM }

func (s *scanDiskSM) Open(context.Context, string, <-chan struct{}) (uint64, error) { return 0, nil }
func (s *scanDiskSM) Sync() error                                                   { return nil }

var (
	_ sm.StateMachine           = (*scanRegularSM)(nil)
	_ sm.ConcurrentStateMachine = (*scanConcurrentSM)(nil)
	_ sm.DiskStateMachine       = (*scanDiskSM)(nil)
)

// BenchmarkApplyWithReaders measures Apply on each tier, alone and against
// four goroutines issuing 20µs local reads back to back. On the basic tier
// the readers exclude Apply by contract, so its "readers=4" time includes
// waiting out a scan. On the concurrent and on-disk tiers they must not
// (#209), so "readers=4" there should track "readers=0".
func BenchmarkApplyWithReaders(b *testing.B) {
	const scan = 20 * time.Microsecond
	tiers := []struct {
		name string
		make func() interface{}
	}{
		{"regular", func() interface{} { return &scanRegularSM{scanSM{scan}} }},
		{"concurrent", func() interface{} { return &scanConcurrentSM{scanSM{scan}} }},
		{"ondisk", func() interface{} { return &scanDiskSM{scanConcurrentSM{scanSM{scan}}} }},
	}
	for _, tier := range tiers {
		for _, readers := range []int{0, 4} {
			b.Run(fmt.Sprintf("%s/readers=%d", tier.name, readers), func(b *testing.B) {
				user := tier.make()
				createFn := func(uint64, uint64) interface{} { return user }
				s, err := NewStateMachine(1, 1, createFn, b.TempDir(), testPolicy(100, 100, 0), nil, true)
				if err != nil {
					b.Fatalf("NewStateMachine = %v", err)
				}
				defer s.Close()

				stop := make(chan struct{})
				var wg sync.WaitGroup
				for i := 0; i < readers; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							select {
							case <-stop:
								return
							default:
							}
							if _, err := s.Lookup(context.Background(), "scan"); err != nil {
								return
							}
						}
					}()
				}

				entries := []proto.Entry{{Term: 1, Type: proto.EntryNormal, Cmd: []byte("v")}}
				results := make([]sm.Result, 1)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					entries[0].Index = uint64(i + 1)
					if err := s.Apply(entries, results); err != nil {
						b.Fatalf("Apply = %v", err)
					}
				}
				b.StopTimer()
				close(stop)
				wg.Wait()
			})
		}
	}
}
