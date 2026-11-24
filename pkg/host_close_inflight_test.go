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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestHostClose_CloseInflight_LostQuorum is #796: Close must end every
// request pending on a shard that has lost quorum. The shard is a
// single-node leader (CheckQuorum off, so it never steps down) that adds an
// unreachable voter: from then on quorum is two and nothing commits or
// confirms, so every Propose, SyncPropose and SyncRead stays pending until
// Close. Close must return within 5s, every caller must see ErrClosed (or
// ErrCanceled), and no goroutine may outlive the host.
//
// TestPendingRegistration_CloseRace_CallerNeverOrphaned
// (host_pending_lifecycle_test.go) covers ReadIndex, RequestSnapshot and
// RequestAddNode racing Close on a live shard; this test adds the lost-quorum
// case, the three client calls and the error class.
func TestHostClose_CloseInflight_LostQuorum(t *testing.T) {
	const callers = 64
	self := currentGoroutineID()
	before := goroutineStacks()

	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	// closeStarted is set before the test's own Close begins, so this
	// cleanup never calls Close again on a shutdown that has not finished.
	closeStarted := false
	defer func() {
		if !closeStarted {
			_ = h.Close()
		}
	}()
	scfg := testShardConfig(1, 1)
	scfg.CheckQuorum = config.BoolPtr(false)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)

	// Lose quorum: the add commits under the one-voter configuration, after
	// which the unreachable replica 2 is needed for every commit and read.
	addCtx, addCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer addCancel()
	rs, err := h.RequestAddNode(addCtx, 1, 2, "127.0.0.1:9999", 0)
	if err != nil {
		t.Fatalf("RequestAddNode: %v", err)
	}
	select {
	case r := <-rs.ResultC():
		if r.Err != nil {
			t.Fatalf("RequestAddNode result: %v", r.Err)
		}
	case <-addCtx.Done():
		t.Fatal("RequestAddNode did not complete")
	}
	rs.Release()
	// deliverProposal completes the result before it decrements pendingOps,
	// so wait for the membership operation's count to drain before the
	// callers start.
	drainDeadline := time.Now().Add(10 * time.Second)
	for h.pendingOps.Load() != 0 {
		if time.Now().After(drainDeadline) {
			t.Fatalf("pending ops = %d after RequestAddNode, want 0", h.pendingOps.Load())
		}
		time.Sleep(time.Millisecond)
	}

	// The callers' deadline is far beyond the test's bounds, so only Close
	// can end their requests.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := []byte(fmt.Sprintf("cmd-%d", i))
			switch i % 3 {
			case 0:
				prs, perr := h.Propose(ctx, 1, cmd)
				if perr != nil {
					errs[i] = fmt.Errorf("Propose admission: %w", perr)
					return
				}
				defer prs.Release()
				select {
				case r := <-prs.ResultC():
					errs[i] = r.Err
				case <-ctx.Done():
					errs[i] = errors.New("Propose result never delivered")
				}
			case 1:
				_, errs[i] = h.SyncPropose(ctx, 1, cmd)
			default:
				_, errs[i] = h.SyncRead(ctx, 1, cmd)
			}
		}(i)
	}

	// Close only once every request is pending, so each one is in flight
	// when the host shuts down rather than refused at admission.
	deadline := time.Now().Add(10 * time.Second)
	for h.pendingOps.Load() < callers {
		if time.Now().After(deadline) {
			t.Fatalf("pending ops = %d, want %d before Close", h.pendingOps.Load(), callers)
		}
		time.Sleep(time.Millisecond)
	}

	// Close runs in a goroutine bounded by a timer, so a shutdown deadlock
	// fails the test within 5s instead of hanging it until the suite timeout.
	closeStarted = true
	closeResult := make(chan error, 1)
	go func() { closeResult <- h.Close() }()
	closeTimer := time.NewTimer(5 * time.Second)
	defer closeTimer.Stop()
	var closeErr error
	select {
	case closeErr = <-closeResult:
	case <-closeTimer.C:
		t.Fatal("Close did not return within 5s")
	}
	// Drain cannot finish while requests wait on a lost quorum, so
	// ErrShutdownTimeout is the documented outcome; any other error fails.
	if closeErr != nil && !errors.Is(closeErr, ErrShutdownTimeout) {
		t.Fatalf("Close: %v", closeErr)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("callers still blocked 5s after Close returned")
	}
	for i, e := range errs {
		if !errors.Is(e, ErrClosed) && !errors.Is(e, ErrCanceled) {
			t.Errorf("caller %d (kind %d): err = %v, want ErrClosed or ErrCanceled", i, i%3, e)
		}
	}
	if got := h.pendingOps.Load(); got != 0 {
		t.Errorf("pendingOps = %d after Close, want 0", got)
	}

	// No goroutine outlives the host.
	cancel()
	addCancel()
	if leaked := leakedModuleGoroutines(before, self, goroutineLeakWait); len(leaked) != 0 {
		t.Fatalf("%d goroutine(s) started during the test still running %v after Close:\n\n%s",
			len(leaked), goroutineLeakWait, strings.Join(leaked, "\n\n"))
	}
}
