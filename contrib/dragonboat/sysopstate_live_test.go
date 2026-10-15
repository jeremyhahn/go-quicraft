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

package dragonboat

import (
	"context"
	"testing"
	"time"
)

// The unit tests in requeststate_test.go cover WrapSysOpState's released
// (nil ResultC) and Release-stops-it paths only: a pool RequestState cannot
// be completed from outside the quicraft package. These tests take a real
// *quicraft.RequestState from a live host, so the wrapper's goroutine sees
// the result arrive on ResultC, the path an embedder relies on.

// sysOpLiveTimeout bounds every wait on a live host. A single-replica commit
// takes milliseconds; the margin absorbs -race and a loaded CI runner.
const sysOpLiveTimeout = 10 * time.Second

// releaseSysOpWithin releases s and fails the test unless Release returns,
// and the wrapper's goroutine has exited, within sysOpLiveTimeout.
func releaseSysOpWithin(t *testing.T, s *SysOpState) {
	t.Helper()
	released := make(chan struct{})
	go func() {
		s.Release()
		// Release waits on wg itself; waiting again proves it drained
		// rather than relying on Release's internal ordering.
		s.wg.Wait()
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(sysOpLiveTimeout):
		t.Fatal("SysOpState.Release did not return: the wrapper goroutine is still blocked")
	}
}

func TestWrapSysOpState_LiveProposal_CompletesOnResult(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)

	ctx, cancel := context.WithTimeout(context.Background(), sysOpLiveTimeout)
	defer cancel()
	// A proposal is the host's asynchronous operation that hands back a
	// *quicraft.RequestState; it completes once the entry commits and
	// applies, delivering its result on ResultC.
	inner, err := nh.host.Propose(ctx, 1, []byte("sysop-live-proposal"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	sysOp := WrapSysOpState(inner)

	select {
	case <-sysOp.CompletedC():
	case <-time.After(sysOpLiveTimeout):
		sysOp.Release()
		t.Fatal("CompletedC did not close after the proposal committed")
	}
	// ResultC is the dragonboat-compatible alias and must agree.
	select {
	case <-sysOp.ResultC():
	default:
		t.Fatal("ResultC not closed after CompletedC closed")
	}

	releaseSysOpWithin(t, sysOp)
}

func TestWrapSysOpState_LiveProposal_ShardStopped(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)

	ctx, cancel := context.WithTimeout(context.Background(), sysOpLiveTimeout)
	defer cancel()
	inner, err := nh.host.Propose(ctx, 1, []byte("sysop-live-stopped"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	sysOp := WrapSysOpState(inner)

	// Stopping the shard fails its pending requests. Whether the proposal
	// committed first or is failed by the stop, a result arrives on
	// ResultC, and the wrapper must signal completion either way: an
	// error result must not leave CompletedC open.
	if err := nh.StopShard(1); err != nil {
		sysOp.Release()
		t.Fatalf("StopShard: %v", err)
	}

	select {
	case <-sysOp.CompletedC():
	case <-time.After(sysOpLiveTimeout):
		sysOp.Release()
		t.Fatal("CompletedC did not close after the shard stopped")
	}

	releaseSysOpWithin(t, sysOp)
}
