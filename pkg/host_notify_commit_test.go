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
	"testing"
	"time"
)

// newNotifyCommitTestHost starts a single-voter, transportless Host on a real
// WAL with HostConfig.NotifyCommit set to notify, runs shard 1 on it and waits
// for its leader.
func newNotifyCommitTestHost(t *testing.T, notify bool) *Host {
	t.Helper()
	cfg := testHostConfig(t)
	cfg.NotifyCommit = notify
	h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
	return h
}

// awaitApply waits for the apply result of rs and fails the test on timeout or
// on a result carrying an error.
func awaitApply(t *testing.T, rs *RequestState, i int) RequestResult {
	t.Helper()
	select {
	case r := <-rs.ApplyResultC():
		if r.Err != nil {
			t.Fatalf("proposal %d: apply result error: %v", i, r.Err)
		}
		return r
	case <-time.After(10 * time.Second):
		t.Fatalf("proposal %d: no apply result within 10s", i)
	}
	return RequestResult{}
}

// With NotifyCommit on, every proposal's CommittedC delivers exactly one
// result with Value 0, nil Data and nil Err, and it has been delivered by the
// time ApplyResultC delivers the apply result. ApplyResultC and ResultC are
// the same channel, and both channels are nil after Release.
func TestNotifyCommit_OnDeliversOneEarlyResultBeforeApply(t *testing.T) {
	h := newNotifyCommitTestHost(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := range 50 {
		rs, err := h.Propose(ctx, 1, fmt.Appendf(nil, "cmd-%d", i))
		if err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
		// (c) ApplyResultC is ResultC.
		if rs.ApplyResultC() != rs.ResultC() {
			t.Fatalf("proposal %d: ApplyResultC() and ResultC() are different channels", i)
		}
		if rs.CommittedC() == nil {
			t.Fatalf("proposal %d: CommittedC() is nil before Release", i)
		}

		awaitApply(t, rs, i)

		// (b) The committed result is already there once the apply result
		// has been delivered; (a) it carries Value 0, nil Data, nil Err.
		select {
		case c := <-rs.CommittedC():
			if c.Value != 0 || c.Data != nil || c.Err != nil {
				t.Fatalf("proposal %d: committed result = {Value:%d Data:%v Err:%v}, want {0 nil nil}",
					i, c.Value, c.Data, c.Err)
			}
		default:
			t.Fatalf("proposal %d: apply result delivered before the committed result", i)
		}
		// (a) Exactly one: no second committed result.
		select {
		case c := <-rs.CommittedC():
			t.Fatalf("proposal %d: second committed result %+v", i, c)
		case <-time.After(time.Millisecond):
		}

		// (d) Both channels are nil after Release.
		rs.Release()
		if rs.CommittedC() != nil {
			t.Fatalf("proposal %d: CommittedC() not nil after Release", i)
		}
		if rs.ApplyResultC() != nil {
			t.Fatalf("proposal %d: ApplyResultC() not nil after Release", i)
		}
	}
}

// With NotifyCommit off, proposals apply while CommittedC never receives.
func TestNotifyCommit_OffCommittedCNeverFires(t *testing.T) {
	h := newNotifyCommitTestHost(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	held := make([]*RequestState, 0, 20)
	defer func() {
		for _, rs := range held {
			rs.Release()
		}
	}()
	for i := range 20 {
		rs, err := h.Propose(ctx, 1, fmt.Appendf(nil, "cmd-%d", i))
		if err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
		held = append(held, rs)
		awaitApply(t, rs, i)
	}
	// Give any stray notification time to land, then check every request.
	time.Sleep(50 * time.Millisecond)
	for i, rs := range held {
		select {
		case c := <-rs.CommittedC():
			t.Fatalf("proposal %d: CommittedC fired with NotifyCommit off: %+v", i, c)
		default:
		}
	}
}

// A proposal refused before it reaches raft delivers nothing on CommittedC.
func TestNotifyCommit_RefusedProposalDeliversNothing(t *testing.T) {
	h := newNotifyCommitTestHost(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := h.PauseShard(ctx, 1); err != nil {
		t.Fatalf("PauseShard: %v", err)
	}
	rs, err := h.Propose(ctx, 1, []byte("cmd"))
	if !errors.Is(err, ErrShardPaused) {
		t.Fatalf("Propose on a paused shard = %v, want ErrShardPaused", err)
	}
	if rs == nil {
		return // nothing returned, so nothing can be delivered
	}
	defer rs.Release()
	select {
	case c := <-rs.CommittedC():
		t.Fatalf("CommittedC fired for a refused proposal: %+v", c)
	case <-time.After(50 * time.Millisecond):
	}
}
