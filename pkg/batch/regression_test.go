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

package batch

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"sync"
	"testing"
	"time"
)

// recordingProposer captures every batch handed to SyncPropose, decoded into
// individual commands, and signals each call so a test can wait for a flush
// instead of sleeping.
type recordingProposer struct {
	mu       sync.Mutex
	cmds     [][]byte
	sizes    []int
	deadline []time.Time
	calls    chan struct{}

	// hold, when non-zero, keeps SyncPropose inside the call for that long or
	// until the propose context ends, whichever comes first. It models a
	// propose that takes real time (waiting out an election, say).
	hold time.Duration
}

func newRecordingProposer() *recordingProposer {
	return &recordingProposer{calls: make(chan struct{}, 64)}
}

func (r *recordingProposer) SyncPropose(ctx context.Context, _ uint64, cmd []byte) error {
	decoded, err := DecodeBatch(cmd)
	if err != nil {
		return err
	}
	r.mu.Lock()
	for _, d := range decoded {
		r.cmds = append(r.cmds, bytes.Clone(d))
	}
	r.sizes = append(r.sizes, len(cmd))
	if d, ok := ctx.Deadline(); ok {
		r.deadline = append(r.deadline, d)
	} else {
		r.deadline = append(r.deadline, time.Time{})
	}
	r.mu.Unlock()

	select {
	case r.calls <- struct{}{}:
	default:
	}

	if r.hold > 0 {
		timer := time.NewTimer(r.hold)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// commands returns a copy of every command the proposer has seen.
func (r *recordingProposer) commands() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.cmds))
	copy(out, r.cmds)
	return out
}

// batchSizes returns the encoded byte size of every batch proposed so far.
func (r *recordingProposer) batchSizes() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.sizes...)
}

// firstDeadline returns the deadline of the first propose context, and whether
// a propose has happened at all.
func (r *recordingProposer) firstDeadline() (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.deadline) == 0 {
		return time.Time{}, false
	}
	return r.deadline[0], true
}

// awaitCall waits for the next SyncPropose call, failing the test rather than
// hanging if none arrives.
func (r *recordingProposer) awaitCall(t *testing.T, within time.Duration, what string) {
	t.Helper()
	select {
	case <-r.calls:
	case <-time.After(within):
		t.Fatalf("no batch was proposed within %v: %s", within, what)
	}
}

// TestSubmit_CopiesCallerBuffer is the regression test for the retained-buffer
// defect. Submit stored the caller's slice BY REFERENCE, and the bytes were
// not read until the flush encoded the batch — up to FlushInterval later, on
// another goroutine. A caller that recycles a pooled buffer once Submit has
// returned (its own deadline expiring is the ordinary way that happens) then
// had the buffer's LATER contents encoded and replicated: every replica
// applies the same wrong command, so there is no divergence to detect it — a
// silently, permanently wrong committed write.
//
// The test pins the ownership property itself rather than one timing window:
// the buffer is refilled while the proposal is provably still in the unflushed
// batch, and the flush is then triggered deterministically. If Submit retains
// the caller's array, the recycled contents reach the proposer.
func TestSubmit_CopiesCallerBuffer(t *testing.T) {
	const (
		original  = "ORIGINAL-COMMAND"
		corrupted = "CORRUPTED-BUFFER"
	)

	p := newRecordingProposer()
	agg, err := NewAggregator(p,
		WithConfig(Config{
			// The second proposal fills the batch and triggers the flush; the
			// timer never does.
			MaxBatchSize:  2,
			FlushInterval: time.Hour,
			QueueSize:     10,
		}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	// A pooled buffer the caller expects to own again once it is done with it.
	buf := []byte(original)
	done := make(chan error, 1)
	go func() { done <- agg.Submit(context.Background(), 1, buf) }()

	// The proposal is in the batch and cannot be encoded until the flush below,
	// so this write is strictly ordered before any read of the caller's array.
	waitForBatchLen(t, agg, 1)
	copy(buf, corrupted)

	if submitErr := agg.Submit(context.Background(), 1, []byte("filler")); submitErr != nil {
		t.Fatalf("filler Submit: %v", submitErr)
	}
	select {
	case submitErr := <-done:
		if submitErr != nil {
			t.Fatalf("Submit: %v", submitErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first Submit never got a result")
	}

	cmds := p.commands()
	if len(cmds) != 2 {
		t.Fatalf("proposer saw %d commands, want 2", len(cmds))
	}
	if got := string(cmds[0]); got != original {
		t.Fatalf("replicated command = %q, want %q: Submit retained the caller's "+
			"buffer and replicated its recycled contents", got, original)
	}
}

// TestSubmit_TimedOutCallerBufferIsNotReplicated is the same defect in the
// shape it was found in: a caller with a 25ms deadline gets DeadlineExceeded
// back while the batch is still unflushed, refills its pooled buffer, and the
// flush encodes whatever is in it at that point. Two independent fixes have to
// hold for this to be safe — Submit must not retain the buffer, and a proposal
// whose caller has already given up must not be carried into the batch at all
// — so what the flush sees must be either the original bytes or nothing.
func TestSubmit_TimedOutCallerBufferIsNotReplicated(t *testing.T) {
	const (
		original  = "ORIGINAL-COMMAND"
		corrupted = "CORRUPTED-BUFFER"
	)

	p := newRecordingProposer()
	agg, err := NewAggregator(p,
		WithConfig(Config{
			MaxBatchSize:  2,
			FlushInterval: time.Hour,
			QueueSize:     10,
		}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	buf := []byte(original)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if submitErr := agg.Submit(ctx, 1, buf); !errors.Is(submitErr, context.DeadlineExceeded) {
		t.Fatalf("Submit error = %v, want context.DeadlineExceeded "+
			"(the flush must still be pending for this test to mean anything)", submitErr)
	}
	waitForBatchLen(t, agg, 1)
	copy(buf, corrupted)

	// Trigger the flush now that the caller has recycled its buffer.
	if submitErr := agg.Submit(context.Background(), 1, []byte("filler")); submitErr != nil {
		t.Fatalf("filler Submit: %v", submitErr)
	}

	for _, cmd := range p.commands() {
		if string(cmd) == corrupted {
			t.Fatalf("the flush replicated the caller's recycled buffer contents %q", corrupted)
		}
	}
}

// TestSubmitNoCopy_DoesNotCopy pins the other half of the ownership contract:
// SubmitNoCopy is the documented zero-copy path, so it must NOT copy. Without
// this the "copy in Submit" fix could be satisfied by copying everywhere,
// silently removing the escape hatch the doc promises.
//
// The check is an IDENTITY check on the backing array of the slice the
// aggregator stored, not a comparison of bytes. Its previous shape said in a
// comment that "comparing the bytes cannot tell a copy from the original, so
// compare identity" and then compared the bytes anyway — through the proposer,
// which never sees the caller's slice at all because EncodeBatch copies every
// command into a fresh payload. Making SubmitNoCopy copy left it passing.
//
// The proposal is inspected while it is still in the unflushed batch, which is
// the only point at which the aggregator's own reference to the caller's array
// is observable.
func TestSubmitNoCopy_DoesNotCopy(t *testing.T) {
	p := newRecordingProposer()
	agg, err := NewAggregator(p,
		WithConfig(Config{
			// Neither the size threshold nor the timer may flush: the proposal
			// must sit in the batch long enough to be inspected.
			MaxBatchSize:  1000,
			FlushInterval: time.Hour,
			QueueSize:     10,
		}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	buf := []byte("TRANSFERRED-BUF")
	done := make(chan error, 1)
	go func() { done <- agg.SubmitNoCopy(context.Background(), 1, buf) }()

	waitForBatchLen(t, agg, 1)
	if got := batchedDataOf(t, agg, 0); &got[0] != &buf[0] {
		t.Fatalf("SubmitNoCopy stored a slice backed by a different array than the "+
			"caller's (%p vs %p): the documented zero-copy path copied, so the "+
			"escape hatch Submit's ownership contract points callers at is gone",
			&got[0], &buf[0])
	}

	// Close drains the batch so the submitting goroutine is released.
	if closeErr := agg.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	select {
	case submitErr := <-done:
		if submitErr != nil {
			t.Fatalf("SubmitNoCopy: %v", submitErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SubmitNoCopy did not return after its batch was flushed")
	}

	cmds := p.commands()
	if len(cmds) != 1 || string(cmds[0]) != string(buf) {
		t.Fatalf("replicated commands = %q, want exactly [%q]", cmds, string(buf))
	}
}

// batchedDataOf returns the data slice the aggregator is holding for the
// proposal at position i of the current unflushed batch.
func batchedDataOf(t *testing.T, a *Aggregator, i int) []byte {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if i >= len(a.currentBatch) {
		t.Fatalf("batch holds %d proposals, want more than %d", len(a.currentBatch), i)
	}
	data := a.currentBatch[i].data
	if len(data) == 0 {
		t.Fatal("the batched proposal carries no data, so its backing array cannot be compared")
	}
	return data
}

// TestFlush_CancelledProposalIsNotReplicated is the regression test for the
// carried-along payload. proposeContext only chose which context to propose
// UNDER; it never removed a dead proposal's bytes from the encoded batch. A
// caller told context.Canceled — which reads as a DEFINITE rejection, not an
// indeterminate outcome — still had its command replicated on the back of
// whichever proposal in the batch was still live.
func TestFlush_CancelledProposalIsNotReplicated(t *testing.T) {
	const (
		cancelled = "CANCELLED-WRITE"
		live      = "LIVE-WRITE"
	)

	p := newRecordingProposer()
	agg, err := NewAggregator(p,
		WithConfig(Config{
			// Two proposals fill the batch, so the flush is triggered by the
			// second Submit and never by the timer.
			MaxBatchSize:  2,
			FlushInterval: time.Hour,
			QueueSize:     10,
		}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	deadCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if submitErr := agg.Submit(deadCtx, 1, []byte(cancelled)); !errors.Is(submitErr, context.Canceled) {
		t.Fatalf("cancelled Submit error = %v, want context.Canceled", submitErr)
	}

	if submitErr := agg.Submit(context.Background(), 1, []byte(live)); submitErr != nil {
		t.Fatalf("live Submit: %v", submitErr)
	}

	cmds := p.commands()
	for _, cmd := range cmds {
		if string(cmd) == cancelled {
			t.Fatalf("a caller told %v had its command replicated anyway: %q",
				context.Canceled, cancelled)
		}
	}
	if len(cmds) != 1 || string(cmds[0]) != live {
		t.Fatalf("replicated commands = %q, want exactly [%q]", cmds, live)
	}
}

// TestProposeContext_DoesNotAdoptShortestCallerDeadline is the regression test
// for one submitter's deadline governing the whole batch. A 200ms caller that
// landed in the batch first made the batch's propose expire after 200ms, so a
// 30s caller sharing it got FlushError{Indeterminate} with 29.8s of its budget
// unspent — and the only remedy the aggregator offers, an at-least-once retry,
// duplicates every command in the batch.
func TestProposeContext_DoesNotAdoptShortestCallerDeadline(t *testing.T) {
	p := newRecordingProposer()
	p.hold = 400 * time.Millisecond

	agg, err := NewAggregator(p,
		WithConfig(Config{
			MaxBatchSize:   2,
			FlushInterval:  time.Hour,
			QueueSize:      10,
			ProposeTimeout: 30 * time.Second,
		}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	shortCtx, cancelShort := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelShort()
	shortDone := make(chan error, 1)
	go func() { shortDone <- agg.Submit(shortCtx, 1, []byte("short")) }()

	// Let the impatient caller land in the batch first.
	waitForBatchLen(t, agg, 1)

	longCtx, cancelLong := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelLong()
	longDone := make(chan error, 1)
	go func() { longDone <- agg.Submit(longCtx, 1, []byte("long")) }()

	select {
	case longErr := <-longDone:
		if longErr != nil {
			t.Fatalf("the 30s caller got %v: its batch was proposed under the "+
				"200ms caller's context", longErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the 30s caller never got a result")
	}
	<-shortDone

	deadline, ok := p.firstDeadline()
	if !ok {
		t.Fatal("no batch was proposed")
	}
	if remaining := time.Until(deadline); remaining < 5*time.Second {
		t.Fatalf("batch proposed with %v left on its deadline, want a "+
			"ProposeTimeout-scale budget: an individual caller's deadline "+
			"governed the whole batch", remaining)
	}
}

// TestProposeContext_CapsLongCallerDeadline is the test for the other
// direction. If ProposeTimeout applied ONLY when the caller supplied no
// deadline of its own, a caller passing a legitimate one-hour deadline would
// bypass it entirely: flush runs on the single flush loop and waits for the
// shard goroutines, so the aggregator would stop flushing FOR EVERY SHARD and
// Close would block behind wg.Wait — exactly the wedge ProposeTimeout exists to
// prevent.
func TestProposeContext_CapsLongCallerDeadline(t *testing.T) {
	p := &ctxBoundProposer{entered: make(chan struct{})}
	agg, err := NewAggregator(p,
		WithConfig(Config{
			MaxBatchSize:   1,
			FlushInterval:  5 * time.Millisecond,
			QueueSize:      10,
			ProposeTimeout: 100 * time.Millisecond,
		}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}

	hourCtx, cancelHour := context.WithTimeout(context.Background(), time.Hour)
	defer cancelHour()

	done := make(chan error, 1)
	go func() { done <- agg.Submit(hourCtx, 1, []byte("patient")) }()

	select {
	case submitErr := <-done:
		var flushErr *FlushError
		if !errors.As(submitErr, &flushErr) {
			t.Fatalf("Submit error = %T (%v), want *FlushError", submitErr, submitErr)
		}
		if !errors.Is(submitErr, context.DeadlineExceeded) {
			t.Fatalf("Submit error = %v, want it to wrap context.DeadlineExceeded", submitErr)
		}
		if !flushErr.Indeterminate {
			t.Error("a propose cut off by ProposeTimeout has an unknown outcome")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a caller-supplied hour-long deadline bypassed ProposeTimeout and " +
			"parked the flush goroutine")
	}

	closed := make(chan error, 1)
	go func() { closed <- agg.Close() }()
	select {
	case closeErr := <-closed:
		if closeErr != nil {
			t.Fatalf("Close: %v", closeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung on wg.Wait behind a flush goroutine parked for an hour")
	}
}

// TestProposeContext_UnrelatedShardKeepsFlushing pins the blast radius of the
// same defect: flush waits for every shard's goroutine, so one shard proposing
// under an hour-long caller deadline stopped shard 2 from ever flushing again.
func TestProposeContext_UnrelatedShardKeepsFlushing(t *testing.T) {
	p := &ctxBoundProposer{entered: make(chan struct{})}
	agg, err := NewAggregator(p,
		WithConfig(Config{
			MaxBatchSize:   2,
			FlushInterval:  5 * time.Millisecond,
			QueueSize:      10,
			ProposeTimeout: 100 * time.Millisecond,
		}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	hourCtx, cancelHour := context.WithTimeout(context.Background(), time.Hour)
	defer cancelHour()
	go func() { _ = agg.Submit(hourCtx, 1, []byte("patient")) }()

	// Give the patient caller time to be picked up and proposed.
	waitForCondition(t, 5*time.Second, "the patient shard never reached the proposer", func() bool {
		select {
		case <-p.entered:
			return true
		default:
			return false
		}
	})

	// An unrelated shard, with a budget far larger than ProposeTimeout.
	otherCtx, cancelOther := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelOther()
	otherDone := make(chan error, 1)
	go func() { otherDone <- agg.Submit(otherCtx, 2, []byte("unrelated")) }()

	select {
	case otherErr := <-otherDone:
		// The proposer never completes a propose, so a FlushError is the
		// expected outcome. What must NOT happen is the caller's own context
		// expiring because the aggregator stopped flushing entirely.
		var flushErr *FlushError
		if !errors.As(otherErr, &flushErr) {
			t.Fatalf("unrelated shard: Submit error = %T (%v), want *FlushError; "+
				"the aggregator stopped flushing while shard 1 held the loop",
				otherErr, otherErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("unrelated shard never got any result: the flush loop was wedged")
	}
}

// TestAwaitResult_PrefersDeliveredResultOverContextError is the regression test
// for the 50/50 select. When the batch result and the caller's context become
// ready together, Go's select chooses uniformly, so a COMMITTED write was
// reported as a timeout half the time. The caller's only recourse is to retry
// a batch of up to MaxBatchSize commands through a path with no at-most-once
// guarantee, so every such report is a potential duplicate application.
//
// Both cases are made ready before the select is evaluated, and the check is
// repeated: an unfixed build fails within the first handful of iterations.
func TestAwaitResult_PrefersDeliveredResultOverContextError(t *testing.T) {
	agg, err := NewAggregator(&mockProposer{},
		WithConfig(Config{MaxBatchSize: 1000, FlushInterval: time.Hour, QueueSize: 10}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	const trials = 200
	for i := range trials {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		p := &proposal{ctx: ctx, shardID: 1, data: []byte("x"), resultCh: make(chan error, 1)}
		p.resultCh <- nil // the batch committed

		if got := agg.awaitResult(ctx, p); got != nil {
			t.Fatalf("iteration %d: awaitResult = %v, want nil: a committed batch "+
				"was reported as a context failure", i, got)
		}
	}
}

// TestFlush_ByteCapSplitsBatch is the regression test for the missing byte
// cap. Only the proposal COUNT triggered a flush, so MaxBatchSize commands of
// a few KiB each build a batch far past the single-entry limit; the host
// rejects the whole batch as EntryTooLarge, the aggregator has no
// split-and-retry, and the next batch forms identically — writes that
// individually succeed fail forever.
func TestFlush_ByteCapSplitsBatch(t *testing.T) {
	const (
		capBytes = 1024
		cmdSize  = 500
		writers  = 12
	)

	p := newRecordingProposer()
	agg, err := NewAggregator(p,
		WithConfig(Config{
			// Neither the count nor the timer can trigger a flush here: only
			// the byte cap can.
			MaxBatchSize:  1000,
			MaxBatchBytes: capBytes,
			FlushInterval: time.Hour,
			QueueSize:     100,
		}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}

	results := make(chan error, writers)
	for i := range writers {
		cmd := bytes.Repeat([]byte{byte('a' + i)}, cmdSize)
		go func() { results <- agg.Submit(context.Background(), 1, cmd) }()
	}

	p.awaitCall(t, 5*time.Second, "the byte cap never triggered a flush")

	// Every writer must be accounted for — already proposed, or sitting in the
	// current batch — before Close, or a straggler that had not yet enqueued
	// would legitimately get a ClosedError and the run would prove nothing.
	waitForCondition(t, 10*time.Second, "not every writer reached the aggregator", func() bool {
		return len(p.commands())+agg.BatchLen() >= writers
	})

	// Close drains whatever the cap left behind, so every writer gets an
	// answer and the test can assert on the complete set of batches.
	closed := make(chan error, 1)
	go func() { closed <- agg.Close() }()
	for i := range writers {
		select {
		case submitErr := <-results:
			if submitErr != nil {
				t.Fatalf("writer %d: %v", i, submitErr)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("writer %d never got a result", i)
		}
	}
	select {
	case closeErr := <-closed:
		if closeErr != nil {
			t.Fatalf("Close: %v", closeErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}

	sizes := p.batchSizes()
	if len(sizes) == 0 {
		t.Fatal("no batch was proposed")
	}
	for i, size := range sizes {
		if size > capBytes {
			t.Errorf("batch %d was %d encoded bytes, above the %d-byte cap: "+
				"the aggregator builds batches the host must reject whole",
				i, size, capBytes)
		}
	}
	if got := len(p.commands()); got != writers {
		t.Errorf("proposer saw %d commands, want %d: the byte cap must split "+
			"batches, never drop commands", got, writers)
	}
}

// TestEncodeBatch_RejectsOverMaxInt32 pins the write-side of the length guard.
// Both decode paths refuse a length prefix above MaxInt32 (it wraps negative
// on a 32-bit int), so encoding one produces a payload this package can never
// read back. The aggregator's payload is a committed Raft entry, so the whole
// batch would be permanently undecodable on every replica.
func TestEncodeBatch_RejectsOverMaxInt32(t *testing.T) {
	if math.MaxInt < math.MaxInt64 {
		t.Skip("int is 32-bit on this platform; a MaxInt32+1 slice is unrepresentable")
	}
	// Never written to, so this costs address space rather than memory. The
	// length goes through a variable so the constant expression does not
	// overflow int on a 32-bit build before the skip above can run.
	n := math.MaxInt32
	oversized := make([]byte, n+1)

	encoded, err := EncodeBatch([][]byte{[]byte("ok"), oversized})
	if err == nil {
		t.Fatalf("EncodeBatch accepted a %d-byte command and produced %d bytes; "+
			"DecodeBatch rejects exactly this payload", len(oversized), len(encoded))
	}
	if encoded != nil {
		t.Error("EncodeBatch must not return a payload alongside an error")
	}
	if !errors.Is(err, ErrEncode) {
		t.Errorf("EncodeBatch error = %v, want it to match ErrEncode", err)
	}
	var encErr *EncodeError
	if !errors.As(err, &encErr) {
		t.Fatalf("EncodeBatch error = %T, want *EncodeError", err)
	}

	// The read side must agree: an over-MaxInt32 length prefix is refused
	// there too, which is why the write side may not emit one.
	if _, decErr := DecodeBatch([]byte{0xFF, 0xFF, 0xFF, 0xFF}); decErr == nil {
		t.Error("DecodeBatch accepted an over-MaxInt32 length prefix")
	}
}

// TestDecodeBatch_LengthPrefixNearMaxInt32DoesNotWrapBound pins the bound check
// that follows the uint32-to-int narrowing of a length prefix. A prefix of
// exactly MaxInt32 passes the MaxInt32 guard, and on a 32-bit build the sum
// off+cmdLen then overflows int to a negative value that slips past an
// off+cmdLen > len(data) comparison and panics the slice expression. Both
// decode paths must refuse it with a DecodeError on every platform; EncodeBatch
// never writes a prefix larger than the bytes that follow it.
func TestDecodeBatch_LengthPrefixNearMaxInt32DoesNotWrapBound(t *testing.T) {
	inputs := map[string][]byte{
		"first prefix":  {0x7f, 0xff, 0xff, 0xff},
		"second prefix": {0x00, 0x00, 0x00, 0x01, 0xaa, 0x7f, 0xff, 0xff, 0xff, 0xbb},
	}
	for name, data := range inputs {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("DecodeBatch panicked on a MaxInt32 length prefix: %v", r)
				}
			}()
			cmds, err := DecodeBatch(data)
			if err == nil {
				t.Fatalf("DecodeBatch accepted a MaxInt32 length prefix: %d commands", len(cmds))
			}
			var de *DecodeError
			if !errors.As(err, &de) {
				t.Fatalf("DecodeBatch error = %v (%T), want *DecodeError", err, err)
			}
		})
	}
}

// waitForBatchLen blocks until the aggregator's unflushed batch holds at least
// n proposals, failing the test rather than proceeding on a wrong premise.
func waitForBatchLen(t *testing.T, a *Aggregator, n int) {
	t.Helper()
	waitForCondition(t, 5*time.Second,
		"the batch never reached the expected length", func() bool {
			return a.BatchLen() >= n
		})
}

// waitForCondition polls cond until it holds, failing the test on timeout.
func waitForCondition(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	for !cond() {
		select {
		case <-deadline.C:
			t.Fatalf("condition not met within %v: %s", within, what)
		case <-ticker.C:
		}
	}
}
