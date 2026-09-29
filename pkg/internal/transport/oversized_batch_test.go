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

package transport

import (
	"crypto/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// dialCounterPair is a sender/receiver transport pair whose sender counts every
// successful outbound dial through the connection-established event.
type dialCounterPair struct {
	sender   *QUICTransport
	receiver *QUICTransport
	handler  *testHandler
	metrics  *dropRecorder
	dials    *atomic.Int64
	target   string
}

// newDialCounterPair starts two transports sharing one mTLS identity and
// registers replica 2 (shard 1) as the receiver, so sender.Send routes to it.
func newDialCounterPair(t *testing.T) *dialCounterPair {
	t.Helper()

	mtls := testMTLSConfig(t)

	recvHandler := newTestHandler()
	recvReg := registry.NewRegistry()
	receiver, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, recvHandler, recvReg)
	if err != nil {
		t.Fatalf("NewQUICTransport(receiver) failed: %v", err)
	}
	if err := receiver.Start(); err != nil {
		t.Fatalf("receiver.Start failed: %v", err)
	}
	t.Cleanup(func() { _ = receiver.Stop() })

	dials := &atomic.Int64{}
	listener := &config.EventListener{
		OnConnectionEstablished: func(config.ConnectionInfo) {
			dials.Add(1)
		},
	}

	metrics := &dropRecorder{}
	sendReg := registry.NewRegistry()
	sender, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		EventListener: listener,
		Metrics:       metrics,
	}, newTestHandler(), sendReg)
	if err != nil {
		t.Fatalf("NewQUICTransport(sender) failed: %v", err)
	}
	if err := sender.Start(); err != nil {
		t.Fatalf("sender.Start failed: %v", err)
	}
	t.Cleanup(func() { _ = sender.Stop() })

	target := receiver.Addr().String()
	sendReg.Register(1, 2, target)

	return &dialCounterPair{
		sender:   sender,
		receiver: receiver,
		handler:  recvHandler,
		metrics:  metrics,
		dials:    dials,
		target:   target,
	}
}

// entriesOfSize builds n entries at consecutive indices, each carrying payload
// bytes of incompressible command data.
func entriesOfSize(t *testing.T, n int, payload int, firstIndex uint64) []proto.Entry {
	t.Helper()
	entries := make([]proto.Entry, n)
	for i := range entries {
		entries[i] = proto.Entry{
			Type:  proto.EntryNormal,
			Index: firstIndex + uint64(i),
			Term:  1,
		}
		if payload > 0 {
			cmd := make([]byte, payload)
			if _, err := rand.Read(cmd); err != nil {
				t.Fatalf("rand.Read failed: %v", err)
			}
			entries[i].Cmd = cmd
		}
	}
	return entries
}

// compressibleEntriesOfSize builds entries exactly as entriesOfSize does but
// fills each command with zeros instead of random bytes.
//
// A batch only splits across frames when its members total more than
// MaxFrameSize, so a test of the split has to build 16MB+ of messages — there
// is no smaller shape that reaches the branch. What that costs on the wire is
// a free choice, because sendableGroup sizes the group on the UNCOMPRESSED
// marshaled size (see frame.go) while sendFrame Snappy-compresses what it
// writes. Zero-filled commands therefore produce byte-for-byte the same
// grouping and the same number of frames as random ones, for a fraction of the
// QUIC and race-detector work. Use entriesOfSize where the point is that the
// payload is incompressible.
func compressibleEntriesOfSize(t *testing.T, n int, payload int, firstIndex uint64) []proto.Entry {
	t.Helper()
	entries := entriesOfSize(t, n, 0, firstIndex)
	for i := range entries {
		entries[i].Cmd = make([]byte, payload)
	}
	return entries
}

// receivedEntries returns the total number of entries the handler observed.
func receivedEntries(h *testHandler) int {
	total := 0
	for _, b := range h.getBatches() {
		for i := range b.Requests {
			total += len(b.Requests[i].Entries)
		}
	}
	return total
}

// BenchmarkSendAdmission measures the send hot path's framing admission
// control against the sizing pass it replaces. sendBatch computes each
// message's Size() exactly once (sendFrame is handed the result) just as the
// single MessageBatch.Size() call did before, so the added cost is the
// per-batch header sizing plus a few integer compares per message.
func BenchmarkSendAdmission(b *testing.B) {
	const addr = "127.0.0.1:12345"

	cases := []struct {
		name    string
		msgs    int
		entries int
		cmdLen  int
	}{
		{name: "heartbeats/16", msgs: 16},
		{name: "replicate/64x64x256B", msgs: 64, entries: 64, cmdLen: 256},
	}

	for _, tc := range cases {
		msgs := make([]proto.Message, tc.msgs)
		for i := range msgs {
			msgs[i] = proto.Message{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1}
			if tc.entries > 0 {
				entries := make([]proto.Entry, tc.entries)
				cmd := make([]byte, tc.cmdLen)
				for j := range entries {
					entries[j] = proto.Entry{Index: uint64(j + 1), Term: 1, Cmd: cmd}
				}
				msgs[i].Entries = entries
			}
		}

		b.Run(tc.name+"/admission", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				headerSize := batchHeaderSize(addr)
				rest := msgs
				for len(rest) > 0 {
					n, _, err := sendableGroup(headerSize, rest)
					if err != nil {
						b.Fatal(err)
					}
					rest = rest[n:]
				}
			}
		})

		b.Run(tc.name+"/batchsize", func(b *testing.B) {
			mb := proto.MessageBatch{SourceAddress: addr, Requests: msgs}
			b.ReportAllocs()
			for b.Loop() {
				if mb.Size() == 0 {
					b.Fatal("unexpected zero size")
				}
			}
		})
	}
}

// TestOversizedReplicateDoesNotStormDials is the regression test for the
// leader/follower dial storm: a Replicate message carrying more entries than
// the decoder accepts (proto.MaxEntriesPerMessage) is rejected by the RECEIVER,
// which resets the stream. Before the fix the sender read that reset as a
// generic write failure, tore the whole connection down and re-dialed, then
// retried the identical message — an unbounded dial loop that also starved the
// follower, because every attempt to ship it the entries it needed destroyed
// the connection.
//
// The batch is unsendable by construction, so the fix is not that it is
// delivered (raft must not build it: see raft.sendAppend's entry-count bound)
// but that it is dropped at the sender WITHOUT touching the connection.
func TestOversizedReplicateDoesNotStormDials(t *testing.T) {
	p := newDialCounterPair(t)

	// Warm the connection with a normal message so exactly one dial has
	// happened before the oversized sends begin.
	p.sender.Send([]proto.Message{{
		Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
		Entries: entriesOfSize(t, 1, 8, 1),
	}})
	p.handler.waitBatch(t, 10*time.Second)

	if got := p.dials.Load(); got != 1 {
		t.Fatalf("expected exactly 1 dial to warm the connection, got %d", got)
	}

	const attempts = 20
	for i := 0; i < attempts; i++ {
		p.sender.Send([]proto.Message{{
			Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			LogIndex: 1, LogTerm: 1,
			Entries: entriesOfSize(t, proto.MaxEntriesPerMessage+1, 0, 2),
		}})
		// Each attempt is an independent trial only once the previous one has
		// been resolved. The message is unsendable, so the sender drops it and
		// records the drop; that record is the event to wait for, and waiting
		// for it is both faster and correct under load.
		want := i + 1
		pollUntil(t, 10*time.Second, time.Millisecond,
			"an unsendable message was never recorded as dropped", func() bool {
				return countDropsWithReason(p.metrics, dropReasonUnsendable) >= want
			})
	}

	dials := p.dials.Load()
	t.Logf("dials after %d oversized sends: %d", attempts, dials)
	if dials != 1 {
		t.Fatalf("oversized batch caused re-dialing: %d dials after %d sends (want 1)", dials, attempts)
	}

	// The connection must still be usable: a normal message sent afterwards is
	// delivered without a new dial.
	before := len(p.handler.getBatches())
	p.sender.Send([]proto.Message{{
		Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2,
		Entries: entriesOfSize(t, 1, 8, 100),
	}})
	pollUntil(t, 10*time.Second, 20*time.Millisecond,
		"normal message was not delivered after an oversized batch", func() bool {
			return len(p.handler.getBatches()) > before
		})
	if got := p.dials.Load(); got != 1 {
		t.Fatalf("recovery required a re-dial: %d dials (want 1)", got)
	}
}

// TestPeerRejectedFrameKeepsConnection covers the other half of the loop: a
// frame the sender considers well-formed but the RECEIVER refuses (here a
// DeploymentID mismatch, but equally a wire-version skew or a decompression
// failure). The peer resets the stream with QUICStreamErrFrameRejected, which
// the sender must read as "that frame was unacceptable", not "this connection
// is dead" — otherwise every rejected frame costs a connection teardown and a
// re-dial, forever.
func TestPeerRejectedFrameKeepsConnection(t *testing.T) {
	mtls := testMTLSConfig(t)

	// Receiver in a DIFFERENT deployment: it authenticates the sender at the
	// TLS layer, then rejects every batch it decodes.
	recvHandler := newTestHandler()
	receiver, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  43,
		MTLSConfig:    mtls,
	}, recvHandler, registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(receiver) failed: %v", err)
	}
	if err := receiver.Start(); err != nil {
		t.Fatalf("receiver.Start failed: %v", err)
	}
	defer func() { _ = receiver.Stop() }()

	dials := &atomic.Int64{}
	sendReg := registry.NewRegistry()
	metrics := &dropRecorder{}
	sender, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		Metrics:       metrics,
		EventListener: &config.EventListener{
			OnConnectionEstablished: func(config.ConnectionInfo) { dials.Add(1) },
		},
	}, newTestHandler(), sendReg)
	if err != nil {
		t.Fatalf("NewQUICTransport(sender) failed: %v", err)
	}
	if err := sender.Start(); err != nil {
		t.Fatalf("sender.Start failed: %v", err)
	}
	defer func() { _ = sender.Stop() }()

	sendReg.Register(1, 2, receiver.Addr().String())

	// As in TestPeerRejectedHeartbeatFrameKeepsConnection, this pacing cannot
	// become a poll on PeerFrameRejections: the counter trails the sends by one
	// write, so waiting for attempt i before making attempt i+1 deadlocks. See
	// #71.
	const attempts = 20
	for i := 0; i < attempts; i++ {
		sender.Send([]proto.Message{{
			Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: entriesOfSize(t, 1, 8, uint64(i+1)),
		}})
		time.Sleep(50 * time.Millisecond)
	}

	got := dials.Load()
	t.Logf("dials after %d rejected sends: %d, peer rejections: %d",
		attempts, got, sender.PeerFrameRejections())
	if got != 1 {
		t.Fatalf("peer frame rejection caused re-dialing: %d dials after %d sends (want 1)", got, attempts)
	}
	if sender.PeerFrameRejections() == 0 {
		t.Fatal("expected the peer's frame rejections to be counted")
	}
}

// TestUnframeableBatchKeepsConnectionOpen asserts the connection-level
// invariant directly: a batch whose serialized payload cannot fit in a frame
// must not close the QUIC connection to the target.
func TestUnframeableBatchKeepsConnectionOpen(t *testing.T) {
	p := newDialCounterPair(t)

	p.sender.Send([]proto.Message{{
		Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
		Entries: entriesOfSize(t, 1, 8, 1),
	}})
	p.handler.waitBatch(t, 10*time.Second)

	p.sender.connMu.RLock()
	conn := p.sender.conns[p.target]
	p.sender.connMu.RUnlock()
	if conn == nil {
		t.Fatal("no cached connection to the target after the warmup send")
	}

	// One message larger than a whole frame: 20 entries of 1MB each.
	p.sender.Send([]proto.Message{{
		Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
		LogIndex: 1, LogTerm: 1,
		Entries: entriesOfSize(t, 20, 1024*1024, 2),
	}})

	assertNever(t, 2*time.Second, 50*time.Millisecond,
		"unframeable batch closed the connection", func() bool {
			select {
			case <-conn.Context().Done():
				return true
			default:
			}
			p.sender.connMu.RLock()
			cached := p.sender.conns[p.target]
			p.sender.connMu.RUnlock()
			return cached != conn
		})

	if got := p.dials.Load(); got != 1 {
		t.Fatalf("unframeable batch triggered a re-dial: %d dials (want 1)", got)
	}
}

// TestCoalescedBatchSplitsAcrossFrames verifies the sender splits a coalesced
// batch that does not fit one frame into several frames instead of dropping
// it: every message must still be delivered, over a single connection.
//
// This is the "payload_bytes=18415406 max_frame_size=16777216" case from the
// field: individually sendable messages that the send queue coalesced into a
// batch no frame can carry. sendBatch is called directly because the
// coalescing depends on the worker's drain timing, which a test cannot force.
func TestCoalescedBatchSplitsAcrossFrames(t *testing.T) {
	p := newDialCounterPair(t)

	p.sender.Send([]proto.Message{{
		Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
		Entries: entriesOfSize(t, 1, 8, 1),
	}})
	p.handler.waitBatch(t, 10*time.Second)

	// Four 5MB messages: each fits a frame on its own, the four together
	// (20MB) do not. The commands are compressible so the split decision —
	// which is made on the uncompressed size — is unchanged while the bytes
	// that actually cross the wire are not 20MB. Under -race on a loaded host
	// that difference is the whole of this test's cost.
	const msgCount = 4
	msgs := make([]proto.Message, msgCount)
	for i := range msgs {
		msgs[i] = proto.Message{
			Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			LogIndex: uint64(i), LogTerm: 1,
			Entries: compressibleEntriesOfSize(t, 5, 1024*1024, uint64(10*i+1)),
		}
	}
	wantEntries := receivedEntries(p.handler) + msgCount*5
	p.sender.sendBatch(p.target, "", msgs)

	pollUntil(t, 30*time.Second, 50*time.Millisecond,
		"messages from an oversized coalesced batch were not delivered", func() bool {
			return receivedEntries(p.handler) >= wantEntries
		})

	if got := p.dials.Load(); got != 1 {
		t.Fatalf("frame splitting triggered a re-dial: %d dials (want 1)", got)
	}
}

// TestUnsendableMessageDoesNotPoisonBatch verifies that one message which can
// never be framed does not take the rest of its batch down with it. Before the
// fix the whole coalesced batch — up to 255 innocent messages, heartbeats
// included — was discarded on the single MarshalFrame failure.
func TestUnsendableMessageDoesNotPoisonBatch(t *testing.T) {
	p := newDialCounterPair(t)

	p.sender.Send([]proto.Message{{
		Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
		Entries: entriesOfSize(t, 1, 8, 1),
	}})
	p.handler.waitBatch(t, 10*time.Second)

	before := receivedEntries(p.handler)
	p.sender.sendBatch(p.target, "", []proto.Message{
		{
			// Unsendable: larger than a whole frame.
			Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			LogIndex: 1, LogTerm: 1,
			Entries: entriesOfSize(t, 20, 1024*1024, 2),
		},
		{
			// Unsendable: more entries than any peer's decoder accepts.
			Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			LogIndex: 1, LogTerm: 1,
			Entries: entriesOfSize(t, proto.MaxEntriesPerMessage+1, 0, 2),
		},
		{
			// Sendable, and it must still arrive.
			Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 3,
			LogIndex: 1, LogTerm: 1,
			Entries: entriesOfSize(t, 2, 16, 500),
		},
	})

	pollUntil(t, 10*time.Second, 20*time.Millisecond,
		"a sendable message was dropped because it shared a batch with an unsendable one",
		func() bool { return receivedEntries(p.handler) >= before+2 })

	if got := p.dials.Load(); got != 1 {
		t.Fatalf("unsendable messages triggered a re-dial: %d dials (want 1)", got)
	}

	// The unsendable messages must be visible as drops, not silently lost.
	var unsendable int
	for _, d := range p.metrics.drops() {
		if d.reason == dropReasonUnsendable {
			unsendable++
		}
	}
	if unsendable != 2 {
		t.Fatalf("expected both unsendable messages to be counted as drops, got %d", unsendable)
	}
}
