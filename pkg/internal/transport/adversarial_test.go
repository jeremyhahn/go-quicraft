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

// Adversarial coverage for the message-frame RECEIVE path.
//
// The fuzzing this package already had (fuzz_test.go) covers frame header
// round-trips and header validation. It never drives handleMessageFrame,
// which is where a frame stops being bytes and becomes a batch the engine
// hands to raft: decompression, batch decode, the ShardID / DeploymentID /
// message-count gates, and the zero-copy detach that has to run before the
// pooled receive buffer is recycled.
//
// REACHABILITY. handleMessageFrame is reachable without a live QUIC
// connection: it takes the inbound *quic.Conn only to consult per-connection
// state, and every such call is nil-guarded (reserveConnMsgRecvBudget,
// inboundAuthorizedForCached, evictStaleOutboundOnRecv via the cleared
// SourceAddress). Existing tests already call it with a nil connection. What
// a nil connection does NOT reach is the certificate-bound SourceAddress
// authorisation and the stale-outbound eviction that hangs off it: with no
// connection there is no peer certificate, inboundAuthorizedForCached returns
// false, the claimed SourceAddress is cleared, and evictStaleOutboundOnRecv
// early-returns. Those two behaviours need a real mTLS session and are
// covered by the connection tests, not here.
//
// The property that matters most here is the one that makes the raft-side
// origin gate real: a message reconstructed from bytes is REMOTE. If a frame
// could ever produce proto.OriginLocal, every local-injection exemption in
// raft's step() -- the exemption a forged wire InstallSnapshot was once able
// to use -- becomes reachable from the network.

package transport

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	mathrand "math/rand"
	"testing"

	"github.com/golang/snappy"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// adversarialDeploymentID is the deployment the transport under test belongs
// to. A batch naming any other deployment must be refused.
const adversarialDeploymentID uint64 = 42

// quietTransportLogs silences the receive path's slog output. Every refused
// frame logs a warning, which at fuzz throughput dominates the runtime.
func quietTransportLogs(tb testing.TB) {
	tb.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelError + 1,
	})))
	tb.Cleanup(func() { slog.SetDefault(prev) })
}

// newAdversarialTransport builds the transport under test and its handler.
func newAdversarialTransport(tb testing.TB) (*QUICTransport, *testHandler) {
	tb.Helper()
	handler := newTestHandler()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  adversarialDeploymentID,
		MTLSConfig:    testMTLSConfig(tb),
	}, handler, registry.NewRegistry())
	if err != nil {
		tb.Fatalf("NewQUICTransport: %v", err)
	}
	return tr, handler
}

// frameCoverage counts what a frame-fuzz run actually reached.
type frameCoverage struct {
	frames          int
	delivered       int
	decodeErrors    int
	deploymentRejct int
	compressed      int
	withEntries     int
	withSnapshot    int
}

// runMessageFrameCase feeds one frame payload through handleMessageFrame and
// asserts the receive path's contract on whatever comes out the other side.
//
// Refusal is a perfectly good outcome and is not asserted against: hostile
// bytes SHOULD be refused. What is asserted is that anything the transport
// does ACCEPT satisfies the invariants the layers above it rely on.
func runMessageFrameCase(tb testing.TB, tr *QUICTransport, handler *testHandler, payload []byte, flags FrameFlags, cov *frameCoverage) {
	tb.Helper()

	before := len(handler.getBatches())
	budgetBefore := tr.msgRecvBudget.Load()

	err := tr.handleMessageFrame(nil, bytes.NewReader(payload), uint32(len(payload)), flags)

	// The aggregate receive-memory budget is reserved before the buffer is
	// allocated and released on every exit path. An unbalanced release leaks
	// the budget: enough leaked frames and the transport refuses every
	// subsequent one, which is a remotely triggerable denial of service on a
	// path whose whole purpose is to bound memory.
	if got := tr.msgRecvBudget.Load(); got != budgetBefore {
		tb.Fatalf("message receive budget leaked: %d before, %d after (err=%v, flags=%#x, len=%d)",
			budgetBefore, got, err, flags, len(payload))
	}

	batches := handler.getBatches()
	if cov != nil {
		cov.frames++
		if err != nil {
			cov.decodeErrors++
		}
		if errors.Is(err, ErrPeerIdentityMismatch) {
			cov.deploymentRejct++
		}
		if flags&FlagCompressed != 0 {
			cov.compressed++
		}
	}
	if len(batches) == before {
		return
	}
	if cov != nil {
		cov.delivered++
	}

	for _, batch := range batches[before:] {
		// Deployment isolation: a batch from a foreign deployment must never
		// reach the handler.
		if batch.DeploymentID != adversarialDeploymentID {
			tb.Fatalf("delivered a batch from deployment %d, want %d",
				batch.DeploymentID, adversarialDeploymentID)
		}
		if len(batch.Requests) > proto.MaxMessagesPerBatch {
			tb.Fatalf("delivered %d messages, over the %d limit",
				len(batch.Requests), proto.MaxMessagesPerBatch)
		}
		for i := range batch.Requests {
			m := &batch.Requests[i]
			// ShardID 0 is reserved; the engine routes by it.
			if m.ShardID == 0 {
				tb.Fatalf("delivered a message with ShardID=0 (type %s, from %d)",
					proto.MessageTypeName(m.Type), m.From)
			}
			// THE origin property. A message reconstructed from bytes is
			// remote by definition, and raft's process-local exemptions --
			// including the one a forged InstallSnapshot once slipped
			// through -- are keyed on exactly this stamp being unforgeable
			// from the wire.
			if m.Origin.IsLocal() {
				tb.Fatalf("delivered a message stamped OriginLocal from the wire (type %s, from %d): every process-local exemption in raft.step is now reachable from the network",
					proto.MessageTypeName(m.Type), m.From)
			}
			if len(m.Entries) > proto.MaxEntriesPerMessage {
				tb.Fatalf("delivered a message with %d entries, over the %d limit",
					len(m.Entries), proto.MaxEntriesPerMessage)
			}
			if cov != nil {
				if len(m.Entries) > 0 {
					cov.withEntries++
				}
				if !m.Snapshot.IsEmpty() {
					cov.withSnapshot++
				}
			}
		}
	}
}

// assertNoBufferAliasing pins the zero-copy detach: Entry.Cmd and
// Snapshot.Checksum alias the POOLED receive buffer after unmarshal, and the
// buffer is recycled as soon as the frame is dispatched. A surviving alias is
// read by the step worker after the buffer has been handed to another frame,
// so the raft layer sees another peer's bytes as its own entry payload.
//
// The check is generic rather than example-based: whatever a frame delivered,
// push a second frame of the same size through to recycle the buffer, then
// re-read the delivered bytes and require them unchanged.
func assertNoBufferAliasing(tb testing.TB, tr *QUICTransport, handler *testHandler, payload []byte) {
	tb.Helper()

	before := len(handler.getBatches())
	if err := tr.handleMessageFrame(nil, bytes.NewReader(payload), uint32(len(payload)), FlagNone); err != nil {
		return
	}
	batches := handler.getBatches()
	if len(batches) == before {
		return
	}

	// Snapshot the delivered payload bytes.
	type record struct {
		label string
		want  []byte
		got   *[]byte
	}
	var records []record
	for bi := range batches[before:] {
		batch := &batches[before+bi]
		for i := range batch.Requests {
			m := &batch.Requests[i]
			for j := range m.Entries {
				cmd := m.Entries[j].Cmd
				if len(cmd) == 0 {
					continue
				}
				want := make([]byte, len(cmd))
				copy(want, cmd)
				records = append(records, record{label: "Entry.Cmd", want: want, got: &m.Entries[j].Cmd})
			}
			if sum := m.Snapshot.Checksum; len(sum) > 0 {
				want := make([]byte, len(sum))
				copy(want, sum)
				records = append(records, record{label: "Snapshot.Checksum", want: want, got: &m.Snapshot.Checksum})
			}
		}
	}
	if len(records) == 0 {
		return
	}

	// Recycle the pooled buffer through a frame of identical length carrying
	// inverted bytes. A surviving alias now reads this frame's content.
	poison := make([]byte, len(payload))
	for i := range payload {
		poison[i] = ^payload[i]
	}
	_ = tr.handleMessageFrame(nil, bytes.NewReader(poison), uint32(len(poison)), FlagNone)

	for _, rec := range records {
		if !bytes.Equal(*rec.got, rec.want) {
			tb.Fatalf("%s still aliased the pooled receive buffer: %x became %x after the buffer was recycled",
				rec.label, rec.want, *rec.got)
		}
	}
}

// frameSeed pairs a corpus entry with its frame flags.
type frameSeed struct {
	name    string
	payload []byte
	flags   FrameFlags
}

// adversarialFrameSeeds is the seed corpus for the frame-decode fuzz target.
// Structured seeds matter here: the wire format is fixed-layout, so a purely
// random byte string is rejected by the batch decoder essentially always and
// the fuzzer would never reach the delivery path at all.
func adversarialFrameSeeds(tb testing.TB) []frameSeed {
	tb.Helper()
	var seeds []frameSeed

	marshal := func(b proto.MessageBatch) []byte {
		buf := make([]byte, b.Size())
		n, err := b.MarshalTo(buf)
		if err != nil {
			tb.Fatalf("MessageBatch MarshalTo: %v", err)
		}
		return buf[:n]
	}
	add := func(name string, b proto.MessageBatch) {
		raw := marshal(b)
		seeds = append(seeds, frameSeed{name: name, payload: raw, flags: FlagNone})
		seeds = append(seeds, frameSeed{
			name:    name + "-compressed",
			payload: snappy.Encode(nil, raw),
			flags:   FlagCompressed,
		})
	}

	base := func(msgs ...proto.Message) proto.MessageBatch {
		return proto.MessageBatch{
			BinVer:        proto.WireVersion,
			DeploymentID:  adversarialDeploymentID,
			SourceEpoch:   7,
			SourceAddress: "10.0.0.2:9000",
			Requests:      msgs,
		}
	}

	// A plain heartbeat batch: the minimal well-formed frame.
	add("heartbeat", base(proto.Message{
		Type: proto.Heartbeat, ShardID: 1, From: 2, To: 1, Term: 5, Commit: 9,
	}))

	// A replication batch with entry payloads, which is what the zero-copy
	// detach exists for.
	add("replicate-with-entries", base(proto.Message{
		Type: proto.Replicate, ShardID: 1, From: 2, To: 1, Term: 5,
		LogIndex: 9, LogTerm: 4, Commit: 9,
		Entries: []proto.Entry{
			{Index: 10, Term: 5, Type: proto.EntryNormal, Cmd: []byte("payload-one")},
			{Index: 11, Term: 5, Type: proto.EntryConfigChange, Cmd: []byte("payload-two")},
		},
	}))

	// An InstallSnapshot carrying a full Membership and a checksum: the exact
	// shape of the forged-snapshot finding, arriving as wire bytes.
	add("install-snapshot-with-membership", base(proto.Message{
		Type: proto.InstallSnapshot, ShardID: 1, From: 2, To: 1, Term: 0,
		Snapshot: proto.Snapshot{
			ShardID: 1, Index: 99, Term: 5,
			Checksum: bytes.Repeat([]byte{0xAB}, 32),
			Membership: proto.Membership{
				ConfigChangeID: ^uint64(0),
				MaxWitnesses:   7,
				Addresses:      map[uint64]string{4: "evil-a", 5: "evil-b"},
				Witnesses:      map[uint64]string{3: "evil-c"},
			},
		},
	}))

	// ShardID 0: reserved, and the gate that drops it is the last thing
	// between a corrupt batch and the engine's routing table.
	add("shard-zero", base(proto.Message{
		Type: proto.Heartbeat, ShardID: 0, From: 2, To: 1, Term: 5,
	}))

	// A foreign deployment.
	foreign := base(proto.Message{Type: proto.Heartbeat, ShardID: 1, From: 2, To: 1, Term: 5})
	foreign.DeploymentID = adversarialDeploymentID + 1
	add("foreign-deployment", foreign)

	// A wrong wire version.
	badVer := base(proto.Message{Type: proto.Heartbeat, ShardID: 1, From: 2, To: 1, Term: 5})
	badVer.BinVer = proto.WireVersion + 1
	add("wrong-binver", badVer)

	// A many-message batch, exercising the per-message loops and the count
	// gate.
	many := make([]proto.Message, 64)
	for i := range many {
		many[i] = proto.Message{
			Type: proto.Heartbeat, ShardID: uint64(i%3) + 1, From: 2, To: 1,
			Term: uint64(i) + 1,
		}
	}
	add("many-messages", base(many...))

	// A batch whose SourceAddress is a spoofed peer address.
	spoof := base(proto.Message{Type: proto.Heartbeat, ShardID: 1, From: 2, To: 1, Term: 5})
	spoof.SourceAddress = "127.0.0.1:1"
	add("spoofed-source-address", spoof)

	// Non-batch inputs: truncations and garbage, so the corpus is not made up
	// only of well-formed frames.
	full := marshal(base(proto.Message{Type: proto.Heartbeat, ShardID: 1, From: 2, To: 1, Term: 5}))
	for _, cut := range []int{1, 8, 24, 40, len(full) - 1} {
		if cut > 0 && cut < len(full) {
			seeds = append(seeds, frameSeed{
				name:    "truncated",
				payload: append([]byte(nil), full[:cut]...),
				flags:   FlagNone,
			})
		}
	}
	seeds = append(seeds,
		frameSeed{name: "empty", payload: nil, flags: FlagNone},
		frameSeed{name: "garbage", payload: bytes.Repeat([]byte{0xFF}, 128), flags: FlagNone},
		frameSeed{name: "garbage-compressed", payload: bytes.Repeat([]byte{0xFF}, 128), flags: FlagCompressed},
		// A snappy stream declaring a huge decompressed length: the
		// decompression-bomb gate.
		frameSeed{name: "snappy-bomb", payload: snappy.Encode(nil, make([]byte, 1<<20)), flags: FlagCompressed},
	)

	return seeds
}

// TestAdversarialMessageFrameSeeds replays the frame seed corpus through the
// receive path. This is what makes the fuzz target meaningful in CI: without
// -fuzz, `go test` runs exactly these inputs.
func TestAdversarialMessageFrameSeeds(t *testing.T) {
	quietTransportLogs(t)
	for _, seed := range adversarialFrameSeeds(t) {
		t.Run(seed.name, func(t *testing.T) {
			tr, handler := newAdversarialTransport(t)
			runMessageFrameCase(t, tr, handler, seed.payload, seed.flags, nil)
			if seed.flags == FlagNone {
				assertNoBufferAliasing(t, tr, handler, seed.payload)
			}
		})
	}
}

// TestAdversarialMessageFrameSweep mutates the structured seeds the way a
// fuzzer would -- bit flips, byte splices, truncations and length lies -- and
// pushes every result through the receive path. It is the deterministic
// stand-in for -fuzz that runs on every commit.
func TestAdversarialMessageFrameSweep(t *testing.T) {
	quietTransportLogs(t)
	tr, handler := newAdversarialTransport(t)
	cov := &frameCoverage{}
	seeds := adversarialFrameSeeds(t)
	rng := mathrand.New(mathrand.NewSource(0xBEEF_F00D))

	for i := 0; i < 4000; i++ {
		seed := seeds[rng.Intn(len(seeds))]
		payload := append([]byte(nil), seed.payload...)
		flags := seed.flags
		switch rng.Intn(5) {
		case 0:
			// Bit flips.
			for f := 0; f < 1+rng.Intn(8) && len(payload) > 0; f++ {
				payload[rng.Intn(len(payload))] ^= 1 << rng.Intn(8)
			}
		case 1:
			// Truncation.
			if len(payload) > 1 {
				payload = payload[:rng.Intn(len(payload))]
			}
		case 2:
			// Extension with arbitrary trailing bytes.
			extra := make([]byte, rng.Intn(64))
			for j := range extra {
				extra[j] = byte(rng.Intn(256))
			}
			payload = append(payload, extra...)
		case 3:
			// Flip the compression flag without recompressing: a frame whose
			// declared encoding contradicts its bytes.
			flags ^= FlagCompressed
		case 4:
			// Splice two seeds together.
			other := seeds[rng.Intn(len(seeds))]
			payload = append(payload, other.payload...)
		}
		runMessageFrameCase(t, tr, handler, payload, flags, cov)

		// The handler retains every batch; drop them so a long sweep does not
		// accumulate unbounded memory.
		if i%256 == 255 {
			tr, handler = newAdversarialTransport(t)
		}
	}

	// Anti-vacuity: a sweep in which every frame is refused at the decoder
	// asserts nothing about the delivery path. These bounds are far below the
	// observed values; they exist to catch a collapse to zero.
	if cov.delivered < 100 {
		t.Errorf("frame sweep delivered only %d batches of %d frames; the mutation set no longer reaches the delivery path and its assertions are vacuous",
			cov.delivered, cov.frames)
	}
	if cov.decodeErrors < 100 {
		t.Errorf("frame sweep produced only %d decode refusals of %d frames; the mutation set is not reaching the refusal paths",
			cov.decodeErrors, cov.frames)
	}
	if cov.withEntries < 20 {
		t.Errorf("frame sweep delivered only %d messages carrying entries; the zero-copy detach path is barely exercised",
			cov.withEntries)
	}
	t.Logf("frame sweep: frames=%d delivered=%d decodeErrors=%d deploymentRejects=%d compressed=%d withEntries=%d withSnapshot=%d",
		cov.frames, cov.delivered, cov.decodeErrors, cov.deploymentRejct,
		cov.compressed, cov.withEntries, cov.withSnapshot)
}
