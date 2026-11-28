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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// wireFixture is one loaded shard behind a handler that captures what it
// delivers, on a host whose window is w.
func wireFixture(t *testing.T, w proto.WireWindow) (*hostMessageHandler, *capturingEngineDeliverer) {
	t.Helper()
	eng, h, _ := newRoutingHandlerFixture(t)
	h.wire = transport.WireWindow{Proto: w, FrameCurrent: transport.WireVersion, FramePrevious: transport.WireVersion}
	node := newEngineNodeWithMembership(t, wireTestShard, 1, map[uint64]string{
		1: "127.0.0.1:6001",
		2: "127.0.0.1:6002",
	})
	eng.LoadNode(node)
	t.Cleanup(func() { eng.UnloadNode(wireTestShard) })
	capEng := &capturingEngineDeliverer{engineDeliverer: eng}
	return &hostMessageHandler{engine: capEng, host: h}, capEng
}

const wireTestShard uint64 = 7

func wireBatch(binVer, maxBinVer uint64, n int) proto.MessageBatch {
	b := proto.MessageBatch{BinVer: binVer, MaxBinVer: maxBinVer, Requests: make([]proto.Message, n)}
	for i := range b.Requests {
		b.Requests[i] = proto.Message{ShardID: wireTestShard, From: 2, To: 1, Type: proto.Heartbeat, Term: 1}
	}
	return b
}

func deliveredCount(c *capturingEngineDeliverer) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.delivered)
}

func TestHostWireWindow_DefaultsToCompileTimeWindow(t *testing.T) {
	if got := (&Host{}).WireWindow(); got != proto.DefaultWireWindow() {
		t.Fatalf("WireWindow() on an unset host = %+v, want %+v", got, proto.DefaultWireWindow())
	}
	next := proto.WireWindow{Current: proto.WireVersion + 1, Previous: proto.WireVersion}
	h := &Host{wire: transport.WireWindow{Proto: next}}
	if got := h.WireWindow(); got != next {
		t.Fatalf("WireWindow() = %+v, want the host's window %+v", got, next)
	}
}

// TestHandleMessage_WireWindowRefusal: a batch whose BinVer is outside the
// window, or whose MaxBinVer is below the window's Previous, never reaches
// the inbox and is recorded as a *WireVersionMismatchError naming both
// sides; a batch inside the window is delivered.
func TestHandleMessage_WireWindowRefusal(t *testing.T) {
	local := proto.WireWindow{Current: 3, Previous: 2}
	cases := []struct {
		name              string
		binVer, maxBinVer uint64
		refused           bool
	}{
		{"current", 3, 3, false},
		{"previous", 2, 3, false},
		{"previous-only peer", 2, 2, false},
		{"bin version below window", 1, 3, true},
		{"bin version above window", 4, 4, true},
		{"peer cannot read previous", 2, 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler, capEng := wireFixture(t, local)
			if err := handler.HandleMessage(wireBatch(tc.binVer, tc.maxBinVer, 3), authorizeAllSenders); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			got := deliveredCount(capEng)
			refusal := handler.wireRefusal(wireTestShard)
			if !tc.refused {
				if got != 3 || refusal != nil {
					t.Fatalf("delivered %d, refusal %v; want 3 delivered and no refusal", got, refusal)
				}
				return
			}
			if got != 0 {
				t.Fatalf("delivered %d messages from a peer outside the window, want 0", got)
			}
			if refusal == nil {
				t.Fatal("no *WireVersionMismatchError recorded")
			}
			var err error = refusal
			if !errors.Is(err, proto.ErrUnsupportedVersion) {
				t.Fatalf("errors.Is(%v, ErrUnsupportedVersion) = false", err)
			}
			want := WireVersionMismatchError{ShardID: wireTestShard, From: 2,
				PeerBinVer: tc.binVer, PeerMaxBinVer: tc.maxBinVer, HasPeerMaxBinVer: true, Local: local}
			if *refusal != want {
				t.Fatalf("refusal = %+v, want %+v", *refusal, want)
			}
			msg := err.Error()
			for _, part := range []string{"current 3", "previous 2"} {
				if !strings.Contains(msg, part) {
					t.Fatalf("error %q does not name this host's %s", msg, part)
				}
			}
			if !strings.Contains(msg, "wire version "+uitoa(tc.binVer)) ||
				!strings.Contains(msg, "reads up to "+uitoa(tc.maxBinVer)) {
				t.Fatalf("error %q does not name the peer's versions", msg)
			}
		})
	}
}

// TestHandleMessage_WireRefusalLogsOncePerChange: a peer stuck outside the
// window logs once, not once per batch; a change of its advertised values
// logs again; a peer back inside the window is accepted with no restart.
func TestHandleMessage_WireRefusalLogsOncePerChange(t *testing.T) {
	handler, capEng := wireFixture(t, proto.WireWindow{Current: 3, Previous: 2})
	var logs bytes.Buffer
	installDefaultLogger(t, slog.NewTextHandler(&logs, nil))
	const marker = "peer wire version outside this host's window"

	for range 10 {
		if err := handler.HandleMessage(wireBatch(1, 1, 50), authorizeAllSenders); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
	}
	if n := strings.Count(logs.String(), marker); n != 1 {
		t.Fatalf("%d refusal lines for one unchanged peer, want 1", n)
	}
	if err := handler.HandleMessage(wireBatch(4, 4, 1), authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if n := strings.Count(logs.String(), marker); n != 2 {
		t.Fatalf("%d refusal lines after the peer changed versions, want 2", n)
	}
	if deliveredCount(capEng) != 0 {
		t.Fatal("a refused message reached the inbox")
	}

	if err := handler.HandleMessage(wireBatch(2, 3, 2), authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if got := deliveredCount(capEng); got != 2 {
		t.Fatalf("delivered %d after the peer came back inside the window, want 2", got)
	}
	if r := handler.wireRefusal(wireTestShard); r != nil {
		t.Fatalf("refusal %v still recorded for a peer inside the window", r)
	}
	if err := handler.HandleMessage(wireBatch(1, 1, 1), authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if n := strings.Count(logs.String(), marker); n != 3 {
		t.Fatalf("%d refusal lines after a regression, want 3", n)
	}
}

// TestHandleMessage_WireRefusalCoversLeaderExec: leader-exec traffic from a
// peer outside the window is refused like raft traffic.
func TestHandleMessage_WireRefusalCoversLeaderExec(t *testing.T) {
	handler, capEng := wireFixture(t, proto.DefaultWireWindow())
	b := wireBatch(proto.WireVersion+1, proto.WireVersion+1, 1)
	b.Requests[0].Type = proto.LeaderExec
	if err := handler.HandleMessage(b, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if deliveredCount(capEng) != 0 || handler.wireRefusal(wireTestShard) == nil {
		t.Fatal("a leader-exec request from a peer outside the window was not refused")
	}
}

func uitoa(v uint64) string { return strconv.FormatUint(v, 10) }

// TestRefuseReceivedWireVersion_RecordsTransportErrors covers the host's
// conversion of the transport's typed refusals into the recorded
// *WireVersionMismatchError.
func TestRefuseReceivedWireVersion_RecordsTransportErrors(t *testing.T) {
	handler := &hostMessageHandler{}
	local := proto.WireWindow{Current: 3, Previous: 2}
	handler.refuseReceivedWireVersion("10.0.0.1:1",
		&proto.BatchVersionError{BinVer: 7, MaxBinVer: 8, HasMaxBinVer: true, Local: local})
	handler.refuseReceivedWireVersion("10.0.0.2:1",
		&transport.FrameVersionError{Found: 9, Current: 4, Previous: 5})
	handler.refuseReceivedWireVersion("10.0.0.3:1", transport.ErrInvalidMagic)

	got := func(addr string) *WireVersionMismatchError {
		v, ok := handler.recvRefusals.Load(addr)
		if !ok {
			return nil
		}
		return v.(*WireVersionMismatchError)
	}
	b := got("10.0.0.1:1")
	if b == nil || b.Frame || b.Source != "10.0.0.1:1" || b.PeerBinVer != 7 || b.PeerMaxBinVer != 8 || !b.HasPeerMaxBinVer ||
		b.Local != local || b.ShardID != 0 || b.From != 0 {
		t.Fatalf("batch refusal recorded as %+v", b)
	}
	f := got("10.0.0.2:1")
	if f == nil || !f.Frame || f.PeerFrameVersion != 9 || f.LocalFrameCurrent != 4 || f.LocalFramePrevious != 5 {
		t.Fatalf("frame refusal recorded as %+v", f)
	}
	for _, r := range []*WireVersionMismatchError{b, f} {
		if !errors.Is(r, proto.ErrUnsupportedVersion) || !strings.Contains(r.Error(), r.Source) {
			t.Fatalf("refusal %q: not ErrUnsupportedVersion or names no source", r)
		}
	}
	if got("10.0.0.3:1") != nil {
		t.Fatal("a non-version error was recorded as a wire refusal")
	}
}

// TestRefuseReceivedWireVersion_PastLimitStillRecords is the regression for a
// cache that, once full, suppressed the diagnostic for every later peer: a
// peer cycling source ports must not silence it for the next one.
func TestRefuseReceivedWireVersion_PastLimitStillRecords(t *testing.T) {
	awaitContainedDiagnostics(t)
	handler := &hostMessageHandler{}
	var logs bytes.Buffer
	installDefaultLogger(t, slog.NewTextHandler(&logs, nil))
	const marker = "peer wire version outside this host's window"
	refusal := &transport.FrameVersionError{Found: 9, Current: 1, Previous: 1}

	for i := range maxRecvRefusals + 10 {
		handler.refuseReceivedWireVersion("10.1.0.1:"+strconv.Itoa(1000+i), refusal)
	}
	awaitContainedDiagnostics(t)
	before := strings.Count(logs.String(), marker)
	const fresh = "192.0.2.7:4242"
	handler.refuseReceivedWireVersion(fresh, refusal)
	if _, ok := handler.recvRefusals.Load(fresh); !ok {
		t.Fatal("a new peer past the cache limit was not recorded")
	}
	awaitContainedDiagnostics(t)
	if n := strings.Count(logs.String(), marker); n != before+1 {
		t.Fatalf("%d refusal lines for a new peer past the cache limit, want 1", n-before)
	}
	if n := handler.recvRefused.Load(); n > maxRecvRefusals {
		t.Fatalf("cache holds %d entries, bound is %d", n, maxRecvRefusals)
	}
}

// TestRefuseReceivedWireVersion_RecordsPastBlockedLogHandler: with the slog
// handler blocked, refusing a new peer still records its
// *WireVersionMismatchError at once on the calling goroutine; only the log
// line waits, and it is written once the handler is released.
func TestRefuseReceivedWireVersion_RecordsPastBlockedLogHandler(t *testing.T) {
	awaitContainedDiagnostics(t)
	var logs bytes.Buffer
	release := make(chan struct{})
	installDefaultLogger(t, &gatedHandler{gate: release, next: slog.NewTextHandler(&logs, nil)})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	handler := &hostMessageHandler{}
	const fresh = "192.0.2.9:4242"
	returned := make(chan struct{})
	go func() {
		handler.refuseReceivedWireVersion(fresh, &transport.FrameVersionError{Found: 9, Current: 1, Previous: 1})
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("refuseReceivedWireVersion blocked behind a blocked log handler")
	}
	v, ok := handler.recvRefusals.Load(fresh)
	if !ok || v.(*WireVersionMismatchError).Source != fresh {
		t.Fatal("a new peer's refusal was not recorded while the log handler was blocked")
	}

	close(release)
	released = true
	awaitContainedDiagnostics(t)
	if n := strings.Count(logs.String(), "peer wire version outside this host's window"); n != 1 {
		t.Fatalf("%d refusal lines after the log handler was released, want 1", n)
	}
}

// gatedHandler holds every record until gate is closed, then forwards it.
type gatedHandler struct {
	gate <-chan struct{}
	next slog.Handler
}

func (h *gatedHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *gatedHandler) Handle(ctx context.Context, r slog.Record) error {
	<-h.gate
	return h.next.Handle(ctx, r)
}
func (h *gatedHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *gatedHandler) WithGroup(string) slog.Handler      { return h }

// TestRefuseReceivedWireVersion_ShortBatchNamesNoMaximum feeds the host the
// refusal of an unsupported-version batch payload of 8 to 31 bytes, too short
// to carry MaxBinVer: the recorded diagnostic names BinVer and never invents
// a maximum the peer did not send. A full-header refusal still names it.
func TestRefuseReceivedWireVersion_ShortBatchNamesNoMaximum(t *testing.T) {
	const far = proto.WireVersion + 2
	full := proto.MessageBatch{BinVer: far, MaxBinVer: far + 1}
	wire := make([]byte, full.Size())
	if _, err := full.MarshalTo(wire); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	const fixed = 32
	for n := 8; n <= fixed; n++ {
		handler := &hostMessageHandler{}
		var mb proto.MessageBatch
		_, err := mb.UnmarshalFromWindow(wire[:n], proto.DefaultWireWindow())
		addr := "10.0.1.1:" + strconv.Itoa(n)
		handler.refuseReceivedWireVersion(addr, err)
		v, ok := handler.recvRefusals.Load(addr)
		if !ok {
			t.Fatalf("%d bytes: refusal %v not recorded", n, err)
		}
		r := v.(*WireVersionMismatchError)
		msg := r.Error()
		if !strings.Contains(msg, "wire version "+uitoa(far)) {
			t.Fatalf("%d bytes: %q does not name BinVer %d", n, msg, far)
		}
		hasMax := n == fixed
		if r.HasPeerMaxBinVer != hasMax {
			t.Fatalf("%d bytes: HasPeerMaxBinVer = %v, want %v", n, r.HasPeerMaxBinVer, hasMax)
		}
		if got := strings.Contains(msg, "reads up to"); got != hasMax {
			t.Fatalf("%d bytes: %q names a maximum = %v, want %v", n, msg, got, hasMax)
		}
		if hasMax && !strings.Contains(msg, "reads up to "+uitoa(far+1)) {
			t.Fatalf("full header: %q does not name MaxBinVer %d", msg, far+1)
		}
	}
}
