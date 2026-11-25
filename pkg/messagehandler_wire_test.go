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
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"

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
			refusal := handler.wireRefusal(wireTestShard, 2)
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
				PeerBinVer: tc.binVer, PeerMaxBinVer: tc.maxBinVer, Local: local}
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
	if r := handler.wireRefusal(wireTestShard, 2); r != nil {
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
	if deliveredCount(capEng) != 0 || handler.wireRefusal(wireTestShard, 2) == nil {
		t.Fatal("a leader-exec request from a peer outside the window was not refused")
	}
}

func uitoa(v uint64) string { return strconv.FormatUint(v, 10) }
