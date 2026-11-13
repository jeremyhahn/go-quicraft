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

package engine

import (
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestEngine_DeliverLocalTo pins the snapshot receive path's injection
// contract: DeliverLocalTo reaches exactly the node it names (not whatever
// node now holds the shard), refuses a nil or stopped node, and stamps the
// delivered message proto.OriginLocal. The engine is never started, so only
// the test drains an inbox.
func TestEngine_DeliverLocalTo(t *testing.T) {
	newEngine := func() *Engine {
		return NewEngine(newTestHostConfig(), newTestLogDB(), newTestSender(), nil, nil, nil)
	}
	newShardNode := func(replicaID uint64) *Node {
		return NewNode(nil, nil, nil, config.Config{ShardID: 10, ReplicaID: replicaID}, nil, "", nil)
	}
	install := proto.Message{Type: proto.InstallSnapshot, From: 2, To: 1, ShardID: 10}

	t.Run("nil node is refused", func(t *testing.T) {
		e := newEngine()
		if e.DeliverLocalTo(nil, install) {
			t.Fatal("DeliverLocalTo(nil) = true, want false")
		}
	})

	t.Run("loaded node receives the message stamped local", func(t *testing.T) {
		e := newEngine()
		n := newShardNode(1)
		e.LoadNode(n)

		msg := install
		msg.Origin = proto.OriginRemote
		if !e.DeliverLocalTo(n, msg) {
			t.Fatal("DeliverLocalTo(loaded node) = false, want true")
		}
		got := n.DrainInbox(nil)
		if len(got) != 1 {
			t.Fatalf("inbox holds %d messages, want 1", len(got))
		}
		if got[0].Type != proto.InstallSnapshot || got[0].From != 2 {
			t.Fatalf("inbox message = %+v, want the delivered install", got[0])
		}
		if got[0].Origin != proto.OriginLocal {
			t.Fatalf("Origin = %v, want OriginLocal", got[0].Origin)
		}
	})

	t.Run("stopped node is refused and its replacement is untouched", func(t *testing.T) {
		e := newEngine()
		old := newShardNode(1)
		e.LoadNode(old)
		e.UnloadNode(10)
		replacement := newShardNode(1)
		e.LoadNode(replacement)
		old.Stop()

		if e.GetNode(10) != replacement {
			t.Fatal("GetNode(10) is not the replacement node")
		}
		if e.DeliverLocalTo(old, install) {
			t.Fatal("DeliverLocalTo(stopped old node) = true, want false")
		}
		if got := replacement.DrainInbox(nil); len(got) != 0 {
			t.Fatalf("replacement inbox holds %d messages after a delivery to the old node, want 0", len(got))
		}
		if got := old.DrainInbox(nil); len(got) != 0 {
			t.Fatalf("stopped node inbox holds %d messages, want 0", len(got))
		}

		if !e.DeliverLocalTo(replacement, install) {
			t.Fatal("DeliverLocalTo(replacement) = false, want true")
		}
		if got := old.DrainInbox(nil); len(got) != 0 {
			t.Fatalf("old node inbox holds %d messages after a delivery to the replacement, want 0", len(got))
		}
		got := replacement.DrainInbox(nil)
		if len(got) != 1 || got[0].Origin != proto.OriginLocal {
			t.Fatalf("replacement inbox = %+v, want one OriginLocal install", got)
		}
	})
}

// TestEngine_TickInterval pins TickInterval as one RTT: RTTMillisecond
// milliseconds, with 0 treated as 1 ms. The engine is never started.
func TestEngine_TickInterval(t *testing.T) {
	cases := []struct {
		name string
		rtt  uint64
		want time.Duration
	}{
		{name: "RTT 200ms", rtt: 200, want: 200 * time.Millisecond},
		{name: "RTT 0 is 1ms", rtt: 0, want: time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newTestHostConfig()
			cfg.RTTMillisecond = tc.rtt
			e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)
			if got := e.TickInterval(); got != tc.want {
				t.Fatalf("TickInterval() = %v, want %v", got, tc.want)
			}
		})
	}
}
