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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// nextBuildWindow is the window of a build one version ahead of this one.
func nextBuildWindow() WireWindow {
	return WireWindow{
		Proto:         proto.WireWindow{Current: proto.WireVersion + 1, Previous: proto.WireVersion},
		FrameCurrent:  WireVersion + 1,
		FramePrevious: WireVersion,
	}
}

// TestSendVersions_FollowCommittedVersion pins the mapping from a shard's
// committed WireVersion to the pair stamped on its traffic: the current pair
// only for a shard switched to the current version, the previous pair for
// everything else, including the zero of a shard that never switched.
func TestSendVersions_FollowCommittedVersion(t *testing.T) {
	w := nextBuildWindow()
	for _, tc := range []struct {
		committed uint64
		payload   uint64
		frame     byte
	}{
		{0, w.Proto.Previous, w.FramePrevious},
		{w.Proto.Previous, w.Proto.Previous, w.FramePrevious},
		{w.Proto.Current, w.Proto.Current, w.FrameCurrent},
		{w.Proto.Current + 1, w.Proto.Previous, w.FramePrevious},
	} {
		p, f := w.sendVersions(tc.committed)
		if p != tc.payload || f != tc.frame {
			t.Errorf("sendVersions(%d) = (%d, %d), want (%d, %d)",
				tc.committed, p, f, tc.payload, tc.frame)
		}
	}
}

// TestBatchSendVersions_MixedBatchTakesLowest pins that a batch carrying
// several shards is stamped with the lowest committed version among them, and
// that a transport with no lookup sends the previous version.
func TestBatchSendVersions_MixedBatchTakesLowest(t *testing.T) {
	w := nextBuildWindow()
	tr := &QUICTransport{}
	tr.cfg.Wire = w

	msgs := []proto.Message{{ShardID: 1}, {ShardID: 1}, {ShardID: 2}}
	if p, f := tr.batchSendVersions(msgs); p != w.Proto.Previous || f != w.FramePrevious {
		t.Fatalf("no lookup: (%d, %d), want the previous pair", p, f)
	}

	committed := map[uint64]uint64{1: w.Proto.Current, 2: w.Proto.Current}
	calls := 0
	tr.SetWireVersionLookup(func(shardID uint64) uint64 {
		calls++
		return committed[shardID]
	})
	if p, f := tr.batchSendVersions(msgs); p != w.Proto.Current || f != w.FrameCurrent {
		t.Fatalf("all switched: (%d, %d), want the current pair", p, f)
	}
	if calls != 2 {
		t.Fatalf("lookup called %d times for two consecutive runs, want 2", calls)
	}

	committed[2] = 0
	if p, f := tr.batchSendVersions(msgs); p != w.Proto.Previous || f != w.FramePrevious {
		t.Fatalf("one shard not switched: (%d, %d), want the previous pair", p, f)
	}
	if p, f := tr.shardSendVersions(1); p != w.Proto.Current || f != w.FrameCurrent {
		t.Fatalf("shardSendVersions(1) = (%d, %d), want the current pair", p, f)
	}
}
