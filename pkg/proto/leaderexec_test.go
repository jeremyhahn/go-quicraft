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

package proto

import (
	"bytes"
	"testing"
)

// TestLeaderExecMessagesRoundTrip pins the two leader-executed request types
// to the wire: both encode, decode to the same fields, and are the last
// values the type gate admits.
func TestLeaderExecMessagesRoundTrip(t *testing.T) {
	for _, in := range []Message{
		{
			Type: LeaderExec, From: 2, To: 1, ShardID: 7, Term: 9,
			Hint: 0xfeedface, LogIndex: 1_500_000_000,
			Entries: []Entry{{Cmd: []byte("renew lease 42")}},
		},
		{
			Type: LeaderExecResp, From: 1, To: 2, ShardID: 7, Term: 9,
			Hint: 0xfeedface, Reject: true, LogIndex: 3,
			Entries: []Entry{{Cmd: []byte("handler failed")}},
		},
	} {
		buf := make([]byte, in.Size())
		n, err := in.MarshalTo(buf)
		if err != nil {
			t.Fatalf("MarshalTo(%s): %v", MessageTypeName(in.Type), err)
		}
		var out Message
		if _, err := out.UnmarshalFrom(buf[:n]); err != nil {
			t.Fatalf("UnmarshalFrom(%s): %v", MessageTypeName(in.Type), err)
		}
		if out.Type != in.Type || out.From != in.From || out.To != in.To ||
			out.ShardID != in.ShardID || out.Term != in.Term ||
			out.Hint != in.Hint || out.Reject != in.Reject ||
			out.LogIndex != in.LogIndex || len(out.Entries) != 1 ||
			!bytes.Equal(out.Entries[0].Cmd, in.Entries[0].Cmd) {
			t.Fatalf("%s round trip: got %+v, want %+v", MessageTypeName(in.Type), out, in)
		}
	}

	if maxMessageType != LeaderExecResp {
		t.Fatalf("maxMessageType = %d, want LeaderExecResp (%d)", maxMessageType, LeaderExecResp)
	}
}
