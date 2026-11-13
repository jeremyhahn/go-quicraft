// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proto

import (
	"bytes"
	"context"
	"testing"
)

// originTestMessage returns a message with every wire field populated, so a
// test that compares encodings compares the whole layout and not a header.
func originTestMessage(origin MessageOrigin) Message {
	return Message{
		Type:      Replicate,
		From:      1,
		To:        2,
		ShardID:   100,
		ReplicaID: 1,
		Term:      5,
		LogTerm:   4,
		LogIndex:  99,
		Commit:    98,
		Reject:    true,
		Hint:      50,
		Entries:   []Entry{{Term: 5, Index: 100, Type: EntryNormal, Cmd: []byte("cmd")}},
		Snapshot:  Snapshot{Index: 90, Term: 4},
		Origin:    origin,
	}
}

// marshalOriginMessage encodes m and fails the test on error.
func marshalOriginMessage(ctx context.Context, t *testing.T, m *Message) []byte {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	buf := make([]byte, m.Size())
	n, err := m.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	return buf[:n]
}

// marshalOriginBatch encodes a batch of count requests stamped OriginLocal.
func marshalOriginBatch(ctx context.Context, t *testing.T, count int) []byte {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	mb := MessageBatch{BinVer: WireVersion, DeploymentID: 7, SourceAddress: "peer"}
	for i := 0; i < count; i++ {
		m := originTestMessage(OriginLocal)
		m.To = uint64(i + 2)
		mb.Requests = append(mb.Requests, m)
	}
	buf := make([]byte, mb.Size())
	n, err := mb.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	return buf[:n]
}

// assertAllRemote fails if any of reqs is not OriginRemote.
func assertAllRemote(ctx context.Context, t *testing.T, reqs []Message) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	for i := range reqs {
		if reqs[i].Origin != OriginRemote {
			t.Fatalf("Requests[%d].Origin = %v (%d), want remote", i, reqs[i].Origin, reqs[i].Origin)
		}
	}
}

func TestMessageOriginZeroValueAndNames(t *testing.T) {
	var zero MessageOrigin
	if zero != OriginRemote {
		t.Fatalf("zero MessageOrigin = %d, want OriginRemote (%d)", zero, OriginRemote)
	}
	var m Message
	if m.Origin != OriginRemote || m.Origin.IsLocal() {
		t.Fatalf("zero Message.Origin = %v, want remote", m.Origin)
	}
	for _, tc := range []struct {
		o       MessageOrigin
		isLocal bool
		name    string
	}{
		{OriginRemote, false, "remote"},
		{OriginLocal, true, "local"},
		// Out of range: never local, and a stable name.
		{OriginLocal + 1, false, "remote"},
		{MessageOrigin(255), false, "remote"},
	} {
		if got := tc.o.IsLocal(); got != tc.isLocal {
			t.Fatalf("MessageOrigin(%d).IsLocal() = %v, want %v", tc.o, got, tc.isLocal)
		}
		if got := tc.o.String(); got != tc.name {
			t.Fatalf("MessageOrigin(%d).String() = %q, want %q", tc.o, got, tc.name)
		}
		if again := tc.o.String(); again != tc.name {
			t.Fatalf("MessageOrigin(%d).String() not stable: %q then %q", tc.o, tc.name, again)
		}
	}
}

func TestMessageOriginUnmarshalFromStampsRemote(t *testing.T) {
	ctx := context.Background()
	src := originTestMessage(OriginLocal)
	buf := marshalOriginMessage(ctx, t, &src)
	for _, stale := range []MessageOrigin{OriginLocal, OriginLocal + 1} {
		dst := Message{Origin: stale}
		n, err := dst.UnmarshalFrom(buf)
		if err != nil {
			t.Fatalf("UnmarshalFrom: %v", err)
		}
		if n != len(buf) {
			t.Fatalf("UnmarshalFrom consumed %d, want %d", n, len(buf))
		}
		if dst.Origin != OriginRemote {
			t.Fatalf("decoded into Origin %d: got %v, want remote", stale, dst.Origin)
		}
		assertMessageEqual(t, &src, &dst)
	}
}

func TestMessageOriginResetClearsLocal(t *testing.T) {
	m := originTestMessage(OriginLocal)
	m.Reset()
	if m.Origin != OriginRemote {
		t.Fatalf("Reset: Origin = %v, want remote", m.Origin)
	}
	m.Origin = OriginLocal + 1
	m.Reset()
	if m.Origin != OriginRemote {
		t.Fatalf("Reset of out-of-range origin: Origin = %d, want remote", m.Origin)
	}
}

func TestMessageOriginNeverReachesTheWire(t *testing.T) {
	ctx := context.Background()
	local := originTestMessage(OriginLocal)
	remote := originTestMessage(OriginRemote)
	other := originTestMessage(OriginLocal + 1)
	if local.Size() != remote.Size() || other.Size() != remote.Size() {
		t.Fatalf("Size depends on Origin: local %d, remote %d, other %d",
			local.Size(), remote.Size(), other.Size())
	}
	want := marshalOriginMessage(ctx, t, &remote)
	if got := marshalOriginMessage(ctx, t, &local); !bytes.Equal(got, want) {
		t.Fatal("OriginLocal message marshals to different bytes than OriginRemote")
	}
	if got := marshalOriginMessage(ctx, t, &other); !bytes.Equal(got, want) {
		t.Fatal("out-of-range Origin marshals to different bytes than OriginRemote")
	}
}

func TestMessageOriginBatchDecodesRemote(t *testing.T) {
	ctx := context.Background()
	buf := marshalOriginBatch(ctx, t, 3)

	t.Run("fresh batch", func(t *testing.T) {
		var mb MessageBatch
		if _, err := mb.UnmarshalFrom(buf); err != nil {
			t.Fatalf("UnmarshalFrom: %v", err)
		}
		if len(mb.Requests) != 3 {
			t.Fatalf("len(Requests) = %d, want 3", len(mb.Requests))
		}
		assertAllRemote(ctx, t, mb.Requests)
	})

	t.Run("reused batch within length", func(t *testing.T) {
		mb := MessageBatch{Requests: make([]Message, 3)}
		for i := range mb.Requests {
			mb.Requests[i].Origin = OriginLocal
		}
		if _, err := mb.UnmarshalFrom(buf); err != nil {
			t.Fatalf("UnmarshalFrom: %v", err)
		}
		assertAllRemote(ctx, t, mb.Requests)
	})

	t.Run("reused batch in spare capacity", func(t *testing.T) {
		backing := make([]Message, 5)
		for i := range backing {
			backing[i].Origin = OriginLocal
		}
		mb := MessageBatch{Requests: backing[:0]}
		if _, err := mb.UnmarshalFrom(buf); err != nil {
			t.Fatalf("UnmarshalFrom: %v", err)
		}
		if len(mb.Requests) != 3 {
			t.Fatalf("len(Requests) = %d, want 3", len(mb.Requests))
		}
		assertAllRemote(ctx, t, mb.Requests)
	})

	t.Run("reused after a decode and Reset", func(t *testing.T) {
		var mb MessageBatch
		if _, err := mb.UnmarshalFrom(buf); err != nil {
			t.Fatalf("UnmarshalFrom: %v", err)
		}
		for i := range mb.Requests {
			mb.Requests[i].Origin = OriginLocal
		}
		mb.Reset()
		if _, err := mb.UnmarshalFromWindow(buf, DefaultWireWindow()); err != nil {
			t.Fatalf("UnmarshalFromWindow: %v", err)
		}
		if len(mb.Requests) != 3 {
			t.Fatalf("len(Requests) = %d, want 3", len(mb.Requests))
		}
		assertAllRemote(ctx, t, mb.Requests)
	})
}
