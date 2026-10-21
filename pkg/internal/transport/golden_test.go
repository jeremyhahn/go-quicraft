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
	"bytes"
	"io"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/golden"
)

var goldenFramePayload = []byte("golden frame payload")

func goldenSnapshotHeader() SnapshotHeader {
	return SnapshotHeader{
		ShardID: 3, ReplicaID: 2, Index: 40, Term: 6,
		ChunkCount: 3, Epoch: 9, From: 1,
	}
}

// TestGoldenFrame pins the QUIC frame header (magic, WireVersion, flags,
// big-endian length) in both directions.
func TestGoldenFrame(t *testing.T) {
	buf := make([]byte, FrameHeaderSize+len(goldenFramePayload))
	n, err := MarshalFrame(buf, goldenFramePayload, FlagCompressed)
	if err != nil {
		t.Fatalf("MarshalFrame: %v", err)
	}
	golden.Check(t, "frame.bin", buf[:n])

	raw := golden.Load(t, "frame.bin")
	r := bytes.NewReader(raw)
	length, flags, err := ReadFrameHeader(r)
	if err != nil {
		t.Fatalf("ReadFrameHeader: %v", err)
	}
	if flags != FlagCompressed {
		t.Fatalf("flags = %#x, want %#x", flags, FlagCompressed)
	}
	if int(length) != len(goldenFramePayload) {
		t.Fatalf("length = %d, want %d", length, len(goldenFramePayload))
	}
	payload, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if !bytes.Equal(payload, goldenFramePayload) {
		t.Fatalf("payload = %q, want %q", payload, goldenFramePayload)
	}
}

// TestGoldenSnapshotHeader pins the snapshot stream header in both directions.
func TestGoldenSnapshotHeader(t *testing.T) {
	in := goldenSnapshotHeader()
	buf := make([]byte, SnapshotHeaderSize)
	n, err := in.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	golden.Check(t, "snapshot-header.bin", buf[:n])

	raw := golden.Load(t, "snapshot-header.bin")
	var got SnapshotHeader
	n, err = got.UnmarshalFrom(raw)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if n != len(raw) {
		t.Fatalf("UnmarshalFrom consumed %d of %d bytes", n, len(raw))
	}
	if got != in {
		t.Fatalf("decoded header = %+v, want %+v", got, in)
	}
	out := make([]byte, SnapshotHeaderSize)
	if _, err := got.MarshalTo(out); err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(out, raw) {
		t.Fatalf("re-encoding the decoded header changed its bytes:\n got  %x\n want %x", out, raw)
	}
}
