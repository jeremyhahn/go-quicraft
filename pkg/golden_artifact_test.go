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
	"reflect"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/golden"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

var goldenArtifactPayload = []byte("golden exported snapshot payload")

func goldenArtifactHeader() snapshotArtifactHeader {
	return snapshotArtifactHeader{
		Index: 40, Term: 6, OnDiskIndex: 39, Compression: 1,
		Membership: proto.Membership{
			ConfigChangeID: 3,
			// Every map is populated: the decoder yields empty maps, not nil.
			Addresses:         map[uint64]string{1: "a:1", 2: "b:2"},
			Observers:         map[uint64]string{3: "c:3"},
			Witnesses:         map[uint64]string{5: "e:5"},
			Removed:           map[uint64]bool{4: true},
			Identities:        map[uint64]string{1: "node-a", 2: "node-b"},
			RemovedIdentities: map[uint64]string{4: "node-d"},
			WireVersion:       proto.WireVersion,
		},
	}
}

// encodeGoldenArtifact writes an exported artifact the way ExportSnapshot
// frames one: header, payload through the hashing writer, then the trailer.
func encodeGoldenArtifact(t *testing.T, h snapshotArtifactHeader, payload []byte) []byte {
	t.Helper()
	header, err := marshalSnapshotArtifactHeader(h)
	if err != nil {
		t.Fatalf("marshalSnapshotArtifactHeader: %v", err)
	}
	var out bytes.Buffer
	out.Write(header)
	w := newArtifactPayloadWriter(&out, header)
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("payload write: %v", err)
	}
	out.Write(w.trailer())
	return out.Bytes()
}

// TestGoldenExportArtifact pins the exported snapshot artifact: the QRSNAP01
// header with its membership, the payload, and the QRSNTRLR trailer whose
// hash is seeded with the header bytes.
func TestGoldenExportArtifact(t *testing.T) {
	in := goldenArtifactHeader()
	golden.Check(t, "export-artifact.bin", encodeGoldenArtifact(t, in, goldenArtifactPayload))

	raw := golden.Load(t, "export-artifact.bin")
	r := bytes.NewReader(raw)
	hdr, hdrRaw, err := readSnapshotArtifactHeader(r)
	if err != nil {
		t.Fatalf("readSnapshotArtifactHeader: %v", err)
	}
	if !reflect.DeepEqual(hdr, in) {
		t.Fatalf("decoded header = %+v, want %+v", hdr, in)
	}
	rest := raw[len(hdrRaw):]
	if len(rest) < snapshotArtifactTrailerSize {
		t.Fatalf("fixture has %d bytes after the header, want at least a trailer", len(rest))
	}
	payload := rest[:len(rest)-snapshotArtifactTrailerSize]
	if !bytes.Equal(payload, goldenArtifactPayload) {
		t.Fatalf("payload = %q, want %q", payload, goldenArtifactPayload)
	}
	tr, err := parseSnapshotArtifactTrailer(rest[len(payload):])
	if err != nil {
		t.Fatalf("parseSnapshotArtifactTrailer: %v", err)
	}
	d := seededArtifactDigest(hdrRaw)
	_, _ = d.Write(payload)
	if err := tr.check(uint64(len(payload)), d.Sum64()); err != nil {
		t.Fatalf("trailer check: %v", err)
	}
	if out := encodeGoldenArtifact(t, hdr, payload); !bytes.Equal(out, raw) {
		t.Fatalf("re-encoding the decoded artifact changed its bytes:\n got  %x\n want %x", out, raw)
	}
}
