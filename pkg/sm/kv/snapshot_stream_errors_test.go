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

package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// snapshotStreamCutAfterKey returns a snapshot stream holding the pair
// count (1), a key length (1) and the key "k", then ending, followed by
// extra bytes of the value length.
func snapshotStreamCutAfterKey(_ context.Context, t *testing.T, extra int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(1)); err != nil {
		t.Fatalf("write count: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(1)); err != nil {
		t.Fatalf("write key length: %v", err)
	}
	buf.WriteString("k")
	valLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(valLen, 1)
	buf.Write(valLen[:extra])
	return buf.Bytes()
}

// assertSnapshotStreamWriterError fails t unless err is the error errWriter
// returns.
func assertSnapshotStreamWriterError(_ context.Context, t *testing.T, err error) {
	t.Helper()
	var ice *InvalidCommandError
	if !errors.As(err, &ice) || ice.Reason != "write error" {
		t.Fatalf("SaveSnapshot = %v, want the writer's error", err)
	}
}

// TestKV_SnapshotStreamSaveReturnsWriterErrorFromEveryField fails the
// writer at the key bytes, the value length and the value bytes of the
// single pair "k" -> "v"; SaveSnapshot must return the writer's error.
func TestKV_SnapshotStreamSaveReturnsWriterErrorFromEveryField(t *testing.T) {
	// Stream layout: count [0,4), keyLen [4,8), key [8,9),
	// valLen [9,13), value [13,14).
	limits := []struct {
		name  string
		limit int
	}{
		{"key_bytes", 8},
		{"value_length", 9},
		{"value_bytes", 13},
	}
	for _, b := range backends() {
		putAll(t, b, "k", "v")
		for _, tc := range limits {
			t.Run(b.name+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				w := &errWriter{limit: tc.limit}
				err := b.save(ctx, w, make(chan struct{}))
				assertSnapshotStreamWriterError(ctx, t, err)
				if w.written != tc.limit {
					t.Fatalf("written = %d, want %d", w.written, tc.limit)
				}
			})
		}
	}
}

// TestKV_SnapshotStreamCutAfterKeyReportsEOF recovers from a stream cut
// after the key: with no value length byte it is io.EOF, with two of the
// four bytes it is io.ErrUnexpectedEOF.
func TestKV_SnapshotStreamCutAfterKeyReportsEOF(t *testing.T) {
	cases := []struct {
		name  string
		extra int
		want  error
	}{
		{"no_value_length", 0, io.EOF},
		{"half_value_length", 2, io.ErrUnexpectedEOF},
	}
	for _, b := range backends() {
		for _, tc := range cases {
			t.Run(b.name+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				stream := snapshotStreamCutAfterKey(ctx, t, tc.extra)
				err := b.recov(ctx, bytes.NewReader(stream), make(chan struct{}))
				if !errors.Is(err, tc.want) {
					t.Fatalf("RecoverFromSnapshot = %v, want %v", err, tc.want)
				}
			})
		}
	}
}

// TestKV_SnapshotStreamFailedRecoveryKeepsData seeds "live" -> "v", fails a
// recovery from each cut stream, and checks the data and hash are unchanged.
func TestKV_SnapshotStreamFailedRecoveryKeepsData(t *testing.T) {
	for _, extra := range []int{0, 2} {
		for _, b := range backends() {
			t.Run(b.name, func(t *testing.T) {
				ctx := context.Background()
				putAll(t, b, "live", "v")
				before, err := b.hash()
				if err != nil {
					t.Fatalf("GetHash = %v", err)
				}
				stream := snapshotStreamCutAfterKey(ctx, t, extra)
				if err := b.recov(ctx, bytes.NewReader(stream), make(chan struct{})); err == nil {
					t.Fatal("RecoverFromSnapshot succeeded on a cut stream")
				}
				got, err := b.bytes([]byte("live"))
				if err != nil || string(got) != "v" {
					t.Fatalf("BytesLookup(live) = %q, %v; want \"v\"", got, err)
				}
				if _, err := b.bytes([]byte("k")); err == nil {
					t.Fatal("key from the failed stream is visible")
				}
				after, err := b.hash()
				if err != nil {
					t.Fatalf("GetHash = %v", err)
				}
				if after != before {
					t.Fatalf("GetHash = %x after failed recovery, want %x", after, before)
				}
			})
		}
	}
}
