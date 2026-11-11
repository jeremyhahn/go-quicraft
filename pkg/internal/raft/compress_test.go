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

package raft

import (
	"bytes"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

func TestDecompressEntry_DecompressionBomb(t *testing.T) {
	// Create a large compressible payload.
	original := bytes.Repeat([]byte("X"), 10000)
	e := proto.Entry{
		Index: 1,
		Term:  1,
		Type:  proto.EntryNormal,
		Cmd:   make([]byte, len(original)),
	}
	copy(e.Cmd, original)

	CompressEntry(&e)
	if e.Type&proto.EntryCompressFlag == 0 {
		t.Fatal("expected compression")
	}

	// Set maxSize smaller than decompressed size.
	err := DecompressEntry(&e, 100)
	if err == nil {
		t.Fatal("expected decompression bomb error")
	}
	if !errors.Is(err, ErrDecompressionBomb) {
		t.Fatalf("expected ErrDecompressionBomb, got: %v", err)
	}
	// Flag should still be set since decompression was rejected.
	if e.Type&proto.EntryCompressFlag == 0 {
		t.Fatal("expected compress flag to remain set after bomb detection")
	}
}

func TestDecompressEntry_CorruptData(t *testing.T) {
	e := proto.Entry{
		Index: 1,
		Term:  1,
		Type:  proto.EntryNormal | proto.EntryCompressFlag,
		Cmd:   []byte{0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA}, // invalid snappy data
	}

	err := DecompressEntry(&e, 0)
	if err == nil {
		t.Fatal("expected error on corrupt compressed data")
	}
	var decompErr *DecompressError
	if !errors.As(err, &decompErr) {
		t.Fatalf("expected DecompressError, got: %T", err)
	}
}

func TestCompressDecompress_RoundTrip(t *testing.T) {
	// One table for the single-entry path: CompressEntry sets the flag only
	// when snappy shrinks the payload, and DecompressEntry with maxSize 0
	// (no limit) restores the bytes and clears the flag, or leaves an
	// unflagged entry untouched.
	tests := []struct {
		name           string
		data           []byte
		wantCompressed bool
	}{
		{"empty", []byte{}, false},
		{"incompressible", []byte{0x01, 0x02, 0x03, 0x04}, false},
		{"small", []byte("hello"), false},
		{"compressible", bytes.Repeat([]byte("abcdefghij"), 100), true},
		{"mixed", append(bytes.Repeat([]byte("AAAA"), 100), bytes.Repeat([]byte{0x01, 0x02}, 50)...), true},
		{"large_no_limit", bytes.Repeat([]byte("Y"), 50000), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := proto.Entry{
				Index: 42,
				Term:  7,
				Type:  proto.EntryNormal,
				Cmd:   make([]byte, len(tt.data)),
			}
			copy(e.Cmd, tt.data)

			CompressEntry(&e)
			if got := e.Type&proto.EntryCompressFlag != 0; got != tt.wantCompressed {
				t.Fatalf("compress flag = %v, want %v", got, tt.wantCompressed)
			}
			if e.Type&proto.EntryTypeMask != proto.EntryNormal {
				t.Fatalf("expected EntryNormal, got %d", e.Type&proto.EntryTypeMask)
			}
			if tt.wantCompressed {
				if len(e.Cmd) >= len(tt.data) {
					t.Fatalf("expected compressed size < %d, got %d", len(tt.data), len(e.Cmd))
				}
			} else if !bytes.Equal(e.Cmd, tt.data) {
				t.Fatal("expected cmd to be unchanged when not compressed")
			}

			err := DecompressEntry(&e, 0)
			if err != nil {
				t.Fatalf("round-trip error: %v", err)
			}
			if !bytes.Equal(e.Cmd, tt.data) {
				t.Fatal("round-trip data mismatch")
			}
			// Type must be clean after round-trip.
			if e.Type&proto.EntryCompressFlag != 0 {
				t.Fatal("compress flag should be cleared after round-trip")
			}
		})
	}
}

func TestCompressEntry_PreservesEntryType(t *testing.T) {
	// Ensure that compression flag ORs into the upper bits without
	// corrupting the lower entry type bits.
	types := []uint64{
		proto.EntryNormal,
		proto.EntryConfigChange,
		proto.EntryEncodedEntry,
	}
	for _, entryType := range types {
		e := proto.Entry{
			Index: 1,
			Term:  1,
			Type:  entryType,
			Cmd:   bytes.Repeat([]byte("test"), 100),
		}
		CompressEntry(&e)
		if e.Type&proto.EntryCompressFlag != 0 {
			// If compressed, verify the lower bits are preserved.
			if e.Type&proto.EntryTypeMask != entryType {
				t.Fatalf("entry type %d: lower bits corrupted after compression", entryType)
			}
			// Decompress and verify type is restored.
			err := DecompressEntry(&e, 0)
			if err != nil {
				t.Fatalf("entry type %d: decompress error: %v", entryType, err)
			}
			if e.Type != entryType {
				t.Fatalf("entry type %d: type not restored after decompress, got %d", entryType, e.Type)
			}
		}
	}
}

func TestCompressEntries_MultiplEntries(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryNormal, Cmd: bytes.Repeat([]byte("aaa"), 200)},
		{Index: 2, Term: 1, Type: proto.EntryNormal, Cmd: nil},
		{Index: 3, Term: 1, Type: proto.EntryNormal, Cmd: bytes.Repeat([]byte("bbb"), 300)},
	}
	originals := make([][]byte, len(entries))
	for i := range entries {
		if entries[i].Cmd != nil {
			originals[i] = make([]byte, len(entries[i].Cmd))
			copy(originals[i], entries[i].Cmd)
		}
	}

	CompressEntries(entries)

	// Entry 0 and 2 should be compressed, entry 1 (nil cmd) should not.
	if entries[0].Type&proto.EntryCompressFlag == 0 {
		t.Fatal("entry 0 should be compressed")
	}
	if entries[1].Type&proto.EntryCompressFlag != 0 {
		t.Fatal("entry 1 (nil cmd) should not be compressed")
	}
	if entries[2].Type&proto.EntryCompressFlag == 0 {
		t.Fatal("entry 2 should be compressed")
	}

	// Decompress all.
	err := DecompressEntries(entries, 0)
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}
	if !bytes.Equal(entries[0].Cmd, originals[0]) {
		t.Fatal("entry 0 data mismatch after round-trip")
	}
	if entries[1].Cmd != nil {
		t.Fatal("entry 1 should remain nil")
	}
	if !bytes.Equal(entries[2].Cmd, originals[2]) {
		t.Fatal("entry 2 data mismatch after round-trip")
	}
}

func TestDecompressEntries_StopsOnFirstError(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryNormal | proto.EntryCompressFlag, Cmd: []byte{0xFF, 0xFE}}, // corrupt
		{Index: 2, Term: 1, Type: proto.EntryNormal, Cmd: []byte("ok")},
	}

	err := DecompressEntries(entries, 0)
	if err == nil {
		t.Fatal("expected error on corrupt entry")
	}
	var decompErr *DecompressError
	if !errors.As(err, &decompErr) {
		t.Fatalf("expected DecompressError, got: %T", err)
	}
}

func TestDecompressError_ErrorString(t *testing.T) {
	inner := errors.New("snappy: corrupt input")
	de := &DecompressError{Err: inner}
	if de.Error() != "raft: decompression failed: snappy: corrupt input" {
		t.Fatalf("unexpected error string: %s", de.Error())
	}
}

func TestDecompressError_Unwrap(t *testing.T) {
	inner := errors.New("snappy: corrupt input")
	de := &DecompressError{Err: inner}
	if !errors.Is(de, inner) {
		t.Fatal("Unwrap should return inner error")
	}
}

func TestCompressEntry_LeaderPropose(t *testing.T) {
	ccBuf := func(t *testing.T) []byte {
		cc := proto.ConfigChange{
			Type:      proto.AddNode,
			ReplicaID: 99,
			Address:   "addr99",
		}
		buf := make([]byte, cc.Size())
		if _, err := cc.MarshalTo(buf); err != nil {
			t.Fatalf("MarshalTo failed: %v", err)
		}
		return buf
	}
	tests := []struct {
		name           string
		compression    config.CompressionType
		entryType      uint64
		cmd            func(t *testing.T) []byte
		wantCompressed bool
	}{
		{
			name:           "snappy_compresses_normal_entry",
			compression:    config.Snappy,
			entryType:      proto.EntryNormal,
			cmd:            func(*testing.T) []byte { return bytes.Repeat([]byte("compressible data "), 100) },
			wantCompressed: true,
		},
		{
			// Config change entries must remain uncompressed for safety.
			name:        "snappy_skips_config_change_entry",
			compression: config.Snappy,
			entryType:   proto.EntryConfigChange,
			cmd:         ccBuf,
		},
		{
			// EntryCompressionType zero value is NoCompression.
			name:        "no_compression_leaves_normal_entry",
			compression: config.NoCompression,
			entryType:   proto.EntryNormal,
			cmd:         func(*testing.T) []byte { return bytes.Repeat([]byte("uncompressed data "), 100) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.EntryCompressionType = tt.compression
			ms := singleNodeMembership()
			p := newTestPeer(t, cfg, proto.State{}, ms)

			// Self-elect as leader.
			for i := 0; i < 100; i++ {
				p.Tick()
				if p.IsLeader() {
					break
				}
			}
			if !p.IsLeader() {
				t.Fatal("peer did not become leader")
			}

			// Drain the no-op entry.
			if p.HasUpdate(0) {
				update, err := p.GetUpdate(true, 0)
				if err != nil {
					t.Fatalf("GetUpdate failed: %v", err)
				}
				if err := p.Commit(update); err != nil {
					t.Fatalf("Commit failed: %v", err)
				}
			}

			original := tt.cmd(t)
			entries := []proto.Entry{
				{Type: tt.entryType, Cmd: make([]byte, len(original))},
			}
			copy(entries[0].Cmd, original)
			if _, err := p.Propose(entries); err != nil {
				t.Fatalf("Propose failed: %v", err)
			}

			lastIdx := p.r.log.lastIndex()
			e, logErr := getEntryForTest(&p.r.log.inmem, lastIdx)
			if logErr != nil {
				t.Fatalf("getEntry failed: %v", logErr)
			}
			if got := e.Type&proto.EntryCompressFlag != 0; got != tt.wantCompressed {
				t.Fatalf("compress flag = %v, want %v", got, tt.wantCompressed)
			}
			if tt.wantCompressed {
				if len(e.Cmd) >= len(original) {
					t.Fatalf("expected compressed cmd to be smaller: %d >= %d", len(e.Cmd), len(original))
				}
			} else if !bytes.Equal(e.Cmd, original) {
				t.Fatal("entry cmd should be unchanged when not compressed")
			}
		})
	}
}

func BenchmarkCompressEntry(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark payload data "), 100)
	b.ResetTimer()
	for b.Loop() {
		e := proto.Entry{
			Index: 1,
			Term:  1,
			Type:  proto.EntryNormal,
			Cmd:   make([]byte, len(data)),
		}
		copy(e.Cmd, data)
		CompressEntry(&e)
	}
}

func BenchmarkDecompressEntry(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark payload data "), 100)
	e := proto.Entry{
		Index: 1,
		Term:  1,
		Type:  proto.EntryNormal,
		Cmd:   make([]byte, len(data)),
	}
	copy(e.Cmd, data)
	CompressEntry(&e)
	compressedCmd := make([]byte, len(e.Cmd))
	copy(compressedCmd, e.Cmd)
	compressedType := e.Type

	b.ResetTimer()
	for b.Loop() {
		e.Cmd = make([]byte, len(compressedCmd))
		copy(e.Cmd, compressedCmd)
		e.Type = compressedType
		_ = DecompressEntry(&e, 0)
	}
}

func BenchmarkCompressDecompress_RoundTrip(b *testing.B) {
	data := bytes.Repeat([]byte("round trip benchmark "), 200)
	b.ResetTimer()
	for b.Loop() {
		e := proto.Entry{
			Index: 1,
			Term:  1,
			Type:  proto.EntryNormal,
			Cmd:   make([]byte, len(data)),
		}
		copy(e.Cmd, data)
		CompressEntry(&e)
		_ = DecompressEntry(&e, 0)
	}
}
