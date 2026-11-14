package proto

import (
	"errors"
	"strings"
	"testing"
)

// TestSnapshotFileContractMarshalToRefusesOverlongFilepath holds MarshalTo to
// its write-side Filepath limit: an overlong Filepath is refused with
// ErrFilepathTooLong and nothing written, whether the buffer is large enough
// or nil (the limit is checked before the buffer size).
func TestSnapshotFileContractMarshalToRefusesOverlongFilepath(t *testing.T) {
	sf := SnapshotFile{Filepath: strings.Repeat("p", MaxFilepathLen+1)}

	buf := make([]byte, sf.Size())
	n, err := sf.MarshalTo(buf)
	if !errors.Is(err, ErrFilepathTooLong) {
		t.Fatalf("MarshalTo(sized buffer) err = %v, want ErrFilepathTooLong", err)
	}
	if n != 0 {
		t.Fatalf("MarshalTo(sized buffer) n = %d, want 0", n)
	}
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("MarshalTo(sized buffer) wrote byte %d = %#x, want nothing written", i, b)
		}
	}

	n, err = sf.MarshalTo(nil)
	if !errors.Is(err, ErrFilepathTooLong) {
		t.Fatalf("MarshalTo(nil) err = %v, want ErrFilepathTooLong", err)
	}
	if n != 0 {
		t.Fatalf("MarshalTo(nil) n = %d, want 0", n)
	}
}

// TestSnapshotFileContractMarshalToRefusesOverlongMetadataBeforeBuffer holds
// MarshalTo to checking the Metadata limit before the buffer size: an
// overlong Metadata with a nil buffer reports ErrMetadataTooLong, not
// ErrBufferTooSmall.
func TestSnapshotFileContractMarshalToRefusesOverlongMetadataBeforeBuffer(t *testing.T) {
	sf := SnapshotFile{Filepath: "f", Metadata: make([]byte, int(MaxMetadataLen)+1)}

	n, err := sf.MarshalTo(nil)
	if errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo(nil) err = ErrBufferTooSmall, want ErrMetadataTooLong first")
	}
	if !errors.Is(err, ErrMetadataTooLong) {
		t.Fatalf("MarshalTo(nil) err = %v, want ErrMetadataTooLong", err)
	}
	if n != 0 {
		t.Fatalf("MarshalTo(nil) n = %d, want 0", n)
	}
}

// TestSnapshotFileContractUnmarshalFromRefusesRecordCutAfterFilepath feeds a
// record whose Filepath decodes and which then holds only 8 of the 16 bytes
// of FileSize and FileID: UnmarshalFrom must return ErrBufferTooSmall from
// its fixed-field check, not panic and not succeed.
func TestSnapshotFileContractUnmarshalFromRefusesRecordCutAfterFilepath(t *testing.T) {
	buf := make([]byte, 4+1+8)
	if w := putString(buf, "f"); w != 5 {
		t.Fatalf("putString wrote %d bytes, want 5", w)
	}
	for i := 5; i < len(buf); i++ {
		buf[i] = 0xAB
	}

	var got SnapshotFile
	n, err := got.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom err = %v, want ErrBufferTooSmall", err)
	}
	if n != 0 {
		t.Fatalf("UnmarshalFrom n = %d, want 0", n)
	}
}

// TestSnapshotFileContractUnmarshalFromAliasesMetadata holds UnmarshalFrom to
// the type doc: Metadata aliases the source buffer (zero-copy), so a write to
// the buffer at the Metadata offset is visible through got.Metadata.
func TestSnapshotFileContractUnmarshalFromAliasesMetadata(t *testing.T) {
	sf := SnapshotFile{Filepath: "snap/file", FileSize: 7, FileID: 9, Metadata: []byte("meta")}
	buf := make([]byte, sf.Size())
	if _, err := sf.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	var got SnapshotFile
	n, err := got.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("UnmarshalFrom n = %d, want %d", n, len(buf))
	}
	if string(got.Metadata) != "meta" {
		t.Fatalf("Metadata = %q, want %q", got.Metadata, "meta")
	}

	// [fpLen:4][fp][FileSize:8][FileID:8][mdLen:4][md]
	mdOff := 4 + len(sf.Filepath) + 8 + 8 + 4
	buf[mdOff] = 'X'
	if got.Metadata[0] != 'X' {
		t.Fatalf("Metadata[0] = %q after writing the source buffer, want 'X' (Metadata must alias the buffer)", got.Metadata[0])
	}
	if string(got.Metadata) != "Xeta" {
		t.Fatalf("Metadata = %q, want %q", got.Metadata, "Xeta")
	}
}
