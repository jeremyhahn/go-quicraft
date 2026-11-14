package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// windowHeader builds a frame header the way frame_test.go does: magic,
// version, flags, then a big-endian payload length.
func windowHeader(version byte, flags FrameFlags, length uint32) [FrameHeaderSize]byte {
	h := [FrameHeaderSize]byte{MagicByte0, MagicByte1, version, byte(flags)}
	binary.BigEndian.PutUint32(h[4:8], length)
	return h
}

// sweepWindow checks every one of the 256 version bytes against w and fails
// unless exactly the bytes in want are accepted and every other byte returns
// ErrUnsupportedVersion.
func sweepWindow(t *testing.T, w WireWindow, want ...byte) {
	t.Helper()
	wanted := make(map[byte]bool, len(want))
	for _, b := range want {
		wanted[b] = true
	}
	accepted := 0
	for v := 0; v < 256; v++ {
		header := windowHeader(byte(v), FlagNone, 10)
		err := ValidateFrameHeaderWindow(header[:], w)
		if wanted[byte(v)] {
			if err != nil {
				t.Fatalf("version 0x%02x is in window %+v and must be accepted, got %v", v, w, err)
			}
			accepted++
			continue
		}
		if !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("version 0x%02x outside window %+v: got %v, want ErrUnsupportedVersion", v, w, err)
		}
	}
	if accepted != len(wanted) {
		t.Fatalf("%d version bytes accepted by window %+v, want exactly %d", accepted, w, len(wanted))
	}
}

// TestValidateFrameHeaderWindow pins the two-member rolling-upgrade window:
// a gate that compared only FrameCurrent, or only FramePrevious, would refuse
// every peer on the other side of an upgrade and must fail here.
func TestValidateFrameHeaderWindow(t *testing.T) {
	t.Run("next build window accepts current and previous", func(t *testing.T) {
		sweepWindow(t, nextBuildWindow(), WireVersion, WireVersion+1)
	})

	t.Run("non-adjacent window accepts exactly its two members", func(t *testing.T) {
		const current, previous byte = 0x7d, 0x13
		w := WireWindow{FrameCurrent: current, FramePrevious: previous}
		sweepWindow(t, w, current, previous)
	})

	t.Run("buffer and magic checks run before the version gate", func(t *testing.T) {
		w := nextBuildWindow()
		outside := WireVersion + 2

		badMagic := windowHeader(outside, FlagNone, 10)
		badMagic[0] ^= 0xFF
		if err := ValidateFrameHeaderWindow(badMagic[:], w); !errors.Is(err, ErrInvalidMagic) {
			t.Fatalf("bad magic with out-of-window version: got %v, want ErrInvalidMagic", err)
		}

		short := windowHeader(outside, FlagNone, 10)
		for n := 0; n < FrameHeaderSize; n++ {
			if err := ValidateFrameHeaderWindow(short[:n], w); !errors.Is(err, ErrBufferTooSmall) {
				t.Fatalf("buffer of %d bytes: got %v, want ErrBufferTooSmall", n, err)
			}
		}
	})

	t.Run("size check applies to in-window versions", func(t *testing.T) {
		w := nextBuildWindow()
		for _, v := range []byte{WireVersion, WireVersion + 1} {
			atMax := windowHeader(v, FlagNone, MaxFrameSize)
			if err := ValidateFrameHeaderWindow(atMax[:], w); err != nil {
				t.Fatalf("version 0x%02x length MaxFrameSize: got %v, want nil", v, err)
			}
			over := windowHeader(v, FlagNone, MaxFrameSize+1)
			if err := ValidateFrameHeaderWindow(over[:], w); !errors.Is(err, ErrFrameTooLarge) {
				t.Fatalf("version 0x%02x length MaxFrameSize+1: got %v, want ErrFrameTooLarge", v, err)
			}
		}
	})

	t.Run("readFrameHeaderWindow honours the window it is given", func(t *testing.T) {
		const length uint32 = 4321
		flags := FrameFlags(0x05)
		header := windowHeader(WireVersion+1, flags, length)

		gotLen, gotFlags, err := readFrameHeaderWindow(bytes.NewReader(header[:]), nextBuildWindow())
		if err != nil {
			t.Fatalf("WireVersion+1 under nextBuildWindow: got %v, want nil", err)
		}
		if gotLen != length || gotFlags != flags {
			t.Fatalf("got length %d flags 0x%02x, want length %d flags 0x%02x", gotLen, gotFlags, length, flags)
		}

		_, _, err = readFrameHeaderWindow(bytes.NewReader(header[:]), DefaultWireWindow())
		if !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("WireVersion+1 under DefaultWireWindow: got %v, want ErrUnsupportedVersion", err)
		}
	})
}
