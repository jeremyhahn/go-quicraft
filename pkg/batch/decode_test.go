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

package batch

import (
	"bytes"
	"errors"
	"testing"
)

// mustEncodeBatch encodes cmds and fails the test if the batch format cannot
// represent them. EncodeBatch reports that as an error rather than emitting a
// payload the decoder would reject; every command in these tests is well
// within the format's limits, so an error here is a test failure.
func mustEncodeBatch(t *testing.T, cmds [][]byte) []byte {
	t.Helper()
	encoded, err := EncodeBatch(cmds)
	if err != nil {
		t.Fatalf("EncodeBatch: unexpected error: %v", err)
	}
	return encoded
}

func TestEncodeBatch_EmptySlice(t *testing.T) {
	t.Parallel()
	result := mustEncodeBatch(t, [][]byte{})
	if len(result) != 0 {
		t.Errorf("EncodeBatch(empty) produced %d bytes, want 0", len(result))
	}
}

func TestEncodeBatch_SingleCommand(t *testing.T) {
	t.Parallel()
	large := make([]byte, 64*1024)
	for i := range large {
		large[i] = byte(i % 256)
	}
	for _, tc := range []struct {
		name string
		cmd  []byte
	}{
		{"small", []byte("hello")},
		{"64KiB", large},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := mustEncodeBatch(t, [][]byte{tc.cmd})

			// One 4-byte length prefix followed by the command bytes.
			if want := lengthPrefixSize + len(tc.cmd); len(result) != want {
				t.Fatalf("EncodeBatch single command produced %d bytes, want %d", len(result), want)
			}

			decoded, err := DecodeBatch(result)
			if err != nil {
				t.Fatalf("DecodeBatch failed: %v", err)
			}
			if len(decoded) != 1 {
				t.Fatalf("decoded %d commands, want 1", len(decoded))
			}
			if !bytes.Equal(decoded[0], tc.cmd) {
				t.Error("decoded command does not match original")
			}
		})
	}
}

func TestEncodeBatch_EmptyCommands(t *testing.T) {
	t.Parallel()
	cmds := [][]byte{
		{},
		{},
		{},
	}
	result := mustEncodeBatch(t, cmds)

	// Each empty command still gets a 4-byte zero length prefix.
	expectedLen := 3 * lengthPrefixSize
	if len(result) != expectedLen {
		t.Fatalf("EncodeBatch with empty commands produced %d bytes, want %d", len(result), expectedLen)
	}

	decoded, err := DecodeBatch(result)
	if err != nil {
		t.Fatalf("DecodeBatch failed: %v", err)
	}
	if len(decoded) != 3 {
		t.Fatalf("decoded %d commands, want 3", len(decoded))
	}
	for i, d := range decoded {
		if len(d) != 0 {
			t.Errorf("decoded[%d] has length %d, want 0", i, len(d))
		}
	}
}

func TestDecodeBatch_EmptyInput(t *testing.T) {
	t.Parallel()
	result, err := DecodeBatch([]byte{})
	if err != nil {
		t.Fatalf("DecodeBatch(empty) returned error: %v", err)
	}
	if result != nil {
		t.Errorf("DecodeBatch(empty) = %v, want nil", result)
	}
}

func TestDecodeBatch_TruncatedPrefix(t *testing.T) {
	t.Parallel()

	// Only 2 bytes when 4 are needed for a length prefix.
	data := []byte{0x00, 0x01}
	_, err := DecodeBatch(data)
	if err == nil {
		t.Fatal("DecodeBatch with truncated prefix should return error")
	}

	var de *DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("error should be *DecodeError, got %T", err)
	}
	if de.Reason != "truncated length prefix" {
		t.Errorf("Reason = %q, want %q", de.Reason, "truncated length prefix")
	}
	if !errors.Is(err, ErrDecode) {
		t.Error("error should match ErrDecode sentinel")
	}
}

func TestDecodeBatch_TruncatedData(t *testing.T) {
	t.Parallel()

	// Length prefix says 10 bytes, but only 3 bytes of data follow.
	data := []byte{
		0x00, 0x00, 0x00, 0x0A, // length = 10
		0x01, 0x02, 0x03, // only 3 bytes of data
	}
	_, err := DecodeBatch(data)
	if err == nil {
		t.Fatal("DecodeBatch with truncated data should return error")
	}

	var de *DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("error should be *DecodeError, got %T", err)
	}
	if de.Reason != "command data exceeds payload boundary" {
		t.Errorf("Reason = %q, want %q", de.Reason, "command data exceeds payload boundary")
	}
}

func TestDecodeBatch_TruncatedSecondPrefix(t *testing.T) {
	t.Parallel()

	// First command is valid (length=1, data=0xFF), then 2 bytes of garbage
	// which is not enough for a second length prefix.
	data := []byte{
		0x00, 0x00, 0x00, 0x01, // length = 1
		0xFF,       // 1 byte of data
		0x00, 0x01, // truncated second prefix
	}
	_, err := DecodeBatch(data)
	if err == nil {
		t.Fatal("DecodeBatch with truncated second prefix should return error")
	}

	if !errors.Is(err, ErrDecode) {
		t.Error("error should match ErrDecode sentinel")
	}
}

func TestDecodeBatch_Garbage(t *testing.T) {
	t.Parallel()

	// A valid prefix claiming a huge length that exceeds the payload.
	data := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err := DecodeBatch(data)
	if err == nil {
		t.Fatal("DecodeBatch with garbage data should return error")
	}

	if !errors.Is(err, ErrDecode) {
		t.Error("error should match ErrDecode sentinel")
	}
}

func TestRoundTrip_MultipleCommands(t *testing.T) {
	t.Parallel()
	original := [][]byte{
		[]byte("cmd1"),
		[]byte("cmd2-longer-data"),
		[]byte("c"),
		[]byte(""),
		[]byte("cmd5-with-binary-\x00\x01\x02"),
	}
	encoded := mustEncodeBatch(t, original)

	// Every command, empty or not, carries its own 4-byte length prefix.
	wantLen := len(original) * lengthPrefixSize
	for _, cmd := range original {
		wantLen += len(cmd)
	}
	if len(encoded) != wantLen {
		t.Fatalf("EncodeBatch produced %d bytes, want %d", len(encoded), wantLen)
	}

	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatalf("round-trip decode failed: %v", err)
	}
	if len(decoded) != len(original) {
		t.Fatalf("decoded %d commands, want %d", len(decoded), len(original))
	}
	for i := range original {
		if !bytes.Equal(decoded[i], original[i]) {
			t.Errorf("decoded[%d] = %q, want %q", i, decoded[i], original[i])
		}
	}
}
