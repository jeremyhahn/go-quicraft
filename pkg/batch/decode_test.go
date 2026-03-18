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

func TestEncodeBatch_NilSlice(t *testing.T) {
	t.Parallel()
	result := EncodeBatch(nil)
	if len(result) != 0 {
		t.Errorf("EncodeBatch(nil) produced %d bytes, want 0", len(result))
	}
}

func TestEncodeBatch_EmptySlice(t *testing.T) {
	t.Parallel()
	result := EncodeBatch([][]byte{})
	if len(result) != 0 {
		t.Errorf("EncodeBatch(empty) produced %d bytes, want 0", len(result))
	}
}

func TestEncodeBatch_SingleCommand(t *testing.T) {
	t.Parallel()
	cmd := []byte("hello")
	result := EncodeBatch([][]byte{cmd})

	// Expected: 4 bytes length prefix + 5 bytes data = 9 bytes
	if len(result) != 9 {
		t.Fatalf("EncodeBatch single command produced %d bytes, want 9", len(result))
	}

	// Verify the data round-trips correctly.
	decoded, err := DecodeBatch(result)
	if err != nil {
		t.Fatalf("DecodeBatch failed: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("decoded %d commands, want 1", len(decoded))
	}
	if !bytes.Equal(decoded[0], cmd) {
		t.Errorf("decoded[0] = %q, want %q", decoded[0], cmd)
	}
}

func TestEncodeBatch_MultipleCommands(t *testing.T) {
	t.Parallel()
	cmds := [][]byte{
		[]byte("PUT key1 val1"),
		[]byte("GET key2"),
		[]byte("DELETE key3"),
	}
	result := EncodeBatch(cmds)

	// Expected: 3 * 4 (prefixes) + 13 + 8 + 11 (data) = 12 + 32 = 44 bytes
	expectedLen := 3*lengthPrefixSize + 13 + 8 + 11
	if len(result) != expectedLen {
		t.Fatalf("EncodeBatch produced %d bytes, want %d", len(result), expectedLen)
	}
}

func TestEncodeBatch_EmptyCommands(t *testing.T) {
	t.Parallel()
	cmds := [][]byte{
		{},
		{},
		{},
	}
	result := EncodeBatch(cmds)

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

func TestEncodeBatch_LargeCommand(t *testing.T) {
	t.Parallel()
	large := make([]byte, 64*1024)
	for i := range large {
		large[i] = byte(i % 256)
	}
	cmds := [][]byte{large}
	result := EncodeBatch(cmds)

	expectedLen := lengthPrefixSize + len(large)
	if len(result) != expectedLen {
		t.Fatalf("EncodeBatch produced %d bytes, want %d", len(result), expectedLen)
	}

	decoded, err := DecodeBatch(result)
	if err != nil {
		t.Fatalf("DecodeBatch failed: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("decoded %d commands, want 1", len(decoded))
	}
	if !bytes.Equal(decoded[0], large) {
		t.Error("decoded large command does not match original")
	}
}

func TestDecodeBatch_NilInput(t *testing.T) {
	t.Parallel()
	result, err := DecodeBatch(nil)
	if err != nil {
		t.Fatalf("DecodeBatch(nil) returned error: %v", err)
	}
	if result != nil {
		t.Errorf("DecodeBatch(nil) = %v, want nil", result)
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

func TestRoundTrip_SingleCommand(t *testing.T) {
	t.Parallel()
	original := [][]byte{[]byte("raft-proposal-data")}
	encoded := EncodeBatch(original)
	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatalf("round-trip decode failed: %v", err)
	}
	if len(decoded) != len(original) {
		t.Fatalf("decoded %d commands, want %d", len(decoded), len(original))
	}
	if !bytes.Equal(decoded[0], original[0]) {
		t.Errorf("decoded[0] = %q, want %q", decoded[0], original[0])
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
	encoded := EncodeBatch(original)
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

func TestRoundTrip_EmptyCommands(t *testing.T) {
	t.Parallel()
	original := [][]byte{{}, {}, {}}
	encoded := EncodeBatch(original)
	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatalf("round-trip decode failed: %v", err)
	}
	if len(decoded) != 3 {
		t.Fatalf("decoded %d commands, want 3", len(decoded))
	}
}

func TestRoundTrip_BinaryData(t *testing.T) {
	t.Parallel()
	// Create commands with all possible byte values.
	cmd := make([]byte, 256)
	for i := range cmd {
		cmd[i] = byte(i)
	}
	original := [][]byte{cmd}
	encoded := EncodeBatch(original)
	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatalf("round-trip decode failed: %v", err)
	}
	if !bytes.Equal(decoded[0], cmd) {
		t.Error("binary data round-trip failed")
	}
}

func TestRoundTrip_ManyCommands(t *testing.T) {
	t.Parallel()

	const count = 1000
	original := make([][]byte, count)
	for i := range original {
		original[i] = []byte{byte(i & 0xFF), byte((i >> 8) & 0xFF)}
	}
	encoded := EncodeBatch(original)
	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatalf("round-trip decode failed: %v", err)
	}
	if len(decoded) != count {
		t.Fatalf("decoded %d commands, want %d", len(decoded), count)
	}
	for i := range original {
		if !bytes.Equal(decoded[i], original[i]) {
			t.Errorf("decoded[%d] mismatch", i)
		}
	}
}
