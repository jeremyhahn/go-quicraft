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
	"errors"
	"testing"
)

// wireEncoder names one command encoder and its documented opcode.
type wireEncoder struct {
	name   string
	op     byte
	encode func(key, value []byte) ([]byte, error)
}

// wireEncoders returns the four command encoders. Get and Delete ignore
// the value argument.
func wireEncoders() []wireEncoder {
	return []wireEncoder{
		{"put", OpPut, EncodePut},
		{"put_if_absent", OpPutIfAbsent, EncodePutIfAbsent},
		{"get", OpGet, func(key, _ []byte) ([]byte, error) { return EncodeGet(key) }},
		{"delete", OpDelete, func(key, _ []byte) ([]byte, error) { return EncodeDelete(key) }},
	}
}

// mustWireEncode runs enc and fails the test on an encode error.
func mustWireEncode(ctx context.Context, t *testing.T, enc wireEncoder, key, value []byte) []byte {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done: %v", err)
	}
	buf, err := enc.encode(key, value)
	if err != nil {
		t.Fatalf("%s: encode = %v", enc.name, err)
	}
	return buf
}

// TestWireLayout_ExactBytes checks the documented [op][keyLen LE][key][value]
// bytes of every encoder.
func TestWireLayout_ExactBytes(t *testing.T) {
	ctx := context.Background()
	want := map[string][]byte{
		"put":           {0x01, 0x02, 0x00, 'a', 'b', 'x', 'y', 'z'},
		"put_if_absent": {0x04, 0x02, 0x00, 'a', 'b', 'x', 'y', 'z'},
		"get":           {0x02, 0x02, 0x00, 'a', 'b'},
		"delete":        {0x03, 0x02, 0x00, 'a', 'b'},
	}
	for _, enc := range wireEncoders() {
		t.Run(enc.name, func(t *testing.T) {
			got := mustWireEncode(ctx, t, enc, []byte("ab"), []byte("xyz"))
			if !bytes.Equal(got, want[enc.name]) {
				t.Errorf("%s = %#v, want %#v", enc.name, got, want[enc.name])
			}
		})
	}

	put := mustWireEncode(ctx, t, wireEncoders()[0], []byte("ab"), []byte("xyz"))
	pia := mustWireEncode(ctx, t, wireEncoders()[1], []byte("ab"), []byte("xyz"))
	if pia[0] != OpPutIfAbsent || !bytes.Equal(put[1:], pia[1:]) {
		t.Errorf("EncodePutIfAbsent = %#v, want EncodePut %#v with byte 0 = 0x04", pia, put)
	}
}

// TestWireLayout_KeyLengthLittleEndian checks a 258-byte key length is
// written low byte first.
func TestWireLayout_KeyLengthLittleEndian(t *testing.T) {
	ctx := context.Background()
	key := bytes.Repeat([]byte{'k'}, 258)
	for _, enc := range wireEncoders() {
		t.Run(enc.name, func(t *testing.T) {
			buf := mustWireEncode(ctx, t, enc, key, []byte("v"))
			if buf[0] != enc.op {
				t.Errorf("op = %#x, want %#x", buf[0], enc.op)
			}
			if buf[1] != 0x02 || buf[2] != 0x01 {
				t.Errorf("keyLen bytes = %#x %#x, want 0x02 0x01", buf[1], buf[2])
			}
			if !bytes.Equal(buf[3:3+258], key) {
				t.Error("key bytes differ from the input key")
			}
		})
	}
}

// TestWireLayout_MaxKeyLengthAccepted checks a key of exactly MaxKeyLength
// encodes with length 0xFFFF and decodes back whole.
func TestWireLayout_MaxKeyLengthAccepted(t *testing.T) {
	ctx := context.Background()
	key := bytes.Repeat([]byte{'m'}, MaxKeyLength)
	for _, enc := range wireEncoders() {
		t.Run(enc.name, func(t *testing.T) {
			buf := mustWireEncode(ctx, t, enc, key, []byte("v"))
			if buf[1] != 0xFF || buf[2] != 0xFF {
				t.Errorf("keyLen bytes = %#x %#x, want 0xFF 0xFF", buf[1], buf[2])
			}
			cmd, err := DecodeCommand(buf)
			if err != nil {
				t.Fatalf("DecodeCommand = %v", err)
			}
			if cmd.Op != enc.op {
				t.Errorf("Op = %d, want %d", cmd.Op, enc.op)
			}
			if len(cmd.Key) != 65535 {
				t.Errorf("len(Key) = %d, want 65535", len(cmd.Key))
			}
		})
	}
}

// TestWireLayout_PutIfAbsentKeyTooLong checks EncodePutIfAbsent refuses a
// MaxKeyLength+1 key with a nil slice and the key's length.
func TestWireLayout_PutIfAbsentKeyTooLong(t *testing.T) {
	buf, err := EncodePutIfAbsent(make([]byte, MaxKeyLength+1), []byte("v"))
	if buf != nil {
		t.Errorf("buf = %d bytes, want nil", len(buf))
	}
	var ktl *KeyTooLongError
	if !errors.As(err, &ktl) {
		t.Fatalf("EncodePutIfAbsent = %v, want *KeyTooLongError", err)
	}
	if ktl.Length != 65536 {
		t.Errorf("Length = %d, want 65536", ktl.Length)
	}
}

// TestWireLayout_DecodeRefusalReasons checks DecodeCommand names each
// refusal reason.
func TestWireLayout_DecodeRefusalReasons(t *testing.T) {
	cases := []struct {
		name   string
		data   []byte
		reason string
	}{
		{"nil", nil, "data too short"},
		{"op_after_put_if_absent", []byte{OpPutIfAbsent + 1, 0x00, 0x00}, "unknown op"},
		{"key_len_exceeds_data", []byte{OpGet, 0x03, 0x00, 'a', 'b'}, "key length exceeds data"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeCommand(tc.data)
			var ice *InvalidCommandError
			if !errors.As(err, &ice) {
				t.Fatalf("DecodeCommand = %v, want *InvalidCommandError", err)
			}
			if ice.Reason != tc.reason {
				t.Errorf("Reason = %q, want %q", ice.Reason, tc.reason)
			}
		})
	}
}

// TestWireLayout_DecodeAliasesInput checks the decoded Key and Value are
// sub-slices of the input, and a get decodes a nil Value.
func TestWireLayout_DecodeAliasesInput(t *testing.T) {
	ctx := context.Background()
	d := mustWireEncode(ctx, t, wireEncoders()[0], []byte("k"), []byte("v"))
	cmd, err := DecodeCommand(d)
	if err != nil {
		t.Fatalf("DecodeCommand = %v", err)
	}
	d[3] = 'K'
	if cmd.Key[0] != 'K' {
		t.Errorf("Key[0] = %q after writing d[3], want 'K'", cmd.Key[0])
	}
	d[4] = 'V'
	if cmd.Value[0] != 'V' {
		t.Errorf("Value[0] = %q after writing d[4], want 'V'", cmd.Value[0])
	}

	g := mustWireEncode(ctx, t, wireEncoders()[2], []byte("k"), nil)
	gcmd, err := DecodeCommand(g)
	if err != nil {
		t.Fatalf("DecodeCommand(get) = %v", err)
	}
	if gcmd.Value != nil {
		t.Errorf("get Value = %#v, want nil", gcmd.Value)
	}
}
