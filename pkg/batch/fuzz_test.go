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

//go:build quicraft_fuzz

package batch

import (
	"bytes"
	"errors"
	"testing"
)

// FuzzDecodeBatch feeds arbitrary bytes to DecodeBatch. The decoder must
// never panic, must report every failure as a *DecodeError matching
// ErrDecode, and every payload it accepts must re-encode byte-for-byte:
// the batch format has exactly one encoding per command list.
func FuzzDecodeBatch(f *testing.F) {
	// Seeds from the package's decode tests.
	seeds := [][][]byte{
		{},
		{[]byte("hello")},
		{{}, {}, {}},
		{
			[]byte("cmd1"),
			[]byte("cmd2-longer-data"),
			[]byte("c"),
			[]byte(""),
			[]byte("cmd5-with-binary-\x00\x01\x02"),
		},
	}
	for _, cmds := range seeds {
		enc, err := EncodeBatch(cmds)
		if err != nil {
			f.Fatalf("EncodeBatch seed: %v", err)
		}
		f.Add(enc)
	}
	f.Add([]byte{0x00, 0x01})                               // truncated prefix
	f.Add([]byte{0x00, 0x00, 0x00, 0x0A, 0x01, 0x02, 0x03}) // truncated data
	f.Add([]byte{0x00, 0x00, 0x00, 0x01, 0xFF, 0x00, 0x01}) // truncated second prefix
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})                   // garbage length
	f.Add([]byte{0x80, 0x00, 0x00, 0x00})                   // length above MaxInt32

	f.Fuzz(func(t *testing.T, data []byte) {
		cmds, err := DecodeBatch(data)
		if err != nil {
			var de *DecodeError
			if !errors.As(err, &de) || !errors.Is(err, ErrDecode) {
				t.Fatalf("DecodeBatch error %T (%v) is not a *DecodeError matching ErrDecode", err, err)
			}
			if cmds != nil {
				t.Fatalf("DecodeBatch returned %d commands with error %v", len(cmds), err)
			}
			return
		}
		enc, err := EncodeBatch(cmds)
		if err != nil {
			t.Fatalf("EncodeBatch of decoded commands: %v", err)
		}
		if !bytes.Equal(enc, data) {
			t.Fatalf("round trip mismatch: input %x, re-encoded %x", data, enc)
		}
	})
}
