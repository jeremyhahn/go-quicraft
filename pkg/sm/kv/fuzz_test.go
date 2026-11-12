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
	"errors"
	"testing"
)

// FuzzDecodeCommand feeds arbitrary bytes to DecodeCommand. The decoder
// must never panic, must report every failure as an *InvalidCommandError,
// and every command it accepts must re-encode through the matching
// Encode* function. Put and PutIfAbsent carry a value and must re-encode
// byte-for-byte. Get and Delete carry none: their encoders write only the
// header and key, so the re-encoding must equal that prefix of the input.
func FuzzDecodeCommand(f *testing.F) {
	type pair struct{ key, value []byte }
	seeds := []pair{
		{[]byte("key"), []byte("value")},
		{[]byte("k"), nil},
		{nil, []byte("v")},
		{nil, nil},
		{[]byte("bin\x00\xff"), []byte("\x00\x01\x02")},
	}
	for _, s := range seeds {
		for _, enc := range []func() ([]byte, error){
			func() ([]byte, error) { return EncodePut(s.key, s.value) },
			func() ([]byte, error) { return EncodePutIfAbsent(s.key, s.value) },
			func() ([]byte, error) { return EncodeGet(s.key) },
			func() ([]byte, error) { return EncodeDelete(s.key) },
		} {
			b, err := enc()
			if err != nil {
				f.Fatalf("encode seed: %v", err)
			}
			f.Add(b)
		}
	}
	f.Add([]byte{})                          // empty
	f.Add([]byte{OpPut, 0x01})               // shorter than the header
	f.Add([]byte{0xFF, 0x00, 0x00})          // unknown op
	f.Add([]byte{OpPut - 1, 0x00, 0x00})     // op below the range
	f.Add([]byte{OpPutIfAbsent + 1, 0, 0})   // op above the range
	f.Add([]byte{OpGet, 0x05, 0x00, 'a'})    // key length exceeds data
	f.Add([]byte{OpGet, 0xFF, 0xFF, 'a'})    // maximum key length, short data
	f.Add([]byte{OpDelete, 0x01, 0x00, 'k'}) // exact delete

	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := DecodeCommand(data)
		if err != nil {
			var ice *InvalidCommandError
			if !errors.As(err, &ice) {
				t.Fatalf("DecodeCommand error %T (%v) is not an *InvalidCommandError", err, err)
			}
			return
		}
		if len(cmd.Key) > MaxKeyLength {
			t.Fatalf("decoded key of %d bytes exceeds MaxKeyLength", len(cmd.Key))
		}
		var (
			enc  []byte
			want = data
		)
		switch cmd.Op {
		case OpPut:
			enc, err = EncodePut(cmd.Key, cmd.Value)
		case OpPutIfAbsent:
			enc, err = EncodePutIfAbsent(cmd.Key, cmd.Value)
		case OpGet:
			enc, err = EncodeGet(cmd.Key)
			want = data[:headerSize+len(cmd.Key)]
		case OpDelete:
			enc, err = EncodeDelete(cmd.Key)
			want = data[:headerSize+len(cmd.Key)]
		default:
			t.Fatalf("DecodeCommand accepted unknown op %d", cmd.Op)
		}
		if err != nil {
			t.Fatalf("re-encode op %d: %v", cmd.Op, err)
		}
		if !bytes.Equal(enc, want) {
			t.Fatalf("round trip mismatch for op %d: input %x, re-encoded %x", cmd.Op, data, enc)
		}
	})
}
