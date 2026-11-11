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

package proto

import (
	"errors"
	"testing"
)

func TestPutGetBoolNonOneIsTrue(t *testing.T) {
	buf := make([]byte, 8)
	putUint64(buf, 42) // any non-zero value should be true
	if !getBool(buf) {
		t.Fatal("getBool: non-zero uint64 should be true")
	}
}

func TestPutGetBytes(t *testing.T) {
	data := []byte("hello, world")
	buf := make([]byte, 4+len(data))
	n := putBytes(buf, data)
	if n != 4+len(data) {
		t.Fatalf("putBytes: wrote %d bytes, want %d", n, 4+len(data))
	}
	got, consumed, err := getBytes(buf, MaxEntrySize, ErrEntrySizeLimitExceeded)
	if err != nil {
		t.Fatalf("getBytes: unexpected error: %v", err)
	}
	if consumed != n {
		t.Fatalf("getBytes: consumed %d bytes, want %d", consumed, n)
	}
	if string(got) != string(data) {
		t.Fatalf("getBytes: got %q, want %q", got, data)
	}
}

func TestGetBytesBufferTooSmall(t *testing.T) {
	// Buffer too short for length prefix
	_, _, err := getBytes([]byte{0, 0}, MaxEntrySize, ErrEntrySizeLimitExceeded)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getBytes: got %v, want ErrBufferTooSmall", err)
	}
}

func TestGetBytesDataTruncated(t *testing.T) {
	buf := make([]byte, 8)
	putUint32(buf, 100) // claims 100 bytes but only 4 bytes of data follow
	_, _, err := getBytes(buf, MaxEntrySize, ErrEntrySizeLimitExceeded)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getBytes: got %v, want ErrBufferTooSmall", err)
	}
}

func TestGetBytesExceedsMaxLen(t *testing.T) {
	buf := make([]byte, 8)
	putUint32(buf, 100)
	_, _, err := getBytes(buf, 10, ErrEntrySizeLimitExceeded)
	if !errors.Is(err, ErrEntrySizeLimitExceeded) {
		t.Fatalf("getBytes: got %v, want ErrEntrySizeLimitExceeded", err)
	}
}

func TestPutGetMapEmpty(t *testing.T) {
	m := map[uint64]string{}
	sz := mapSize(m)
	buf := make([]byte, sz)
	n := putMap(buf, m)
	if n != 4 {
		t.Fatalf("putMap(empty): wrote %d, want 4", n)
	}
	got, consumed, err := getMap(buf)
	if err != nil {
		t.Fatalf("getMap: unexpected error: %v", err)
	}
	if consumed != 4 {
		t.Fatalf("getMap: consumed %d, want 4", consumed)
	}
	if len(got) != 0 {
		t.Fatalf("getMap: got %d entries, want 0", len(got))
	}
}

func TestGetMapBufferTooSmall(t *testing.T) {
	_, _, err := getMap([]byte{0, 0})
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getMap: got %v, want ErrBufferTooSmall", err)
	}
}

func TestGetMapValueTruncated(t *testing.T) {
	// count=1, key present, value length present but value data truncated
	buf := make([]byte, 4+8+4) // count + key + valueLen, no value data
	putUint32(buf, 1)
	putUint64(buf[4:], 1)
	putUint32(buf[12:], 10) // claims 10 bytes but none available
	_, _, err := getMap(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getMap: got %v, want ErrBufferTooSmall", err)
	}
}

func TestPutGetBoolMapRoundTrip(t *testing.T) {
	m := map[uint64]bool{
		5: true,
		2: false,
		9: true,
	}
	sz := boolMapSize(m)
	buf := make([]byte, sz)
	n := putBoolMap(buf, m)
	if n != sz {
		t.Fatalf("putBoolMap: wrote %d, want %d", n, sz)
	}
	got, consumed, err := getBoolMap(buf)
	if err != nil {
		t.Fatalf("getBoolMap: unexpected error: %v", err)
	}
	if consumed != n {
		t.Fatalf("getBoolMap: consumed %d, want %d", consumed, n)
	}
	if len(got) != len(m) {
		t.Fatalf("getBoolMap: got %d entries, want %d", len(got), len(m))
	}
	for k, v := range m {
		if got[k] != v {
			t.Fatalf("getBoolMap[%d]: got %v, want %v", k, got[k], v)
		}
	}
}

func TestPutGetBoolMapEmpty(t *testing.T) {
	m := map[uint64]bool{}
	buf := make([]byte, boolMapSize(m))
	putBoolMap(buf, m)
	got, _, err := getBoolMap(buf)
	if err != nil {
		t.Fatalf("getBoolMap: unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("getBoolMap: got %d entries, want 0", len(got))
	}
}

func TestGetBoolMapBufferTooSmall(t *testing.T) {
	_, _, err := getBoolMap([]byte{0, 0})
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getBoolMap: got %v, want ErrBufferTooSmall", err)
	}
}

func TestGetBoolMapTooManyEntries(t *testing.T) {
	buf := make([]byte, 4)
	putUint32(buf, MaxMembershipEntries+1)
	_, _, err := getBoolMap(buf)
	if !errors.Is(err, ErrTooManyMembershipEntries) {
		t.Fatalf("getBoolMap: got %v, want ErrTooManyMembershipEntries", err)
	}
}

func TestBoolMapSortedKeys(t *testing.T) {
	m := map[uint64]bool{
		100: true,
		1:   false,
		50:  true,
	}
	keys := sortedBoolKeys(m)
	if len(keys) != 3 || keys[0] != 1 || keys[1] != 50 || keys[2] != 100 {
		t.Fatalf("sortedBoolKeys: got %v, want [1, 50, 100]", keys)
	}
}

func TestMapSize(t *testing.T) {
	tests := []struct {
		name string
		m    map[uint64]string
		want int
	}{
		{"empty", map[uint64]string{}, 4},
		{"two entries", map[uint64]string{1: "a", 2: "bb"}, 4 + (8 + 4 + 1) + (8 + 4 + 2)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapSize(tt.m)
			if got != tt.want {
				t.Fatalf("mapSize: got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBoolMapSize(t *testing.T) {
	tests := []struct {
		name string
		m    map[uint64]bool
		want int
	}{
		{"empty", map[uint64]bool{}, 4},
		{"three entries", map[uint64]bool{1: true, 2: false, 3: true}, 4 + 3*16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := boolMapSize(tt.m)
			if got != tt.want {
				t.Fatalf("boolMapSize: got %d, want %d", got, tt.want)
			}
		})
	}
}
