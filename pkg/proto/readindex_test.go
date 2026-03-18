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

func TestReadyToReadMarshalUnmarshalRoundTrip(t *testing.T) {
	ri := ReadyToRead{
		Key:   42,
		Index: 100,
	}
	buf := make([]byte, ri.Size())
	n, err := ri.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != readIndexSize {
		t.Fatalf("MarshalTo: wrote %d, want %d", n, readIndexSize)
	}

	var ri2 ReadyToRead
	consumed, err := ri2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != readIndexSize {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, readIndexSize)
	}
	if ri2.Key != ri.Key {
		t.Fatalf("Key: got %d, want %d", ri2.Key, ri.Key)
	}
	if ri2.Index != ri.Index {
		t.Fatalf("Index: got %d, want %d", ri2.Index, ri.Index)
	}
}

func TestReadyToReadMarshalUnmarshalZero(t *testing.T) {
	ri := ReadyToRead{}
	buf := make([]byte, ri.Size())
	ri.MarshalTo(buf)
	var ri2 ReadyToRead
	ri2.UnmarshalFrom(buf)
	if ri2.Key != 0 || ri2.Index != 0 {
		t.Fatal("Zero ReadyToRead round-trip failed")
	}
}

func TestReadyToReadSize(t *testing.T) {
	ri := ReadyToRead{}
	if ri.Size() != 16 {
		t.Fatalf("Size: got %d, want 16", ri.Size())
	}
}

func TestReadyToReadBufferTooSmallMarshal(t *testing.T) {
	ri := ReadyToRead{Key: 1, Index: 1}
	buf := make([]byte, readIndexSize-1)
	_, err := ri.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo: got %v, want ErrBufferTooSmall", err)
	}
}

func TestReadyToReadBufferTooSmallUnmarshal(t *testing.T) {
	buf := make([]byte, readIndexSize-1)
	var ri ReadyToRead
	_, err := ri.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestReadyToReadReset(t *testing.T) {
	ri := ReadyToRead{Key: 42, Index: 100}
	ri.Reset()
	if ri.Key != 0 || ri.Index != 0 {
		t.Fatal("Reset did not clear all fields")
	}
}

func TestReadyToReadMaxValues(t *testing.T) {
	ri := ReadyToRead{
		Key:   ^uint64(0),
		Index: ^uint64(0),
	}
	buf := make([]byte, ri.Size())
	ri.MarshalTo(buf)
	var ri2 ReadyToRead
	ri2.UnmarshalFrom(buf)
	if ri2.Key != ri.Key || ri2.Index != ri.Index {
		t.Fatal("Max value round-trip failed")
	}
}
