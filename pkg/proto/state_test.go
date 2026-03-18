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

func TestStateMarshalUnmarshalRoundTrip(t *testing.T) {
	s := State{
		Term:   10,
		Vote:   3,
		Commit: 50,
	}
	buf := make([]byte, s.Size())
	n, err := s.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != stateSize {
		t.Fatalf("MarshalTo: wrote %d, want %d", n, stateSize)
	}

	var s2 State
	consumed, err := s2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != stateSize {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, stateSize)
	}
	if s2.Term != s.Term || s2.Vote != s.Vote || s2.Commit != s.Commit {
		t.Fatalf("Round-trip mismatch: got {%d, %d, %d}, want {%d, %d, %d}",
			s2.Term, s2.Vote, s2.Commit, s.Term, s.Vote, s.Commit)
	}
}

func TestStateMarshalUnmarshalZero(t *testing.T) {
	s := State{}
	buf := make([]byte, s.Size())
	s.MarshalTo(buf)
	var s2 State
	s2.UnmarshalFrom(buf)
	if s2.Term != 0 || s2.Vote != 0 || s2.Commit != 0 {
		t.Fatal("Zero state round-trip failed")
	}
}

func TestStateMarshalUnmarshalMaxValues(t *testing.T) {
	s := State{
		Term:   ^uint64(0),
		Vote:   ^uint64(0),
		Commit: ^uint64(0),
	}
	buf := make([]byte, s.Size())
	s.MarshalTo(buf)
	var s2 State
	s2.UnmarshalFrom(buf)
	if s2.Term != s.Term || s2.Vote != s.Vote || s2.Commit != s.Commit {
		t.Fatal("Max value state round-trip failed")
	}
}

func TestStateSize(t *testing.T) {
	s := State{}
	if s.Size() != 24 {
		t.Fatalf("Size: got %d, want 24", s.Size())
	}
}

func TestStateIsEmpty(t *testing.T) {
	tests := []struct {
		name  string
		state State
		want  bool
	}{
		{"zero state", State{}, true},
		{"term set", State{Term: 1}, false},
		{"vote set", State{Vote: 1}, false},
		{"commit set", State{Commit: 1}, false},
		{"all set", State{Term: 1, Vote: 2, Commit: 3}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.state.IsEmpty(); got != tt.want {
				t.Fatalf("IsEmpty: got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStateBufferTooSmallMarshal(t *testing.T) {
	s := State{Term: 1}
	buf := make([]byte, stateSize-1)
	_, err := s.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo: got %v, want ErrBufferTooSmall", err)
	}
}

func TestStateBufferTooSmallUnmarshal(t *testing.T) {
	buf := make([]byte, stateSize-1)
	var s State
	_, err := s.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestStateReset(t *testing.T) {
	s := State{Term: 10, Vote: 3, Commit: 50}
	s.Reset()
	if s.Term != 0 || s.Vote != 0 || s.Commit != 0 {
		t.Fatal("Reset did not clear all fields")
	}
	if !s.IsEmpty() {
		t.Fatal("Reset state should be empty")
	}
}

func BenchmarkStateMarshalTo(b *testing.B) {
	s := State{Term: 10, Vote: 3, Commit: 50}
	buf := make([]byte, s.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		s.MarshalTo(buf)
	}
}

func BenchmarkStateUnmarshalFrom(b *testing.B) {
	s := State{Term: 10, Vote: 3, Commit: 50}
	buf := make([]byte, s.Size())
	s.MarshalTo(buf)
	var s2 State
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		s2.UnmarshalFrom(buf)
	}
}
