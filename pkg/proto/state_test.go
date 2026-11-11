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

func TestStateSize(t *testing.T) {
	s := State{}
	if s.Size() != 32 {
		t.Fatalf("Size: got %d, want 32", s.Size())
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
		{"vote window set", State{VoteWindow: 1}, false},
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
	s := State{Term: 10, Vote: 3, Commit: 50, VoteWindow: 7}
	s.Reset()
	if s != (State{}) {
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
