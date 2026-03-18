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

package engine

import (
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// testSender is a test implementation of MessageSender that records
// all sent messages for verification.
type testSender struct {
	mu        sync.Mutex
	msgs      []proto.Message
	snapshots []proto.Message
}

func newTestSender() *testSender {
	return &testSender{}
}

func (s *testSender) Send(msgs []proto.Message) {
	s.mu.Lock()
	s.msgs = append(s.msgs, msgs...)
	s.mu.Unlock()
}

func (s *testSender) SendSnapshot(msg proto.Message) {
	s.mu.Lock()
	s.snapshots = append(s.snapshots, msg)
	s.mu.Unlock()
}

func (s *testSender) snapshotCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.snapshots)
}

func (s *testSender) sentSnapshots() []proto.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]proto.Message, len(s.snapshots))
	copy(result, s.snapshots)
	return result
}

func (s *testSender) sentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs)
}

func (s *testSender) sentMessages() []proto.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]proto.Message, len(s.msgs))
	copy(result, s.msgs)
	return result
}

// Verify testSender implements MessageSender at compile time.
var _ MessageSender = (*testSender)(nil)

func TestMessageSender_Interface(t *testing.T) {
	s := newTestSender()
	var sender MessageSender = s

	msgs := []proto.Message{
		{Type: proto.Heartbeat, From: 1, To: 2, ShardID: 10},
		{Type: proto.Replicate, From: 1, To: 3, ShardID: 10},
	}
	sender.Send(msgs)

	if s.sentCount() != 2 {
		t.Fatalf("expected 2 sent messages, got %d", s.sentCount())
	}
}

func TestMessageSender_EmptySend(t *testing.T) {
	s := newTestSender()
	s.Send(nil)
	if s.sentCount() != 0 {
		t.Fatalf("expected 0 sent messages, got %d", s.sentCount())
	}
	s.Send([]proto.Message{})
	if s.sentCount() != 0 {
		t.Fatalf("expected 0 sent messages, got %d", s.sentCount())
	}
}

func TestMessageSender_ConcurrentSend(t *testing.T) {
	s := newTestSender()
	const goroutines = 10
	const msgsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for range msgsPerGoroutine {
				s.Send([]proto.Message{
					{ShardID: uint64(id)},
				})
			}
		}(g)
	}
	wg.Wait()

	expected := goroutines * msgsPerGoroutine
	if s.sentCount() != expected {
		t.Fatalf("expected %d sent messages, got %d", expected, s.sentCount())
	}
}
