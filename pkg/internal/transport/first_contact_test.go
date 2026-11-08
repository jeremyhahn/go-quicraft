// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestFirstContactBoundOutlastsEveryStageOfAColdSend pins the derivation
// beside sendFirstContact to the bounds the transport applies to itself: the
// wait must be at least as long as the dial, the stream open and the frame
// write together, because a cold Send that has not arrived by then has been
// dropped, not delayed, and only a re-send can deliver it.
func TestFirstContactBoundOutlastsEveryStageOfAColdSend(t *testing.T) {
	if firstContactBound < dialTimeout+streamOpenTimeout+sendWriteDeadline {
		t.Fatalf("firstContactBound=%v is below dial %v + stream open %v + write %v: "+
			"a cold Send the transport has not abandoned yet would fail the test",
			firstContactBound, dialTimeout, streamOpenTimeout, sendWriteDeadline)
	}
	if firstContactAttempts < 2 {
		t.Fatalf("firstContactAttempts=%d: a single attempt cannot survive an abandoned dial",
			firstContactAttempts)
	}
}

// TestSendUntilDeliveredResendsThenGivesUp drives the helper's core at a
// small scale against a target the transport drops on the spot (unresolved
// in the registry), and checks both halves of its behaviour: it sends once
// per attempt, and it fails only after every attempt's wait has elapsed.
func TestSendUntilDeliveredResendsThenGivesUp(t *testing.T) {
	rec := &dropRecorder{}
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	t1, _, cleanup := transportPairWith(t, 42, handler1, handler2, func(c *Config) {
		c.Metrics = rec
	})
	defer cleanup()

	const attempts = 3
	const wait = 20 * time.Millisecond
	unrouted := []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 99, Term: 1},
	}

	start := time.Now()
	err := sendUntilDelivered(t1, handler2, unrouted, attempts, wait)
	elapsed := time.Since(start)

	var fcErr *firstContactError
	if !errors.As(err, &fcErr) {
		t.Fatalf("err = %v, want *firstContactError", err)
	}
	if fcErr.attempts != attempts || fcErr.wait != wait {
		t.Fatalf("error reports %d attempts of %v, want %d of %v",
			fcErr.attempts, fcErr.wait, attempts, wait)
	}
	if elapsed < attempts*wait {
		t.Fatalf("gave up after %v, before the %d attempts' %v had elapsed", elapsed, attempts, attempts*wait)
	}

	sends := 0
	for _, d := range rec.drops() {
		if d.reason == dropReasonUnresolvedTarget && d.to == 99 {
			sends++
		}
	}
	if sends != attempts {
		t.Fatalf("transport saw %d sends to the unrouted target, want one per attempt (%d)", sends, attempts)
	}
}
