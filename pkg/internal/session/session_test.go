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

package session

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

func TestNewClientSession(t *testing.T) {
	t.Run("creates session with correct ID and index", func(t *testing.T) {
		sess := newClientSession(42, 100)
		if sess.clientID != 42 {
			t.Errorf("clientID = %d, want 42", sess.clientID)
		}
		if sess.respondedTo != 0 {
			t.Errorf("respondedTo = %d, want 0", sess.respondedTo)
		}
		if len(sess.responses) != 0 {
			t.Errorf("len(responses) = %d, want 0", len(sess.responses))
		}
		if sess.lastActiveIndex != 100 {
			t.Errorf("lastActiveIndex = %d, want 100", sess.lastActiveIndex)
		}
	})

	t.Run("different IDs create distinct sessions", func(t *testing.T) {
		s1 := newClientSession(1, 0)
		s2 := newClientSession(2, 0)
		if s1.clientID == s2.clientID {
			t.Error("sessions should have different clientIDs")
		}
	})
}

func TestClientSessionIsExpired(t *testing.T) {
	t.Run("not expired when recently active", func(t *testing.T) {
		sess := newClientSession(1, 1000)
		if sess.isExpired(1050, 100) {
			t.Error("session active at index 1000 should not be expired at index 1050 with threshold 100")
		}
	})

	t.Run("expired when inactive beyond threshold", func(t *testing.T) {
		sess := newClientSession(1, 100)
		if !sess.isExpired(300, 100) {
			t.Error("session active at index 100 should be expired at index 300 with threshold 100")
		}
	})

	t.Run("not expired at boundary", func(t *testing.T) {
		sess := newClientSession(1, 100)
		if sess.isExpired(200, 100) {
			t.Error("session should not be expired at exact threshold boundary")
		}
	})

	t.Run("not expired when threshold is zero", func(t *testing.T) {
		sess := newClientSession(1, 0)
		if sess.isExpired(1000000, 0) {
			t.Error("session should never expire when threshold is 0")
		}
	})

	t.Run("not expired when current index is behind last active", func(t *testing.T) {
		sess := newClientSession(1, 500)
		if sess.isExpired(100, 10) {
			t.Error("session should not be expired when current index < lastActiveIndex")
		}
	})
}

func TestClientSessionTouchIndex(t *testing.T) {
	t.Run("updates lastActiveIndex", func(t *testing.T) {
		sess := newClientSession(1, 100)
		sess.touchIndex(200)
		if sess.lastActiveIndex != 200 {
			t.Errorf("lastActiveIndex = %d, want 200", sess.lastActiveIndex)
		}
	})

	t.Run("prevents expiration", func(t *testing.T) {
		sess := newClientSession(1, 100)
		sess.touchIndex(950)
		if sess.isExpired(1000, 100) {
			t.Error("session should not be expired after touchIndex")
		}
	})

	t.Run("does not decrease lastActiveIndex", func(t *testing.T) {
		sess := newClientSession(1, 500)
		sess.touchIndex(300)
		if sess.lastActiveIndex != 500 {
			t.Errorf("lastActiveIndex = %d, want 500 (should not decrease)", sess.lastActiveIndex)
		}
	})
}

func TestClientSessionEvictUpTo(t *testing.T) {
	t.Run("evicts entries up to respondedTo", func(t *testing.T) {
		sess := newClientSession(1, 0)
		sess.responses[1] = sm.Result{Value: 1}
		sess.responses[2] = sm.Result{Value: 2}
		sess.responses[3] = sm.Result{Value: 3}
		sess.responses[4] = sm.Result{Value: 4}

		sess.evictUpTo(2)

		if _, exists := sess.responses[1]; exists {
			t.Error("series 1 should be evicted")
		}
		if _, exists := sess.responses[2]; exists {
			t.Error("series 2 should be evicted")
		}
		if _, exists := sess.responses[3]; !exists {
			t.Error("series 3 should still exist")
		}
		if _, exists := sess.responses[4]; !exists {
			t.Error("series 4 should still exist")
		}
	})

	t.Run("no-op when nothing to evict", func(t *testing.T) {
		sess := newClientSession(1, 0)
		sess.responses[5] = sm.Result{Value: 5}
		sess.evictUpTo(3) // all series are > 3
		if len(sess.responses) != 1 {
			t.Errorf("len(responses) = %d, want 1", len(sess.responses))
		}
	})

	t.Run("evicts all when respondedTo is high", func(t *testing.T) {
		sess := newClientSession(1, 0)
		sess.responses[1] = sm.Result{Value: 1}
		sess.responses[2] = sm.Result{Value: 2}
		sess.evictUpTo(100)
		if len(sess.responses) != 0 {
			t.Errorf("len(responses) = %d, want 0", len(sess.responses))
		}
	})
}
