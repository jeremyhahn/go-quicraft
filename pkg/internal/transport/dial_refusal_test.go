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

package transport

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
)

// These pin the lifetime of remembered dial refusals. A refusal was removed
// only when the same (target, identity) was looked up again after its
// cooldown: nothing swept the rest, and CloseTarget left them behind, so a
// long-lived transport held one for every refused pair it had ever seen.

// TestCloseTarget_ClearsDialRefusals: CloseTarget forgets the address's
// remembered refusals, so the next send to it dials instead of returning a
// refusal recorded against whoever held the address before.
func TestCloseTarget_ClearsDialRefusals(t *testing.T) {
	ids := &replicaIdentities{}
	var failedDials atomic.Int64
	t1, t2, cleanup := transportPairWith(t, 1, newTestHandler(), newTestHandler(), func(c *Config) {
		c.ReplicaIdentity = ids.get
		c.EventListener = &config.EventListener{
			OnConnectionFailed: func(config.ConnectionInfo) { failedDials.Add(1) },
		}
	})
	defer cleanup()
	target := t2.Addr().String()

	var refused *DialIdentityError
	if _, err := t1.getConnection(target, strangerIdentity); !errors.As(err, &refused) {
		t.Fatalf("getConnection = %v, want a DialIdentityError", err)
	}
	if n := dialRefusalCount(t1); n != 1 {
		t.Fatalf("%d remembered refusals, want 1", n)
	}

	t1.CloseTarget(target)
	if n := dialRefusalCount(t1); n != 0 {
		t.Fatalf("%d remembered refusals after CloseTarget, want 0", n)
	}
	if _, err := t1.getConnection(target, strangerIdentity); !errors.As(err, &refused) {
		t.Fatalf("getConnection after CloseTarget = %v, want a DialIdentityError", err)
	}
	if n := failedDials.Load(); n != 2 {
		t.Fatalf("%d refused dials, want 2: CloseTarget must clear the cooldown", n)
	}
}

// newIdleTransport builds a transport that is never started, for tests that
// drive its maintenance directly.
func newIdleTransport(t *testing.T) *QUICTransport {
	t.Helper()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	return tr
}

// refuse records a dial refusal for (target, want) made at at.
func refuse(tr *QUICTransport, target, want string, at time.Time) {
	tr.dialRefusals.Store(dialRefusalKey{target: target, want: want},
		dialRefusal{at: at, err: &DialError{Target: target, Err: &DialIdentityError{}}})
}

// TestSweepDialRefusals_RemovesOnlyExpired: the sweep forgets the refusals
// whose cooldown has passed and keeps the ones still cooling down.
func TestSweepDialRefusals_RemovesOnlyExpired(t *testing.T) {
	tr := newIdleTransport(t)
	now := time.Now()
	refuse(tr, "10.0.0.1:1", "spki:a", now.Add(-dialRefusalCooldown))
	refuse(tr, "10.0.0.2:1", "spki:b", now.Add(-2*dialRefusalCooldown))
	refuse(tr, "10.0.0.3:1", "spki:c", now)

	tr.sweepDialRefusals(now)

	if n := dialRefusalCount(tr); n != 1 {
		t.Fatalf("%d remembered refusals after the sweep, want 1", n)
	}
	if _, kept := tr.dialRefusals.Load(dialRefusalKey{target: "10.0.0.3:1", want: "spki:c"}); !kept {
		t.Fatal("the sweep forgot a refusal still inside its cooldown")
	}
}

// TestConnectionCleanup_SweepsExpiredDialRefusals: the connection-cleanup
// tick runs the sweep.
func TestConnectionCleanup_SweepsExpiredDialRefusals(t *testing.T) {
	tr := newIdleTransport(t)
	tr.cleanupInterval = time.Millisecond
	tr.stopC = make(chan struct{})
	refuse(tr, "10.0.0.1:1", "spki:a", time.Now().Add(-dialRefusalCooldown))

	done := make(chan bool, 1)
	go func() { done <- tr.connectionCleanupOnce() }()
	pollUntil(t, 5*time.Second, time.Millisecond,
		"the cleanup tick never swept the expired refusal",
		func() bool { return dialRefusalCount(tr) == 0 })
	close(tr.stopC)
	if panicked := <-done; panicked {
		t.Fatal("connectionCleanupOnce panicked")
	}
}

// TestCloseTarget_ClearsOnlyThatAddressesRefusals: every identity's refusal at
// the closed address goes; another address's stays.
func TestCloseTarget_ClearsOnlyThatAddressesRefusals(t *testing.T) {
	tr := newIdleTransport(t)
	now := time.Now()
	refuse(tr, "10.0.0.1:1", "spki:a", now)
	refuse(tr, "10.0.0.1:1", "spki:b", now)
	refuse(tr, "10.0.0.1:1", "", now)
	refuse(tr, "10.0.0.2:1", "spki:a", now)

	tr.CloseTarget("10.0.0.1:1")

	if n := dialRefusalCount(tr); n != 1 {
		t.Fatalf("%d remembered refusals after CloseTarget, want 1", n)
	}
	if _, kept := tr.dialRefusals.Load(dialRefusalKey{target: "10.0.0.2:1", want: "spki:a"}); !kept {
		t.Fatal("CloseTarget forgot another address's refusal")
	}
}

// TestDialRefusals_BoundedUnderUniqueFailures: a stream of refusals, each for
// a pair never refused before, leaves at most one cleanup interval's worth
// remembered — the map tracks the recent refusal rate, not the transport's
// history.
func TestDialRefusals_BoundedUnderUniqueFailures(t *testing.T) {
	tr := newIdleTransport(t)
	const perInterval, intervals = 64, 50
	start := time.Now()
	for i := range intervals {
		tick := start.Add(time.Duration(i) * connectionCleanupInterval)
		for j := range perInterval {
			refuse(tr, fmt.Sprintf("10.%d.%d.1:1", i, j), "spki:a", tick)
		}
		if n := dialRefusalCount(tr); n > perInterval {
			t.Fatalf("interval %d: %d remembered refusals, want at most %d", i, n, perInterval)
		}
		tr.sweepDialRefusals(tick.Add(connectionCleanupInterval))
	}
	if n := dialRefusalCount(tr); n != 0 {
		t.Fatalf("%d remembered refusals after the last sweep, want 0", n)
	}
}

// dialRefusalCount reports how many dial refusals tr remembers.
func dialRefusalCount(tr *QUICTransport) int {
	n := 0
	tr.dialRefusals.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}
