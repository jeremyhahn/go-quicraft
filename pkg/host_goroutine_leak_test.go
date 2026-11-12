package quicraft

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// goroutineLeakModulePath is the frame prefix that marks a goroutine as
// started by this module.
const goroutineLeakModulePath = "github.com/jeremyhahn/go-quicraft"

// goroutineLeakWait bounds how long the test waits for the Host's
// goroutines to exit after Close.
const goroutineLeakWait = 5 * time.Second

// goroutineStacks returns the stack of every live goroutine, keyed by the
// "goroutine N" header so goroutines present before the Host can be
// excluded by identity.
func goroutineStacks() map[string]string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	stacks := make(map[string]string)
	for _, g := range bytes.Split(buf, []byte("\n\n")) {
		s := string(g)
		header, _, _ := strings.Cut(s, "\n")
		// "goroutine 12 [running]:" -> "goroutine 12"
		if i := strings.Index(header, " ["); i > 0 {
			header = header[:i]
		}
		stacks[header] = s
	}
	return stacks
}

// currentGoroutineID returns the "goroutine N" header of the caller.
func currentGoroutineID() string {
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	header, _, _ := strings.Cut(string(buf), "\n")
	if i := strings.Index(header, " ["); i > 0 {
		header = header[:i]
	}
	return header
}

// moduleGoroutinesSince returns the stacks of goroutines that were not in
// before, are not the caller, and name a frame in this module.
func moduleGoroutinesSince(before map[string]string, self string) []string {
	var leaked []string
	for id, s := range goroutineStacks() {
		if id == self {
			continue
		}
		if _, ok := before[id]; ok {
			continue
		}
		if strings.Contains(s, goroutineLeakModulePath) {
			leaked = append(leaked, s)
		}
	}
	return leaked
}

// leakedModuleGoroutines polls moduleGoroutinesSince until no goroutine
// that is new since before, other than self, names a frame in this module,
// or until wait has passed. It returns the stacks still left (nil if none).
func leakedModuleGoroutines(before map[string]string, self string, wait time.Duration) []string {
	deadline := time.Now().Add(wait)
	for {
		leaked := moduleGoroutinesSince(before, self)
		if len(leaked) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return leaked
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// runHostLifecycleLeakCheck runs NewHost, StartShard (one replica, the
// in-memory test state machine), a few proposals, optionally StopShard,
// and Close, then requires every goroutine the Host started to exit.
func runHostLifecycleLeakCheck(t *testing.T, stopShard bool) {
	t.Helper()
	self := currentGoroutineID()
	before := goroutineStacks()

	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			h.Close()
		}
	}()

	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := h.SyncPropose(ctx, 1, []byte(fmt.Sprintf("key-%d=value", i)))
		cancel()
		if err != nil {
			t.Fatalf("SyncPropose %d failed: %v", i, err)
		}
	}

	if stopShard {
		if err := h.StopShard(1); err != nil {
			t.Fatalf("StopShard failed: %v", err)
		}
	}
	closed = true
	if err := h.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	leaked := leakedModuleGoroutines(before, self, goroutineLeakWait)
	if len(leaked) == 0 {
		return
	}
	t.Fatalf("%d goroutine(s) started by the Host still running %v after Close:\n\n%s",
		len(leaked), goroutineLeakWait, strings.Join(leaked, "\n\n"))
}

// TestHostGoroutineLeak_StopShardThenClose proves that the full lifecycle
// with an explicit StopShard leaves no goroutine behind.
func TestHostGoroutineLeak_StopShardThenClose(t *testing.T) {
	runHostLifecycleLeakCheck(t, true)
}

// TestHostGoroutineLeak_CloseWithoutStopShard proves that Close alone,
// with the shard still running, leaves no goroutine behind.
func TestHostGoroutineLeak_CloseWithoutStopShard(t *testing.T) {
	runHostLifecycleLeakCheck(t, false)
}

// plantedLeakBlock is the planted leak: a function declared in this module
// that blocks until release is closed, so its stack names a module frame.
func plantedLeakBlock(started chan<- struct{}, release <-chan struct{}) {
	close(started)
	<-release
}

// TestHostGoroutineLeak_DetectsPlantedLeak shows the leak check can fail:
// it reports exactly a planted goroutine while it runs, and nothing once
// the goroutine is released.
func TestHostGoroutineLeak_DetectsPlantedLeak(t *testing.T) {
	self := currentGoroutineID()
	before := goroutineStacks()

	started := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	go plantedLeakBlock(started, release)
	<-started

	leaked := leakedModuleGoroutines(before, self, 100*time.Millisecond)
	if len(leaked) != 1 {
		t.Fatalf("leak check reported %d goroutine(s), want exactly the planted one:\n\n%s",
			len(leaked), strings.Join(leaked, "\n\n"))
	}
	if !strings.Contains(leaked[0], "plantedLeakBlock") {
		t.Fatalf("leak check reported a goroutine that is not the planted one:\n\n%s", leaked[0])
	}

	released = true
	close(release)
	if leaked := leakedModuleGoroutines(before, self, goroutineLeakWait); len(leaked) != 0 {
		t.Fatalf("leak check reported %d goroutine(s) after the planted one was released:\n\n%s",
			len(leaked), strings.Join(leaked, "\n\n"))
	}
}
