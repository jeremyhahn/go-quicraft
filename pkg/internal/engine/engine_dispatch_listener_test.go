package engine

import (
	"bytes"
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// dispatchListenerWait bounds every wait in these tests.
const dispatchListenerWait = 10 * time.Second

// newDispatchListenerEngine builds an engine the way engine_test.go does and
// returns it with a context bounded by dispatchListenerWait.
func newDispatchListenerEngine(ctx context.Context, t *testing.T) (*Engine, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, dispatchListenerWait)
	t.Cleanup(cancel)
	return NewEngine(newTestHostConfig(), newTestLogDB(), newTestSender(), nil, nil, nil), ctx
}

// dispatchListenerGoroutineID returns the id of the calling goroutine, read
// from the first line of its stack trace ("goroutine N [running]:").
func dispatchListenerGoroutineID(ctx context.Context, t *testing.T) uint64 {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Errorf("context done before reading the goroutine id: %v", err)
		return 0
	}
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	buf = bytes.TrimPrefix(buf, []byte("goroutine "))
	if i := bytes.IndexByte(buf, ' '); i > 0 {
		buf = buf[:i]
	}
	id, err := strconv.ParseUint(string(buf), 10, 64)
	if err != nil {
		t.Errorf("cannot parse goroutine id from %q: %v", buf, err)
	}
	return id
}

// waitDispatchListener waits for done to close or ctx to end.
func waitDispatchListener(ctx context.Context, t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("%s: %v", what, ctx.Err())
	}
}

// assertDispatchListenerInline asserts that DispatchListener runs fn before it
// returns, on the calling goroutine, and reports true.
func assertDispatchListenerInline(ctx context.Context, t *testing.T, e *Engine, phase string) {
	t.Helper()
	caller := dispatchListenerGoroutineID(ctx, t)
	ran := false
	var ranOn uint64
	ok := e.DispatchListener(func() {
		ran = true
		ranOn = dispatchListenerGoroutineID(ctx, t)
	})
	if !ok {
		t.Fatalf("%s: DispatchListener returned false, want true", phase)
	}
	if !ran {
		t.Fatalf("%s: fn had not run when DispatchListener returned", phase)
	}
	if ranOn != caller {
		t.Fatalf("%s: fn ran on goroutine %d, want the caller's %d", phase, ranOn, caller)
	}
}

func TestEngine_DispatchListener_InlineBeforeStartAndAfterStop(t *testing.T) {
	e, ctx := newDispatchListenerEngine(context.Background(), t)
	assertDispatchListenerInline(ctx, t, e, "before Start")
	e.Start()
	e.Stop()
	assertDispatchListenerInline(ctx, t, e, "after Stop")
	if got := e.ListenerNotificationsDropped(); got != 0 {
		t.Fatalf("ListenerNotificationsDropped = %d, want 0", got)
	}
}

func TestEngine_DispatchListener_RunsInOrderOnDispatcherWhileRunning(t *testing.T) {
	e, ctx := newDispatchListenerEngine(context.Background(), t)
	e.Start()
	defer e.Stop()

	const n = 100
	caller := dispatchListenerGoroutineID(ctx, t)
	order := make(chan int, n)
	goroutines := make(chan uint64, n)
	for i := 0; i < n; i++ {
		if !e.DispatchListener(func() {
			goroutines <- dispatchListenerGoroutineID(ctx, t)
			order <- i
		}) {
			t.Fatalf("DispatchListener(%d) returned false on a non-full queue", i)
		}
	}
	for want := 0; want < n; want++ {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("function %d ran in position %d", got, want)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for function %d: %v", want, ctx.Err())
		}
		if id := <-goroutines; id == caller {
			t.Fatalf("function %d ran on the caller's goroutine %d", want, id)
		}
	}
	if got := e.ListenerNotificationsDropped(); got != 0 {
		t.Fatalf("ListenerNotificationsDropped = %d, want 0", got)
	}
}

func TestEngine_DispatchListener_FullQueueRefusesAndCountsDropped(t *testing.T) {
	e, ctx := newDispatchListenerEngine(context.Background(), t)
	e.Start()
	defer e.Stop()

	release, _ := blockDispatcher(t, e.listeners)
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	ran := make(chan struct{}, listenerQueueSize)
	for i := 0; i < listenerQueueSize; i++ {
		if !e.DispatchListener(func() { ran <- struct{}{} }) {
			t.Fatalf("DispatchListener(%d) refused below listenerQueueSize", i)
		}
	}
	base := e.ListenerNotificationsDropped()

	const refusals = 5
	done := make(chan struct{})
	errs := make(chan string, refusals)
	go func() {
		defer close(done)
		for i := 1; i <= refusals; i++ {
			if e.DispatchListener(func() { errs <- "a refused function ran" }) {
				errs <- "DispatchListener accepted on a full queue"
				return
			}
			if got := e.ListenerNotificationsDropped(); got != base+uint64(i) {
				errs <- "ListenerNotificationsDropped = " + strconv.FormatUint(got, 10) +
					" after refusal " + strconv.Itoa(i) + ", want " + strconv.FormatUint(base+uint64(i), 10)
				return
			}
		}
	}()
	waitDispatchListener(ctx, t, done, "refusals on a full queue blocked")
	select {
	case msg := <-errs:
		t.Fatal(msg)
	default:
	}

	close(release)
	released = true
	for i := 0; i < listenerQueueSize; i++ {
		select {
		case <-ran:
		case <-ctx.Done():
			t.Fatalf("queued function %d never ran: %v", i, ctx.Err())
		}
	}
	select {
	case msg := <-errs:
		t.Fatal(msg)
	default:
	}
	if got := e.ListenerNotificationsDropped(); got != base+refusals {
		t.Fatalf("ListenerNotificationsDropped = %d, want %d", got, base+refusals)
	}
}

func TestEngine_DispatchListener_PanicIsContained(t *testing.T) {
	e, ctx := newDispatchListenerEngine(context.Background(), t)
	e.Start()
	defer e.Stop()

	if !e.DispatchListener(func() { panic("listener panic under test") }) {
		t.Fatal("DispatchListener refused the panicking function")
	}
	after := make(chan struct{})
	if !e.DispatchListener(func() { close(after) }) {
		t.Fatal("DispatchListener refused the function after the panic")
	}
	waitDispatchListener(ctx, t, after, "function dispatched after a panic never ran")
}
