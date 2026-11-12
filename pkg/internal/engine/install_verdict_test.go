package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// installVerdictWait bounds how long a test waits for a verdict it expects.
const installVerdictWait = 5 * time.Second

// recvInstallVerdict returns the verdict waiting on ch, failing the test if
// none arrives before ctx ends.
func recvInstallVerdict(ctx context.Context, t *testing.T, ch <-chan InstallVerdict, what string) InstallVerdict {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-ctx.Done():
		t.Fatalf("%s: no verdict delivered: %v", what, ctx.Err())
		return InstallVerdict{}
	}
}

// requireNoInstallVerdict fails the test if ch holds a verdict now. The
// registry sends under installMu on a buffered channel, so any verdict it
// will ever deliver for a completed operation is already in the buffer.
func requireNoInstallVerdict(ctx context.Context, t *testing.T, ch <-chan InstallVerdict, what string) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	select {
	case v := <-ch:
		t.Fatalf("%s: unexpected verdict %+v", what, v)
	default:
	}
}

// requireInstallOutcome receives one verdict from ch and checks its outcome.
func requireInstallOutcome(ctx context.Context, t *testing.T, ch <-chan InstallVerdict, want InstallOutcome, what string) InstallVerdict {
	t.Helper()
	v := recvInstallVerdict(ctx, t, ch, what)
	if v.Outcome != want {
		t.Fatalf("%s: outcome = %d, want %d", what, v.Outcome, want)
	}
	return v
}

func TestInstallVerdict_StoppedOrFailedNodeAnswersAbandonedAtOnce(t *testing.T) {
	tests := []struct {
		name string
		end  func(*Node)
	}{
		{name: "stopped", end: func(n *Node) { n.Stop() }},
		{name: "failed", end: func(n *Node) { n.markFailed() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), installVerdictWait)
			defer cancel()
			node := newFenceTestNode(1)
			tt.end(node)
			ch := node.AwaitInstallVerdict(500)
			// Already in the buffer: no wait is allowed.
			select {
			case v := <-ch:
				if v.Outcome != InstallAbandoned {
					t.Fatalf("outcome = %d, want InstallAbandoned", v.Outcome)
				}
			default:
				t.Fatal("AwaitInstallVerdict on an ended node returned an empty channel")
			}
			requireNoInstallVerdict(ctx, t, ch, "second verdict")
			// Nothing was registered: a later signal reaches no one.
			node.installMu.Lock()
			registered := len(node.installVerdicts)
			node.installMu.Unlock()
			if registered != 0 {
				t.Fatalf("ended node registered %d waits", registered)
			}
		})
	}
}

func TestInstallVerdict_ReRegistrationSupersedesEarlierWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), installVerdictWait)
	defer cancel()
	node := newFenceTestNode(1)
	first := node.AwaitInstallVerdict(500)
	requireNoInstallVerdict(ctx, t, first, "first before re-registration")
	second := node.AwaitInstallVerdict(500)
	requireInstallOutcome(ctx, t, first, InstallSuperseded, "first after re-registration")
	requireNoInstallVerdict(ctx, t, second, "second before signal")

	errRefused := errors.New("refused")
	node.signalInstallVerdict(500, InstallVerdict{Outcome: InstallRefused, Err: errRefused})
	v := requireInstallOutcome(ctx, t, second, InstallRefused, "second after signal")
	if !errors.Is(v.Err, errRefused) {
		t.Fatalf("second verdict err = %v, want %v", v.Err, errRefused)
	}
	requireNoInstallVerdict(ctx, t, first, "first after signal")
	// The signal consumed the registration: a repeat reaches no one.
	node.signalInstallVerdict(500, InstallVerdict{Outcome: InstallAdopted})
	requireNoInstallVerdict(ctx, t, second, "second after repeat signal")
}

func TestInstallVerdict_CancelWithdrawsRegistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), installVerdictWait)
	defer cancel()
	node := newFenceTestNode(1)
	kept := node.AwaitInstallVerdict(400)
	cancelled := node.AwaitInstallVerdict(500)
	node.CancelInstallVerdict(500)
	node.signalInstallVerdict(500, InstallVerdict{Outcome: InstallAdopted})
	requireNoInstallVerdict(ctx, t, cancelled, "cancelled after signal")

	// A cancel of an index never registered changes nothing.
	node.CancelInstallVerdict(999)
	node.CancelInstallVerdict(500)
	requireNoInstallVerdict(ctx, t, kept, "kept after stray cancels")
	node.signalInstallVerdict(400, InstallVerdict{Outcome: InstallDeclined})
	requireInstallOutcome(ctx, t, kept, InstallDeclined, "kept after signal")

	// Neither does a cancel on a node with no registry yet, nor a stop after
	// a cancel: the cancelled wait stays unanswered.
	fresh := newFenceTestNode(2)
	fresh.CancelInstallVerdict(1)
	ch := node.AwaitInstallVerdict(600)
	node.CancelInstallVerdict(600)
	node.Stop()
	requireNoInstallVerdict(ctx, t, ch, "cancelled after stop")
	requireNoInstallVerdict(ctx, t, cancelled, "cancelled after stop")
}

func TestInstallVerdict_StopOrFailAnswersEveryWaitOnce(t *testing.T) {
	tests := []struct {
		name string
		end  func(*Node)
	}{
		{name: "stop", end: func(n *Node) { n.Stop() }},
		{name: "fail", end: func(n *Node) { n.markFailed() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), installVerdictWait)
			defer cancel()
			node := newFenceTestNode(1)
			indexes := []uint64{100, 200, 300, 400, 500}
			chans := make([]<-chan InstallVerdict, len(indexes))
			for i, index := range indexes {
				chans[i] = node.AwaitInstallVerdict(index)
			}
			tt.end(node)
			// The first termination path alone answers every wait, before
			// any other path runs that could supply a missing verdict.
			for _, ch := range chans {
				requireInstallOutcome(ctx, t, ch, InstallAbandoned, "wait")
			}
			// A second stop or fail must not answer anything again.
			node.Stop()
			node.markFailed()
			for i, ch := range chans {
				requireNoInstallVerdict(ctx, t, ch, "wait after abandon")
				// A late signal for the index reaches no one.
				node.signalInstallVerdict(indexes[i], InstallVerdict{Outcome: InstallAdopted})
				requireNoInstallVerdict(ctx, t, ch, "wait after late signal")
			}
		})
	}
}

// installVerdictRaceWait is one registration in the concurrent test.
type installVerdictRaceWait struct {
	ch        <-chan InstallVerdict
	cancelled bool
}

func TestInstallVerdict_ConcurrentRegisterCancelStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), installVerdictWait)
	defer cancel()
	const workers, perWorker = 8, 64
	node := newFenceTestNode(1)
	waits := make([][]installVerdictRaceWait, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			own := make([]installVerdictRaceWait, 0, perWorker)
			for i := 0; i < perWorker; i++ {
				// Each worker owns its indexes, so a cancel withdraws only
				// the worker's own registration.
				index := uint64(w*perWorker + i + 1)
				ch := node.AwaitInstallVerdict(index)
				cancelled := i%3 == 0
				if cancelled {
					node.CancelInstallVerdict(index)
				}
				own = append(own, installVerdictRaceWait{ch: ch, cancelled: cancelled})
			}
			waits[w] = own
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		node.Stop()
	}()
	close(start)
	wg.Wait()

	node.installMu.Lock()
	left := len(node.installVerdicts)
	node.installMu.Unlock()
	if left != 0 {
		t.Fatalf("%d waits left registered after stop", left)
	}
	for w := range waits {
		for i, wait := range waits[w] {
			if wait.cancelled {
				// Answered at most once: before the cancel by the stop, or
				// at once if registered after it; never later.
				select {
				case v := <-wait.ch:
					if v.Outcome != InstallAbandoned {
						t.Fatalf("worker %d wait %d: outcome = %d, want InstallAbandoned", w, i, v.Outcome)
					}
				default:
				}
			} else {
				requireInstallOutcome(ctx, t, wait.ch, InstallAbandoned, "uncancelled wait")
			}
			requireNoInstallVerdict(ctx, t, wait.ch, "wait answered twice")
		}
	}
}
