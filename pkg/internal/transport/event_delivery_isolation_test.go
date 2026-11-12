package transport

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// snapshotSentSample is one IncSnapshotsSent observation.
type snapshotSentSample struct {
	shardID uint64
	bytes   int64
	elapsed time.Duration
}

// snapshotSentRecorder records IncSnapshotsSent and ignores every other metric.
type snapshotSentRecorder struct {
	noopTransportMetrics
	mu    sync.Mutex
	sends []snapshotSentSample
	// exit, when set, runs after the sample is recorded, so a test can
	// make the collector panic or call runtime.Goexit.
	exit func()
}

func (r *snapshotSentRecorder) IncSnapshotsSent(shardID uint64, bytes int64, d time.Duration) {
	r.mu.Lock()
	r.sends = append(r.sends, snapshotSentSample{shardID: shardID, bytes: bytes, elapsed: d})
	exit := r.exit
	r.mu.Unlock()
	if exit != nil {
		exit()
	}
}

func (r *snapshotSentRecorder) samples() []snapshotSentSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]snapshotSentSample(nil), r.sends...)
}

// requireReturnsNormally runs fn on its own goroutine and fails the test
// unless fn returns normally (no panic, no Goexit) before ctx is done.
func requireReturnsNormally(ctx context.Context, t *testing.T, what string, fn func()) {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		returned := false
		defer func() {
			if r := recover(); r != nil {
				result <- fmt.Errorf("%s panicked: %v", what, r)
				return
			}
			if !returned {
				result <- fmt.Errorf("%s did not return (Goexit escaped)", what)
			}
		}()
		fn()
		returned = true
		result <- nil
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatalf("%s never finished: %v", what, ctx.Err())
	}
}

func TestEventDeliveryIsolation_DeliverEventSkipsNilAndRunsInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var order []string
	a := func() { order = append(order, "a") }
	b := func() { order = append(order, "b") }
	requireReturnsNormally(ctx, t, "deliverEvent(nil, a, nil, b)", func() { deliverEvent(nil, a, nil, b) })
	if want := []string{"a", "b"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("deliverEvent(nil, a, nil, b) ran %v, want %v", order, want)
	}
}

func TestEventDeliveryIsolation_DeliverEventAllNilRunsNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	requireReturnsNormally(ctx, t, "deliverEvent(nil, nil)", func() { deliverEvent(nil, nil) })
	requireReturnsNormally(ctx, t, "deliverEvent()", func() { deliverEvent() })
}

func TestEventDeliveryIsolation_DeliverEventContainsExits(t *testing.T) {
	for exitName, exit := range hostileLogExits {
		t.Run(exitName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ran := false
			b := func() { ran = true }
			requireReturnsNormally(ctx, t, "deliverEvent(exit, b)", func() { deliverEvent(exit, b) })
			if !ran {
				t.Fatalf("deliverEvent: the call after one that exits with %s did not run", exitName)
			}
		})
	}
}

func TestEventDeliveryIsolation_SnapshotEventNilWithoutCallback(t *testing.T) {
	info := config.SnapshotInfo{ShardID: 7, ReplicaID: 3, Index: 11}
	noListener := &QUICTransport{}
	if call := noListener.snapshotEvent(onSnapshotSent, info); call != nil {
		t.Fatal("snapshotEvent with a nil eventListener returned a call, want nil")
	}
	emptyListener := &QUICTransport{eventListener: &config.EventListener{}}
	if call := emptyListener.snapshotEvent(onSnapshotSent, info); call != nil {
		t.Fatal("snapshotEvent with an empty EventListener returned a call, want nil")
	}
}

func TestEventDeliveryIsolation_SnapshotEventPassesInfoThrough(t *testing.T) {
	info := config.SnapshotInfo{ShardID: 7, ReplicaID: 3, Index: 11}
	var got []config.SnapshotInfo
	tr := &QUICTransport{eventListener: &config.EventListener{
		OnSnapshotSent: func(i config.SnapshotInfo) { got = append(got, i) },
	}}
	call := tr.snapshotEvent(onSnapshotSent, info)
	if call == nil {
		t.Fatal("snapshotEvent with OnSnapshotSent set returned nil")
	}
	call()
	if want := []config.SnapshotInfo{info}; !reflect.DeepEqual(got, want) {
		t.Fatalf("OnSnapshotSent received %+v, want %+v", got, want)
	}
}

func TestEventDeliveryIsolation_ReportSnapshotSentMetricSurvivesExits(t *testing.T) {
	for exitName, exit := range hostileLogExits {
		t.Run(exitName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			rec := &snapshotSentRecorder{}
			tr := &QUICTransport{
				eventListener: &config.EventListener{
					OnSnapshotSent: func(config.SnapshotInfo) { exit() },
				},
				metrics: rec,
			}
			requireReturnsNormally(ctx, t, "reportSnapshotSent", func() {
				tr.reportSnapshotSent(config.SnapshotInfo{ShardID: 7}, 1, 5, time.Millisecond)
			})
			want := []snapshotSentSample{{shardID: 7, bytes: 5, elapsed: time.Millisecond}}
			if got := rec.samples(); !reflect.DeepEqual(got, want) {
				t.Fatalf("IncSnapshotsSent recorded %+v, want %+v once despite OnSnapshotSent %s", got, want, exitName)
			}
		})
	}
}

// TestEventDeliveryIsolation_ReportSnapshotSentContainsMetricExits makes the
// IncSnapshotsSent collector itself panic or call runtime.Goexit after it
// records its sample, with a listener that returns normally and with one
// that also exits: reportSnapshotSent must still return normally, the
// sample must be recorded exactly once, and OnSnapshotSent must still run.
func TestEventDeliveryIsolation_ReportSnapshotSentContainsMetricExits(t *testing.T) {
	for metricExitName, metricExit := range hostileLogExits {
		for _, listenerExits := range []bool{false, true} {
			name := metricExitName + "/listener-returns"
			if listenerExits {
				name = metricExitName + "/listener-also-exits"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				requireSnapshotSentContained(ctx, t, metricExit, listenerExits)
			})
		}
	}
}

func requireSnapshotSentContained(ctx context.Context, t *testing.T, metricExit func(), listenerExits bool) {
	t.Helper()
	rec := &snapshotSentRecorder{exit: metricExit}
	var listenerCalls atomic.Int32
	tr := &QUICTransport{
		eventListener: &config.EventListener{
			OnSnapshotSent: func(config.SnapshotInfo) {
				listenerCalls.Add(1)
				if listenerExits {
					metricExit()
				}
			},
		},
		metrics: rec,
	}
	requireReturnsNormally(ctx, t, "reportSnapshotSent", func() {
		tr.reportSnapshotSent(config.SnapshotInfo{ShardID: 9}, 2, 11, 3*time.Millisecond)
	})
	want := []snapshotSentSample{{shardID: 9, bytes: 11, elapsed: 3 * time.Millisecond}}
	if got := rec.samples(); !reflect.DeepEqual(got, want) {
		t.Fatalf("IncSnapshotsSent recorded %+v, want %+v exactly once", got, want)
	}
	if got := listenerCalls.Load(); got != 1 {
		t.Fatalf("OnSnapshotSent ran %d times, want 1", got)
	}
}
