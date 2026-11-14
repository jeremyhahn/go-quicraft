package batch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricBatchesTotal  = "quicraft_batch_batches_total"
	metricBatchSize     = "quicraft_batch_batch_size"
	metricFlushDuration = "quicraft_batch_flush_duration_seconds"
)

// flushCountersCounter returns the value of the counter series of the named
// family whose labels include every pair in labels, and whether that series
// exists at all.
func flushCountersCounter(ctx context.Context, t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before Gather: %v", err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			matched := 0
			for _, lp := range m.GetLabel() {
				if want, ok := labels[lp.GetName()]; ok && want == lp.GetValue() {
					matched++
				}
			}
			if matched == len(labels) {
				return m.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

// flushCountersHistogram returns the sample count and sum of the named
// unlabelled histogram, or zeros when it has no series.
func flushCountersHistogram(ctx context.Context, t *testing.T, reg *prometheus.Registry, name string) (uint64, float64) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before Gather: %v", err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			h := m.GetHistogram()
			return h.GetSampleCount(), h.GetSampleSum()
		}
	}
	return 0, 0
}

// flushCountersBatch reads batches_total{shard="7",status=status}.
func flushCountersBatch(ctx context.Context, t *testing.T, reg *prometheus.Registry, status string) (float64, bool) {
	t.Helper()
	return flushCountersCounter(ctx, t, reg, metricBatchesTotal, map[string]string{"shard": "7", "status": status})
}

// flushCountersWaitBatchLen polls until the aggregator's current batch holds
// n proposals or ctx expires.
func flushCountersWaitBatchLen(ctx context.Context, t *testing.T, agg *Aggregator, n int) {
	t.Helper()
	for agg.BatchLen() != n {
		select {
		case <-ctx.Done():
			t.Fatalf("BatchLen() = %d, want %d before the deadline", agg.BatchLen(), n)
		case <-time.After(time.Millisecond):
		}
	}
}

// flushCountersAggregator builds an aggregator over mp with a fresh registry.
func flushCountersAggregator(ctx context.Context, t *testing.T, mp *mockProposer, cfg Config) (*Aggregator, *prometheus.Registry) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before NewAggregator: %v", err)
	}
	reg := prometheus.NewRegistry()
	agg, err := NewAggregator(mp, WithConfig(cfg), WithRegistry(reg))
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	t.Cleanup(func() { _ = agg.Close() })
	return agg, reg
}

// flushCountersSubmitPair runs two concurrent Submits to shard 7 and returns
// their errors.
func flushCountersSubmitPair(ctx context.Context, t *testing.T, agg *Aggregator) [2]error {
	t.Helper()
	var errs [2]error
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = agg.Submit(ctx, 7, []byte{byte('a' + i)})
		}()
	}
	wg.Wait()
	return errs
}

func TestFlushCounters_SizeFlushSuccess(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mp := &mockProposer{}
	agg, reg := flushCountersAggregator(ctx, t, mp, Config{MaxBatchSize: 2, FlushInterval: time.Hour, QueueSize: 10})

	for i, err := range flushCountersSubmitPair(ctx, t, agg) {
		if err != nil {
			t.Fatalf("Submit %d = %v, want nil", i, err)
		}
	}

	if got, _ := flushCountersBatch(ctx, t, reg, "attempted"); got != 1 {
		t.Errorf("batches_total{status=attempted} = %v, want 1", got)
	}
	if got, _ := flushCountersBatch(ctx, t, reg, "success"); got != 1 {
		t.Errorf("batches_total{status=success} = %v, want 1", got)
	}
	if got, ok := flushCountersBatch(ctx, t, reg, "failed"); ok {
		t.Errorf("batches_total{status=failed} series exists with %v, want no series", got)
	}
	if got := flushReasonCount(t, reg, flushReasonSize); got != 1 {
		t.Errorf("flush_reason_total{reason=size} = %v, want 1", got)
	}
	if count, sum := flushCountersHistogram(ctx, t, reg, metricBatchSize); count != 1 || sum != 2 {
		t.Errorf("batch_size count=%d sum=%v, want count=1 sum=2", count, sum)
	}
}

func TestFlushCounters_SizeFlushProposerError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mp := &mockProposer{err: errors.New("boom")}
	agg, reg := flushCountersAggregator(ctx, t, mp, Config{MaxBatchSize: 2, FlushInterval: time.Hour, QueueSize: 10})

	for i, err := range flushCountersSubmitPair(ctx, t, agg) {
		var fe *FlushError
		if !errors.As(err, &fe) {
			t.Fatalf("Submit %d = %v, want *FlushError", i, err)
		}
		if fe.Indeterminate {
			t.Errorf("Submit %d: FlushError.Indeterminate = true, want false for a proposer rejection", i)
		}
	}

	if got, _ := flushCountersBatch(ctx, t, reg, "attempted"); got != 1 {
		t.Errorf("batches_total{status=attempted} = %v, want 1", got)
	}
	if got, _ := flushCountersBatch(ctx, t, reg, "failed"); got != 1 {
		t.Errorf("batches_total{status=failed} = %v, want 1", got)
	}
	if got, ok := flushCountersBatch(ctx, t, reg, "success"); ok {
		t.Errorf("batches_total{status=success} series exists with %v, want no series", got)
	}
	if count, _ := flushCountersHistogram(ctx, t, reg, metricFlushDuration); count != 1 {
		t.Errorf("flush_duration_seconds count = %d, want 1", count)
	}
}

func TestFlushCounters_CloseFlush(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mp := &mockProposer{}
	agg, reg := flushCountersAggregator(ctx, t, mp, Config{MaxBatchSize: 100, FlushInterval: time.Hour, QueueSize: 10})

	done := make(chan error, 1)
	go func() { done <- agg.Submit(ctx, 7, []byte("x")) }()
	flushCountersWaitBatchLen(ctx, t, agg, 1)
	if err := agg.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Submit = %v, want nil from the close flush", err)
	}

	if got := flushReasonCount(t, reg, flushReasonClose); got != 1 {
		t.Errorf("flush_reason_total{reason=close} = %v, want 1", got)
	}
	for _, other := range []string{flushReasonSize, flushReasonTime, flushReasonBytes} {
		if got := flushReasonCount(t, reg, other); got != 0 {
			t.Errorf("flush_reason_total{reason=%q} = %v, want 0", other, got)
		}
	}
}

func TestFlushCounters_BytesThenCloseFlush(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mp := &mockProposer{}
	agg, reg := flushCountersAggregator(ctx, t, mp, Config{MaxBatchSize: 100, MaxBatchBytes: 10, FlushInterval: time.Hour, QueueSize: 10})

	// "aaaaaa" plus its 4-byte length prefix fills MaxBatchBytes exactly; it
	// must not flush on its own.
	first := make(chan error, 1)
	go func() { first <- agg.Submit(ctx, 7, []byte("aaaaaa")) }()
	flushCountersWaitBatchLen(ctx, t, agg, 1)

	// "b" overflows the non-empty batch: the first proposal flushes for bytes.
	second := make(chan error, 1)
	go func() { second <- agg.Submit(ctx, 7, []byte("b")) }()
	if err := <-first; err != nil {
		t.Fatalf("first Submit = %v, want nil from the bytes flush", err)
	}
	flushCountersWaitBatchLen(ctx, t, agg, 1)

	if err := agg.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second Submit = %v, want nil from the close flush", err)
	}

	if got := flushReasonCount(t, reg, flushReasonBytes); got != 1 {
		t.Errorf("flush_reason_total{reason=bytes} = %v, want 1", got)
	}
	if got := flushReasonCount(t, reg, flushReasonClose); got != 1 {
		t.Errorf("flush_reason_total{reason=close} = %v, want 1", got)
	}
	if got, _ := flushCountersBatch(ctx, t, reg, "attempted"); got != 2 {
		t.Errorf("batches_total{status=attempted} = %v, want 2", got)
	}
	if got, _ := flushCountersBatch(ctx, t, reg, "success"); got != 2 {
		t.Errorf("batches_total{status=success} = %v, want 2", got)
	}
	if calls := len(mp.getCalls()); calls != 2 {
		t.Errorf("proposer calls = %d, want 2", calls)
	}
}

func TestFlushCounters_AbandonedBatchFails(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mp := &mockProposer{}
	agg, reg := flushCountersAggregator(ctx, t, mp, Config{MaxBatchSize: 100, FlushInterval: time.Hour, QueueSize: 10})

	subCtx, subCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- agg.Submit(subCtx, 7, []byte("x")) }()
	flushCountersWaitBatchLen(ctx, t, agg, 1)
	subCancel()

	var fe *FlushError
	if err := <-done; !errors.As(err, &fe) {
		t.Fatalf("Submit = %v, want *FlushError", err)
	}
	if err := agg.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}

	if got, _ := flushCountersBatch(ctx, t, reg, "attempted"); got != 1 {
		t.Errorf("batches_total{status=attempted} = %v, want 1", got)
	}
	if got, _ := flushCountersBatch(ctx, t, reg, "failed"); got != 1 {
		t.Errorf("batches_total{status=failed} = %v, want 1", got)
	}
	if count, _ := flushCountersHistogram(ctx, t, reg, metricBatchSize); count != 0 {
		t.Errorf("batch_size count = %d, want 0: an all-abandoned batch is not proposed", count)
	}
	if calls := len(mp.getCalls()); calls != 0 {
		t.Errorf("proposer calls = %d, want 0", calls)
	}
}
