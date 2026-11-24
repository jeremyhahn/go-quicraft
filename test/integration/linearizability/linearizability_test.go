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

//go:build integration

package linearizability_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// ---------------------------------------------------------------------------
// Client Timeouts
// ---------------------------------------------------------------------------

const (
	// proposeTimeout bounds a single client write.
	proposeTimeout = 5 * time.Second

	// readTimeout bounds a single linearizable read.
	readTimeout = 5 * time.Second

	// checkTimeout bounds the porcupine search; an undecided history fails.
	checkTimeout = 300 * time.Second
)

// ---------------------------------------------------------------------------
// Porcupine KV Model
// ---------------------------------------------------------------------------

// kvInput describes an operation submitted to the KV store.
type kvInput struct {
	Op    string // "put", "append" or "get"
	Key   string
	Value string // written value (put) or appended token (append)
}

// kvOutput is a KV operation result; Unknown marks an indeterminate outcome.
type kvOutput struct {
	Value   string // resulting value: the read value (get) or the accumulated value (append)
	Ok      bool   // the write definitely took effect
	Unknown bool   // the outcome is unknown
}

// appendValue returns the value that results from appending token to current.
func appendValue(current, token string) string {
	if current == "" {
		return token
	}
	return current + appendSep + token
}

// partitionByKey splits an event history into one partition per key. Call
// events carry the key in their input; return events are matched back to
// their call by Id, which is why the recorder emits calls before returns.
func partitionByKey(history []porcupine.Event) [][]porcupine.Event {
	keyOf := make(map[int]string, len(history))
	partitions := make(map[string][]porcupine.Event)
	for _, ev := range history {
		var key string
		if ev.Kind == porcupine.CallEvent {
			key = ev.Value.(kvInput).Key
			keyOf[ev.Id] = key
		} else {
			matched, ok := keyOf[ev.Id]
			if !ok {
				// A return with no preceding call is unmatchable. The
				// recorder never produces one.
				continue
			}
			key = matched
		}
		partitions[key] = append(partitions[key], ev)
	}

	keys := make([]string, 0, len(partitions))
	for k := range partitions {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := make([][]porcupine.Event, 0, len(keys))
	for _, k := range keys {
		result = append(result, partitions[k])
	}
	return result
}

// kvModel returns a nondeterministic porcupine.Model for a linearizable KV store, partitioned by key.
func kvModel() porcupine.Model {
	nm := kvSpec()
	nm.PartitionEvent = partitionByKey
	return nm.ToModel()
}

// kvSingleKeyModel returns the same model for a history already reduced to one key.
func kvSingleKeyModel() porcupine.Model {
	nm := kvSpec()
	nm.PartitionEvent = func(events []porcupine.Event) [][]porcupine.Event {
		return [][]porcupine.Event{events}
	}
	return nm.ToModel()
}

// kvSpec is the shared nondeterministic specification; the caller supplies the
// partitioning.
func kvSpec() porcupine.NondeterministicModel {
	return porcupine.NondeterministicModel{
		Init: func() []interface{} {
			return []interface{}{""}
		},
		Step: func(state interface{}, input interface{}, output interface{}) []interface{} {
			val := state.(string)
			inp := input.(kvInput)
			out := output.(kvOutput)

			switch inp.Op {
			case "put":
				switch {
				case out.Unknown:
					// The write may or may not have been applied.
					return []interface{}{val, inp.Value}
				case out.Ok:
					return []interface{}{inp.Value}
				default:
					// The write definitely did not take effect.
					return []interface{}{val}
				}
			case "append":
				applied := appendValue(val, inp.Value)
				switch {
				case out.Unknown:
					return []interface{}{val, applied}
				case out.Ok:
					// A successful append returns the exact accumulated value; a double apply cannot.
					if out.Value != applied {
						return nil
					}
					return []interface{}{applied}
				default:
					return []interface{}{val}
				}
			case "get":
				if out.Unknown {
					return []interface{}{val}
				}
				if out.Value != val {
					return nil
				}
				return []interface{}{val}
			default:
				return nil
			}
		},
		Equal: func(state1, state2 interface{}) bool {
			return state1.(string) == state2.(string)
		},
		DescribeOperation: func(input interface{}, output interface{}) string {
			inp := input.(kvInput)
			out := output.(kvOutput)
			switch {
			case out.Unknown:
				return fmt.Sprintf("%s(%s, %s) -> unknown", inp.Op, inp.Key, inp.Value)
			case inp.Op == "put":
				return fmt.Sprintf("Put(%s, %s) -> ok=%v", inp.Key, inp.Value, out.Ok)
			case inp.Op == "append":
				return fmt.Sprintf("Append(%s, %s) -> %q", inp.Key, inp.Value, out.Value)
			case inp.Op == "get":
				return fmt.Sprintf("Get(%s) -> %q", inp.Key, out.Value)
			default:
				return fmt.Sprintf("Unknown(%v) -> %v", inp, out)
			}
		},
		DescribeState: func(state interface{}) string {
			val := state.(string)
			if val == "" {
				return "<empty>"
			}
			return fmt.Sprintf("%q", val)
		},
	}
}

// ---------------------------------------------------------------------------
// Operation Recorder
// ---------------------------------------------------------------------------

// opRecord is one client operation as observed from outside the cluster.
type opRecord struct {
	id          int
	clientID    int
	input       kvInput
	output      kvOutput
	callSeq     int64
	returnSeq   int64
	determinate bool
	err         error
}

// opRecorder records every operation a client issues, including failed and indeterminate ones.
type opRecorder struct {
	mu         sync.Mutex
	seq        int64
	ops        []*opRecord
	violations []string
	errCensus  map[string]int
	// done counts determinately completed operations. It is atomic so a
	// running test can watch progress without taking the recorder lock.
	done atomic.Int64
}

// completed returns the number of operations that have completed
// determinately so far. Safe to call while clients are running.
func (r *opRecorder) completed() int64 {
	return r.done.Load()
}

// mark returns the recorder's logical clock now: every operation begun after
// the call has a callSeq above it.
func (r *opRecorder) mark() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seq
}

// ackedSince counts the determinately acknowledged writes (put, append) and
// reads (get) whose call began after mark.
func (r *opRecorder) ackedSince(mark int64) (writes, reads int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, op := range r.ops {
		if op.callSeq <= mark || !op.determinate {
			continue
		}
		if op.input.Op == "get" {
			reads++
		} else {
			writes++
		}
	}
	return writes, reads
}

// requireServedSince fails the test unless the cluster acknowledged at least
// one write and one read that began after mark, so a failover test cannot
// pass on traffic from before the recovery it is meant to exercise.
func requireServedSince(t *testing.T, r *opRecorder, mark int64, phase string) {
	t.Helper()
	writes, reads := r.ackedSince(mark)
	t.Logf("acknowledged after %s: writes=%d reads=%d", phase, writes, reads)
	if writes == 0 || reads == 0 {
		t.Fatalf("the cluster acknowledged writes=%d reads=%d after %s; both must "+
			"be above zero, or the workload ended before the recovered cluster "+
			"served it", writes, reads, phase)
	}
}

// begin registers a call and returns its record. Every begin MUST be followed
// by exactly one finish or indeterminate.
func (r *opRecorder) begin(clientID int, input kvInput) *opRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	rec := &opRecord{
		id:       len(r.ops),
		clientID: clientID,
		input:    input,
		callSeq:  r.seq,
	}
	r.ops = append(r.ops, rec)
	return rec
}

// finish records a determinate outcome and stamps the return.
func (r *opRecorder) finish(rec *opRecord, output kvOutput) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	rec.output = output
	rec.returnSeq = r.seq
	rec.determinate = true
	r.done.Add(1)
}

// indeterminate records an operation whose outcome is unknown. The record
// stays in the history; events() emits its return at the very end.
func (r *opRecorder) indeterminate(rec *opRecord, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec.err = err
	rec.determinate = false
	if err == nil {
		return
	}
	if r.errCensus == nil {
		r.errCensus = make(map[string]int)
	}
	r.errCensus[err.Error()]++
}

// violation records a protocol breach that the model cannot express, such as
// a proposal that reports success while the state machine returns a result
// for an entry it never applied. Any violation fails the test.
func (r *opRecorder) violation(format string, args ...interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.violations = append(r.violations, fmt.Sprintf(format, args...))
}

// stats returns the recorded totals: all operations, those that completed
// determinately, and the determinate writes and reads separately.
func (r *opRecorder) stats() (total, determinate, writes, reads int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	total = len(r.ops)
	for _, rec := range r.ops {
		if !rec.determinate {
			continue
		}
		determinate++
		if rec.input.Op == "get" {
			reads++
		} else {
			writes++
		}
	}
	return total, determinate, writes, reads
}

// events converts the history to porcupine events in real-time order, with indeterminate returns as Unknown at the end.
func (r *opRecorder) events() []porcupine.Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	type stamped struct {
		seq   int64
		event porcupine.Event
	}

	timed := make([]stamped, 0, 2*len(r.ops))
	pending := make([]porcupine.Event, 0, 16)

	for _, rec := range r.ops {
		timed = append(timed, stamped{
			seq: rec.callSeq,
			event: porcupine.Event{
				ClientId: rec.clientID,
				Kind:     porcupine.CallEvent,
				Value:    rec.input,
				Id:       rec.id,
			},
		})
		if rec.determinate {
			timed = append(timed, stamped{
				seq: rec.returnSeq,
				event: porcupine.Event{
					ClientId: rec.clientID,
					Kind:     porcupine.ReturnEvent,
					Value:    rec.output,
					Id:       rec.id,
				},
			})
			continue
		}
		pending = append(pending, porcupine.Event{
			ClientId: rec.clientID,
			Kind:     porcupine.ReturnEvent,
			Value:    kvOutput{Unknown: true},
			Id:       rec.id,
		})
	}

	sort.Slice(timed, func(i, j int) bool { return timed[i].seq < timed[j].seq })

	events := make([]porcupine.Event, 0, len(timed)+len(pending))
	for _, s := range timed {
		events = append(events, s.event)
	}
	return append(events, pending...)
}

// report returns the recorded violations and the census of client-visible
// errors, for diagnostics.
func (r *opRecorder) report() (violations []string, census map[string]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	violations = make([]string, len(r.violations))
	copy(violations, r.violations)
	census = make(map[string]int, len(r.errCensus))
	for k, v := range r.errCensus {
		census[k] = v
	}
	return violations, census
}

// ---------------------------------------------------------------------------
// Typed Errors
// ---------------------------------------------------------------------------

// LinearizabilityViolationError is returned when porcupine detects a
// linearizability violation in the recorded operation history.
type LinearizabilityViolationError struct {
	Result        porcupine.CheckResult
	NumOps        int
	Indeterminate int
	Duration      time.Duration
}

// Error describes the failed check, distinguishing Illegal from Unknown.
func (e *LinearizabilityViolationError) Error() string {
	if e.Result == porcupine.Unknown {
		return fmt.Sprintf(
			"linearizability check did not finish within %v: result=%s, ops=%d, "+
				"indeterminate=%d. The budget expired, which is NOT by itself "+
				"evidence that the history is legal: porcupine finds a "+
				"linearization quickly when one exists, so a search that runs "+
				"long is one that is backtracking, and an illegal history is "+
				"the most expensive case there is. Read the per-partition "+
				"diagnosis above -- it names the key and gives it a definite "+
				"verdict. Do not dismiss this as an intractable history until "+
				"that diagnosis says every partition is Ok",
			e.Duration, e.Result, e.NumOps, e.Indeterminate,
		)
	}
	return fmt.Sprintf(
		"linearizability violation: result=%s, ops=%d, indeterminate=%d, duration=%v",
		e.Result, e.NumOps, e.Indeterminate, e.Duration,
	)
}

// ---------------------------------------------------------------------------
// Throughput Floors
// ---------------------------------------------------------------------------

// baselineWritesPerClientSecond is the slowest measured per-client write rate on this harness.
const baselineWritesPerClientSecond = 5.4

// floorFraction is the share of that baseline a run must reach. At 0.30 a run
// has to complete roughly a third of the slowest measured rate: the tightest
// test in the table above (5Node_HighThroughput) still has a 3.3x margin,
// while every mutation that collapses throughput lands one to three orders of
// magnitude below the floor.
const floorFraction = 0.30

// opsFloor is the minimum work a run must actually perform for its
// linearizability verdict to mean anything. A history that is empty, or that
// contains only writes, or only reads, is trivially linearizable.
type opsFloor struct {
	writes int
	reads  int
}

// floorFor derives an operation floor from the shape of a run. active is the
// portion of the run during which the cluster is expected to be serving:
// chaos tests pass a reduced value because elections and catch-up legitimately
// stall the clients. Reads and writes alternate, so the same floor applies to
// both sides.
func floorFor(numClients int, active time.Duration) opsFloor {
	perSide := int(float64(numClients) * active.Seconds() * baselineWritesPerClientSecond * floorFraction)
	if perSide < 1 {
		perSide = 1
	}
	return opsFloor{writes: perSide, reads: perSide}
}

// ---------------------------------------------------------------------------
// Linearizability Check Helper
// ---------------------------------------------------------------------------

// logPartitionCensus reports the per-key shape porcupine actually searches:
// the widest partition, the deepest concentration of indeterminate operations,
// and how many partitions there are. These are the only three numbers that
// explain a checker timeout.
func logPartitionCensus(t *testing.T, events []porcupine.Event) {
	t.Helper()

	parts := partitionByKey(events)
	if len(parts) == 0 {
		return
	}

	type shape struct {
		key   string
		ops   int
		indet int
	}
	shapes := make([]shape, 0, len(parts))
	for _, p := range parts {
		var sh shape
		for _, ev := range p {
			switch {
			case ev.Kind == porcupine.CallEvent:
				sh.ops++
				sh.key = ev.Value.(kvInput).Key
			case ev.Value.(kvOutput).Unknown:
				sh.indet++
			}
		}
		shapes = append(shapes, sh)
	}
	sort.Slice(shapes, func(i, j int) bool {
		if shapes[i].indet != shapes[j].indet {
			return shapes[i].indet > shapes[j].indet
		}
		return shapes[i].ops > shapes[j].ops
	})

	var sb strings.Builder
	limit := min(len(shapes), 5)
	for _, sh := range shapes[:limit] {
		fmt.Fprintf(&sb, "\n  %s: %d ops, %d indeterminate", sh.key, sh.ops, sh.indet)
	}
	t.Logf("partition census: %d partitions; worst by indeterminate depth:%s", len(shapes), sb.String())
}

// perPartitionCheckTimeout bounds the per-key re-check that runs only after
// the whole-history check has already failed. It is generous because it is
// spent on ONE key, and buying a definite Ok/Illegal for that key is what
// turns an unactionable timeout into a report.
const perPartitionCheckTimeout = 60 * time.Second

// diagnosePartitions re-checks each key and reports those not Ok with the operations porcupine could not place.
func diagnosePartitions(events []porcupine.Event) (verdicts []partitionVerdict, decided, total int) {
	model := kvSingleKeyModel()
	parts := partitionByKey(events)
	for _, part := range parts {
		start := time.Now()
		res := porcupine.CheckEventsTimeout(model, part, perPartitionCheckTimeout)
		if res == porcupine.Ok {
			decided++
			continue
		}
		key, ops, indeterminate := partitionShape(part)
		verdicts = append(verdicts, partitionVerdict{
			Key:           key,
			Result:        res,
			Ops:           ops,
			Indeterminate: indeterminate,
			Elapsed:       time.Since(start),
			Events:        part,
		})
	}
	return verdicts, decided, len(parts)
}

// partitionVerdict is one key's independent result.
type partitionVerdict struct {
	Key           string
	Result        porcupine.CheckResult
	Ops           int
	Indeterminate int
	Elapsed       time.Duration
	Events        []porcupine.Event
}

// reportPartitionDiagnosis runs the per-key re-check and fails the test with
// the key-level detail the aggregate result cannot carry.
func reportPartitionDiagnosis(t *testing.T, events []porcupine.Event) {
	t.Helper()

	verdicts, decided, total := diagnosePartitions(events)
	t.Logf("per-partition diagnosis: %d of %d partitions decided Ok", decided, total)
	for _, v := range verdicts {
		t.Errorf("partition %s: result=%s after %v (%d ops, %d indeterminate)\n%s",
			v.Key, v.Result, v.Elapsed.Round(time.Millisecond), v.Ops, v.Indeterminate,
			formatPartition(v.Events))
	}
}

// partitionShape returns the key a partition belongs to and its operation and
// indeterminate counts.
func partitionShape(part []porcupine.Event) (key string, ops, indeterminate int) {
	for _, ev := range part {
		if ev.Kind == porcupine.CallEvent {
			ops++
			key = ev.Value.(kvInput).Key
			continue
		}
		if ev.Value.(kvOutput).Unknown {
			indeterminate++
		}
	}
	return key, ops, indeterminate
}

// formatPartition renders one key's history in the order porcupine sees it.
// A partition is a few hundred events, so the whole thing is printed: a
// truncated history cannot be replayed, and replaying it is the point.
func formatPartition(part []porcupine.Event) string {
	var sb strings.Builder
	for _, ev := range part {
		kind := "call  "
		if ev.Kind == porcupine.ReturnEvent {
			kind = "return"
		}
		fmt.Fprintf(&sb, "  client=%d id=%d %s %v\n", ev.ClientId, ev.Id, kind, ev.Value)
	}
	return sb.String()
}

// pruneNeverReadIndeterminatePuts drops indeterminate PUTs whose value no determinate GET returned; the reduction is exact.
func pruneNeverReadIndeterminatePuts(events []porcupine.Event) ([]porcupine.Event, int) {
	type putRec struct {
		key   string
		value string
	}
	calls := make(map[int]kvInput, len(events)/2)
	unknownPuts := make(map[int]putRec)
	read := make(map[putRec]bool)
	for _, ev := range events {
		if ev.Kind == porcupine.CallEvent {
			calls[ev.Id] = ev.Value.(kvInput)
			continue
		}
		in, ok := calls[ev.Id]
		if !ok {
			continue
		}
		out := ev.Value.(kvOutput)
		switch {
		case in.Op == "put" && out.Unknown:
			unknownPuts[ev.Id] = putRec{in.Key, in.Value}
		case in.Op == "get" && !out.Unknown:
			read[putRec{in.Key, out.Value}] = true
		}
	}
	drop := make(map[int]bool)
	for id, p := range unknownPuts {
		if !read[p] {
			drop[id] = true
		}
	}
	if len(drop) == 0 {
		return events, 0
	}
	kept := make([]porcupine.Event, 0, len(events)-2*len(drop))
	for _, ev := range events {
		if !drop[ev.Id] {
			kept = append(kept, ev)
		}
	}
	return kept, len(drop)
}

// pruneIndeterminateGets drops every indeterminate GET; the reduction is exact.
func pruneIndeterminateGets(events []porcupine.Event) ([]porcupine.Event, int) {
	calls := make(map[int]kvInput, len(events)/2)
	drop := make(map[int]bool)
	for _, ev := range events {
		if ev.Kind == porcupine.CallEvent {
			calls[ev.Id] = ev.Value.(kvInput)
			continue
		}
		if in, ok := calls[ev.Id]; ok && in.Op == "get" && ev.Value.(kvOutput).Unknown {
			drop[ev.Id] = true
		}
	}
	if len(drop) == 0 {
		return events, 0
	}
	kept := make([]porcupine.Event, 0, len(events)-2*len(drop))
	for _, ev := range events {
		if !drop[ev.Id] {
			kept = append(kept, ev)
		}
	}
	return kept, len(drop)
}

// checkLinearizability runs porcupine on the recorded history and fails the
// test if the result is not Ok, if the run recorded fewer operations than the
// floor, or if any client observed a protocol violation.
func checkLinearizability(t *testing.T, recorder *opRecorder, floor opsFloor) {
	t.Helper()

	total, determinate, writes, reads := recorder.stats()
	violations, census := recorder.report()

	// Partition census. A check that returns Unknown does so because ONE
	// partition was too expensive, never because the history was large in
	// aggregate -- porcupine decides each key independently. Without this the
	// only evidence a failure leaves is the total, which cannot distinguish a
	// wide-but-shallow history from a single pathological key.
	logPartitionCensus(t, recorder.events())

	t.Logf("history: %d operations recorded, %d completed (%d writes, %d reads), %d indeterminate",
		total, determinate, writes, reads, total-determinate)
	if len(census) > 0 {
		keys := make([]string, 0, len(census))
		for k := range census {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sb strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&sb, "\n  %dx %s", census[k], k)
		}
		t.Logf("client-visible errors:%s", sb.String())
	}

	if len(violations) > 0 {
		shown := violations
		if len(shown) > 20 {
			shown = shown[:20]
		}
		t.Errorf("protocol violations observed by clients (%d total):\n  %s",
			len(violations), strings.Join(shown, "\n  "))
	}

	// A collapsed history is trivially linearizable, so throughput is part of
	// the correctness assertion, not a nice-to-have statistic.
	if writes < floor.writes {
		t.Fatalf("write throughput collapsed: %d completed writes, floor is %d; %s",
			writes, floor.writes, leaderGapSummary(t))
	}
	if reads < floor.reads {
		t.Fatalf("read throughput collapsed: %d completed reads, floor is %d; %s",
			reads, floor.reads, leaderGapSummary(t))
	}

	events, pruned := pruneNeverReadIndeterminatePuts(recorder.events())
	if pruned > 0 {
		t.Logf("pruned %d indeterminate put(s) whose value no get ever returned; "+
			"see pruneNeverReadIndeterminatePuts for why that cannot change the verdict", pruned)
	}
	events, prunedGets := pruneIndeterminateGets(events)
	if prunedGets > 0 {
		t.Logf("pruned %d indeterminate get(s) that never returned; "+
			"see pruneIndeterminateGets for why that cannot change the verdict", prunedGets)
	}
	model := kvModel()
	checkStart := time.Now()
	result := porcupine.CheckEventsTimeout(model, events, checkTimeout)
	checkDuration := time.Since(checkStart)

	t.Logf("porcupine result: %s (checked %d events in %v)", result, len(events), checkDuration)

	if result != porcupine.Ok {
		// The whole-history verdict names no key, and the head of the history
		// is warmup. Re-check each partition on its own: porcupine decides
		// keys independently, so exactly the partitions that are undecidable
		// or violating show up here, and their operations are few enough to
		// print.
		reportPartitionDiagnosis(t, events)
		t.Fatalf("%v", &LinearizabilityViolationError{
			Result:        result,
			NumOps:        total,
			Indeterminate: total - determinate,
			Duration:      checkDuration,
		})
	}
}

// ---------------------------------------------------------------------------
// Retry Helpers
// ---------------------------------------------------------------------------

// isRetryableError returns true if the error is transient and the operation
// should be retried on a different node or after a short delay.
func isRetryableError(err error) bool {
	return errors.Is(err, quicraft.ErrTimeout) ||
		errors.Is(err, quicraft.ErrCanceled) ||
		errors.Is(err, quicraft.ErrNotLeader) ||
		errors.Is(err, quicraft.ErrShardNotReady) ||
		errors.Is(err, quicraft.ErrBusy) ||
		errors.Is(err, quicraft.ErrDropped) ||
		errors.Is(err, quicraft.ErrAborted) ||
		errors.Is(err, quicraft.ErrClosed) ||
		errors.Is(err, quicraft.ErrDraining) ||
		errors.Is(err, quicraft.ErrReadIndexNotReady) ||
		errors.Is(err, quicraft.ErrRejected)
}

// retryBackoff returns how long a client waits before retrying after err.
// Every error is retried; the classification only controls the pause. No
// error path drops the operation, because a dropped operation is invisible to
// the checker.
func retryBackoff(err error) time.Duration {
	if isRetryableError(err) {
		return 5 * time.Millisecond
	}
	return 25 * time.Millisecond
}

// Bounds of the back-off a client applies while the host it asks knows no
// leader. A flat 5 ms retry during an election is about 200 refusals a second
// per client, which fills the soak logs with errors when nothing is wrong.
const (
	leaderUnknownMinBackoff = 5 * time.Millisecond
	leaderUnknownMaxBackoff = 100 * time.Millisecond
)

// leaderUnknown reports whether err says the host knows no leader for the
// shard: a NotLeaderError without a leader hint, or a bare ErrNotLeader.
func leaderUnknown(err error) bool {
	var nle *quicraft.NotLeaderError
	if errors.As(err, &nle) {
		return nle.LeaderID == 0
	}
	return errors.Is(err, quicraft.ErrNotLeader)
}

// retryPacer is one client's retry pause. While the host knows no leader the
// pause doubles from leaderUnknownMinBackoff up to leaderUnknownMaxBackoff;
// any other error, or a completed operation, resets it, so the retry stays
// fast whenever a leader is known and failover throughput is still measured.
// It changes only the pause: every attempt is still recorded.
type retryPacer struct {
	next time.Duration
}

// wait returns the pause before retrying after err.
func (p *retryPacer) wait(err error) time.Duration {
	if !leaderUnknown(err) {
		p.next = 0
		return retryBackoff(err)
	}
	if p.next == 0 {
		p.next = leaderUnknownMinBackoff
	} else {
		p.next = min(2*p.next, leaderUnknownMaxBackoff)
	}
	return p.next
}

// reset returns the pacer to its fast retry after a completed operation.
func (p *retryPacer) reset() {
	p.next = 0
}

// pause sleeps for d, returning early if the run is being torn down.
func pause(d time.Duration, done <-chan struct{}) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}

// findAnyLiveHost returns a live host from the cluster, preferring the leader.
// Falls back to any non-stopped host if the leader is unknown.
func findAnyLiveHost(cluster *testCluster, shardID uint64) *quicraft.Host {
	leaderIdx := cluster.findLeader(shardID)
	if leaderIdx >= 0 {
		cluster.hostsMu.RLock()
		h := cluster.hosts[leaderIdx]
		cluster.hostsMu.RUnlock()
		return h
	}
	cluster.hostsMu.RLock()
	defer cluster.hostsMu.RUnlock()
	for i, h := range cluster.hosts {
		if !cluster.stopped[i].Load() {
			return h
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Client Worker
// ---------------------------------------------------------------------------

// maxAttemptsPerOp bounds retries of one logical operation, keeping indeterminate chains short enough to check.
const maxAttemptsPerOp = 10

// minOpInterval is the least time between one client's attempt starts, jittered, bounding history size on any host.
const minOpInterval = 10 * time.Millisecond

// clientWorker runs Put and Get operations against the cluster for the given
// duration. Every operation is recorded, whether it succeeded, failed, or
// timed out with an unknown outcome.
func clientWorker(
	t *testing.T,
	cluster *testCluster,
	shardID uint64,
	clientID int,
	numKeys int,
	duration time.Duration,
	recorder *opRecorder,
	done <-chan struct{},
) {
	deadline := time.Now().Add(duration)
	opIdx := 0
	attempt := 0
	completed := 0
	var pacer retryPacer
	var nextOp time.Time

	// A client that ran its full window without completing a single operation
	// is a wedge, not slow progress. The aggregate floor can be met by the
	// other clients while one is stuck, so each client asserts for itself.
	defer func() {
		if completed == 0 && time.Now().After(deadline) {
			t.Errorf("client %d completed no operations in %v", clientID, duration)
		}
	}()

	for time.Now().Before(deadline) {
		select {
		case <-done:
			return
		default:
		}

		// See minOpInterval: the history is bounded by op count, not by how
		// fast the host is.
		if wait := time.Until(nextOp); wait > 0 {
			pause(wait, done)
			continue
		}
		nextOp = time.Now().Add(minOpInterval + rand.N(minOpInterval))

		// Advance the key every other operation so the put and get at opIdx and opIdx+1 share a key.
		key := fmt.Sprintf("key-%d", (opIdx/2)%numKeys)
		host := findAnyLiveHost(cluster, shardID)
		if host == nil {
			pause(10*time.Millisecond, done)
			continue
		}

		if opIdx%2 == 0 {
			// Each retry writes a distinct value.
			value := fmt.Sprintf("v-%d-%d-%d", clientID, opIdx, attempt)
			rec := recorder.begin(clientID, kvInput{Op: "put", Key: key, Value: value})

			ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
			//quicraft:direct-propose ANY live host on purpose: the client exercises forwarding and records
			// each attempt individually, so a helper that transparently re-resolved
			// and retried would erase the indeterminate outcomes the checker needs
			result, err := host.SyncPropose(ctx, shardID, encodeKVPut(key, value))
			cancel()

			if err != nil {
				recorder.indeterminate(rec, err)
				attempt++
				pause(pacer.wait(err), done)
				if attempt >= maxAttemptsPerOp {
					// Give up on this write and move on. The attempts stay in
					// the history as indeterminate operations; only the chain
					// is cut. See maxAttemptsPerOp.
					opIdx++
					attempt = 0
				}
				continue
			}

			// kvStateMachine returns the stored value's length; anything else means the entry was not applied.
			if want := uint64(len(value)); result.Value != want {
				recorder.violation(
					"put(%s,%q) reported success but the state machine returned Value=%d, want %d",
					key, value, result.Value, want)
				recorder.indeterminate(rec, nil)
				attempt++
				pause(5*time.Millisecond, done)
				continue
			}

			recorder.finish(rec, kvOutput{Value: value, Ok: true})
		} else {
			rec := recorder.begin(clientID, kvInput{Op: "get", Key: key})

			ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
			//quicraft:direct-read ANY live host on purpose: the client reads
			// from whichever replica it was given, and a transparent re-resolve
			// would erase the indeterminate outcomes the checker needs.
			result, err := host.SyncRead(ctx, shardID, encodeKVGet(key))
			cancel()

			if err != nil {
				recorder.indeterminate(rec, err)
				attempt++
				pause(pacer.wait(err), done)
				if attempt >= maxAttemptsPerOp {
					opIdx++
					attempt = 0
				}
				continue
			}

			val := ""
			if result != nil {
				var ok bool
				val, ok = result.(string)
				if !ok {
					recorder.violation("get(%s) returned %T, want string", key, result)
					recorder.indeterminate(rec, nil)
					attempt++
					if attempt >= maxAttemptsPerOp {
						opIdx++
						attempt = 0
					}
					continue
				}
			}

			recorder.finish(rec, kvOutput{Value: val})
		}

		completed++
		opIdx++
		attempt = 0
		pacer.reset()
	}
}

// ---------------------------------------------------------------------------
// Session Client Worker
// ---------------------------------------------------------------------------

// sessionDupEvery is how often the session worker re-proposes the same (clientID, seriesID).
const sessionDupEvery = 8

// sessionClientWorker drives sessions with a non-idempotent append workload, so a duplicate apply is visible.
func sessionClientWorker(
	t *testing.T,
	cluster *testCluster,
	shardID uint64,
	clientID int,
	numKeys int,
	duration time.Duration,
	recorder *opRecorder,
	done <-chan struct{},
) {
	deadline := time.Now().Add(duration)

	session := acquireSession(cluster, shardID, deadline, done)
	if session == nil {
		t.Errorf("client %d: could not obtain a session before the deadline", clientID)
		return
	}

	opIdx := 0
	var pacer retryPacer
	// open is the append still being retried under its series, recorded as one operation.
	var open *opRecord

	for time.Now().Before(deadline) {
		select {
		case <-done:
			return
		default:
		}

		key := fmt.Sprintf("sess-key-%d", (opIdx/2)%numKeys)
		host := findAnyLiveHost(cluster, shardID)
		if host == nil {
			pause(10*time.Millisecond, done)
			continue
		}

		if opIdx%2 == 0 {
			// The token is fixed across retries of one series.
			token := fmt.Sprintf("t%d-%d", clientID, opIdx)
			rec := open
			if rec == nil {
				rec = recorder.begin(clientID, kvInput{Op: "append", Key: key, Value: token})
			}

			// Every sessionDupEvery-th append is issued twice under one seriesID.
			retrySameSeries := opIdx%(2*sessionDupEvery) == 0
			value, err := proposeAppend(host, session, key, token, retrySameSeries, recorder)
			if err != nil {
				recorder.indeterminate(rec, err)
				open = rec
				// A rejected or invalid session cannot be retried: the series
				// is gone, so a fresh one has to be registered before the
				// client can make progress again. The next attempt runs under
				// a new series and is a new operation; this record stays
				// indeterminate.
				if errors.Is(err, quicraft.ErrRejected) || errors.Is(err, quicraft.ErrInvalidSession) {
					open = nil
					if fresh := acquireSession(cluster, shardID, deadline, done); fresh != nil {
						session = fresh
					}
				}
				pause(pacer.wait(err), done)
				continue
			}

			recorder.finish(rec, kvOutput{Value: value, Ok: true})
			open = nil
		} else {
			rec := recorder.begin(clientID, kvInput{Op: "get", Key: key})

			ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
			//quicraft:direct-read ANY live host on purpose: the client reads
			// from whichever replica it was given, and a transparent re-resolve
			// would erase the indeterminate outcomes the checker needs.
			result, err := host.SyncRead(ctx, shardID, encodeKVGet(key))
			cancel()

			if err != nil {
				recorder.indeterminate(rec, err)
				pause(pacer.wait(err), done)
				continue
			}

			val := ""
			if result != nil {
				var ok bool
				val, ok = result.(string)
				if !ok {
					recorder.violation("get(%s) returned %T, want string", key, result)
					recorder.indeterminate(rec, nil)
					continue
				}
			}

			recorder.finish(rec, kvOutput{Value: val})
		}

		opIdx++
		pacer.reset()
	}
}

// acquireSession obtains a session from any live host, retrying until the
// deadline. Returns nil if no session could be obtained.
func acquireSession(cluster *testCluster, shardID uint64, deadline time.Time, done <-chan struct{}) *quicraft.Session {
	var pacer retryPacer
	for time.Now().Before(deadline) {
		select {
		case <-done:
			return nil
		default:
		}

		host := findAnyLiveHost(cluster, shardID)
		if host == nil {
			pause(10*time.Millisecond, done)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		session, err := host.NewSession(ctx, shardID)
		cancel()
		if err == nil {
			return session
		}
		pause(pacer.wait(err), done)
	}
	return nil
}

// proposeAppend issues one session append and returns the accumulated value; with retrySameSeries it re-proposes the same series and requires the cached response.
func proposeAppend(
	host *quicraft.Host,
	session *quicraft.Session,
	key, token string,
	retrySameSeries bool,
	recorder *opRecorder,
) (string, error) {
	cmd := encodeKVAppend(key, token)

	first, err := proposeOnce(host, session, cmd)
	if err != nil {
		return "", err
	}

	if retrySameSeries {
		seriesID := session.SeriesID()
		second, retryErr := proposeOnce(host, session, cmd)
		switch {
		case retryErr != nil:
			// The retry itself is allowed to fail transiently; the original
			// already succeeded, so the operation's outcome stands.
		case string(second.Data) != string(first.Data) || second.Value != first.Value:
			recorder.violation(
				"session retry of clientID=%d seriesID=%d was applied again: first result (Value=%d, Data=%q), retry result (Value=%d, Data=%q)",
				session.ClientID(), seriesID,
				first.Value, string(first.Data), second.Value, string(second.Data))
		}
	}

	session.ProposalCompleted()
	return string(first.Data), nil
}

// proposeOnce submits one session proposal and waits for its result without advancing the series.
func proposeOnce(host *quicraft.Host, session *quicraft.Session, cmd []byte) (sm.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
	defer cancel()

	rs, err := host.ProposeWithSession(ctx, session, cmd)
	if err != nil {
		return sm.Result{}, err
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return sm.Result{}, result.Err
		}
		return sm.Result{Value: result.Value, Data: result.Data}, nil
	case <-ctx.Done():
		return sm.Result{}, quicraft.ErrTimeout
	}
}

// ---------------------------------------------------------------------------
// Warmup Helper
// ---------------------------------------------------------------------------

// warmupCluster proposes a few entries to ensure QUIC connections are
// established and the leader has committed an entry in its current term
// (required for ReadIndex per PhD 6.4).
func warmupCluster(t *testing.T, cluster *testCluster, shardID uint64) {
	t.Helper()

	if cluster.findLeader(shardID) < 0 {
		t.Fatal("no leader found for warmup")
	}

	// Each proposal resolves the leader again. Binding the host once and
	// reusing it is the cached-leader defect: this helper runs at the start of
	// nearly every linearizability test, immediately after an election, which
	// is exactly when leadership is most likely to move.
	for i := range 3 {
		key := fmt.Sprintf("warmup-%d", i)
		result := proposeToLeader(t, cluster, shardID, encodeKVPut(key, "ok"), defaultTimeout)
		if result.Value != uint64(len("ok")) {
			t.Fatalf("warmup proposal %d returned Value=%d, want %d", i, result.Value, len("ok"))
		}
	}

	// Check a linearizable read through proposer.Read, which re-resolves the leader.
	result := proposer.Read(t, cluster.leaderHost, shardID, encodeKVGet("warmup-0"), defaultTimeout)
	val, ok := result.(string)
	if !ok || val != "ok" {
		t.Fatalf("warmup read returned unexpected value: %v", result)
	}
}

// runClients launches numClients clientWorker goroutines and returns a
// function that waits for them and releases the done channel.
func runClients(
	t *testing.T,
	cluster *testCluster,
	shardID uint64,
	numClients, numKeys int,
	duration time.Duration,
	recorder *opRecorder,
) func() {
	t.Helper()
	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(t, cluster, shardID, clientID, numKeys, duration, recorder, done)
		}(i)
	}
	return func() {
		wg.Wait()
		close(done)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestLinearizability_3Node_Normal verifies that 8 concurrent clients
// performing Put and Get operations on a 3-node QuicRaft cluster produce
// a linearizable history. This is the core correctness property:
// every read must return the value of the most recent write that
// precedes it in real-time ordering.
func TestLinearizability_3Node_Normal(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 8
		numKeys    = 10
		shardID    = 1
		testDur    = 5 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)
	wait()

	checkLinearizability(t, recorder, floorFor(numClients, testDur))
}

// TestLinearizability_3Node_LeaderFailover verifies that linearizability is
// maintained across a leader failover. Clients run continuously while the
// current leader is killed, forcing a new election. Operations that fail
// during failover are retried, and the full history must still be
// linearizable from an external observer's perspective.
func TestLinearizability_3Node_LeaderFailover(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 4
		numKeys    = 10
		shardID    = 1
		// killAt is the normal operation before the leader is killed.
		killAt = 2 * time.Second
	)
	// The run after the kill is sized in election rounds, not seconds, so it
	// spans the same number of elections in either harness mode: 3s with the
	// 1s round of the default mode, 30s in WAL-sync mode's 10s round.
	testDur := killAt + 3*electionRound

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)

	// After killAt of normal operation, kill the leader.
	chaosTimer := time.NewTimer(killAt)
	<-chaosTimer.C
	chaosTimer.Stop()
	t.Logf("stopping leader node[%d]", leaderIdx)
	cluster.stopNode(leaderIdx)

	// Wait for a new leader to be elected from surviving nodes.
	electStart := time.Now()
	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("new leader elected: node[%d] in %v (budget %v)", newLeaderIdx, time.Since(electStart), failoverTimeout)
	elected := recorder.mark()

	wait()

	requireServedSince(t, recorder, elected, "the new leader's election")
	// One election is expected to stall the clients, so only the serving
	// portion of the run counts toward the floor.
	checkLinearizability(t, recorder, floorFor(numClients, testDur-2*electionRound))
}

// TestLinearizability_4Node_ElectionSafety checks election safety on an even-sized shard; one-vote-per-term is covered by TestElectionSafety_SpentTermCannotBeWonTwice.
func TestLinearizability_4Node_ElectionSafety(t *testing.T) {
	const (
		numNodes   = 4
		numClients = 4
		numKeys    = 10
		shardID    = 1
		// killAt is the normal operation before the leader is killed.
		killAt = 3 * time.Second
	)
	// The run after the kill and the wait before the old leader restarts are
	// sized in election rounds, not seconds, so they span the same number of
	// elections in either harness mode: 9s and 4s with the 1s round of the
	// default mode, 90s and 40s in WAL-sync mode's 10s round.
	testDur := killAt + 9*electionRound
	restartAfter := 4 * electionRound

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)

	// Kill the leader at killAt: the surviving 3 of 4 replicas are exactly
	// the quorum, so the new term must be won unanimously.
	killTimer := time.NewTimer(killAt)
	<-killTimer.C
	killTimer.Stop()
	t.Logf("stopping leader node[%d]", leaderIdx)
	cluster.stopNode(leaderIdx)

	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("new leader elected: node[%d]", newLeaderIdx)
	elected := recorder.mark()

	// Restart the old leader restartAfter later. It comes back believing it
	// led its old term; the guard keeps watching it as it rejoins.
	restartTimer := time.NewTimer(restartAfter)
	<-restartTimer.C
	restartTimer.Stop()
	t.Logf("restarting old leader node[%d]", leaderIdx)
	cluster.restartNode(leaderIdx, shardID)

	waitForCondition(t, failoverTimeout, "old leader rejoins", func() bool {
		cluster.hostsMu.RLock()
		h := cluster.hosts[leaderIdx]
		cluster.hostsMu.RUnlock()
		_, err := h.ShardInfo(shardID)
		return err == nil
	})
	rejoined := recorder.mark()

	wait()

	requireServedSince(t, recorder, elected, "the new leader's election")
	requireServedSince(t, recorder, rejoined, "the old leader's rejoin")
	checkLinearizability(t, recorder, floorFor(numClients, testDur-4*electionRound))
}

// TestLinearizability_5Node_MultipleLeaderFailovers stresses repeated leader
// elections and log reconciliation. Two successive leader kills force back-to-back
// elections while clients continuously issue Put/Get operations. A 5-node
// cluster is used because surviving 2 leader kills requires at least 3
// remaining nodes to form a majority quorum.
func TestLinearizability_5Node_MultipleLeaderFailovers(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 4
		numKeys    = 10
		shardID    = 1
		// killAt is the normal operation before the first leader is killed.
		killAt = 3 * time.Second
	)
	// The run after the first kill and the wait before the second are sized
	// in election rounds, not seconds, so they span the same number of
	// elections in either harness mode: 12s and 5s with the 1s round of the
	// default mode, 120s and 50s in WAL-sync mode's 10s round.
	testDur := killAt + 12*electionRound
	secondKillAfter := 5 * electionRound

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)

	// First leader kill at killAt.
	firstKillTimer := time.NewTimer(killAt)
	<-firstKillTimer.C
	firstKillTimer.Stop()
	t.Logf("stopping first leader node[%d]", leaderIdx)
	cluster.stopNode(leaderIdx)

	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("new leader elected: node[%d]", newLeaderIdx)

	// Second leader kill secondKillAfter later.
	secondKillTimer := time.NewTimer(secondKillAfter)
	<-secondKillTimer.C
	secondKillTimer.Stop()
	t.Logf("stopping second leader node[%d]", newLeaderIdx)
	cluster.stopNode(newLeaderIdx)

	finalLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("final leader elected: node[%d]", finalLeaderIdx)
	finalElected := recorder.mark()

	wait()

	requireServedSince(t, recorder, finalElected, "the final leader's election")
	// Two elections are expected to stall the clients.
	checkLinearizability(t, recorder, floorFor(numClients, testDur-4*electionRound))
}

// TestLinearizability_5Node_MinorityDown verifies that a 5-node cluster
// continues to serve linearizable operations when 2 follower nodes are
// stopped. The remaining 3 nodes form a majority quorum and must maintain
// correctness under degraded conditions.
func TestLinearizability_5Node_MinorityDown(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 8
		numKeys    = 10
		shardID    = 1
		testDur    = 8 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)

	// At 2s, stop 2 follower nodes (not the leader).
	minorityTimer := time.NewTimer(2 * time.Second)
	<-minorityTimer.C
	minorityTimer.Stop()
	stoppedCount := 0
	for i := range numNodes {
		if i == leaderIdx {
			continue
		}
		if stoppedCount >= 2 {
			break
		}
		t.Logf("stopping follower node[%d]", i)
		cluster.stopNode(i)
		stoppedCount++
	}

	// Confirm leader is still alive with degraded quorum.
	cluster.waitForClusterLeader(shardID, failoverTimeout)

	wait()

	// The leader is never killed here, so the cluster serves throughout.
	checkLinearizability(t, recorder, floorFor(numClients, testDur-1*time.Second))
}

// TestLinearizability_3Node_SingleKeyContention tests serialization correctness
// under maximum write contention. All 8 clients target the same key, creating
// the worst-case scenario for Raft log ordering. The linearizability check
// verifies that all reads observe a consistent sequence of values.
func TestLinearizability_3Node_SingleKeyContention(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 8
		numKeys    = 1 // all clients target the same key
		shardID    = 1
		testDur    = 5 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)
	wait()

	checkLinearizability(t, recorder, floorFor(numClients, testDur))
}

// TestLinearizability_3Node_FollowerRestart verifies that WAL recovery and
// log catchup maintain linearizability. A follower is stopped while clients
// continue writing, then restarted using the same WAL/NodeHost directories.
// The restarted node recovers from its write-ahead log and catches up with
// the cluster. All operations must remain linearizable throughout.
func TestLinearizability_3Node_FollowerRestart(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 4
		numKeys    = 10
		shardID    = 1
		testDur    = 12 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	// Pick a follower to stop/restart (not the leader).
	followerIdx := -1
	for i := range numNodes {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)

	// At 2s, stop the follower.
	stopTimer := time.NewTimer(2 * time.Second)
	<-stopTimer.C
	stopTimer.Stop()
	t.Logf("stopping follower node[%d]", followerIdx)
	cluster.stopNode(followerIdx)

	// At 6s, restart the follower (WAL recovery + log catchup).
	restartTimer := time.NewTimer(4 * time.Second)
	<-restartTimer.C
	restartTimer.Stop()
	t.Logf("restarting follower node[%d]", followerIdx)
	cluster.restartNode(followerIdx, shardID)

	// Wait for the restarted node to rejoin and catch up.
	waitForCondition(t, failoverTimeout, "restarted follower catches up", func() bool {
		if cluster.stopped[followerIdx].Load() {
			return false
		}
		cluster.hostsMu.RLock()
		h := cluster.hosts[followerIdx]
		cluster.hostsMu.RUnlock()
		_, err := h.ShardInfo(shardID)
		return err == nil
	})
	t.Logf("follower node[%d] has rejoined the cluster", followerIdx)

	wait()

	// The leader is never killed, so only follower churn slows the clients.
	checkLinearizability(t, recorder, floorFor(numClients, testDur-2*time.Second))
}

// TestLinearizability_5Node_RollingRestart restarts followers one at a time under load and checks the history is linearizable.
func TestLinearizability_5Node_RollingRestart(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 4
		numKeys    = 10
		shardID    = 1
		testDur    = 20 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	// Collect follower indices (skip the leader).
	followers := make([]int, 0, numNodes-1)
	for i := range numNodes {
		if i != leaderIdx {
			followers = append(followers, i)
		}
	}

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)

	for round, followerIdx := range followers[:3] {
		stopTimer := time.NewTimer(3 * time.Second)
		<-stopTimer.C
		stopTimer.Stop()
		t.Logf("round %d: stopping follower node[%d]", round, followerIdx)
		cluster.stopNode(followerIdx)

		restartTimer := time.NewTimer(3 * time.Second)
		<-restartTimer.C
		restartTimer.Stop()
		t.Logf("round %d: restarting follower node[%d]", round, followerIdx)
		cluster.restartNode(followerIdx, shardID)

		waitForCondition(t, failoverTimeout,
			fmt.Sprintf("follower node[%d] catches up", followerIdx),
			func() bool {
				cluster.hostsMu.RLock()
				h := cluster.hosts[followerIdx]
				cluster.hostsMu.RUnlock()
				_, err := h.ShardInfo(shardID)
				return err == nil
			})
		t.Logf("round %d: follower node[%d] has rejoined", round, followerIdx)
	}

	wait()

	checkLinearizability(t, recorder, floorFor(numClients, testDur-4*time.Second))
}

// TestLinearizability_3Node_SnapshotDuringLoad verifies that automatic
// snapshots and log compaction do not violate linearizability. With
// SnapshotEntries set very low (50), multiple snapshots fire during the
// test, exercising the snapshot creation, log truncation, and recovery
// code paths under concurrent client load.
func TestLinearizability_3Node_SnapshotDuringLoad(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 8
		numKeys    = 10
		shardID    = 1
		testDur    = 10 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	// Use aggressive snapshot settings to trigger frequent compaction.
	cluster.startShardWithConfig(shardID, func(sid, rid uint64) config.Config {
		c := config.Config{
			ShardID:            sid,
			ReplicaID:          rid,
			SnapshotEntries:    50,
			CompactionOverhead: 10,
		}
		c.SetDefaults()
		return c
	})
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)
	wait()

	checkLinearizability(t, recorder, floorFor(numClients, testDur))
}

// TestLinearizability_3Node_LeaderRestart verifies that restarting a former
// leader does not violate linearizability. The old leader may have
// uncommitted entries from its leadership term that must be discarded when
// it rejoins as a follower. The restarted node recovers from its WAL and
// adopts the new leader's log.
func TestLinearizability_3Node_LeaderRestart(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 4
		numKeys    = 10
		shardID    = 1
		// killAt is the normal operation before the leader is stopped.
		killAt = 2 * time.Second
	)
	// The run after the stop and the wait before the old leader restarts are
	// sized in election rounds, not seconds, so they span the same number of
	// elections in either harness mode: 10s and 4s with the 1s round of the
	// default mode, 100s and 40s in WAL-sync mode's 10s round.
	testDur := killAt + 10*electionRound
	restartAfter := 4 * electionRound

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)

	// At killAt, stop the leader.
	stopLeaderTimer := time.NewTimer(killAt)
	<-stopLeaderTimer.C
	stopLeaderTimer.Stop()
	t.Logf("stopping leader node[%d]", leaderIdx)
	cluster.stopNode(leaderIdx)

	// Wait for a new leader from the remaining 2 nodes.
	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("new leader elected: node[%d]", newLeaderIdx)
	elected := recorder.mark()

	// restartAfter later, restart the old leader (becomes follower).
	restartLeaderTimer := time.NewTimer(restartAfter)
	<-restartLeaderTimer.C
	restartLeaderTimer.Stop()
	t.Logf("restarting old leader node[%d]", leaderIdx)
	cluster.restartNode(leaderIdx, shardID)

	waitForCondition(t, failoverTimeout, "old leader rejoins as follower", func() bool {
		if cluster.stopped[leaderIdx].Load() {
			return false
		}
		cluster.hostsMu.RLock()
		h := cluster.hosts[leaderIdx]
		cluster.hostsMu.RUnlock()
		_, err := h.ShardInfo(shardID)
		return err == nil
	})
	t.Logf("old leader node[%d] has rejoined the cluster", leaderIdx)
	rejoined := recorder.mark()

	wait()

	requireServedSince(t, recorder, elected, "the new leader's election")
	requireServedSince(t, recorder, rejoined, "the old leader's rejoin")
	// One election is expected to stall the clients.
	checkLinearizability(t, recorder, floorFor(numClients, testDur-4*electionRound))
}

// TestLinearizability_5Node_MembershipAddRemove verifies that dynamic
// membership changes (adding and removing nodes) do not violate
// linearizability. A 6th node is added to a running 5-node cluster, then
// a follower is removed. Raft joint consensus ensures safety during
// configuration transitions while clients continuously read and write.
func TestLinearizability_5Node_MembershipAddRemove(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 4
		numKeys    = 10
		shardID    = 1
		testDur    = 15 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	// The index is deliberately NOT retained. Leadership is not pinned for the
	// life of this test — a membership change and roughly eight seconds separate
	// this call from the removal below — so a leader captured here is stale by
	// the time it is used. See the removal block for what that cost.
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)

	// At 3s, add a 6th node.
	addNodeTimer := time.NewTimer(3 * time.Second)
	<-addNodeTimer.C
	addNodeTimer.Stop()
	newIdx := cluster.addNode(shardID)
	t.Logf("added node[%d] to cluster", newIdx)

	// Wait for the new node to catch up.
	waitForCondition(t, failoverTimeout, "new node catches up", func() bool {
		cluster.hostsMu.RLock()
		h := cluster.hosts[newIdx]
		cluster.hostsMu.RUnlock()
		_, err := h.ShardInfo(shardID)
		return err == nil
	})
	t.Logf("node[%d] has joined and caught up", newIdx)

	// At 8s, remove a follower (not the leader, not the newly added node).
	removeNodeTimer := time.NewTimer(5 * time.Second)
	<-removeNodeTimer.C
	removeNodeTimer.Stop()

	// Re-read the leader now and pick a node that is neither leader nor newly added.
	currentLeader := cluster.waitForClusterLeader(shardID, failoverTimeout)
	removeIdx := -1
	for i := range numNodes {
		// Exclude newIdx explicitly.
		if i != currentLeader && i != newIdx {
			removeIdx = i
			break
		}
	}
	if removeIdx < 0 {
		t.Fatalf("no removable follower: leader=%d newIdx=%d numNodes=%d", currentLeader, newIdx, numNodes)
	}
	t.Logf("removing follower node[%d] (leader is node[%d])", removeIdx, currentLeader)
	cluster.removeNode(removeIdx, shardID)

	// Confirm the cluster still has a leader after removal.
	cluster.waitForClusterLeader(shardID, failoverTimeout)

	wait()

	checkLinearizability(t, recorder, floorFor(numClients, testDur-2*time.Second))
}

// TestLinearizability_3Node_SessionProposal checks session proposals stay linearizable under a non-idempotent workload with deliberate duplicates.
func TestLinearizability_3Node_SessionProposal(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 4
		numKeys    = 10
		shardID    = 1
		testDur    = 5 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			sessionClientWorker(t, cluster, shardID, clientID, numKeys, testDur, recorder, done)
		}(i)
	}

	wg.Wait()
	close(done)

	// Session proposals are slower than raw proposals: every eighth write
	// costs an extra round trip, and session registration eats into the run.
	checkLinearizability(t, recorder, floorFor(numClients, testDur-1*time.Second))
}

// TestLinearizability_3Node_LeaseRead verifies that lease-based reads
// (which skip the ReadIndex heartbeat round-trip) produce a linearizable
// history. LeaseRead relies on the leader's lease not having expired,
// making it faster but requiring clock assumptions. This test ensures the
// optimization does not sacrifice correctness.
func TestLinearizability_3Node_LeaseRead(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 8
		numKeys    = 10
		shardID    = 1
		testDur    = 5 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	// Enable LeaseRead for all nodes.
	cluster.startShardWithConfig(shardID, func(sid, rid uint64) config.Config {
		c := config.Config{
			ShardID:   sid,
			ReplicaID: rid,
			LeaseRead: true,
		}
		c.SetDefaults()
		return c
	})
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)
	wait()

	checkLinearizability(t, recorder, floorFor(numClients, testDur))
}

// TestLinearizability_5Node_HighThroughput is a sustained high-throughput
// stress test with 16 concurrent clients on a 5-node cluster. The longer
// duration and higher client count exercise internal batching, pipeline
// buffering, and rate limiting paths that shorter tests may not trigger.
func TestLinearizability_5Node_HighThroughput(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 16
		numKeys    = 20
		shardID    = 1
		testDur    = 30 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	startLeaderTermGuard(t, cluster, shardID)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	wait := runClients(t, cluster, shardID, numClients, numKeys, testDur, recorder)
	wait()

	checkLinearizability(t, recorder, floorFor(numClients, testDur))
}
