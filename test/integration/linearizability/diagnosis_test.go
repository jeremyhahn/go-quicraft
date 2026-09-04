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
	"fmt"
	"testing"

	"github.com/anishathalye/porcupine"
)

// synthHistory builds a well-formed history over the given keys. The key named
// by corrupt gets a read that returns a value never written, which no
// linearization can explain.
func synthHistory(keys []string, opsPerKey int, corrupt string) []porcupine.Event {
	var events []porcupine.Event
	id := 0
	for _, key := range keys {
		current := ""
		for n := range opsPerKey {
			myID := id
			id++
			if n%2 == 0 {
				value := fmt.Sprintf("%s-v%d", key, n)
				events = append(events,
					porcupine.Event{Kind: porcupine.CallEvent, Value: kvInput{Op: "put", Key: key, Value: value}, Id: myID},
					porcupine.Event{Kind: porcupine.ReturnEvent, Value: kvOutput{Ok: true}, Id: myID})
				current = value
				continue
			}
			observed := current
			if key == corrupt && n == opsPerKey-1 {
				observed = "value-never-written"
			}
			events = append(events,
				porcupine.Event{Kind: porcupine.CallEvent, Value: kvInput{Op: "get", Key: key}, Id: myID},
				porcupine.Event{Kind: porcupine.ReturnEvent, Value: kvOutput{Value: observed}, Id: myID})
		}
	}
	return events
}

// TestDiagnosePartitions_IsolatesTheOffendingKey is the guarantee that makes a
// failed soak run actionable: porcupine reports ONE verdict for the whole
// history, which names no key, so the harness re-checks each partition on its
// own. This asserts that the re-check clears every healthy key and reports the
// single bad one.
func TestDiagnosePartitions_IsolatesTheOffendingKey(t *testing.T) {
	keys := []string{"key-0", "key-1", "key-2", "key-3"}
	events := synthHistory(keys, 20, "key-2")

	if porcupine.CheckEvents(kvModel(), events) {
		t.Fatal("fixture is not a violating history: whole-history check passed")
	}

	verdicts, decided, total := diagnosePartitions(events)
	if total != len(keys) {
		t.Errorf("partitioned into %d, want %d", total, len(keys))
	}
	if decided != len(keys)-1 {
		t.Errorf("%d partitions decided Ok, want %d", decided, len(keys)-1)
	}
	if len(verdicts) != 1 {
		t.Fatalf("got %d non-Ok verdicts, want 1: %+v", len(verdicts), verdicts)
	}
	if verdicts[0].Key != "key-2" {
		t.Errorf("blamed %q, want %q", verdicts[0].Key, "key-2")
	}
	if verdicts[0].Result != porcupine.Illegal {
		t.Errorf("result=%s, want Illegal", verdicts[0].Result)
	}
	if verdicts[0].Ops != 20 {
		t.Errorf("ops=%d, want 20", verdicts[0].Ops)
	}
	if formatPartition(verdicts[0].Events) == "" {
		t.Error("verdict carries no printable history")
	}
}

// TestDiagnosePartitions_CleanHistory records the measured claim the harness
// now relies on: a legal history decides quickly and blames nobody.
func TestDiagnosePartitions_CleanHistory(t *testing.T) {
	events := synthHistory([]string{"key-0", "key-1"}, 40, "")

	if !porcupine.CheckEvents(kvModel(), events) {
		t.Fatal("clean fixture did not check Ok")
	}
	verdicts, decided, total := diagnosePartitions(events)
	if len(verdicts) != 0 {
		t.Errorf("clean history produced verdicts: %+v", verdicts)
	}
	if decided != total || total != 2 {
		t.Errorf("decided=%d total=%d, want 2 and 2", decided, total)
	}
}
