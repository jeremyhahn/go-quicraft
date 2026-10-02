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
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// The two fixtures under testdata/ are REAL single-key partitions dumped by
// reportPartitionDiagnosis from CI runs that returned Unknown: key-43 of
// TestSoak_RollingRestartUnderLoad and key-9 of
// TestLinearizability_4Node_ElectionSafety. Offline, key-43 was still Unknown
// after a 30-minute budget. Both are legal.
var fixtureLine = regexp.MustCompile(`^client=(\d+) id=(\d+) (call|return)\s+\{(.*)\}$`)
var fixtureCall = regexp.MustCompile(`^(put|get|append) (\S+) ?(.*)$`)
var fixtureRet = regexp.MustCompile(`^(.*?) (true|false) (true|false)$`)

func loadFixture(t *testing.T, name string) []porcupine.Event {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	defer f.Close()
	var evs []porcupine.Event
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		m := fixtureLine.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		cid, _ := strconv.Atoi(m[1])
		id, _ := strconv.Atoi(m[2])
		if m[3] == "call" {
			c := fixtureCall.FindStringSubmatch(m[4])
			if c == nil {
				t.Fatalf("fixture %s: bad call %q", name, m[4])
			}
			evs = append(evs, porcupine.Event{ClientId: cid, Kind: porcupine.CallEvent, Value: kvInput{Op: c[1], Key: c[2], Value: c[3]}, Id: id})
			continue
		}
		r := fixtureRet.FindStringSubmatch(m[4])
		if r == nil {
			t.Fatalf("fixture %s: bad return %q", name, m[4])
		}
		evs = append(evs, porcupine.Event{ClientId: cid, Kind: porcupine.ReturnEvent, Value: kvOutput{Value: r[1], Ok: r[2] == "true", Unknown: r[3] == "true"}, Id: id})
	}
	return evs
}

// TestPrune_ResolvesTheCIUnknowns is the whole argument for the prune, on the
// two histories that motivated it. Raw, each exhausts a short budget; pruned,
// each decides Ok in milliseconds. The short raw budget keeps the test fast
// while still demonstrating the search does not finish on its own -- key-43
// did not finish in 30 minutes.
func TestPrune_ResolvesTheCIUnknowns(t *testing.T) {
	for _, tc := range []struct {
		name          string
		indeterminate int
		expectPruned  int
	}{
		{"ci_key43_soak_unknown.txt", 24, 23},
		{"ci_key9_electionsafety_unknown.txt", 25, 24},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := loadFixture(t, tc.name)
			if res := porcupine.CheckEventsTimeout(kvModel(), raw, 2*time.Second); res != porcupine.Unknown {
				t.Fatalf("raw history decided %s within 2s; the fixture no longer demonstrates the cost", res)
			}
			pruned, n := pruneNeverReadIndeterminatePuts(raw)
			if n != tc.expectPruned {
				t.Fatalf("pruned %d, want %d of %d indeterminate", n, tc.expectPruned, tc.indeterminate)
			}
			start := time.Now()
			res := porcupine.CheckEventsTimeout(kvModel(), pruned, 30*time.Second)
			if res != porcupine.Ok {
				t.Fatalf("pruned history is %s, want Ok", res)
			}
			t.Logf("%s: raw=Unknown@2s, pruned(%d of %d)=Ok in %v", tc.name, n, tc.indeterminate, time.Since(start).Round(time.Millisecond))
		})
	}
}

// TestPrune_IsExactlyNeverReadIndeterminatePuts pins the three rules the
// reduction depends on: a never-read indeterminate PUT goes; an indeterminate
// PUT that some get returned STAYS; an indeterminate APPEND stays no matter
// what, because it changes what later reads accumulate.
func TestPrune_IsExactlyNeverReadIndeterminatePuts(t *testing.T) {
	ev := func(id, cid int, kind porcupine.EventKind, v interface{}) porcupine.Event {
		return porcupine.Event{ClientId: cid, Kind: kind, Value: v, Id: id}
	}
	call, ret := porcupine.CallEvent, porcupine.ReturnEvent
	events := []porcupine.Event{
		ev(1, 0, call, kvInput{Op: "put", Key: "k", Value: "seen"}),
		ev(2, 1, call, kvInput{Op: "put", Key: "k", Value: "never"}),
		ev(3, 2, call, kvInput{Op: "append", Key: "s", Value: "tok"}),
		ev(4, 0, call, kvInput{Op: "get", Key: "k"}),
		ev(4, 0, ret, kvOutput{Value: "seen"}),
		ev(1, 0, ret, kvOutput{Unknown: true}),
		ev(2, 1, ret, kvOutput{Unknown: true}),
		ev(3, 2, ret, kvOutput{Unknown: true}),
	}
	pruned, n := pruneNeverReadIndeterminatePuts(events)
	if n != 1 {
		t.Fatalf("pruned %d, want exactly 1 (the never-read put)", n)
	}
	keep := map[int]bool{}
	for _, e := range pruned {
		keep[e.Id] = true
	}
	if keep[2] {
		t.Error("never-read indeterminate put (id 2) was kept")
	}
	if !keep[1] {
		t.Error("indeterminate put that a get returned (id 1) was pruned; that changes the verdict")
	}
	if !keep[3] {
		t.Error("indeterminate append (id 3) was pruned; appends are never prunable")
	}
}
