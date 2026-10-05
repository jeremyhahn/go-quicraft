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

package revocation

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// A slog handler is user code (slog.SetDefault). The checker logged inside its
// critical windows — a failed reload before marking the set stale, a changed
// CRL before committing the new set, the initial load's failure before
// starting the reload loop — so a handler that panicked or called
// runtime.Goexit left revocation fail-open or frozen.

// exitingRecordHandler runs exit on every record whose message starts with
// prefix and discards everything else.
type exitingRecordHandler struct {
	prefix string
	exit   func()
}

func (h exitingRecordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h exitingRecordHandler) Handle(_ context.Context, r slog.Record) error {
	if strings.HasPrefix(r.Message, h.prefix) {
		h.exit()
	}
	return nil
}
func (h exitingRecordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h exitingRecordHandler) WithGroup(string) slog.Handler      { return h }

var recordExits = map[string]func(){
	"panic":  func() { panic("slog handler panicked") },
	"Goexit": runtime.Goexit,
}

// installExitingRecords makes the default slog handler exit on records
// starting with prefix until the test ends.
func installExitingRecords(t *testing.T, prefix string, exit func()) {
	t.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(exitingRecordHandler{prefix: prefix, exit: exit}))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

// runToEnd runs fn on a goroutine of its own and waits for it to end however
// it ends.
func runToEnd(t *testing.T, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = recover() }()
		fn()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the call never ended")
	}
}

// TestReloadCRLs_FailedReadMarksStaleWhateverTheLogDoes: a reload whose CRL
// file cannot be read marks the set stale — enforcement fails closed — even
// when the warning's handler panics or exits.
func TestReloadCRLs_FailedReadMarksStaleWhateverTheLogDoes(t *testing.T) {
	for exitName, exit := range recordExits {
		t.Run(exitName, func(t *testing.T) {
			ca := newTestCA(t)
			path := writeCRLFile(t, t.TempDir(), "crl.pem", ca.createCRL(t, 77))
			checker := NewChecker(Config{CRLPaths: []string{path}, Mode: ModeCRL}, ca.cert)
			if err := checker.ReloadCRLs(); err != nil {
				t.Fatalf("ReloadCRLs: %v", err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatalf("Remove: %v", err)
			}
			installExitingRecords(t, "CRL file read failed", exit)
			runToEnd(t, func() { _ = checker.ReloadCRLs() })

			if _, err := checker.IsRevoked(ca.issueLeaf(t, 78)); !errors.Is(err, ErrCRLStale) {
				t.Fatalf("unknown serial after a failed reload: err=%v, want ErrCRLStale (fail closed)", err)
			}
		})
	}
}

// TestReloadCRLs_CommitsBeforeLogging: a reload that read a changed CRL
// commits the new revoked set even when the "reloaded" record's handler
// panics or exits.
func TestReloadCRLs_CommitsBeforeLogging(t *testing.T) {
	for exitName, exit := range recordExits {
		t.Run(exitName, func(t *testing.T) {
			ca := newTestCA(t)
			dir := t.TempDir()
			path := writeCRLFile(t, dir, "crl.pem", ca.createCRL(t, 100))
			checker := NewChecker(Config{CRLPaths: []string{path}}, ca.cert)
			if err := checker.ReloadCRLs(); err != nil {
				t.Fatalf("ReloadCRLs: %v", err)
			}
			writeCRLFile(t, dir, "crl.pem", ca.createCRL(t, 100, 200))
			installExitingRecords(t, "CRL reloaded", exit)
			runToEnd(t, func() { _ = checker.ReloadCRLs() })

			if !crlRevokedFor(checker, ca.issueLeaf(t, 200)) {
				t.Fatal("serial 200 is not revoked: the reloaded CRL was never committed")
			}
		})
	}
}

// TestStart_StartsReloadLoopWhateverTheLogDoes: Start runs the periodic reload
// even when the initial load fails and its warning's handler panics or exits;
// the loop is what later picks the CRL up.
func TestStart_StartsReloadLoopWhateverTheLogDoes(t *testing.T) {
	for exitName, exit := range recordExits {
		t.Run(exitName, func(t *testing.T) {
			ca := newTestCA(t)
			dir := t.TempDir()
			path := dir + "/crl.pem" // not there yet: the initial load fails
			checker := NewChecker(Config{
				CRLPaths:      []string{path},
				CheckInterval: 10 * time.Millisecond,
			}, ca.cert)
			defer checker.Stop()
			installExitingRecords(t, "initial CRL reload failed", exit)
			runToEnd(t, checker.Start)

			writeCRLFile(t, dir, "crl.pem", ca.createCRL(t, 300))
			deadline := time.After(5 * time.Second)
			for !crlRevokedFor(checker, ca.issueLeaf(t, 300)) {
				select {
				case <-deadline:
					t.Fatal("the reload loop never loaded the CRL: Start did not start it")
				case <-time.After(5 * time.Millisecond):
				}
			}
		})
	}
}
