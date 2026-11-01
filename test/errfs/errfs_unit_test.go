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

// This file deliberately carries NO build tag, unlike the fault-injection suites
// that use ErrorFS. The injector's own behaviour must be verified in an ordinary
// build, because a defect in it does not produce a failing test — it produces a
// suite of recovery tests that pass without ever injecting anything. A test run
// with WithNoSync(true) can never land an injection on a flush, and a sweep that
// self-calibrates onto whatever operation happens to fail proves nothing. An
// injector that silently does nothing reads as coverage.
package errfs

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
)

var errUnit = errors.New("errfs: injected for unit test")

// realFS returns a delegate rooted at a temp dir, so the injector is exercised
// against a genuine filesystem rather than a second stub.
func realFS(t *testing.T) (waldb.FS, string) {
	t.Helper()
	return waldb.DefaultFS(), t.TempDir()
}

// TestInjectAtFiresOnTheNthOperationExactly pins the counting contract every
// sweep in this package depends on. Off-by-one here silently shifts which
// operation each test believes it is failing.
func TestInjectAtFiresOnTheNthOperationExactly(t *testing.T) {
	delegate, dir := realFS(t)
	efs := New(delegate, 3, errUnit)

	for i := 1; i <= 2; i++ {
		if err := efs.MkdirAll(filepath.Join(dir, "a", "b"), 0o755); err != nil {
			t.Fatalf("op %d returned %v; injection fired before injectAt", i, err)
		}
	}
	if efs.Injected() {
		t.Fatal("Injected() reported true before the injection point")
	}

	err := efs.MkdirAll(filepath.Join(dir, "a", "b"), 0o755)
	if err == nil {
		t.Fatal("op 3 succeeded; injectAt=3 never fired, so every sweep in this " +
			"package would search past the operation it means to fail")
	}
	var injected *InjectedError
	if !errors.As(err, &injected) {
		t.Fatalf("error %v is not an *InjectedError; a caller cannot tell an "+
			"injected failure from a real one", err)
	}
	if injected.OpCount != 3 {
		t.Errorf("InjectedError.OpCount = %d, want 3", injected.OpCount)
	}
	if !errors.Is(err, errUnit) {
		t.Error("the injected error does not unwrap to the error supplied to New")
	}
	if !efs.Injected() {
		t.Error("Injected() reported false after the injection fired")
	}
	if got := efs.Count(); got < 3 {
		t.Errorf("Count() = %d after three operations, want at least 3", got)
	}
}

// TestOnlyOpsRestrictsWhichOperationCanFire pins the guard against
// self-calibrating sweeps: a sweep must fail the operation it NAMES. If the
// filter were ignored, a test claiming to cover an fsync failure could be
// passing on an unrelated Open failure instead.
func TestOnlyOpsRestrictsWhichOperationCanFire(t *testing.T) {
	delegate, dir := realFS(t)
	efs := New(delegate, 1, errUnit).OnlyOps("Sync")

	// Many non-Sync operations, none of which may fire.
	for i := 0; i < 5; i++ {
		if err := efs.MkdirAll(filepath.Join(dir, "d"), 0o755); err != nil {
			t.Fatalf("MkdirAll fired under OnlyOps(\"Sync\"): %v", err)
		}
	}
	if efs.Injected() {
		t.Fatal("an operation outside the filter fired; a sweep can no longer " +
			"assert WHICH operation it failed")
	}

	// The filtered operation must still fire.
	f, err := efs.OpenFile(filepath.Join(dir, "f"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err == nil {
		t.Fatal("Sync did not fire despite being the only permitted op")
	}

	// And clearing the filter restores the unrestricted behaviour.
	efs.Reset(1, errUnit)
	efs.ClearOpsFilter()
	if err := efs.MkdirAll(filepath.Join(dir, "e"), 0o755); err == nil {
		t.Error("ClearOpsFilter did not restore injection on unfiltered ops")
	}
}

// TestShortWriteDisturbsTheDelegateBeforeFailing pins the torn-write mode. A
// short write that reported an error WITHOUT writing anything would leave the
// medium clean, and the recovery paths that exist to handle a partial record
// would never be entered.
func TestShortWriteDisturbsTheDelegateBeforeFailing(t *testing.T) {
	delegate, dir := realFS(t)
	name := filepath.Join(dir, "torn")

	efs := New(delegate, 0, errUnit)
	f, err := efs.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	payload := []byte("0123456789")
	efs.Reset(1, errUnit)
	efs.OnlyOps("WriteAt")
	efs.ShortWrite(4)

	if _, err := f.WriteAt(payload, 0); err == nil {
		t.Fatal("the short write did not report an error")
	}
	if err := f.Close(); err != nil {
		t.Logf("close after injected short write: %v", err)
	}

	got, err := os.ReadFile(name) //nolint:gosec // test-controlled temp path
	if err != nil {
		t.Fatalf("reading the torn file: %v", err)
	}
	if len(got) == len(payload) {
		t.Fatal("the whole payload landed despite ShortWrite(4): the medium is " +
			"intact, so no torn-record recovery path is exercised")
	}
	if len(got) != 4 {
		t.Errorf("wrote %d bytes, want the 4 requested by ShortWrite", len(got))
	}
}

// TestOperationsDelegateWhenNotInjecting pins that ErrorFS is transparent with
// no injection armed. If a wrapper method failed to delegate, every test using
// it would fail for a reason unrelated to fault injection.
func TestOperationsDelegateWhenNotInjecting(t *testing.T) {
	delegate, dir := realFS(t)
	efs := New(delegate, 0, errUnit)

	sub := filepath.Join(dir, "sub")
	if err := efs.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	name := filepath.Join(sub, "file")
	f, err := efs.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.WriteAt([]byte("data"), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, err := efs.ReadDir(sub)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("ReadDir returned %d entries, want 1", len(entries))
	}

	ro, err := efs.Open(name)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close (read-only): %v", err)
	}

	if efs.Injected() {
		t.Error("Injected() reported true with injectAt=0 (disarmed)")
	}
}

// TestInjectedErrorMessagesIdentifyThemselves pins the diagnostics. An injected
// failure that reads like a real one sends the reader looking for a bug in the
// WAL instead of at the test's own injection point.
func TestInjectedErrorMessagesIdentifyThemselves(t *testing.T) {
	ie := &InjectedError{Operation: "Sync", OpCount: 7, Err: errUnit}
	msg := ie.Error()
	for _, want := range []string{"Sync", "7"} {
		if !contains(msg, want) {
			t.Errorf("InjectedError.Error() = %q, missing %q", msg, want)
		}
	}
	if !errors.Is(ie, errUnit) {
		t.Error("InjectedError does not unwrap to its cause")
	}

	uo := &UnknownOpError{Name: "Fsynk"}
	if !contains(uo.Error(), "Fsynk") {
		t.Errorf("UnknownOpError.Error() = %q, missing the offending name", uo.Error())
	}

	lr := &LossReplayError{Err: errUnit}
	if !errors.Is(lr, errUnit) {
		t.Error("LossReplayError does not unwrap to its cause")
	}
	if lr.Error() == "" {
		t.Error("LossReplayError.Error() is empty")
	}
}

// contains is a substring check kept local so this file needs no extra imports.
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestStatAndRemoveDelegate covers the remaining FS surface so a wrapper that
// forgets to call through is caught here rather than as a confusing failure
// inside a WAL recovery test.
func TestStatAndRemoveDelegate(t *testing.T) {
	delegate, dir := realFS(t)
	efs := New(delegate, 0, errUnit)

	name := filepath.Join(dir, "target")
	if err := os.WriteFile(name, []byte("x"), 0o600); err != nil {
		t.Fatalf("seeding the file: %v", err)
	}

	info, err := efs.Stat(name)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 1 {
		t.Errorf("Stat reported size %d, want 1", info.Size())
	}

	if err := efs.Remove(name); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := efs.Stat(name); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Stat after Remove = %v, want fs.ErrNotExist", err)
	}
}
