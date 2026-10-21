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

package golden

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
)

// recorder captures a fatal failure instead of ending the test, so the
// helper's failure paths can be asserted.
type recorder struct {
	testing.TB
	msg string
}

func (r *recorder) Helper() {}

func (r *recorder) Fatalf(format string, args ...any) {
	r.msg = fmt.Sprintf(format, args...)
}

func setUpdate(t *testing.T, v bool) {
	t.Helper()
	old := *update
	*update = v
	t.Cleanup(func() { *update = old })
}

func TestCheckMatchesAndReportsTheFirstDifference(t *testing.T) {
	t.Chdir(t.TempDir())
	setUpdate(t, true)
	if !Updating() {
		t.Fatal("Updating() = false with the flag set")
	}
	Check(t, "x.bin", []byte{1, 2, 3})
	setUpdate(t, false)

	Check(t, "x.bin", []byte{1, 2, 3})
	if got := Load(t, "x.bin"); !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("Load = %x", got)
	}

	r := &recorder{TB: t}
	Check(r, "x.bin", []byte{1, 9, 3})
	if !strings.Contains(r.msg, "at offset 1") {
		t.Fatalf("mismatch message = %q, want the first differing offset", r.msg)
	}
	r = &recorder{TB: t}
	Check(r, "x.bin", []byte{1, 2})
	if !strings.Contains(r.msg, "at offset 2") {
		t.Fatalf("truncation message = %q, want offset 2", r.msg)
	}
}

func TestLoadAndCheckFailOnAMissingFixture(t *testing.T) {
	t.Chdir(t.TempDir())
	setUpdate(t, false)
	r := &recorder{TB: t}
	Load(r, "absent.bin")
	if !strings.Contains(r.msg, "absent.bin") {
		t.Fatalf("missing-fixture message = %q", r.msg)
	}
}

func TestCheckUpdateReportsWriteFailures(t *testing.T) {
	t.Chdir(t.TempDir())
	setUpdate(t, true)
	// A regular file where the directory belongs fails MkdirAll.
	if err := os.WriteFile("testdata", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	r := &recorder{TB: t}
	Check(r, "x.bin", []byte{1})
	if r.msg == "" {
		t.Fatal("Check -update-golden succeeded with no writable directory")
	}
	if err := os.Remove("testdata"); err != nil {
		t.Fatal(err)
	}
	// A directory where the fixture belongs fails WriteFile.
	if err := os.MkdirAll(Path("x.bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	r = &recorder{TB: t}
	Check(r, "x.bin", []byte{1})
	if r.msg == "" {
		t.Fatal("Check -update-golden succeeded writing over a directory")
	}
}
