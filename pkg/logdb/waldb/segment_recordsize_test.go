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

package waldb

import (
	"errors"
	"testing"
)

// TestSegment_Write_RecordTooLarge is the L2 regression test: a single record
// whose framed size exceeds the segment size limit must fail with the permanent
// ErrRecordTooLarge (so the caller splits it), NOT ErrSegmentFull — which would
// make writeRecord rotate to a fresh segment and retry forever, exhausting the
// SaveState retry budget and failing the node.
func TestSegment_Write_RecordTooLarge(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 4096
	const maxFileSize = 64 * 1024

	seg, _, err := createSegment(defaultFS(), dir, 1, blockSize, maxFileSize)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	defer seg.Close()

	// A record larger than the whole segment can never fit, even fresh.
	big := make([]byte, maxFileSize+1)
	if _, err := seg.Write(big); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Write(oversized) = %v, want ErrRecordTooLarge", err)
	}
	if errors.Is(err, ErrSegmentFull) {
		t.Fatal("oversized record must NOT report ErrSegmentFull (would trigger futile rotate+retry)")
	}

	// A record that fits still writes (the guard must not be over-eager).
	if _, err := seg.Write(make([]byte, 1024)); err != nil {
		t.Fatalf("Write(small) = %v, want nil", err)
	}
}
