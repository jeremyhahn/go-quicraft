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

// Package lockedmem reads the kernel's own view of this process's mappings, so
// a test can assert what the KERNEL believes about a page rather than what the
// library that mapped it says.
//
// # Why this exists
//
// go-enclave's MemoryEnclave.Freeze mprotects a filled region read-only, which
// is what turns a stray write through a retained slice from silent corruption of
// key material into a fault. This repository freezes at three sites — a share
// value, a reconstructed secret, an unsealed root key — and a MISSING Freeze is
// invisible: every functional assertion still passes, the material still reads
// back correctly, and the only difference is a protection nothing in the process
// observes until something writes.
//
// Proving the fault directly means writing to the pages, which kills the
// process: the address is mapped-but-read-only and outside the Go heap, so the
// runtime reports it as an unrecoverable fault rather than a catchable panic.
// go-enclave pins the fault itself that way, in a subprocess. What a CONSUMER
// needs is cheaper and different — that its own call sites actually froze — and
// /proc/self/maps answers it in-process without touching the memory.
//
// Linux only. Every function reports whether the information was available so a
// caller can skip rather than fail on a kernel that does not publish it.
package lockedmem

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Protection is the permission string the kernel reports for a mapping, in
// /proc/self/maps order: read, write, execute, private-or-shared. "r--p" is a
// frozen enclave's data region; "rw-p" is one still writable.
type Protection string

// ReadOnly reports whether the mapping is readable and NOT writable, which is
// the state MemoryEnclave.Freeze establishes.
func (p Protection) ReadOnly() bool {
	return len(p) >= 2 && p[0] == 'r' && p[1] == '-'
}

// ProtectionOf reports the kernel's permissions for the mapping containing b's
// first byte.
//
// b must alias memory this process has mapped — in practice a slice taken from
// MemoryEnclave.Open, whose backing pages are the enclave's own. It reports an
// error when /proc/self/maps cannot be read or lists no mapping covering the
// address; both are "this kernel cannot answer", which callers should skip on
// rather than fail on, since neither says anything about the code under test.
//
// It reads the address ONLY — it never dereferences b — so it is safe against a
// region whose protections are exactly what is in question.
func ProtectionOf(b []byte) (Protection, error) {
	if len(b) == 0 {
		return "", fmt.Errorf("lockedmem: cannot locate the mapping of an empty slice")
	}
	addr := addressOf(b)

	f, err := os.Open("/proc/self/maps")
	if err != nil {
		return "", fmt.Errorf("lockedmem: reading /proc/self/maps: %w", err)
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	// A process with many mappings produces long lines; the default 64 KiB
	// buffer is ample, but a truncated scan would silently report "not found",
	// so the limit is raised rather than trusted.
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		lo, hi, ok := parseRange(fields[0])
		if !ok || addr < lo || addr >= hi {
			continue
		}
		return Protection(fields[1]), nil
	}
	if err := sc.Err(); err != nil {
		return "", fmt.Errorf("lockedmem: scanning /proc/self/maps: %w", err)
	}
	return "", fmt.Errorf("lockedmem: no mapping covers address %#x", addr)
}

// parseRange splits a "start-end" field of /proc/self/maps into its bounds.
func parseRange(field string) (lo, hi uint64, ok bool) {
	dash := strings.IndexByte(field, '-')
	if dash <= 0 {
		return 0, 0, false
	}
	lo, err := strconv.ParseUint(field[:dash], 16, 64)
	if err != nil {
		return 0, 0, false
	}
	hi, err = strconv.ParseUint(field[dash+1:], 16, 64)
	if err != nil {
		return 0, 0, false
	}
	return lo, hi, true
}
