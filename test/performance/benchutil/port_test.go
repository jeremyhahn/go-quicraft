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

package benchutil

import (
	"net"
	"os"
	"strconv"
	"testing"
)

// ephemeralRange reports the kernel's ephemeral port range, falling back to the
// Linux defaults when the sysctl is unreadable.
func ephemeralRange(t *testing.T) (lo, hi int) {
	t.Helper()
	data, err := os.ReadFile(ephemeralRangeFile)
	if err != nil {
		return defaultEphemeralLo, defaultEphemeralHi
	}
	lo, hi, err = parsePortRange(string(data))
	if err != nil {
		return defaultEphemeralLo, defaultEphemeralHi
	}
	return lo, hi
}

// TestReserveTCPPortOutsideEphemeralRange is the regression test for the
// five-node dragonboat failure
//
//	NewNodeHost[3] failed: listen tcp 127.0.0.1:41461: bind: address already in use
//
// The harness used to reserve ports by listening on 127.0.0.1:0 and closing the
// listener, which returns a port from the kernel's ephemeral range. The kernel
// assigns local ports for OUTBOUND connections from that same range, so between
// the close and the engine's bind, a transport connection opened by the
// preceding benchmark could be given the reserved port. A reservation drawn
// from outside the ephemeral range cannot be taken that way, and this test
// asserts exactly that property -- it fails against the old listen-on-zero
// implementation, whose ports are by construction inside the range.
func TestReserveTCPPortOutsideEphemeralRange(t *testing.T) {
	lo, hi := ephemeralRange(t)

	for i := 0; i < 16; i++ {
		addr, err := ReserveTCPPort()
		if err != nil {
			t.Fatalf("ReserveTCPPort: %v", err)
		}
		_, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("SplitHostPort(%q): %v", addr, err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			t.Fatalf("Atoi(%q): %v", portStr, err)
		}
		if port >= lo && port <= hi {
			t.Fatalf("reserved port %d falls inside the kernel ephemeral range %d-%d; "+
				"the kernel can hand it to an outbound connection before the engine binds it", port, lo, hi)
		}
	}
}

// TestReserveTCPPortUnique asserts that no two reservations in a process return
// the same port, so a cluster's nodes cannot be handed colliding addresses.
func TestReserveTCPPortUnique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 64; i++ {
		addr, err := ReserveTCPPort()
		if err != nil {
			t.Fatalf("ReserveTCPPort: %v", err)
		}
		if seen[addr] {
			t.Fatalf("ReserveTCPPort returned %s twice", addr)
		}
		seen[addr] = true
	}
}

// TestReserveTCPPortBindable asserts a reservation is actually usable: every
// port handed out for a five-node cluster must still bind after all five have
// been reserved, which is the sequence that failed in the smoke run.
func TestReserveTCPPortBindable(t *testing.T) {
	const nodes = 5

	addrs := make([]string, nodes)
	for i := range addrs {
		addr, err := ReserveTCPPort()
		if err != nil {
			t.Fatalf("ReserveTCPPort: %v", err)
		}
		addrs[i] = addr
	}

	listeners := make([]net.Listener, 0, nodes)
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()
	for i, addr := range addrs {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("node %d could not bind its reserved address %s: %v", i, addr, err)
		}
		listeners = append(listeners, l)
	}
}

func TestParsePortRange(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		lo      int
		hi      int
		wantErr bool
	}{
		{name: "linux default", in: "32768\t60999\n", lo: 32768, hi: 60999},
		{name: "space separated", in: "1024 65535", lo: 1024, hi: 65535},
		{name: "one field", in: "32768", wantErr: true},
		{name: "non numeric", in: "lo hi", wantErr: true},
		{name: "inverted", in: "60999 32768", wantErr: true},
		{name: "out of range", in: "32768 70000", wantErr: true},
		{name: "empty", in: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lo, hi, err := parsePortRange(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parsePortRange(%q) = %d, %d, nil; want error", tt.in, lo, hi)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePortRange(%q): %v", tt.in, err)
			}
			if lo != tt.lo || hi != tt.hi {
				t.Fatalf("parsePortRange(%q) = %d, %d; want %d, %d", tt.in, lo, hi, tt.lo, tt.hi)
			}
		})
	}
}

func TestReservationWindow(t *testing.T) {
	tests := []struct {
		name        string
		ephemeralLo int
		ephemeralHi int
		lo          int
		hi          int
		wantErr     bool
	}{
		{name: "above the linux default", ephemeralLo: 32768, ephemeralHi: 60999, lo: 61000, hi: 65535},
		{name: "below a range reaching the top", ephemeralLo: 32768, ephemeralHi: 65535, lo: 1024, hi: 32767},
		{name: "no room on either side", ephemeralLo: 1024, ephemeralHi: 65535, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lo, hi, err := reservationWindow(tt.ephemeralLo, tt.ephemeralHi)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("reservationWindow(%d, %d) = %d, %d, nil; want error",
						tt.ephemeralLo, tt.ephemeralHi, lo, hi)
				}
				return
			}
			if err != nil {
				t.Fatalf("reservationWindow(%d, %d): %v", tt.ephemeralLo, tt.ephemeralHi, err)
			}
			if lo != tt.lo || hi != tt.hi {
				t.Fatalf("reservationWindow(%d, %d) = %d, %d; want %d, %d",
					tt.ephemeralLo, tt.ephemeralHi, lo, hi, tt.lo, tt.hi)
			}
		})
	}
}

// TestReservationWindowDisjointFromEphemeral asserts the window never overlaps
// the range it is meant to avoid, for every shape of ephemeral range.
func TestReservationWindowDisjointFromEphemeral(t *testing.T) {
	ranges := [][2]int{
		{32768, 60999},
		{1025, 65535},
		{49152, 65535},
		{10000, 20000},
	}
	for _, r := range ranges {
		lo, hi, err := reservationWindow(r[0], r[1])
		if err != nil {
			t.Fatalf("reservationWindow(%d, %d): %v", r[0], r[1], err)
		}
		if lo > hi {
			t.Fatalf("reservationWindow(%d, %d) = %d, %d; empty window", r[0], r[1], lo, hi)
		}
		if hi >= r[0] && lo <= r[1] {
			t.Fatalf("window %d-%d overlaps ephemeral range %d-%d", lo, hi, r[0], r[1])
		}
	}
}

// findFreePortWindow locates a run of n consecutive free ports inside the
// process reservation window, for tests that need to drive the allocator
// against a window small enough to exhaust.
func findFreePortWindow(t *testing.T, n int) (lo, hi int) {
	t.Helper()
	portState.once.Do(initPortWindow)
	if portState.err != nil {
		t.Skipf("no reservation window on this host: %v", portState.err)
	}
	for base := portState.hi - n + 1; base >= portState.lo; base -= n {
		listeners := make([]net.Listener, 0, n)
		ok := true
		for p := base; p < base+n; p++ {
			l, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(p))
			if err != nil {
				ok = false
				break
			}
			listeners = append(listeners, l)
		}
		for _, l := range listeners {
			l.Close()
		}
		if ok {
			return base, base + n - 1
		}
	}
	t.Skipf("no run of %d free ports in %d-%d", n, portState.lo, portState.hi)
	return 0, 0
}

// TestReserveTCPPortRecyclesWhenWindowExhausted asserts that ports are reusable
// once their cluster is gone. The election benchmarks build and tear down a
// full cluster on every b.N iteration, so a long run asks for far more ports
// than any window holds; an allocator that never reuses one would turn the
// EADDRINUSE flake it replaced into a guaranteed failure late in the run.
func TestReserveTCPPortRecyclesWhenWindowExhausted(t *testing.T) {
	const windowSize = 4

	portState.once.Do(initPortWindow)

	portState.mu.Lock()
	saveLo, saveHi, saveNext := portState.lo, portState.hi, portState.next
	saveTaken, saveRecent := portState.taken, portState.recent
	portState.mu.Unlock()
	defer func() {
		portState.mu.Lock()
		portState.lo, portState.hi, portState.next = saveLo, saveHi, saveNext
		portState.taken, portState.recent = saveTaken, saveRecent
		portState.mu.Unlock()
	}()

	lo, hi := findFreePortWindow(t, windowSize)

	portState.mu.Lock()
	portState.lo, portState.hi, portState.next = lo, hi, lo
	portState.taken = make(map[int]bool)
	portState.recent = nil
	portState.mu.Unlock()

	seen := make(map[string]int)
	for i := 0; i < windowSize*8; i++ {
		addr, err := ReserveTCPPort()
		if err != nil {
			t.Fatalf("reservation %d of %d in a %d-port window: %v",
				i+1, windowSize*8, windowSize, err)
		}
		seen[addr]++
	}
	if len(seen) != windowSize {
		t.Fatalf("reservations covered %d distinct ports; want %d (the whole window)",
			len(seen), windowSize)
	}
}
