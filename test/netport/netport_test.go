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

package netport

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"testing"
)

// reuseAddrListen binds addr the way the QUIC transport binds its listener
// (pkg/internal/transport/quic.go, Start), so a test can stand in for a live
// host holding a port.
func reuseAddrListen(addr string) (net.PacketConn, error) {
	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
		},
	}
	return lc.ListenPacket(context.Background(), "udp", addr)
}

// forget drops one port from the in-process reservation record, so a test can
// exercise the probe bind rather than the taken-set that would shadow it.
func forget(port int) {
	state.mu.Lock()
	defer state.mu.Unlock()
	delete(state.taken, port)
	kept := state.recent[:0]
	for _, p := range state.recent {
		if p != port {
			kept = append(kept, p)
		}
	}
	state.recent = kept
}

// rewindTo points the scan cursor at a specific port, so the next reservation
// probes that port first.
//
// Without this a test cannot reach a chosen port: the cursor advances past
// every port it hands out and only returns after lapping the whole window,
// which for the real window is thousands of reservations away.
func rewindTo(port int) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.next = port
}

// kernelEphemeralRange reports the range the kernel assigns from for bind(:0)
// and for the local port of an outbound connection, falling back to the Linux
// defaults where the sysctl is unreadable.
func kernelEphemeralRange(t *testing.T) (lo, hi int) {
	t.Helper()
	data, err := os.ReadFile(ephemeralRangeFile)
	if err != nil {
		return defaultEphemeralLo, defaultEphemeralHi
	}
	l, h, perr := parsePortRange(string(data))
	if perr != nil {
		return defaultEphemeralLo, defaultEphemeralHi
	}
	return l, h
}

// portOf extracts the numeric port from a reserved address.
func portOf(t *testing.T, addr string) int {
	t.Helper()
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("reserved address %q is not host:port: %v", addr, err)
	}
	p, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("reserved address %q has a non-numeric port: %v", addr, err)
	}
	return p
}

// TestReserveUDPAvoidsTheKernelEphemeralRange is the load-bearing property, and
// the one that separates this package from the helper it replaces.
//
// A port inside ip_local_port_range is a port the kernel may hand to somebody
// else — the next reservation, or the local end of any outbound connection —
// between the moment it is reserved and the moment the host binds it. That is
// #131. A port outside the range cannot be assigned by the kernel to anything,
// so the interval stops mattering.
//
// The check is deterministic: the old close-then-return helper drew every port
// from inside the range and fails here on the first reservation, not on an
// unlucky one.
func TestReserveUDPAvoidsTheKernelEphemeralRange(t *testing.T) {
	lo, hi := kernelEphemeralRange(t)

	const reservations = 64
	for i := range reservations {
		addr, err := ReserveUDP()
		if err != nil {
			t.Fatalf("reservation %d: %v", i, err)
		}
		p := portOf(t, addr)
		if p >= lo && p <= hi {
			t.Fatalf("reservation %d returned %s, whose port %d lies inside the "+
				"kernel ephemeral range %d-%d; the kernel may assign that port to "+
				"another socket before the host binds it, which is exactly the "+
				"collision this package exists to remove", i, addr, p, lo, hi)
		}
	}
	t.Logf("checked %d reservations against ephemeral range %d-%d", reservations, lo, hi)
}

// TestReserveUDPNeverRepeatsAPort binds the concurrent case #131 observed: a
// cluster asks for several addresses at once and must not be handed one twice.
func TestReserveUDPNeverRepeatsAPort(t *testing.T) {
	const (
		goroutines   = 8
		perGoroutine = 24
	)

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = make(map[string]int)
		errs []error
	)
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for range perGoroutine {
				addr, err := ReserveUDP()
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
				} else {
					seen[addr]++
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	for _, err := range errs {
		t.Fatalf("reservation failed: %v", err)
	}
	for addr, n := range seen {
		if n > 1 {
			t.Fatalf("%s was handed out %d times; two hosts given one address "+
				"either fail shard membership validation or, in different shards, "+
				"bind the same port with SO_REUSEADDR and silently split each "+
				"other's datagrams", addr, n)
		}
	}
	if want := goroutines * perGoroutine; len(seen) != want {
		t.Fatalf("expected %d distinct addresses, got %d", want, len(seen))
	}
}

// TestReserveUDPSkipsPortsHeldByALiveListener covers the cross-process case:
// another test binary's host already holds a port in the window, and the probe
// must refuse it.
//
// The listener here is bound the way the QUIC transport binds
// (pkg/internal/transport/quic.go sets SO_REUSEADDR), because a probe that set
// SO_REUSEADDR itself would bind straight through such a holder and report the
// port free. The probe in scanWindow is deliberately a plain ListenPacket for
// that reason, and this is the test that says so.
func TestReserveUDPSkipsPortsHeldByALiveListener(t *testing.T) {
	held, err := ReserveUDP()
	if err != nil {
		t.Fatalf("reserve address to hold: %v", err)
	}
	heldPort := portOf(t, held)
	pc, err := reuseAddrListen(held)
	if err != nil {
		t.Fatalf("bind held address %s: %v", held, err)
	}
	defer pc.Close()

	// Forget the reservation, so only the live listener stands between the scan
	// and that port, and rewind the cursor onto it so the very next scan probes
	// it first. Both are needed: the taken-set would shadow the probe, and the
	// cursor would otherwise not return to this port for a full lap of the
	// window — which is how this test once passed against a probe that bound
	// straight through the holder.
	forget(heldPort)
	rewindTo(heldPort)

	const reservations = 128
	for i := range reservations {
		addr, err := ReserveUDP()
		if err != nil {
			t.Fatalf("reservation %d: %v", i, err)
		}
		if addr == held {
			t.Fatalf("reservation %d returned %s while a live SO_REUSEADDR "+
				"listener holds it; the probe bind failed to detect the holder",
				i, addr)
		}
		// Only the first reservation starts at the held port; put the cursor
		// back so every iteration re-probes it rather than marching away.
		forget(heldPort)
		rewindTo(heldPort)
	}
}

// TestWindowIsDisjointFromTheEphemeralRange checks the derivation itself, for
// the configurations a differently-tuned runner can present.
func TestWindowIsDisjointFromTheEphemeralRange(t *testing.T) {
	cases := []struct {
		name           string
		ephLo, ephHi   int
		wantLo, wantHi int
		wantErr        bool
	}{
		{name: "stock linux, window above", ephLo: 32768, ephHi: 60999, wantLo: 61000, wantHi: 65535},
		{name: "widened to the top, window below", ephLo: 32768, ephHi: 65535, wantLo: 1024, wantHi: 32767},
		{name: "whole space ephemeral", ephLo: 1024, ephHi: 65535, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lo, hi, err := window(tc.ephLo, tc.ephHi)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected no window for ephemeral range %d-%d, got %d-%d",
						tc.ephLo, tc.ephHi, lo, hi)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if lo != tc.wantLo || hi != tc.wantHi {
				t.Fatalf("window(%d, %d) = %d-%d, want %d-%d",
					tc.ephLo, tc.ephHi, lo, hi, tc.wantLo, tc.wantHi)
			}
			if lo <= tc.ephHi && hi >= tc.ephLo {
				t.Fatalf("window %d-%d overlaps ephemeral range %d-%d",
					lo, hi, tc.ephLo, tc.ephHi)
			}
		})
	}
}

// TestParsePortRange covers the sysctl reader, including the values that must
// be rejected rather than silently narrowing the window.
func TestParsePortRange(t *testing.T) {
	cases := []struct {
		name           string
		in             string
		wantLo, wantHi int
		wantErr        bool
	}{
		{name: "stock", in: "32768\t60999\n", wantLo: 32768, wantHi: 60999},
		{name: "spaces", in: "  1024   65535  ", wantLo: 1024, wantHi: 65535},
		{name: "one field", in: "32768", wantErr: true},
		{name: "three fields", in: "1 2 3", wantErr: true},
		{name: "non-numeric", in: "lo hi", wantErr: true},
		{name: "inverted", in: "60999 32768", wantErr: true},
		{name: "above 65535", in: "32768 70000", wantErr: true},
		{name: "port zero", in: "0 60999", wantErr: true},
		{name: "empty", in: "", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lo, hi, err := parsePortRange(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parsePortRange(%q) = %d, %d, want an error", tc.in, lo, hi)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePortRange(%q): %v", tc.in, err)
			}
			if lo != tc.wantLo || hi != tc.wantHi {
				t.Fatalf("parsePortRange(%q) = %d, %d, want %d, %d", tc.in, lo, hi, tc.wantLo, tc.wantHi)
			}
		})
	}
}

// TestStartOffsetStaysInTheWindow checks the PID seed lands in range for every
// PID the kernel can produce, including the multiplication overflowing.
func TestStartOffsetStaysInTheWindow(t *testing.T) {
	const span = 4536
	for _, pid := range []int{0, 1, 2, 999, 4194304, 1 << 22, 1<<31 - 1, 1<<62 - 1} {
		off := startOffset(pid, span)
		if off < 0 || off >= span {
			t.Fatalf("startOffset(%d, %d) = %d, outside [0, %d)", pid, span, off, span)
		}
	}
	if off := startOffset(1234, 0); off != 0 {
		t.Fatalf("startOffset with an empty span = %d, want 0", off)
	}
}

// TestErrorsCarryTheirContext checks the typed errors say enough to act on.
func TestErrorsCarryTheirContext(t *testing.T) {
	for _, err := range []error{
		&WindowExhaustedError{Lo: 61000, Hi: 65535},
		&NoWindowError{EphemeralLo: 1024, EphemeralHi: 65535},
		&MalformedPortRangeError{Value: "nonsense"},
	} {
		if err.Error() == "" {
			t.Fatalf("%T has an empty message", err)
		}
	}
}

// withWindow installs a reservation window of the test's choosing and restores
// the process-wide state afterwards, so a test can drive the recycle path
// without binding the several thousand ports the real window holds.
func withWindow(t *testing.T, lo, hi int) {
	t.Helper()
	state.once.Do(initWindow)

	state.mu.Lock()
	defer state.mu.Unlock()

	savedLo, savedHi := state.lo, state.hi
	savedNext, savedErr := state.next, state.err
	savedTaken, savedRecent := state.taken, state.recent
	t.Cleanup(func() {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.lo, state.hi = savedLo, savedHi
		state.next, state.err = savedNext, savedErr
		state.taken, state.recent = savedTaken, savedRecent
	})

	state.lo, state.hi, state.next = lo, hi, lo
	state.taken = make(map[int]bool)
	state.recent = nil
	state.err = nil
}

// holdAllBut fills the installed window with live SO_REUSEADDR listeners,
// leaving at most free ports unheld, and reports how many it left. It stands in
// for the condition #131 was reported under: a loaded host where most of the
// window is already bound by other test binaries' nodes.
//
// The ports are discovered by binding rather than assumed free, so a port some
// unrelated process on the host already holds simply counts towards the held
// set instead of failing the test.
func holdAllBut(t *testing.T, lo, hi, free int) int {
	t.Helper()

	var held []net.PacketConn
	t.Cleanup(func() {
		for _, pc := range held {
			_ = pc.Close()
		}
	})

	var bindable []int
	for p := lo; p <= hi; p++ {
		pc, err := reuseAddrListen("127.0.0.1:" + strconv.Itoa(p))
		if err != nil {
			continue // already held by something outside this test
		}
		bindable = append(bindable, p)
		held = append(held, pc)
	}
	if len(bindable) <= free {
		t.Skipf("only %d of the %d-port window %d-%d is bindable on this host; "+
			"cannot leave %d free and saturate the rest",
			len(bindable), hi-lo+1, lo, hi, free)
	}

	// Release the last `free` of them, so exactly that many are reservable.
	for i := len(held) - free; i < len(held); i++ {
		if err := held[i].Close(); err != nil {
			t.Fatalf("release port %d: %v", bindable[i], err)
		}
	}
	held = held[:len(held)-free]
	return free
}

// TestSaturatedWindowFailsRatherThanReissuingAnInFlightPort covers the one
// piece of state machinery whose failure is #131 coming back.
//
// A window is finite and a long integration run allocates far more ports than
// it holds, so reservations must eventually be forgotten and the window reused.
// The reservation a caller is still holding must not be among them: a five-node
// cluster reserves five addresses and only then constructs five hosts, so a
// port handed out moments ago may not yet be bound. Reissuing that port hands
// two nodes one address — the defect, reached by a different route.
//
// Over a mostly-free window the scan cursor hides this: it advances past each
// port it hands out, so a port cannot come back until the cursor has lapped the
// whole span, and the retain horizon is never consulted. The horizon only
// carries weight when the window is SATURATED — most of it held by live hosts,
// which is the loaded runner #131 was reported on. There a lap is as short as
// the number of free ports, and the only thing standing between the caller and
// a duplicate is the record of what is still in flight.
//
// The contract in that state is to fail loudly. An address that two hosts share
// is sometimes a silent fault (see the package comment: two SO_REUSEADDR binds
// on one port both succeed and split each other's datagrams), so a test that
// cannot get a distinct port must be told, not quietly handed a used one.
func TestSaturatedWindowFailsRatherThanReissuingAnInFlightPort(t *testing.T) {
	// Small enough to saturate quickly, and free (8) well below the retain
	// horizon (span/2 = 32) so exhaustion is reached while every reservation
	// is still pinned as in flight.
	const (
		lo   = 61200
		hi   = 61263
		span = hi - lo + 1
		free = 8
	)
	withWindow(t, lo, hi)
	holdAllBut(t, lo, hi, free)

	retain := inFlightRetain
	if retain > span/2 {
		retain = span / 2
	}
	if free >= retain {
		t.Fatalf("test is misconfigured: %d free ports is not below the retain "+
			"horizon %d, so exhaustion would never be reached", free, retain)
	}

	seen := make(map[int]int)
	for i := range free {
		addr, err := ReserveUDP()
		if err != nil {
			t.Fatalf("reservation %d of %d free ports failed: %v", i, free, err)
		}
		p := portOf(t, addr)
		if prev, ok := seen[p]; ok {
			t.Fatalf("port %d was handed out at reservation %d and again at %d "+
				"while only %d ports are free and none has been bound yet; "+
				"two hosts would be given one address",
				p, prev, i, free)
		}
		seen[p] = i
	}

	// The window now holds nothing reservable: every free port is in flight and
	// every other port is held by a live listener.
	addr, err := ReserveUDP()
	if err == nil {
		t.Fatalf("reservation %d returned %s from a window whose %d free ports "+
			"have all been handed out and none bound; the in-flight record was "+
			"dropped and a port still in a caller's hands was reissued",
			free, addr, free)
	}
	var exhausted *WindowExhaustedError
	if !errors.As(err, &exhausted) {
		t.Fatalf("expected *WindowExhaustedError once the window is saturated, got %T: %v", err, err)
	}
	t.Logf("%d free ports in a %d-port window, all handed out distinctly, "+
		"then refused with: %v", free, span, err)
}

// TestFreeWindowSpreadsReservationsAcrossItsSpan pins the cursor's own
// contribution, which the saturated case deliberately holds still: over a
// window with room, successive reservations march across the whole span rather
// than circling a handful of ports at one end.
func TestFreeWindowSpreadsReservationsAcrossItsSpan(t *testing.T) {
	const (
		lo   = 61300
		hi   = 61363
		span = hi - lo + 1
	)
	withWindow(t, lo, hi)

	seen := make(map[int]bool)
	for i := range span {
		addr, err := ReserveUDP()
		if err != nil {
			t.Skipf("reservation %d in window %d-%d: %v "+
				"(too little of the window is free on this host)", i, lo, hi, err)
		}
		p := portOf(t, addr)
		if p < lo || p > hi {
			t.Fatalf("reservation %d returned port %d, outside the installed window %d-%d",
				i, p, lo, hi)
		}
		if seen[p] {
			t.Fatalf("port %d was handed out twice within a single lap of a "+
				"%d-port window", p, span)
		}
		seen[p] = true
	}
	t.Logf("%d distinct ports over one lap of a %d-port window", len(seen), span)
}
