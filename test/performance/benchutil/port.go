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
	"strings"
	"sync"
)

// Loopback TCP port reservation for engine harnesses whose transport cannot
// bind port 0.
//
// Dragonboat and etcd/raft both need a concrete RaftAddress before the server
// exists, so the harness has to pick the port itself. The obvious way -- listen
// on 127.0.0.1:0, read the assigned port, close the listener, hand the number
// to the engine -- draws that number from the kernel's ephemeral port range:
// exactly the pool the kernel also draws from when it assigns a local port to
// an OUTBOUND connection. Between the close and the engine's bind, any outbound
// connection opened by a transport still running from the previous benchmark
// can be handed that port, and the engine's bind then fails with EADDRINUSE.
//
// That is not hypothetical. It is what broke the five-node dragonboat
// scenarios:
//
//	BenchmarkProposeThroughput_5N_Dragonboat
//	    NewNodeHost[3] failed: listen tcp 127.0.0.1:41461: bind: address already in use
//
// with the same log showing the transport's own outbound sockets sitting on
// 127.0.0.1:39458 and 127.0.0.1:38009 -- neighbours of 41461 in the very same
// range. The five-node scenarios lose the race and the one- and three-node
// scenarios do not because the reservation window scales with node count (five
// ports reserved up front, then five NodeHost startups before the last one
// binds) while the density of competing outbound sockets scales with the square
// of it (a five-node mesh opens twenty connections, a three-node mesh six).
//
// ReserveTCPPort removes the race rather than narrowing it: it allocates from a
// window OUTSIDE the kernel's ephemeral range, which the kernel will never
// auto-assign to anything, and remembers what it has handed out so two
// reservations in the same process can never collide either.

// ephemeralRangeFile is the sysctl exposing the kernel's ephemeral port range,
// the pool bind(:0) and outbound connect() both draw from.
const ephemeralRangeFile = "/proc/sys/net/ipv4/ip_local_port_range"

// defaultEphemeralLo and defaultEphemeralHi are the Linux defaults, used when
// the sysctl cannot be read (a non-Linux developer machine, say).
const (
	defaultEphemeralLo = 32768
	defaultEphemeralHi = 60999
)

// portState guards the reservation window and the set of ports already handed
// out for the lifetime of the process.
var portState struct {
	once   sync.Once
	mu     sync.Mutex
	lo     int
	hi     int
	next   int
	err    error
	taken  map[int]bool
	recent []int
}

// recentPortRetain is the number of most-recent reservations kept pinned when
// the window is recycled. A caller may hold a reservation for a moment before
// binding it -- a five-node cluster reserves all five ports, then starts five
// servers -- and those must not be handed out again in the meantime. Anything
// older than this has long since been bound or abandoned.
const recentPortRetain = 256

// ReserveTCPPort returns a loopback address ("127.0.0.1:port") that no other
// caller in this process has been given and that the kernel will not assign to
// an outbound connection, so the caller may bind it later without racing.
//
// The returned port is confirmed bindable at reservation time. The caller is
// expected to bind it itself shortly afterwards; ReserveTCPPort does not hold
// the socket open, because the engines being benchmarked bind by address, not
// by inherited file descriptor.
func ReserveTCPPort() (string, error) {
	portState.once.Do(initPortWindow)

	portState.mu.Lock()
	defer portState.mu.Unlock()

	if portState.err != nil {
		return "", portState.err
	}

	// Two passes. The first honors every port handed out so far; the second
	// forgets all but the most recent, so ports can be recycled. Recycling is
	// necessary because a run allocates far more ports than the window holds --
	// the election benchmarks build and tear down a whole cluster per iteration
	// -- and it is safe here in a way it never was in the ephemeral range: the
	// probe bind proves the port is genuinely free, and no kernel-assigned
	// outbound socket can be given it between the probe and the engine's bind.
	if addr, ok := scanPortWindow(); ok {
		return addr, nil
	}
	recyclePortWindow()
	if addr, ok := scanPortWindow(); ok {
		return addr, nil
	}

	return "", &PortWindowExhaustedError{Lo: portState.lo, Hi: portState.hi}
}

// scanPortWindow walks the whole window once, starting where the last
// reservation left off, and returns the first port that is neither already
// handed out nor occupied by another process. The caller must hold portState.mu.
func scanPortWindow() (string, bool) {
	span := portState.hi - portState.lo + 1
	if span <= 0 {
		return "", false
	}
	offset := portState.next - portState.lo
	for i := 0; i < span; i++ {
		p := portState.lo + (offset+i)%span
		if portState.taken[p] {
			continue
		}
		addr := "127.0.0.1:" + strconv.Itoa(p)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			// Occupied by something outside this process. Skip it.
			continue
		}
		l.Close()
		portState.taken[p] = true
		portState.recent = append(portState.recent, p)
		portState.next = portState.lo + (p-portState.lo+1)%span
		return addr, true
	}
	return "", false
}

// recyclePortWindow forgets every reservation except the most recent ones,
// which may still be in flight between reservation and bind. The caller must
// hold portState.mu.
func recyclePortWindow() {
	retain := recentPortRetain
	if span := portState.hi - portState.lo + 1; retain > span/2 {
		retain = span / 2
	}

	keep := portState.recent
	if len(keep) > retain {
		keep = keep[len(keep)-retain:]
	}

	portState.taken = make(map[int]bool, len(keep))
	for _, p := range keep {
		portState.taken[p] = true
	}
	portState.recent = append(portState.recent[:0:0], keep...)
}

// MustReserveTCPPort is ReserveTCPPort for callers that cannot proceed without
// a port. It reports the failure through fatalf, which is expected to be a
// testing (*B).Fatalf or equivalent, and returns the empty string if fatalf
// returns.
func MustReserveTCPPort(fatalf func(format string, args ...any)) string {
	addr, err := ReserveTCPPort()
	if err != nil {
		fatalf("reserve loopback TCP port: %v", err)
		return ""
	}
	return addr
}

// PortWindowExhaustedError reports that every port in the reservation window is
// either already handed out or occupied by another process.
type PortWindowExhaustedError struct {
	Lo int
	Hi int
}

// Error implements the error interface.
func (e *PortWindowExhaustedError) Error() string {
	return "no free port in the benchmark reservation window " +
		strconv.Itoa(e.Lo) + "-" + strconv.Itoa(e.Hi) +
		"; every port is occupied or already reserved by this run"
}

// NoPortWindowError reports that the kernel's ephemeral range leaves no room
// for a reservation window on either side of it.
type NoPortWindowError struct {
	EphemeralLo int
	EphemeralHi int
}

// Error implements the error interface.
func (e *NoPortWindowError) Error() string {
	return "ephemeral port range " + strconv.Itoa(e.EphemeralLo) + "-" +
		strconv.Itoa(e.EphemeralHi) + " leaves no room for benchmark port " +
		"reservation; narrow it via net.ipv4.ip_local_port_range so ports " +
		"remain free above 1024 or below 65535"
}

// initPortWindow resolves the reservation window once per process.
func initPortWindow() {
	portState.taken = make(map[int]bool)

	lo, hi := defaultEphemeralLo, defaultEphemeralHi
	if data, err := os.ReadFile(ephemeralRangeFile); err == nil {
		if l, h, perr := parsePortRange(string(data)); perr == nil {
			lo, hi = l, h
		}
	}

	wlo, whi, err := reservationWindow(lo, hi)
	if err != nil {
		portState.err = err
		return
	}
	portState.lo, portState.hi, portState.next = wlo, whi, wlo
}

// parsePortRange parses the two whitespace-separated values of
// net.ipv4.ip_local_port_range.
func parsePortRange(s string) (lo, hi int, err error) {
	fields := strings.Fields(s)
	if len(fields) != 2 {
		return 0, 0, &MalformedPortRangeError{Value: strings.TrimSpace(s)}
	}
	lo, err = strconv.Atoi(fields[0])
	if err != nil {
		return 0, 0, &MalformedPortRangeError{Value: strings.TrimSpace(s)}
	}
	hi, err = strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, &MalformedPortRangeError{Value: strings.TrimSpace(s)}
	}
	if lo < 1 || hi > 65535 || lo > hi {
		return 0, 0, &MalformedPortRangeError{Value: strings.TrimSpace(s)}
	}
	return lo, hi, nil
}

// MalformedPortRangeError reports an unparseable ip_local_port_range value.
type MalformedPortRangeError struct {
	Value string
}

// Error implements the error interface.
func (e *MalformedPortRangeError) Error() string {
	return "malformed " + ephemeralRangeFile + " value " + strconv.Quote(e.Value)
}

// reservationWindow returns a port window disjoint from the kernel's ephemeral
// range [ephemeralLo, ephemeralHi]. It prefers the space above the range, which
// on a stock Linux is 61000-65535, and falls back to the registered-port space
// below it.
func reservationWindow(ephemeralLo, ephemeralHi int) (lo, hi int, err error) {
	if ephemeralHi < 65535 {
		return ephemeralHi + 1, 65535, nil
	}
	if ephemeralLo > 1024 {
		return 1024, ephemeralLo - 1, nil
	}
	return 0, 0, &NoPortWindowError{EphemeralLo: ephemeralLo, EphemeralHi: ephemeralHi}
}
