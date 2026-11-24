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

// Package netport reserves loopback ports outside the kernel's ip_local_port_range, recorded in-process and claimed across processes by flock under <os.TempDir()>/quicraft-netport, so nothing else is handed them before the host binds.
package netport

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// ephemeralRangeFile is the sysctl exposing the kernel's ephemeral port range:
// the pool that bind(:0) and outbound connect() both draw from, and the pool a
// reservation must therefore avoid.
const ephemeralRangeFile = "/proc/sys/net/ipv4/ip_local_port_range"

// defaultEphemeralLo and defaultEphemeralHi are the Linux defaults, used when
// the sysctl cannot be read.
const (
	defaultEphemeralLo = 32768
	defaultEphemeralHi = 60999
)

// inFlightRetain is how many recent reservations stay pinned when the window is recycled.
const inFlightRetain = 512

// probeConfig binds the candidate port. It is deliberately the zero value: see
// probe for why it carries no socket options.
var probeConfig net.ListenConfig

// state is the process-wide reservation window and the ports already handed
// out from it.
var state struct {
	once   sync.Once
	mu     sync.Mutex
	lo     int
	hi     int
	next   int
	err    error
	taken  map[int]bool
	recent []int

	// lockDir holds the per-port lock files that claim a port across
	// processes; empty disables the claim. held maps each port this process
	// has claimed to the function that drops the claim, and heldOrder lists
	// them oldest first so only the most recent inFlightRetain stay held.
	lockDir   string
	held      map[int]func()
	heldOrder []int
}

// ReserveUDP returns a 127.0.0.1 address no caller in this process holds, nothing is bound to, and the kernel will not assign; bind it soon.
func ReserveUDP() (string, error) {
	return reserve("udp")
}

// ReserveTCP is ReserveUDP for a TCP listener, used by the test/performance baselines.
func ReserveTCP() (string, error) {
	return reserve("tcp")
}

// reserve hands out one port from the shared TCP/UDP window, proven bindable on network.
func reserve(network string) (string, error) {
	state.once.Do(initWindow)

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.err != nil {
		return "", state.err
	}

	// Two passes. The first honors every port handed out so far; the second
	// forgets all but the most recent, so the window can be recycled.
	if addr, ok := scanWindow(network); ok {
		return addr, nil
	}
	recycleWindow()
	if addr, ok := scanWindow(network); ok {
		return addr, nil
	}

	return "", &WindowExhaustedError{Lo: state.lo, Hi: state.hi}
}

// MustReserveUDP is ReserveUDP for callers that cannot proceed without an
// address. It reports failure through fatalf, which is expected to be a testing
// (*T).Fatalf or equivalent, and returns the empty string if fatalf returns.
func MustReserveUDP(fatalf func(format string, args ...any)) string {
	addr, err := ReserveUDP()
	if err != nil {
		fatalf("reserve loopback UDP port: %v", err)
		return ""
	}
	return addr
}

// MustReserveTCP is ReserveTCP for callers that cannot proceed without an
// address, on the same terms as MustReserveUDP. A benchmark passes (*B).Fatalf.
func MustReserveTCP(fatalf func(format string, args ...any)) string {
	addr, err := ReserveTCP()
	if err != nil {
		fatalf("reserve loopback TCP port: %v", err)
		return ""
	}
	return addr
}

// scanWindow walks the whole window once, starting where the last reservation
// left off, and returns the first port that is neither already handed out nor
// held by a listener in this or any other process on network. The caller must
// hold state.mu.
func scanWindow(network string) (string, bool) {
	span := state.hi - state.lo + 1
	if span <= 0 {
		return "", false
	}
	offset := state.next - state.lo
	for i := 0; i < span; i++ {
		p := state.lo + (offset+i)%span
		if state.taken[p] {
			continue
		}
		if !claim(p) {
			// Reserved by another process that has not bound it yet. Skip it.
			continue
		}
		addr := "127.0.0.1:" + strconv.Itoa(p)
		if !probe(network, addr) {
			// Held by something outside this process. Skip it.
			unclaim(p)
			continue
		}
		state.taken[p] = true
		state.recent = append(state.recent, p)
		state.next = state.lo + (p-state.lo+1)%span
		trimClaims()
		return addr, true
	}
	return "", false
}

// claim takes this process's cross-process claim on port p, and reports
// whether p is free of any other process's claim. A claim this process already
// holds is kept, so a port recycled within the process is not refused by its
// own lock.
func claim(p int) bool {
	if state.lockDir == "" {
		return true
	}
	if _, ok := state.held[p]; ok {
		return true
	}
	unlock, busy := lockPort(filepath.Join(state.lockDir, strconv.Itoa(p)+".lock"))
	if busy {
		return false
	}
	if unlock != nil {
		state.held[p] = unlock
		state.heldOrder = append(state.heldOrder, p)
	}
	return true
}

// unclaim drops this process's claim on port p, if it holds one.
func unclaim(p int) {
	unlock, ok := state.held[p]
	if !ok {
		return
	}
	unlock()
	delete(state.held, p)
	for i, q := range state.heldOrder {
		if q == p {
			state.heldOrder = append(state.heldOrder[:i], state.heldOrder[i+1:]...)
			break
		}
	}
}

// trimClaims drops claims older than the most recent inFlightRetain.
func trimClaims() {
	for len(state.heldOrder) > inFlightRetain {
		p := state.heldOrder[0]
		state.heldOrder = state.heldOrder[1:]
		if unlock, ok := state.held[p]; ok {
			unlock()
			delete(state.held, p)
		}
	}
}

// probe reports whether addr can be bound on network now, with no socket options; a failed Close counts as not free.
func probe(network, addr string) bool {
	if network == "udp" {
		pc, err := probeConfig.ListenPacket(context.Background(), network, addr)
		if err != nil {
			return false
		}
		return pc.Close() == nil
	}
	l, err := probeConfig.Listen(context.Background(), network, addr)
	if err != nil {
		return false
	}
	return l.Close() == nil
}

// recycleWindow forgets every reservation except the most recent ones, which
// may still be in flight between reservation and bind. The caller must hold
// state.mu.
func recycleWindow() {
	retain := inFlightRetain
	if span := state.hi - state.lo + 1; retain > span/2 {
		retain = span / 2
	}

	keep := state.recent
	if len(keep) > retain {
		keep = keep[len(keep)-retain:]
	}

	state.taken = make(map[int]bool, len(keep))
	for _, p := range keep {
		state.taken[p] = true
	}
	state.recent = append(state.recent[:0:0], keep...)
}

// initWindow resolves the reservation window once per process.
func initWindow() {
	state.taken = make(map[int]bool)
	state.held = make(map[int]func())
	state.lockDir = lockDirectory()

	lo, hi := defaultEphemeralLo, defaultEphemeralHi
	if data, err := os.ReadFile(ephemeralRangeFile); err == nil {
		if l, h, perr := parsePortRange(string(data)); perr == nil {
			lo, hi = l, h
		}
	}

	wlo, whi, err := window(lo, hi)
	if err != nil {
		state.err = err
		return
	}
	state.lo, state.hi = wlo, whi
	state.next = wlo + startOffset(os.Getpid(), whi-wlo+1)
}

// lockDirectory returns the directory every process on the host shares for
// its per-port lock files, or "" when the platform has no cross-process claim
// or the directory cannot be created, leaving the probe as the only guard.
func lockDirectory() string {
	if !crossProcessClaims {
		return ""
	}
	dir := filepath.Join(os.TempDir(), "quicraft-netport")
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return ""
	}
	return dir
}

// startOffset seeds each process's window start from its PID.
func startOffset(pid, span int) int {
	if span <= 0 {
		return 0
	}
	// A small multiplicative scramble, so PIDs allocated close together (which
	// is what a test runner does) start far apart rather than adjacent.
	// Typed and multiplied in uint64 so the constant fits on a 32-bit int
	// platform and the product cannot go negative.
	const scramble uint64 = 2654435761
	return int((uint64(pid) * scramble) % uint64(span))
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

// window returns a port window disjoint from the kernel's ephemeral range
// [ephemeralLo, ephemeralHi]. It prefers the space above the range, which on a
// stock Linux is 61000-65535, and falls back to the registered-port space
// below it.
func window(ephemeralLo, ephemeralHi int) (lo, hi int, err error) {
	if ephemeralHi < 65535 {
		return ephemeralHi + 1, 65535, nil
	}
	if ephemeralLo > 1024 {
		return 1024, ephemeralLo - 1, nil
	}
	return 0, 0, &NoWindowError{EphemeralLo: ephemeralLo, EphemeralHi: ephemeralHi}
}

// WindowExhaustedError reports that every port in the reservation window is
// either already handed out or held by another process.
type WindowExhaustedError struct {
	Lo int
	Hi int
}

// Error implements the error interface.
func (e *WindowExhaustedError) Error() string {
	return "no free port in the loopback reservation window " +
		strconv.Itoa(e.Lo) + "-" + strconv.Itoa(e.Hi) +
		"; every port is held by a listener or already reserved by this process"
}

// NoWindowError reports that the kernel's ephemeral range leaves no room for a
// reservation window on either side of it.
type NoWindowError struct {
	EphemeralLo int
	EphemeralHi int
}

// Error implements the error interface.
func (e *NoWindowError) Error() string {
	return "ephemeral port range " + strconv.Itoa(e.EphemeralLo) + "-" +
		strconv.Itoa(e.EphemeralHi) + " leaves no room for a loopback port " +
		"reservation window; narrow net.ipv4.ip_local_port_range so ports " +
		"remain free above 1024 or below 65535"
}

// MalformedPortRangeError reports an unparseable ip_local_port_range value.
type MalformedPortRangeError struct {
	Value string
}

// Error implements the error interface.
func (e *MalformedPortRangeError) Error() string {
	return "malformed " + ephemeralRangeFile + " value " + strconv.Quote(e.Value)
}
