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

// Package netport reserves loopback UDP ports for tests that must name a
// listen address before the host that binds it exists.
//
// # The race this exists to remove
//
// The obvious way to spell the reservation is:
//
//	pc, _ := net.ListenPacket("udp", "127.0.0.1:0")
//	addr := pc.LocalAddr().String()
//	pc.Close()
//	return addr
//
// The port is free at the instant it is read and nothing holds it afterwards,
// so between the Close and the host's later bind the kernel may hand the same
// number to somebody else. "Somebody else" is not exotic: 127.0.0.1:0 draws
// from the kernel's ephemeral range,
//
//	$ cat /proc/sys/net/ipv4/ip_local_port_range
//	32768	60999
//
// which is the identical pool the kernel draws from for the local port of an
// OUTBOUND connection, and the identical pool the NEXT call to this helper
// draws from. The second of those is the common failure: a four-node cluster
// asks for four ports and is handed the same one twice, and the engine's
// membership validation refuses the shard before the test has asserted
// anything.
//
//	lease_read_test.go:716: StartShard[0] shard=1 failed: quicraft: invalid
//	member in shard 1: replica 2 address "127.0.0.1:36761": address is already
//	assigned to replica 1
//
// # Why a duplicate address is worse than that failure suggests
//
// The engine caught it only because both replicas were members of one shard.
// Two hosts in different shards, or in different packages of a parallel `go
// test` run, are not compared to each other by anything. They would both bind
// successfully and silently split each other's datagrams, because the QUIC
// transport sets SO_REUSEADDR (pkg/internal/transport/quic.go, Start) and Linux
// lets two SO_REUSEADDR UDP sockets share one address, delivering to whichever
// bound last. Measured on the CI host:
//
//	two SO_REUSEADDR binds on 127.0.0.1:39651 BOTH SUCCEEDED
//	datagram delivered to: second
//
// A duplicate port is therefore not reliably a loud failure. It is sometimes a
// silent one, which is why narrowing the window was not an acceptable fix.
//
// # Measured
//
// Modelling newTestCluster — reserve five addresses, then bind all five with
// SO_REUSEADDR — against 24 goroutines churning the ephemeral pool, 360,000
// reservations each way on a 24-core host:
//
//	old freePort : 42 duplicate addresses within a cluster, 150 EADDRINUSE binds
//	netport      :  0                                   ,   0
//
// That is roughly one duplicate per 6,900 adjacent reservations, which is why
// it reads as an occasional CI failure rather than a reproducible one: a full
// suite makes a few thousand adjacent pairs, so it loses this coin every few
// runs. The 150 failed binds are the same race caught one step later, and are
// the EADDRINUSE documented in test/performance/benchutil.
//
// Reservation costs 10-20us at suite volume (52ms for 5,000), so the window
// scan is not a cost the suite notices.
//
// # The fix
//
// Reserve from a window OUTSIDE ip_local_port_range. The kernel never
// auto-assigns a port from outside that range — not to a bind(:0), not to an
// outbound connect() — so nothing but this package can be handed a port it has
// handed out, and the interval between reservation and bind stops mattering.
//
// The same reasoning places dummy member addresses on a distinct loopback host
// rather than on a higher port, so they cannot collide with live nodes: "high
// enough" is a guess that a differently-configured runner invalidates while a
// space the kernel cannot choose from is not. The window here is derived from
// the running kernel's own sysctl for the same reason.
//
// Three collision sources, and what happens to each:
//
//   - Kernel re-assignment between reservation and bind — the duplicate-port
//     failure and the EADDRINUSE documented in test/performance/benchutil. REMOVED: the
//     window is disjoint from the pool the kernel assigns from.
//   - Two reservations in one process — the case a cluster hits. REMOVED:
//     a port handed out is recorded, and never handed out again while it may
//     still be in flight.
//   - Two reservations in different processes, which `go test ./...` creates by
//     running packages in parallel. NARROWED, not removed, and deliberately so:
//     each process starts its scan at a PID-derived offset rather than at the
//     bottom of the window, so concurrent packages do not march over each
//     other from the same end, and every candidate is probe-bound with a plain
//     socket first, which does detect a live host another process has already
//     started (a plain bind is refused by an SO_REUSEADDR holder; measured).
//     What remains is the ordinary interval between one process's probe and
//     its own bind.
//
// Removing that last one needs the transport to accept a pre-bound
// net.PacketConn, so the socket the test holds is the socket the host uses.
// quic-go supports exactly that (quic.Transport{Conn: ...}), but
// QUICTransport.Start binds from Config.ListenAddress, a string, and no
// PacketConn appears anywhere in pkg/. Adding one is a change to the engine's
// public configuration, which is outside this package's scope.
package netport

import (
	"context"
	"net"
	"os"
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

// inFlightRetain is how many of the most recent reservations stay pinned when
// the window is recycled.
//
// A caller may hold a reservation for a moment before binding it — a five-node
// cluster reserves five addresses, then constructs five hosts — and those must
// not be handed out again in the meantime. A long integration run allocates far
// more ports than the window holds, so recycling is necessary; anything older
// than this many reservations has long since been bound or abandoned.
const inFlightRetain = 512

// probeConfig binds the candidate port. It is deliberately the zero value: see
// probe for why it must carry no socket options.
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
}

// ReserveUDP returns a loopback address ("127.0.0.1:port") that no other caller
// in this process has been given, that no live listener currently holds, and
// that the kernel will not assign to anything on its own. The caller is
// expected to bind it shortly afterwards.
//
// The socket used to prove the port bindable is not kept open, because the
// engines these tests drive bind by address and cannot inherit a descriptor.
// The window makes that safe: see the package comment.
func ReserveUDP() (string, error) {
	return reserve("udp")
}

// ReserveTCP is ReserveUDP for a TCP listener. Everything the package comment
// says applies unchanged: the class it removes is a port drawn from the
// kernel's ephemeral range being reassigned between the probe and the bind, and
// nothing about that is specific to UDP. It is here rather than in a second
// copy of this package because a copy is how the two drift — the window, the
// recycle policy and the PID offset all have to agree or the guarantee is only
// as good as whichever copy a caller reached for.
//
// This engine speaks QUIC and reserves UDP; the TCP form exists for the
// comparison harnesses in test/performance, whose baselines (etcd/raft,
// Dragonboat) are TCP and which hit the identical EADDRINUSE class.
func ReserveTCP() (string, error) {
	return reserve("tcp")
}

// reserve hands out one port from the window, proven bindable on network.
//
// # One window for both networks
//
// TCP and UDP port spaces are independent in the kernel, so 40000/tcp and
// 40000/udp could safely be issued to two callers. They are not: one window,
// one taken set, one cursor. Sharing is strictly MORE conservative than the
// kernel requires, costs nothing a test can observe — the window is thousands
// of ports wide and a process reserves tens — and it is what keeps the recycle
// policy and the PID start offset single. Two sets would be two policies to
// keep in step, which is the defect this package was folded together to end.
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
		addr := "127.0.0.1:" + strconv.Itoa(p)
		if !probe(network, addr) {
			// Held by something outside this process. Skip it.
			continue
		}
		state.taken[p] = true
		state.recent = append(state.recent, p)
		state.next = state.lo + (p-state.lo+1)%span
		return addr, true
	}
	return "", false
}

// probe reports whether addr can be bound on network right now. It opens the
// socket and closes it again; the caller gets the address, not the descriptor.
//
// A ZERO ListenConfig, deliberately: no Control hook, so the probe carries no
// socket options of its own and is refused by a live holder, which is how a
// host another process already started is detected. A UDP probe that set
// SO_REUSEADDR itself would bind straight through such a host and report the
// port free. TCP needs no such care in the other direction — Go sets
// SO_REUSEADDR on a TCP listener by default, and SO_REUSEADDR permits binding
// over a socket in TIME_WAIT, never over a LIVE listener; two live listeners on
// one address need SO_REUSEPORT, which nothing here sets.
//
// A failed Close is treated as "not free" rather than ignored: the port's state
// is then unknown, and handing out an address whose probe socket may still be
// open is exactly the race this package exists to remove.
//
// (net.ListenPacket and net.Listen would be the same binds, but noctx requires
// the ListenConfig spelling.)
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

// startOffset spreads concurrently running test binaries across the window.
//
// `go test ./...` runs packages in parallel, so several processes reserve from
// this window at once. If they all began at the bottom they would probe the
// same ports in the same order and differ only by who won each probe, which is
// the collision this package exists to avoid. Seeding from the PID is enough:
// it is distinct for every live process, needs no shared state, and the probe
// bind still catches the case where two offsets converge.
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
