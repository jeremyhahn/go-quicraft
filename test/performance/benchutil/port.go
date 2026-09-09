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

import "github.com/jeremyhahn/go-quicraft/test/netport"

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
// range.
//
// # Why this file is now four lines
//
// The fix -- allocate from a window OUTSIDE the kernel's ephemeral range, and
// remember what has been handed out -- is identical to the one the integration
// suites needed for UDP, and this file used to be a second, TCP-shaped copy of
// it: the same window derivation, the same two-pass scan, the same recycle
// policy, the same PID start offset, ~200 lines of it. Two copies of a
// guarantee are two things to keep in step, and the guarantee is then only as
// strong as whichever copy a caller reached for.
//
// test/netport is the one implementation (#163). It grew ReserveTCP for these
// harnesses; everything else about the reservation -- window, taken set,
// cursor, recycling -- is shared, and its package comment carries the full
// reasoning and the measurements.

// ReserveTCPPort returns a loopback address that no other caller in this
// process has been given, that no live listener holds, and that the kernel will
// not assign on its own.
func ReserveTCPPort() (string, error) {
	return netport.ReserveTCP()
}

// MustReserveTCPPort is ReserveTCPPort for callers that cannot proceed without
// a port. It reports the failure through fatalf, which is expected to be a
// testing (*B).Fatalf or equivalent, and returns the empty string if fatalf
// returns.
func MustReserveTCPPort(fatalf func(format string, args ...any)) string {
	return netport.MustReserveTCP(fatalf)
}
