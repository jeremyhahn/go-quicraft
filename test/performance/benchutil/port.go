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

// TCP port reservation for harnesses that need an address before the server exists, delegated to test/netport, which allocates outside the ephemeral range.

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
