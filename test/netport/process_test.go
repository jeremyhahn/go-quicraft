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
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The environment that turns a re-executed test binary into a reserver child,
// and the line protocol it speaks to its parent on stdout.
const (
	childLockDirEnv = "NETPORT_TEST_CHILD_LOCKDIR"
	childLoEnv      = "NETPORT_TEST_CHILD_LO"
	childHiEnv      = "NETPORT_TEST_CHILD_HI"

	childReady = "netport-child ready"
	childPort  = "netport-child port "
	childDone  = "netport-child done"
)

// crossProcessReservations is how many ports each child reserves. It stays
// within inFlightRetain, so every claim a child makes is still held when the
// parent compares the two sets.
const crossProcessReservations = 500

// TestReserveUDPNeverRepeatsAPortAcrossProcesses runs two reservers that share
// nothing but the lock directory, both scanning from the same port and neither
// binding what it reserves. That is the converged case #309 measured: without a
// cross-process claim the probe passes every port for both, and the two
// processes hand out identical sequences.
func TestReserveUDPNeverRepeatsAPortAcrossProcesses(t *testing.T) {
	if dir := os.Getenv(childLockDirEnv); dir != "" {
		runReserverChild(t, dir)
		return
	}
	if !crossProcessClaims {
		t.Skip("no cross-process port claim on this platform")
	}

	state.once.Do(initWindow)
	state.mu.Lock()
	lo, hi, err := state.lo, state.hi, state.err
	state.mu.Unlock()
	if err != nil {
		t.Skipf("no reservation window on this host: %v", err)
	}
	const span = 4 * crossProcessReservations
	if hi-lo+1 < span {
		t.Skipf("window %d-%d is narrower than the %d ports this test needs", lo, hi, span)
	}
	wlo, whi := hi-span+1, hi

	dir := t.TempDir()
	children := []*reserverChild{
		startReserverChild(t, dir, wlo, whi),
		startReserverChild(t, dir, wlo, whi),
	}
	for _, c := range children {
		c.await(t, c.ready, "ready")
	}
	// Release both at once, so the scans run concurrently.
	for _, c := range children {
		if _, err := io.WriteString(c.stdin, "go\n"); err != nil {
			t.Fatalf("start child: %v", err)
		}
	}
	for _, c := range children {
		c.await(t, c.done, "done")
	}
	// Both have finished reserving and still hold their claims; let them exit.
	for _, c := range children {
		c.finish(t)
	}

	owner := make(map[string]int)
	for i, c := range children {
		if len(c.ports) != crossProcessReservations {
			t.Fatalf("child %d reserved %d ports; want %d\n%s",
				i, len(c.ports), crossProcessReservations, c.log())
		}
		for _, addr := range c.ports {
			if j, dup := owner[addr]; dup {
				t.Errorf("%s was reserved by child %d and by child %d", addr, j, i)
				continue
			}
			owner[addr] = i
		}
	}
}

// runReserverChild is the child half: reserve from the given window and lock
// directory, starting at its bottom, report every address, then hold the
// claims until the parent closes stdin.
func runReserverChild(t *testing.T, dir string) {
	lo, err := strconv.Atoi(os.Getenv(childLoEnv))
	if err != nil {
		t.Fatalf("child window lo: %v", err)
	}
	hi, err := strconv.Atoi(os.Getenv(childHiEnv))
	if err != nil {
		t.Fatalf("child window hi: %v", err)
	}

	state.once.Do(initWindow)
	state.mu.Lock()
	state.lo, state.hi, state.next = lo, hi, lo
	state.taken = make(map[int]bool)
	state.recent = nil
	state.err = nil
	state.lockDir = dir
	state.mu.Unlock()

	in := bufio.NewReader(os.Stdin)
	fmt.Println(childReady)
	if _, err := in.ReadString('\n'); err != nil {
		t.Fatalf("wait for start: %v", err)
	}
	for i := range crossProcessReservations {
		addr, err := ReserveUDP()
		if err != nil {
			t.Fatalf("reservation %d: %v", i+1, err)
		}
		fmt.Println(childPort + addr)
	}
	fmt.Println(childDone)
	_, _ = io.Copy(io.Discard, in)
}

// reserverChild is the parent's handle on one child process.
type reserverChild struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	ready  chan struct{}
	done   chan struct{}
	eof    chan struct{}
	ports  []string
	output []string
}

func startReserverChild(t *testing.T, dir string, lo, hi int) *reserverChild {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0],
		"-test.run=^TestReserveUDPNeverRepeatsAPortAcrossProcesses$", "-test.count=1")
	cmd.Env = append(os.Environ(),
		childLockDirEnv+"="+dir,
		childLoEnv+"="+strconv.Itoa(lo),
		childHiEnv+"="+strconv.Itoa(hi))
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("child stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("child stdout: %v", err)
	}
	cmd.Stderr = cmd.Stdout // the pipe's write end, so a failure report is kept
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	c := &reserverChild{
		cmd:   cmd,
		stdin: stdin,
		ready: make(chan struct{}),
		done:  make(chan struct{}),
		eof:   make(chan struct{}),
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-c.eof
		_ = cmd.Wait()
	})
	go c.read(stdout)
	return c
}

// read consumes the child's stdout until EOF. Nothing else touches ports or
// output until eof is closed.
func (c *reserverChild) read(stdout io.Reader) {
	defer close(c.eof)
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == childReady:
			close(c.ready)
		case line == childDone:
			close(c.done)
		case strings.HasPrefix(line, childPort):
			c.ports = append(c.ports, strings.TrimPrefix(line, childPort))
		default:
			c.output = append(c.output, line)
		}
	}
}

// await waits for the child to reach a step, failing with its output if it
// exits or stalls first.
func (c *reserverChild) await(t *testing.T, step chan struct{}, name string) {
	t.Helper()
	select {
	case <-step:
	case <-c.eof:
		t.Fatalf("child exited before %s:\n%s", name, c.log())
	case <-time.After(time.Minute):
		t.Fatalf("child did not reach %s within a minute", name)
	}
}

// finish closes the child's stdin, reads it to EOF and checks it passed.
func (c *reserverChild) finish(t *testing.T) {
	t.Helper()
	_ = c.stdin.Close()
	<-c.eof
	if err := c.cmd.Wait(); err != nil {
		t.Fatalf("child: %v\n%s", err, c.log())
	}
}

func (c *reserverChild) log() string {
	return strings.Join(c.output, "\n")
}
