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

//go:build linux

package seal

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
)

// vmLockedKB reports this process's currently mlock'd memory in kilobytes, from
// /proc/self/status. It is the kernel's own accounting rather than the library's,
// which is the point: a bookkeeping bug in either go-enclave or this package
// cannot hide from it.
func vmLockedKB(t *testing.T) int {
	t.Helper()
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Skipf("cannot read /proc/self/status: %v", err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "VmLck:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		kb, err := strconv.Atoi(fields[1])
		if err != nil {
			break
		}
		return kb
	}
	t.Skip("VmLck is not reported by this kernel")
	return 0
}

// TestInitializeAndUnseal_ReleaseTheirRootKeyEnclave is the mutation target for
// the borrow discipline this issue introduced.
//
// Initialize and Unseal each hold the root key in a locked enclave for the whole
// of their body, and each releases it with a deferred Destroy that REPLACES the
// eleven hand-written wipeBytes(rootKey) calls the old code carried on its error
// paths. The wipes were visible in the diff; a missing Destroy is not — it leaks
// silently, and every functional assertion in this package still passes.
//
// So the assertion is on the kernel's own accounting. Each enclave costs one
// locked PAGE (a 32-byte key is charged 4 KiB), so dropping either Destroy leaks
// ~4 KiB per cycle and the growth over the loop is unmistakable against the
// slack allowed below.
//
// Linux-only: VmLck is a Linux interface. That is the CI platform, and the test
// skips rather than failing anywhere it is unavailable.
func TestInitializeAndUnseal_ReleaseTheirRootKeyEnclave(t *testing.T) {
	const cycles = 60

	s := newPlainStrategy(t)
	defer s.Close()

	// Warm-up, OUTSIDE the measurement. memguard allocates its per-process
	// session coffer in locked memory on first use, and the Go runtime may lock
	// pages of its own; both are one-time and would otherwise read as a leak.
	warm := MustNewBarrier(testBarrierConfig(1))
	if err := warm.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("warm-up Initialize: %v", err)
	}
	warmBlob := warm.SealedRootKeyData()
	warm2 := MustNewBarrier(testBarrierConfig(1))
	if err := warm2.SetSealedRootKey(warmBlob); err != nil {
		t.Fatalf("warm-up SetSealedRootKey: %v", err)
	}
	if err := warm2.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("warm-up Unseal: %v", err)
	}

	before := vmLockedKB(t)

	// Barriers are retained for the whole loop deliberately. A barrier holds its
	// root key as a ProtectedKey — memguard ciphertext on the ordinary heap, not
	// locked memory — so holding them must NOT move VmLck. Letting them be
	// collected instead would let a GC cycle mask a real leak.
	kept := make([]*Barrier, 0, cycles*2)
	for i := 0; i < cycles; i++ {
		b := MustNewBarrier(testBarrierConfig(1))
		if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
			t.Fatalf("cycle %d Initialize: %v", i, err)
		}
		blob := b.SealedRootKeyData()

		b2 := MustNewBarrier(testBarrierConfig(1))
		if err := b2.SetSealedRootKey(blob); err != nil {
			t.Fatalf("cycle %d SetSealedRootKey: %v", i, err)
		}
		if err := b2.Unseal(context.Background(), s, Credentials{}); err != nil {
			t.Fatalf("cycle %d Unseal: %v", i, err)
		}
		kept = append(kept, b, b2)
	}

	after := vmLockedKB(t)
	growth := after - before

	// Slack, not zero: this measures the whole process, and an unrelated
	// allocation elsewhere in the test binary could add a page or two. A real
	// leak is 60 cycles x 4 KiB = 240 KiB per dropped Destroy, two orders of
	// magnitude above this.
	const slackKB = 32
	if growth > slackKB {
		t.Fatalf("locked memory grew %d kB across %d Initialize+Unseal cycles "+
			"(%d -> %d, slack %d kB): a root-key enclave is not being destroyed, "+
			"so the borrow's lifetime is no longer the wipe",
			growth, cycles, before, after, slackKB)
	}

	// Keep the barriers alive to here, so the loop above cannot be optimized or
	// collected out from under the measurement.
	if len(kept) != cycles*2 {
		t.Fatalf("kept %d barriers, want %d", len(kept), cycles*2)
	}
}
