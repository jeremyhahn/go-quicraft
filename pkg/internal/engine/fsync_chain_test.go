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

package engine

import (
	"errors"
	"reflect"
	"testing"
)

// TestSyncDirChainWith_WalksToEveryRoot is the regression test for
// SyncDirChainWith silently degrading to a LEAF-ONLY fsync.
//
// Deciding containment with a string prefix over leaf+sep and stopAt+sep
// breaks for exactly the two values deepestExistingAncestor
// returns when nothing above the leaf pre-exists: stopAt "/" builds the prefix
// "//" and stopAt "." builds "./", and neither prefixes a CLEANED leaf. Both
// fell through to syncing the leaf alone, skipping every ancestor os.MkdirAll
// had just created — so a crash could roll back the directory entries of the
// tree a snapshot or WAL segment had just been fsynced into. It is reachable
// with a RELATIVE HostConfig.NodeHostDir (permitted: only DataDir must be
// absolute) and with an absolute path whose entire tree is new.
//
// The `stopAt == ""` test also has to run BEFORE Clean, because
// filepath.Clean("") is ".", which is a legitimate stop point (the working
// directory) rather than "no stop point at all".
func TestSyncDirChainWith_WalksToEveryRoot(t *testing.T) {
	cases := []struct {
		name   string
		leaf   string
		stopAt string
		want   []string
	}{
		{
			name:   "absolute filesystem root",
			leaf:   "/a/b/c",
			stopAt: "/",
			want:   []string{"/a/b/c", "/a/b", "/a", "/"},
		},
		{
			name:   "relative working directory",
			leaf:   "a/b/c",
			stopAt: ".",
			want:   []string{"a/b/c", "a/b", "a", "."},
		},
		{
			name:   "empty stopAt means no stop point: leaf only",
			leaf:   "/a/b/c",
			stopAt: "",
			want:   []string{"/a/b/c"},
		},
		{
			name:   "stopAt equal to leaf syncs just it",
			leaf:   "/a/b/c",
			stopAt: "/a/b/c",
			want:   []string{"/a/b/c"},
		},
		{
			name:   "ordinary nested stop point",
			leaf:   "/a/b/c",
			stopAt: "/a",
			want:   []string{"/a/b/c", "/a/b", "/a"},
		},
		{
			name:   "leaf not under stopAt: leaf only",
			leaf:   "/a/b",
			stopAt: "/x",
			want:   []string{"/a/b"},
		},
		{
			name:   "sibling prefix must not count as containment",
			leaf:   "/ab/c",
			stopAt: "/a",
			want:   []string{"/ab/c"},
		},
		{
			name:   "uncleaned inputs are normalised",
			leaf:   "/a//b/c/",
			stopAt: "/a/",
			want:   []string{"/a/b/c", "/a/b", "/a"},
		},
		{
			name:   "relative leaf with absolute stopAt is not comparable: leaf only",
			leaf:   "a/b",
			stopAt: "/x",
			want:   []string{"a/b"},
		},
		{
			name:   "relative chain under a relative stop point",
			leaf:   "data/shard-1/replica-1",
			stopAt: "data",
			want:   []string{"data/shard-1/replica-1", "data/shard-1", "data"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var synced []string
			err := SyncDirChainWith(func(dir string) error {
				synced = append(synced, dir)
				return nil
			}, tc.leaf, tc.stopAt)
			if err != nil {
				t.Fatalf("SyncDirChainWith returned %v", err)
			}
			if !reflect.DeepEqual(synced, tc.want) {
				t.Fatalf("synced %v, want %v: every directory MkdirAll may have "+
					"created needs its own fsync or its parent's entry is not durable",
					synced, tc.want)
			}
		})
	}
}

// TestSyncDirChainWith_PropagatesError verifies the walk stops at the first
// failing directory and returns its error rather than continuing up the chain.
func TestSyncDirChainWith_PropagatesError(t *testing.T) {
	boom := errors.New("fsync failed")
	var synced []string
	err := SyncDirChainWith(func(dir string) error {
		synced = append(synced, dir)
		if dir == "/a/b" {
			return boom
		}
		return nil
	}, "/a/b/c", "/")
	if !errors.Is(err, boom) {
		t.Fatalf("expected the syncDir error to propagate, got %v", err)
	}
	if want := []string{"/a/b/c", "/a/b"}; !reflect.DeepEqual(synced, want) {
		t.Fatalf("synced %v, want %v: the walk must stop at the first failure", synced, want)
	}
}
