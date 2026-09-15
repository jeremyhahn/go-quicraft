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
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
)

// Version is the project version the QuicRaft benchmark records, stamped
// at build time:
//
//	go test -c -tags=benchmark -ldflags "-X github.com/jeremyhahn/go-quicraft/test/performance/benchutil.Version=$(cat VERSION)"
//
// which is what the quicraft Dockerfile does from the VERSION file in its
// build context -- the same file `make build` stamps into the binaries, so a
// stored result names the release it measured. Empty when the binary was
// built without the flag; ProjectVersion then reads the file itself.
var Version string

// versionFile is the root of the project's single source of truth for its
// version (release-versioning.md; the release recipe writes it).
const versionFile = "VERSION"

// ProjectVersion returns the project version for a benchmark result: the
// build-time stamp when there is one, otherwise the contents of the VERSION
// file found by walking up from the working directory -- a bare
// `go test -tags=benchmark` runs in the package directory, three levels
// below it. It fails rather than returns a placeholder: a result that
// records "0.0.0" (#189) is a result that cannot be attributed to anything.
func ProjectVersion() (string, error) {
	if Version != "" {
		return Version, nil
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("benchutil: project version: %w", err)
	}
	for {
		raw, readErr := os.ReadFile(filepath.Join(dir, versionFile))
		if readErr == nil {
			if v := strings.TrimSpace(string(raw)); v != "" {
				return v, nil
			}
			return "", fmt.Errorf("benchutil: %s is empty", filepath.Join(dir, versionFile))
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("benchutil: no build-time Version and no %s file above %s; stamp the binary or run from inside the repository", versionFile, dir)
		}
		dir = parent
	}
}

// ModuleVersion returns the version of the named module as linked into this
// binary, from the build info the Go toolchain embeds -- so a benchmark that
// measures dragonboat records the dragonboat its go.mod actually pins, and a
// pin bump cannot leave the recorded version behind (#189: "v4.0.0" was
// recorded while v4.0.1 was linked). A module replaced by a directory has no
// version; the error names it.
func ModuleVersion(path string) (string, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", fmt.Errorf("benchutil: no build info in this binary; cannot report the version of %s", path)
	}
	for _, dep := range info.Deps {
		if dep.Path != path {
			continue
		}
		version := dep.Version
		if dep.Replace != nil {
			version = dep.Replace.Version
		}
		// A directory replacement carries "(devel)" (or nothing) for a
		// version: the toolchain's way of saying there is none.
		if version == "" || version == "(devel)" {
			where := path
			if dep.Replace != nil {
				where = dep.Replace.Path
			}
			return "", fmt.Errorf("benchutil: %s is linked from %s, which has no version", path, where)
		}
		return version, nil
	}
	return "", fmt.Errorf("benchutil: %s is not linked into this binary", path)
}
