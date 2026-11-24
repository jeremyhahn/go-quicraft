package main

import (
	"strings"
	"time"
)

// forkLatest stands in for the latest version of a replace onto a different
// module path, which no version comparison can judge.
const forkLatest = "n/a (replaced by a different module)"

// finding is the verdict on one dependency.
type finding struct {
	Dep    dependency
	Latest string
	Behind bool
	// Unused is set for a replace that applies to no module in the build list.
	Unused bool
	// Lookup is set when the latest version could not be looked up.
	Lookup *LookupError
	// Fresh is set when Latest was released less than freshWindow ago:
	// an undocumented lag behind it is reported, not failed.
	Fresh bool
	// Released is when Latest was released; zero when unknown.
	Released time.Time
	// Untouched is set under -base when this line's path and version match the merge base; a lag is then reported, not failed.
	Untouched bool
}

// freshWindow is how long a new release may go unadopted before a lag
// behind it, with no go.mod reason comment, fails the check.
const freshWindow = 7 * 24 * time.Hour

// assess judges one dependency against its go list entry: latest is the highest tag for first-party modules, else Update, else the highest release for a replace.
func assess(dep dependency, mod listedModule, listed bool, firstParty string, now time.Time) finding {
	f := finding{Dep: dep}
	switch {
	case !listed:
		f.Lookup = &LookupError{Module: dep.Module, Reason: "go list returned no entry for it"}
		return f
	case mod.Error != nil:
		f.Lookup = &LookupError{Module: dep.Module, Reason: mod.Error.Err}
		return f
	case mod.Version == "" && dep.Kind == kindReplace:
		f.Unused = true
		return f
	case mod.Version == "":
		f.Lookup = &LookupError{Module: dep.Module, Reason: "not a known dependency"}
		return f
	case dep.Path != dep.Module:
		// A fork: holding it is always a decision that needs its reason.
		f.Latest, f.Behind = forkLatest, true
		return f
	}
	f.Latest = latestVersion(dep, mod, strings.HasPrefix(dep.Path, firstParty))
	f.Behind = isBehind(dep.Version, f.Latest)
	if f.Behind && mod.Update != nil && mod.Update.Version == f.Latest {
		f.Released = mod.Update.Time
		f.Fresh = isFresh(f.Released, now)
	}
	return f
}

// isFresh reports whether a release at released is younger than
// freshWindow at now. An unknown (zero) release time is never fresh.
func isFresh(released, now time.Time) bool {
	if released.IsZero() {
		return false
	}
	return now.Sub(released) < freshWindow
}

func latestVersion(dep dependency, mod listedModule, firstParty bool) string {
	if dep.Kind == kindRequire && !firstParty {
		if mod.Update != nil {
			return mod.Update.Version
		}
		return dep.Version
	}
	candidates := mod.Versions
	if !firstParty {
		if releases := releasesOnly(mod.Versions); len(releases) > 0 {
			candidates = releases
		}
	}
	if len(candidates) > 0 {
		return candidates[len(candidates)-1]
	}
	if mod.Update != nil {
		return mod.Update.Version
	}
	return dep.Version
}

// isBehind compares current and latest in semantic-version order; a pseudo-version is behind any named tag.
func isBehind(current, latest string) bool {
	// SemVer precedence, not the position in Versions.
	return latest != current && semverLess(current, latest)
}

func releasesOnly(versions []string) []string {
	var releases []string
	for _, v := range versions {
		if !isPrerelease(v) {
			releases = append(releases, v)
		}
	}
	return releases
}

// isPrerelease reports a prerelease or pseudo-version: a - before any +build suffix.
func isPrerelease(version string) bool {
	core, _, _ := strings.Cut(version, "+")
	return strings.Contains(core, "-")
}
