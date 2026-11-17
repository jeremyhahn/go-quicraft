package main

import (
	"errors"
	"fmt"
	"io"
)

// bumpable reports whether -bump moves this finding: a require behind its
// latest, with no reason comment, past the fresh window, looked up, used,
// and not a fork onto another module path.
func bumpable(f finding) bool {
	d := f.Dep
	return f.Lookup == nil && !f.Unused && f.Behind && !f.Fresh &&
		d.Reason == "" && d.Path == d.Module && f.Latest != forkLatest
}

// bumpModule runs `go get path@latest` for each bumpable require in dir,
// and `go get path@fixed` for each required module, direct or // indirect,
// govulncheck reports vulnerable with a fixed version, then `go mod tidy`, and prints each move. A replace that would move is
// printed as a hand edit, since go get does not rewrite a replace.
func bumpModule(edit moduleEditor, scan vulnScanner, dir string, findings []finding, stdout io.Writer) error {
	// A failed lookup (go list -e reports it as an Error entry, not a
	// failing exit) leaves that requirement unchecked: stop before any edit
	// rather than report a bump that never looked at it.
	var lookups []error
	for _, f := range findings {
		if f.Lookup != nil {
			lookups = append(lookups, fmt.Errorf("deps-bump: %s:%d: %w", f.Dep.File, f.Dep.Line, f.Lookup))
		}
	}
	if len(lookups) > 0 {
		lookups = append(lookups, fmt.Errorf("deps-bump: FAILED: the latest version of %d module(s) in %s could not be looked up; go.mod left unchanged", len(lookups), dir))
		return errors.Join(lookups...)
	}
	// deps-check fails a vulnerable version of any required module, direct
	// or // indirect, so the bump plans each one with a fixed version too:
	// to the highest fixed version, or to latest when that is higher.
	fixes, err := vulnFixes(scan, dir)
	if err != nil {
		return err
	}
	type step struct{ path, from, to string }
	var plan []step
	planned := make(map[string]int)
	for i := range findings {
		f := &findings[i]
		if !bumpable(*f) {
			continue
		}
		d := f.Dep
		if d.Kind == kindReplace {
			if _, err := fmt.Fprintf(stdout, "%s:%d: replace %s %s -> %s needs a hand edit\n", d.File, d.Line, d.Module, d.Version, f.Latest); err != nil {
				return err
			}
			continue
		}
		planned[d.Path] = len(plan)
		plan = append(plan, step{path: d.Path, from: d.Version, to: f.Latest})
	}
	for _, v := range fixes {
		if at, ok := planned[v.Module]; ok {
			if semverLess(plan[at].to, v.fixed) {
				plan[at].to = v.fixed
			}
			continue
		}
		planned[v.Module] = len(plan)
		plan = append(plan, step{path: v.Module, from: v.Version, to: v.fixed})
	}
	for _, s := range plan {
		if err := edit.Get(dir, s.path+"@"+s.to); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(stdout, "bumped %s %s -> %s\n", s.path, s.from, s.to); err != nil {
			return err
		}
	}
	if len(plan) == 0 {
		return nil
	}
	return edit.Tidy(dir)
}

// vulnFix is a vulnerable required module version and the lowest version
// that fixes every advisory against it.
type vulnFix struct {
	vulnFinding
	fixed string
}

// vulnFixes scans dir and returns each required module, direct or
// // indirect, that govulncheck reports vulnerable and that has a fixed
// version, in a stable order. A version with an advisory that names no fix
// is left for deps-check to fail: no bump repairs it.
func vulnFixes(scan vulnScanner, dir string) ([]vulnFix, error) {
	hits, err := scan.Scan(dir)
	if err != nil {
		return nil, err
	}
	required, err := requiredModules(dir)
	if err != nil {
		return nil, err
	}
	var out []vulnFix
	for _, v := range vulnFindings(dir, hits, required) {
		if !v.Required || toolchainModule(v.Module) {
			continue
		}
		fixed := ""
		for _, a := range v.Advisories {
			if a.Fixed == "" {
				fixed = ""
				break
			}
			if fixed == "" || semverLess(fixed, a.Fixed) {
				fixed = a.Fixed
			}
		}
		if fixed != "" && semverLess(v.Version, fixed) {
			out = append(out, vulnFix{vulnFinding: v, fixed: fixed})
		}
	}
	return out, nil
}
