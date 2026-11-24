package main

import "strings"

// semverCompare orders two module versions by SemVer 2.0 precedence and
// returns -1, 0 or +1. The leading "v" and any +build metadata are ignored.
// MAJOR.MINOR.PATCH compare numerically; a version with a prerelease sorts
// before the same version without one; prerelease identifiers compare
// left to right, numeric ones numerically and below alphanumeric ones,
// alphanumeric ones in ASCII order, and a shorter run of equal identifiers
// first. A Go pseudo-version is a SemVer prerelease of its base, so it
// orders after the tag it was cut from (vX.Y.(Z+1)-0.timestamp-hash or
// vX.Y.Z-pre.0.timestamp-hash) and before the next tag.
func semverCompare(a, b string) int {
	ac, ap := splitSemver(a)
	bc, bp := splitSemver(b)
	for i := range 3 {
		if c := compareNumeric(field(ac, i), field(bc, i)); c != 0 {
			return c
		}
	}
	switch {
	case ap == bp:
		return 0
	case ap == "":
		return 1
	case bp == "":
		return -1
	}
	ai, bi := strings.Split(ap, "."), strings.Split(bp, ".")
	for i := 0; i < len(ai) && i < len(bi); i++ {
		if c := compareIdentifier(ai[i], bi[i]); c != 0 {
			return c
		}
	}
	return compareInt(len(ai), len(bi))
}

// semverLess reports whether version a has lower SemVer precedence than b.
func semverLess(a, b string) bool { return semverCompare(a, b) < 0 }

// splitSemver returns the dot-separated core and the prerelease of v.
func splitSemver(v string) ([]string, string) {
	v = strings.TrimPrefix(v, "v")
	v, _, _ = strings.Cut(v, "+")
	v, pre, _ := strings.Cut(v, "-")
	return strings.SplitN(v, ".", 3), pre
}

func field(parts []string, i int) string {
	if i < len(parts) {
		return parts[i]
	}
	return "0"
}

// compareIdentifier compares one prerelease identifier pair.
func compareIdentifier(a, b string) int {
	an, bn := isNumeric(a), isNumeric(b)
	switch {
	case an && bn:
		return compareNumeric(a, b)
	case an:
		return -1
	case bn:
		return 1
	}
	return strings.Compare(a, b)
}

// compareNumeric compares two digit strings of any length without
// overflow: leading zeros dropped, the longer is larger, else lexically.
func compareNumeric(a, b string) int {
	a, b = strings.TrimLeft(a, "0"), strings.TrimLeft(b, "0")
	if c := compareInt(len(a), len(b)); c != 0 {
		return c
	}
	return strings.Compare(a, b)
}

func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func compareInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
