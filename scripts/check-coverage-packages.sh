#!/usr/bin/env bash
#
# check-coverage-packages.sh — enforce a coverage floor on every package in a
# Go coverage profile, so no package hides under the module total.
#
# Usage: scripts/check-coverage-packages.sh <profile> <minimum-percent> [<import-path-prefix>]
#
# Percentages match `go test -cover`: covered statements over all statements,
# with blocks deduplicated by file position (merged profiles can repeat them).
# Only packages at or beneath the optional prefix are judged; others are
# reported but never fail. A missing or empty profile, or no package under the
# prefix, is a failure.
set -euo pipefail

profile=${1:?usage: $0 <profile> <minimum-percent> [<import-path-prefix>]}
min=${2:?usage: $0 <profile> <minimum-percent> [<import-path-prefix>]}
prefix=${3:-}

if [ ! -s "$profile" ]; then
	echo "FAIL: coverage profile $profile is missing or empty"
	exit 1
fi
case "$min" in
	'' | *[!0-9.]*)
		echo "FAIL: minimum '$min' is not a number"
		exit 1
		;;
esac

awk -v min="$min" -v prefix="$prefix" '
NR == 1 && /^mode:/ { next }
{
	# "import/path/file.go:12.34,15.2 3 1" -> block, statements, count
	split($1, loc, ":")
	file = loc[1]
	pkg = file
	sub(/\/[^\/]*$/, "", pkg)
	block = $1
	stmts = $2 + 0
	count = $3 + 0
	if (!(block in seen)) {
		seen[block] = 1
		total[pkg] += stmts
		blockStmts[block] = stmts
		blockPkg[block] = pkg
	}
	if (count > 0 && !(block in hit)) {
		hit[block] = 1
		covered[pkg] += stmts
	}
}
END {
	judged = 0
	failed = 0
	n = 0
	for (p in total) names[++n] = p
	# insertion sort: portable awk has no asort
	for (i = 2; i <= n; i++) {
		v = names[i]
		for (j = i - 1; j >= 1 && names[j] > v; j--) names[j + 1] = names[j]
		names[j + 1] = v
	}
	for (i = 1; i <= n; i++) {
		p = names[i]
		if (total[p] == 0) continue
		pct = 100.0 * covered[p] / total[p]
		# The prefix is a whole import path: the package itself or anything
		# beneath it, never a sibling that merely shares its leading bytes.
		inScope = (prefix == "" || p == prefix || index(p, prefix "/") == 1)
		if (!inScope) {
			printf "skip  %6.1f%%  %s\n", pct, p
			continue
		}
		judged++
		if (pct + 1e-9 < min) {
			printf "FAIL  %6.1f%%  %s (below %s%%)\n", pct, p, min
			failed++
		} else {
			printf "ok    %6.1f%%  %s\n", pct, p
		}
	}
	if (judged == 0) {
		printf "FAIL: no package under prefix %s has statements in the profile\n", prefix
		exit 1
	}
	if (failed > 0) {
		printf "FAIL: %d of %d packages below %s%%\n", failed, judged, min
		exit 1
	}
	printf "all %d packages at or above %s%%\n", judged, min
}' "$profile"
