#!/usr/bin/env bash
# Copyright 2026 Jeremy Hahn
# SPDX-License-Identifier: Apache-2.0
#
# check-wire-version-tests.sh
#
# Regression tests for scripts/check-wire-version.sh. Each case builds a
# throwaway git repository with a release tag and a HEAD commit, runs the gate
# against the tag, and checks the exit status. Run from the repository root:
#   bash scripts/check-wire-version-tests.sh
set -euo pipefail

root=$(git rev-parse --show-toplevel)
gate="$root/scripts/check-wire-version.sh"
golden=pkg/proto/testdata/wire_layout.golden
# The v1.0.0-rc.1 golden: recorded before the derivation line existed.
baseline_commit=eea5ebb8
baseline=$(git -C "$root" show "$baseline_commit:$golden")
current=$(cat "$root/$golden")

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fails=0

# golden_with <version> <derivation-or-empty> <sha256>
golden_with() {
	printf '# test golden\nversion %s\n' "$1"
	if [ -n "$2" ]; then printf 'derivation %s\n' "$2"; fi
	printf 'sha256 %s\n' "$3"
}

# run_case <name> <want-exit> <tag-golden> <head-golden> [<fixture-change>]
run_case() {
	local name=$1 want=$2 old=$3 new=$4 fixture=${5:-}
	local repo="$work/$(printf '%s' "$name" | tr -c 'a-zA-Z0-9' '-')"
	mkdir -p "$repo/pkg/proto/testdata/golden"
	git -C "$repo" init -q
	git -C "$repo" config user.name test
	git -C "$repo" config user.email test@example.invalid
	git -C "$repo" config commit.gpgsign false
	git -C "$repo" config tag.gpgsign false
	printf 'fixture\n' >"$repo/pkg/proto/testdata/golden/f.bin"
	printf '%s\n' "$old" >"$repo/$golden"
	git -C "$repo" add -A
	git -C "$repo" commit -q -m tag
	git -C "$repo" tag v0.0.0-test
	printf '%s\n' "$new" >"$repo/$golden"
	if [ -n "$fixture" ]; then printf '%s\n' "$fixture" >"$repo/pkg/proto/testdata/golden/f.bin"; fi
	git -C "$repo" add -A
	git -C "$repo" commit -q --allow-empty -m head
	local got=0
	(cd "$repo" && bash "$gate" v0.0.0-test) >"$repo.log" 2>&1 || got=$?
	if [ "$got" -ne "$want" ]; then
		echo "FAIL: $name: exit $got, want $want" >&2
		tail -n 5 "$repo.log" >&2
		fails=$((fails + 1))
	else
		echo "ok: $name"
	fi
}

later_sha=0000000000000000000000000000000000000000000000000000000000000001

run_case "unchanged golden passes" 0 "$current" "$current"
run_case "verified derivation migration passes" 0 "$baseline" "$current"
run_case "migration with changed pinned fixtures fails" 1 "$baseline" "$current" changed
run_case "later encoding change without a version bump fails" 1 \
	"$baseline" "$(golden_with 1 encoded-fixtures "$later_sha")"
run_case "later encoding change after the migration tag fails" 1 \
	"$current" "$(golden_with 1 encoded-fixtures "$later_sha")"
run_case "later encoding change with a version bump passes" 0 \
	"$baseline" "$(golden_with 2 encoded-fixtures "$later_sha")"
run_case "unknown derivation change without a version bump fails" 1 \
	"$current" "$(golden_with 1 other-derivation "$later_sha")"

if [ "$fails" -ne 0 ]; then
	echo "check-wire-version-tests: $fails case(s) failed" >&2
	exit 1
fi
echo "check-wire-version-tests: all cases passed"
