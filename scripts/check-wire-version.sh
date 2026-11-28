#!/usr/bin/env bash
# Copyright 2026 Jeremy Hahn
# SPDX-License-Identifier: Apache-2.0
#
# check-wire-version.sh <last-release-tag>
#
# Release gate for the Raft wire version. pkg/proto/testdata/wire_layout.golden
# records a fingerprint of the encoded MessageBatch and SnapshotChunk fixtures,
# how it was derived and the WireVersion it was recorded at. When the
# fingerprint differs between <last-release-tag> and HEAD while the version line
# did not move, the wire layout changed without a WireVersion bump: fail.
#
# One exception: the one-time move of the fingerprint derivation to
# encoded-fixtures. Fingerprints across that move are not comparable, so the
# gate passes it only for the exact verified pair below (the v1.0.0-rc.1
# baseline, recorded without a derivation line, to the first encoded-fixtures
# fingerprint), with the version unchanged and the pinned encoded fixtures in
# pkg/proto/testdata/golden/ unchanged since the tag. Any other fingerprint
# change without a WireVersion bump fails, including a later encoding change
# with a regenerated fingerprint.
set -euo pipefail

golden=pkg/proto/testdata/wire_layout.golden
migration_from=fbdf0062f194b1a5a23a19f06cf316c4193fcaacd5313f31d175ea347662b899
migration_to=28b9142a380d437c1f2efd5ed388bcbd0bfce5ce5fe7e6f302a71be6cad62a36

if [ $# -ne 1 ] || [ -z "$1" ]; then
	echo "usage: $0 <last-release-tag>" >&2
	exit 2
fi
tag=$1

if ! git rev-parse -q --verify "refs/tags/$tag^{commit}" >/dev/null; then
	echo "check-wire-version: tag $tag does not exist" >&2
	exit 2
fi
if [ ! -f "$golden" ]; then
	echo "check-wire-version: $golden is missing at HEAD" >&2
	exit 1
fi
if ! old=$(git show "$tag:$golden" 2>/dev/null); then
	echo "check-wire-version: $tag has no $golden; nothing to compare, skipping"
	exit 0
fi
new=$(cat "$golden")

line() { printf '%s\n' "$1" | grep -m 1 "^$2 " || true; }

if [ "$old" = "$new" ]; then
	echo "check-wire-version: wire layout unchanged since $tag ($(line "$new" version))"
	exit 0
fi
if [ "$(line "$old" derivation)" = "" ] &&
	[ "$(line "$old" sha256)" = "sha256 $migration_from" ] &&
	[ "$(line "$new" derivation)" = "derivation encoded-fixtures" ] &&
	[ "$(line "$new" sha256)" = "sha256 $migration_to" ] &&
	[ "$(line "$old" version)" = "$(line "$new" version)" ] &&
	git diff --quiet "$tag" HEAD -- pkg/proto/testdata/golden/; then
	echo "check-wire-version: fingerprint derivation changed since $tag" \
		"('$(line "$old" derivation)' -> '$(line "$new" derivation)'), pkg/proto/testdata/golden/" \
		"unchanged ($(line "$new" version))"
	exit 0
fi
if [ "$(line "$old" version)" = "$(line "$new" version)" ]; then
	echo "check-wire-version: the Raft wire layout changed since $tag but WireVersion did not." >&2
	echo "  $tag: $(line "$old" version), $(line "$old" sha256)" >&2
	echo "  HEAD: $(line "$new" version), $(line "$new" sha256)" >&2
	echo "Bump WireVersion in pkg/proto/constants.go, set PreviousWireVersion to the old value," >&2
	echo "and regenerate: go test ./pkg/proto/ -run TestWireLayoutGolden -update" >&2
	exit 1
fi
echo "check-wire-version: wire layout changed since $tag with a WireVersion bump ($(line "$old" version) -> $(line "$new" version))"
