#!/usr/bin/env bash
# scripts/ci-image-history.sh: fail when an image carries a private address.
# A pool build takes the LAN caches as BuildKit
# secrets, which no layer and no history entry keeps; a build argument's
# value, by contrast, is recorded in every RUN of the history. A pool-built
# image may one day be pushed beyond the LAN, and internal
# addresses out of everything public, metadata included. For each image it
# reads the history (every entry's command and comment) and the config (env,
# labels, command) and fails on any RFC 1918 address. It names the image and
# the history entry, never the address.
#
#   usage: scripts/ci-image-history.sh <image>...
set -euo pipefail

if [ $# -eq 0 ]; then
	echo "usage: scripts/ci-image-history.sh <image>..." >&2
	exit 2
fi

octet='(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])'
private="(^|[^0-9.])(10\.$octet|172\.(1[6-9]|2[0-9]|3[01])|192\.168)\.$octet\.$octet([^0-9]|\$)"

status=0
for image in "$@"; do
	if ! history=$(docker history --no-trunc --format '{{.CreatedBy}} {{.Comment}}' "$image") ||
		! config=$(docker image inspect --format '{{json .Config}}' "$image"); then
		echo "ci-image-history: cannot read $image" >&2
		exit 1
	fi
	while IFS=: read -r entry _; do
		echo "ci-image-history: $image: history entry $entry carries a private address" >&2
		status=1
	done < <(grep -En "$private" <<<"$history" || :)
	if grep -Eq "$private" <<<"$config"; then
		echo "ci-image-history: $image: its config (env, labels, command) carries a private address" >&2
		status=1
	fi
done
exit "$status"
