#!/bin/sh
# api-check.sh -- compare the exported API of every public package against
# the last release tag, and fail on an incompatible change once v1.0.0 has
# shipped unless this release is a new MAJOR.
#
# The public packages are every non-main package of each module in
# API_MODULES (default: the root module and contrib/dragonboat) outside an
# internal/ or test/ directory. For each one, the package at the base tag and
# the package in the working tree are written to export data with apidiff
# (golang.org/x/exp/cmd/apidiff, pinned by the root go.mod's tool directive)
# and compared. A public package present at the base and gone now is an
# incompatible change; a new package is a compatible one.
#
# The verdict follows docs/compatibility/engineering.md:
#   - no release tag yet:          nothing to compare; reported, pass.
#   - base tag before v1.0.0:      every incompatible change is printed; pass.
#   - base tag v1.0.0 or later:    an incompatible change fails, unless the
#                                  major in VERSION is above the base's major.
#
# The base is the newest tag reachable from HEAD matching v[0-9]*, or
# API_BASE when set. It is read with git archive, so no worktree is created
# and the clone is not touched. API_DIFF_BIN names a prebuilt apidiff; without
# it the pinned tool is built from the root module into a temporary directory,
# so nothing is installed on the host.
#
# Exit 0 = compatible, or incompatible where the policy allows it. Exit 1 =
# an incompatible change the version does not permit. Exit 2 = the check
# could not run, which is not a pass.

set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
root=$(pwd)

GOWORK=off
export GOWORK

API_MODULES=${API_MODULES:-". contrib/dragonboat"}

fail() {
	echo "api-check: $*" >&2
	exit 2
}

command -v go >/dev/null 2>&1 || fail "no go toolchain on PATH; refusing to report a pass"
command -v git >/dev/null 2>&1 || fail "no git on PATH; refusing to report a pass"

base=${API_BASE:-}
if [ -z "$base" ]; then
	base=$(git describe --tags --abbrev=0 --match 'v[0-9]*' HEAD 2>/dev/null || true)
fi
if [ -z "$base" ]; then
	echo "api-check: no release tag (v*) is reachable from HEAD; there is no released API to compare against"
	exit 0
fi
git rev-parse -q --verify "$base^{commit}" >/dev/null 2>&1 || fail "base $base is not a commit"

[ -f VERSION ] || fail "no VERSION file"
version=$(tr -d ' \t\r\n' <VERSION)
version=${version#v}

# major prints the MAJOR of a vMAJOR.MINOR.PATCH[-pre] string.
major() {
	m=${1#v}
	m=${m%%.*}
	case $m in
	'' | *[!0-9]*) fail "cannot read a major version from '$1'" ;;
	esac
	echo "$m"
}
base_major=$(major "$base")
new_major=$(major "$version")

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM

if [ -n "${API_DIFF_BIN:-}" ]; then
	apidiff=$API_DIFF_BIN
else
	apidiff=$tmp/apidiff
	go build -o "$apidiff" golang.org/x/exp/cmd/apidiff || fail "cannot build the pinned apidiff"
fi

mkdir "$tmp/base"
git archive --format=tar "$base" | tar -x -C "$tmp/base" || fail "cannot extract $base"

# public_packages lists the module's public packages, one import path a line.
public_packages() {
	(cd "$1" && go list -f '{{if ne .Name "main"}}{{.ImportPath}}{{end}}' ./...) |
		awk 'NF && $0 !~ /\/(internal|test)(\/|$)/' | sort
}

incompatible=0
report=$tmp/report
: >"$report"
for mod in $API_MODULES; do
	[ -f "$mod/go.mod" ] || fail "$mod is not a module"
	if [ ! -f "$tmp/base/$mod/go.mod" ]; then
		echo "api-check: $mod is new since $base"
		continue
	fi
	public_packages "$tmp/base/$mod" >"$tmp/old.list" || fail "cannot list $mod at $base"
	public_packages "$mod" >"$tmp/new.list" || fail "cannot list $mod"
	comm -13 "$tmp/old.list" "$tmp/new.list" | while read -r pkg; do
		echo "api-check: new package $pkg"
	done
	while read -r pkg; do
		if ! grep -qxF "$pkg" "$tmp/new.list"; then
			printf '%s\n- package removed\n' "$pkg" >>"$report"
			incompatible=1
			continue
		fi
		(cd "$tmp/base/$mod" && "$apidiff" -w "$tmp/old.exp" "$pkg") || fail "cannot read $pkg at $base"
		(cd "$mod" && "$apidiff" -w "$tmp/new.exp" "$pkg") || fail "cannot read $pkg"
		"$apidiff" -incompatible "$tmp/old.exp" "$tmp/new.exp" >"$tmp/diff" || fail "cannot compare $pkg"
		if [ -s "$tmp/diff" ]; then
			{
				echo "$pkg"
				cat "$tmp/diff"
			} >>"$report"
			incompatible=1
		fi
	done <"$tmp/old.list"
done

cd "$root"
if [ "$incompatible" -eq 0 ]; then
	echo "api-check: the exported API is compatible with $base"
	exit 0
fi

echo "api-check: incompatible changes since $base:"
cat "$report"
if [ "$base_major" -lt 1 ]; then
	echo "api-check: $base is before v1.0.0, so these are reported and do not fail"
	exit 0
fi
if [ "$new_major" -gt "$base_major" ]; then
	echo "api-check: VERSION $version is a new major over $base, so these are permitted"
	exit 0
fi
echo "api-check: FAIL: an incompatible change after v1.0.0 needs a new major; VERSION is $version and the base is $base" >&2
exit 1
