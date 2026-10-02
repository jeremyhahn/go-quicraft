#!/bin/sh
# deps-check.sh -- fail when a DIRECT dependency of any module in this repo is
# behind its newest release.
#
# Runs `go list -m -u` over every module in the tree.
#
# A module is exempt only if the comment block directly above its `require`
# line in go.mod contains the words DOCUMENTED PIN. There is no ignore list or
# flag, so a pin is always recorded beside the version it holds.
#
# Exit 0 = every module current or documented. Exit 1 = at least one
# undocumented lag. Exit 2 = the check could not run, which is not a pass.

set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

GOWORK=off
export GOWORK

command -v go >/dev/null 2>&1 || {
	echo "deps-check: no go toolchain on PATH; refusing to report a pass" >&2
	exit 2
}

# Module directories, sorted for stable output. Directories the go tool
# ignores (names starting with . or _, and testdata) are pruned, the same rule
# the Makefile's ci-modules uses, so agent worktrees under .claude/ are never
# walked; vendored and fetched trees are pruned too.
mods=$(find . -mindepth 1 -type d \( -name '.*' -o -name '_*' -o -name testdata \
	-o -name vendor -o -name node_modules \) -prune -o \
	-name go.mod -print | sed 's|/go.mod$||' | sort)

test -n "$mods" || {
	echo "deps-check: found no go.mod anywhere; refusing to report a pass" >&2
	exit 2
}

status=0

for dir in $mods; do
	# The exit status is checked: `go list -m -u` prints nothing when it
	# cannot resolve the module graph (go.sum mismatch, unreachable proxy),
	# and that must be exit 2, not a pass.
	#
	# A module replaced by a directory (the nested modules' `=> ../..`) has
	# no update, whatever tag the registry reports. The replacement path is
	# appended as a fourth field and judged below; a replacement by another
	# module version is still checked.
	if ! raw=$(cd "$dir" && go list -m -u \
		-f '{{if and (not .Indirect) .Update}}{{.Path}} {{.Version}} -> {{.Update.Version}}{{if .Replace}} {{.Replace.Path}}{{end}}{{end}}' \
		all 2>&1); then
		echo "deps-check: 'go list -m -u' failed in $dir; refusing to report a pass" >&2
		printf '%s\n' "$raw" | sed 's/^/    /' >&2
		exit 2
	fi
	behind=$(printf '%s\n' "$raw" | grep -v '^$' || true)

	test -n "$behind" || continue

	while IFS= read -r line; do
		test -n "$line" || continue
		path=${line%% *}
		case "$line" in
		*' -> '*' '.*|*' -> '*' '/*)
			echo "ok   $dir: ${line% *}  [replaced by a directory]"
			continue ;;
		esac

		# Exempt iff the comment block immediately above this module's
		# require line says DOCUMENTED PIN. Any other non-blank line clears
		# the flag, so a marker cannot leak to a later require line.
		documented=$(awk -v want="$path" '
			/^[[:space:]]*\/\// { if (index($0, "DOCUMENTED PIN")) seen = 1; next }
			/^[[:space:]]*$/    { next }
			{
				if (seen) {
					n = split($0, f, /[[:space:]]+/)
					for (i = 1; i <= n; i++)
						if (f[i] == want) { print "yes"; exit }
				}
				seen = 0
			}
		' "$dir/go.mod")

		if [ "$documented" = yes ]; then
			echo "ok   $dir: $line  [DOCUMENTED PIN]"
		else
			echo "LAG  $dir: $line"
			status=1
		fi
	# Fed by a heredoc, not a pipe: a piped loop runs in a subshell and
	# would lose status=1.
	done <<EOF
$behind
EOF
done

if [ "$status" -ne 0 ]; then
	cat >&2 <<'EOF'

deps-check: a direct dependency is behind its newest release.

Every dependency is kept on its current release. To clear this:

  1. Read the release notes; crypto and transport libraries have changed
     behaviour here before.
  2. go get <module>@<version> && go mod tidy && go mod verify, per module.
  3. If the new version needs work before it compiles, add a comment containing
     DOCUMENTED PIN above the require line saying what breaks and what would
     unpin it. Do not add an exclusion to this script.
EOF
	exit 1
fi

echo "deps-check: every direct dependency is current (or carries a DOCUMENTED PIN)."
