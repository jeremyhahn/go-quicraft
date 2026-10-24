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
# A newest release published less than GRACE_DAYS (7) days ago is reported as
# NEW, not failed: a release that young has not had its notes read or its
# fixes settled, and a nightly that turns red on the day of an upstream tag
# blocks merges for no defect here (CA m823). Once it is older than that, an
# undocumented lag fails as before. A release whose publication time the
# proxy does not report is treated as old, so the check fails closed.
#
# Exit 0 = every module current, documented, or behind only a release under
# GRACE_DAYS old. Exit 1 = at least one undocumented lag behind an older
# release. Exit 2 = the check could not run, or the update lookup failed for a
# direct dependency (reported as an ERROR line): a module that could not be
# checked is not current, so neither is a pass.

# -f: lines are word-split into fields below, and an error message must not glob.
set -euf

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

GRACE_DAYS=7
now=$(date -u +%s)
grace_end=$((now - GRACE_DAYS * 86400))

status=0
young=0
lookups_failed=0

for dir in $mods; do
	# A run that cannot be trusted is exit 2, never a pass. That covers both
	# ways the lookup can fail: `go list` exiting non-zero because it cannot
	# resolve the module graph (go.sum mismatch), and a per-module update
	# lookup that errored (unreachable proxy or VCS, GOPROXY=off, a cache it
	# cannot write). The second still exits 0 and leaves .Update empty, so
	# -e is passed and .Error is read: a module with an error is an ERROR
	# line, not "current".
	#
	# Each record starts with a marker. "ERR path version replacement|-
	# message" is a failed lookup; the message is the error text, whose later lines (if
	# any) carry no marker and are skipped, so only its first line is
	# reported. "UPD path version -> update release-time [replacement]" is
	# a module behind its newest release; release-time is that release's
	# publication time in Unix seconds, or - when the proxy does not report
	# one.
	#
	# A module replaced by a directory (the nested modules' `=> ../..`) has
	# no update, whatever tag the registry reports. The replacement path is
	# appended as a last field and judged below; a replacement by another
	# module version is still checked.
	if ! raw=$(cd "$dir" && go list -m -u -e \
		-f '{{if and (not .Main) (not .Indirect)}}{{if .Error}}ERR {{.Path}} {{.Version}} {{if .Replace}}{{.Replace.Path}}{{else}}-{{end}} {{.Error.Err}}{{else if .Update}}UPD {{.Path}} {{.Version}} -> {{.Update.Version}} {{if .Update.Time}}{{.Update.Time.Unix}}{{else}}-{{end}}{{if .Replace}} {{.Replace.Path}}{{end}}{{end}}{{end}}' \
		all 2>&1); then
		echo "deps-check: 'go list -m -u' failed in $dir; refusing to report a pass" >&2
		printf '%s\n' "$raw" | sed 's/^/    /' >&2
		exit 2
	fi
	behind=$(printf '%s\n' "$raw" | grep -E '^(ERR|UPD) ' || true)

	test -n "$behind" || continue

	while IFS= read -r line; do
		test -n "$line" || continue
		# shellcheck disable=SC2086 # the fields are split on purpose
		set -- $line
		if [ "$1" = ERR ]; then
			case "$4" in
			.*|/*)
				# Replaced by a directory: there is no release to look
				# up, so a failed lookup does not matter (#188).
				continue ;;
			esac
			echo "ERROR $dir: $2 $3: update lookup failed: ${line#ERR "$2" "$3" "$4" }"
			lookups_failed=1
			continue
		fi
		shift
		path=$1
		released=$5
		repl=${6:-}
		line="$1 $2 -> $4"
		case "$repl" in
		.*|/*)
			echo "ok   $dir: $line  [replaced by a directory]"
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
		elif [ "$released" != - ] && [ "$released" -gt "$grace_end" ]; then
			echo "NEW  $dir: $line  [released $(((now - released) / 86400))d ago, under ${GRACE_DAYS}d: reported, not failed]"
			young=1
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

Every dependency is kept on its current release. A release under 7 days old
is reported as NEW and does not fail; a LAG line is behind an older one.
To clear this:

  1. Read the release notes; crypto and transport libraries have changed
     behaviour here before.
  2. go get <module>@<version> && go mod tidy && go mod verify, per module.
  3. If the new version needs work before it compiles, add a comment containing
     DOCUMENTED PIN above the require line saying what breaks and what would
     unpin it. Do not add an exclusion to this script.
EOF
fi

if [ "$lookups_failed" -ne 0 ]; then
	echo "deps-check: an update lookup failed (ERROR lines above); a module that could not be checked is not current, refusing to report a pass" >&2
	exit 2
fi

if [ "$status" -ne 0 ]; then
	exit 1
fi

if [ "$young" -ne 0 ]; then
	echo "deps-check: no failing lag; NEW lines are releases under ${GRACE_DAYS} days old, to bump as the next row."
else
	echo "deps-check: every direct dependency is current (or carries a DOCUMENTED PIN)."
fi
