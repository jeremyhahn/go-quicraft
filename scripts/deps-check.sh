#!/bin/sh
# deps-check.sh -- fail when a direct dependency is behind its newest release older than GRACE_DAYS (7), unless go.mod marks it DOCUMENTED PIN; exit 0 current, 1 lagging, 2 could not run.

# -f: lines are word-split into fields below, and an error message must not glob.
set -euf

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

GOWORK=off
export GOWORK

command -v go >/dev/null 2>&1 || {
	echo "deps-check: no go toolchain on PATH; refusing to report a pass" >&2
	exit 2
}

# Module directories, sorted; dot, underscore, testdata, vendored and fetched trees are pruned like the Makefile's ci-modules.
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
	# A failed lookup exits 2: records are ERR (failed lookup, first line) or UPD (behind, with release time and any replacement).
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
				# Replaced by a directory: no release to look up, so a failed lookup does not matter.
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

		# Exempt iff the comment block directly above this require line says DOCUMENTED PIN.
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
