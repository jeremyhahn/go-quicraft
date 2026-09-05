#!/bin/sh
# deps-check.sh -- fail when a DIRECT dependency of any module in this repo is
# behind its newest release.
#
# WHY THIS EXISTS, AND WHY IT IS NOT DEPENDABOT (go-quicraft#86)
#
# `.github/dependabot.yml` in this repository is thorough: eleven update
# entries covering every Go module, the Rust openraft rig, the actions and the
# Docker bases. It has also never run, not once. Dependabot is a github.com
# service, and this repository has no `github` remote -- it has never been
# mirrored. `git ls-remote github` answers "does not appear to be a git
# repository". So the config describes coverage that does not exist, and
# nightly.yml cited it twice as the reason the benchmark and contrib trees do
# not need scanning. A believed-in gate that never fires is worse than an
# absent one: it stops anyone from building the real thing.
#
# All work happens on the Gitea plane, so the watcher has to run there. This
# script is that watcher. It runs the exact command CLAUDE.md tells a developer
# to run, over every module, on a schedule -- so drift shows up on its own
# rather than when a CVE breaks the nightly Trivy gate, which is how the last
# six bumps were found (#50 and #86).
#
# THE PIN ESCAPE HATCH
#
# conventions/devops.md §9 already says a `require` held below latest "must
# carry a comment in the go.mod naming the reason ... an undocumented pin is a
# defect". This script makes that sentence mechanical rather than aspirational:
# a module is exempt if and only if the comment block directly above its
# `require` line contains the words DOCUMENTED PIN. Nothing else exempts it --
# there is no ignore list in this file and no flag to pass, because a pin
# recorded anywhere but beside the version is a pin the next reader will not
# find. Worked example: test/performance/etcd/go.mod held go.etcd.io/raft/v3
# at v3.6.0 under this marker from #86 until #127 migrated it to v3.7.0 and
# retired the pin; the tree carries no DOCUMENTED PIN today.
#
# Exit 0 = every module current or documented. Exit 1 = at least one
# undocumented lag. Exit 2 = the check could not run, which is NOT a pass.

set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

GOWORK=off
export GOWORK

command -v go >/dev/null 2>&1 || {
	echo "deps-check: no go toolchain on PATH; refusing to report a pass" >&2
	exit 2
}

# Module directories, deepest-first is irrelevant; sorted for stable output.
# -prune on vendor/node_modules keeps this from wandering into fetched trees.
mods=$(find . \( -name vendor -o -name node_modules -o -name .git \) -prune -o \
	-name go.mod -print | sed 's|/go.mod$||' | sort)

test -n "$mods" || {
	echo "deps-check: found no go.mod anywhere; refusing to report a pass" >&2
	exit 2
}

status=0

for dir in $mods; do
	# The exact command from CLAUDE.md and from issue #86's verification
	# section. Kept character-for-character so the thing CI runs and the
	# thing a developer is told to run cannot answer differently.
	#
	# Its exit status is CHECKED. `go list -m -u` prints nothing to stdout
	# when it cannot resolve the module graph -- a go.sum mismatch, an
	# unreachable proxy, a Gitea outage -- and an earlier draft of this
	# script piped that straight into `|| true` and reported "everything
	# current". A gate that answers "clean" when it could not look is the
	# vacuous gate conventions/devops.md §1 calls worse than no gate, and it
	# was caught here only because a negative test went green when it should
	# have gone red. Failure to look is exit 2, never a pass.
	if ! raw=$(cd "$dir" && go list -m -u \
		-f '{{if and (not .Indirect) .Update}}{{.Path}} {{.Version}} -> {{.Update.Version}}{{end}}' \
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

		# Exempt iff the comment block immediately above this module's
		# require line says DOCUMENTED PIN. Any non-comment, non-blank
		# line clears the flag, so a marker attached to some OTHER
		# require line further up does not leak down to this one.
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
	# Fed by a HEREDOC, not `printf ... | while read`. A pipe would run the
	# loop in a subshell, `status=1` would die with it, and the script would
	# exit 0 having just printed LAG lines -- the exact vacuous-gate shape
	# the exit-status check above exists to prevent, one line further down.
	done <<EOF
$behind
EOF
done

if [ "$status" -ne 0 ]; then
	cat >&2 <<'EOF'

deps-check: a direct dependency is behind its newest release.

The standing instruction (owner, 2026-09-03) is that every library sits on its
current production release. To clear this:

  1. Read the release notes. conventions/devops.md §9 and this repo's CLAUDE.md
     both say so, and both cite cases where it mattered -- x/crypto v0.55.0 was
     a CRITICAL CVE (#50), quic-go v0.60.0 was a FIPS capability held three
     months (#85), x/crypto v0.56.0 was CVE-2026-56855 (#86).
  2. go get <module>@<version> && go mod tidy && go mod verify, per module.
  3. If the new version does not compile and adapting it is its own task, file
     that task and add a DOCUMENTED PIN comment above the require line naming
     what breaks and which issue unpins it (devops.md §9). Do NOT add an
     exclusion here.
EOF
	exit 1
fi

echo "deps-check: every direct dependency is current (or carries a DOCUMENTED PIN)."
