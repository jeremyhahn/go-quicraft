#!/usr/bin/env bash
#
# bench-compare.sh — statistically defensible hot-path benchmark comparison.
#
# The problem this solves: raw `go test -bench` numbers on a loaded developer
# box are worthless for regression detection. BenchmarkInMemoryMerge has been
# observed at 1137 ns and 5507 ns across two *identical* back-to-back runs on
# the dev machine — a 5x spread. Any regression smaller than that disappears
# into the noise, and naive "run baseline block, then run current block"
# measurement invents regressions that do not exist.
#
# The method implemented here (validated: variance fell to ~5%, medians matched
# to within 0.5% on an unmodified tree):
#
#   1. COMPILE BOTH SIDES ON THE INVOKING MACHINE, with the same toolchain and
#      GOWORK=off, so the baseline and current test binaries differ only by
#      source revision. Go test binaries are self-contained, so they can then
#      be shipped anywhere.
#   2. RUN THEM SOMEWHERE. Two supported modes, identical in every respect
#      except execution location:
#        - local  (PERF_HOST unset): run on the invoking machine. This is the
#          default and is fully supported, including in CI.
#        - remote (PERF_HOST=user@host): ssh the binaries to a dedicated host
#          and run them there. A quiet, dedicated machine yields tighter
#          confidence intervals; a shared or loaded one widens them.
#   3. INTERLEAVE the two sides round-robin (cur, base, cur, base, ...) rather
#      than running them as two contiguous blocks. Slow thermal/scheduler
#      drift is then shared equally by both sides instead of being attributed
#      entirely to whichever side ran second. Back-to-back blocks are exactly
#      what produced a bogus 2x "regression" on InMemoryMerge/RaftLogAppend
#      that vanished under this method.
#   4. USE A FIXED ITERATION COUNT (-test.benchtime=Nx), never a duration.
#      With a duration, a slow round silently runs fewer iterations, so the two
#      sides are not doing the same work and the samples are not comparable.
#   5. TAKE MANY ROUNDS and compare distributions (benchstat: median + CI +
#      p-value), never single numbers.
#
# Usage:
#   scripts/bench-compare.sh compare    # baseline vs working tree (default)
#   scripts/bench-compare.sh baseline   # record working tree as a stored baseline
#
# See docs/testing/benchmarking.md for the full rationale and for how to
# read the output.

set -euo pipefail

# ---------------------------------------------------------------------------
# Logging helpers. Everything diagnostic goes to stderr so stdout stays clean.
# ---------------------------------------------------------------------------

info() { printf '==> %s\n' "$*" >&2; }
# note() is for accurate, low-key caveats that do not indicate a problem.
# warn() is for things the caller probably wants to fix.
note() { printf 'note: %s\n' "$*" >&2; }
warn() { printf 'WARNING: %s\n' "$*" >&2; }
die() {
	printf 'ERROR: %s\n' "$*" >&2
	exit 1
}

# ---------------------------------------------------------------------------
# Configuration (all overridable from the environment / Makefile).
# ---------------------------------------------------------------------------

MODE="${1:-compare}"

# Git ref to compare against. Anything `git rev-parse` accepts.
BASELINE="${BASELINE:-origin/main}"

# Where the compiled benchmark binaries are executed.
#   unset/empty  -> run on this machine (supported default, including in CI)
#   user@host    -> ssh the binaries there and run them on that host
# Set it to a quiet, dedicated machine when you want the tightest intervals.
PERF_HOST="${PERF_HOST:-}"

# Packages to benchmark. The default is the hot path CLAUDE.md requires to be
# benchmarked on every change: append (raft log), replicate (proto codec on the
# wire path) and apply (engine).
BENCH_PKGS="${BENCH_PKGS:-./pkg/internal/raft/ ./pkg/internal/engine/ ./pkg/proto/}"

# -test.bench regex. "." means every benchmark in the selected packages.
BENCH_FILTER="${BENCH_FILTER:-.}"

# Number of interleaved rounds per side. benchstat needs >= 6 samples before it
# will report a confidence interval and >= 4 before it will report a p-value,
# so 8 is the smallest comfortable default.
BENCH_ROUNDS="${BENCH_ROUNDS:-8}"

# Fixed iteration count. MUST end in 'x'. A duration (e.g. "1s") is rejected:
# see the header comment, point 4.
BENCH_ITERS="${BENCH_ITERS:-100000x}"

# Per-invocation test binary timeout.
BENCH_TIMEOUT="${BENCH_TIMEOUT:-30m}"

# Where run artifacts land. Defaults to a timestamped dir under build/.
BENCH_OUT_DIR="${BENCH_OUT_DIR:-}"

# Set to 1 to keep the baseline worktree around between runs (saves a rebuild).
BENCH_KEEP_WORKTREE="${BENCH_KEEP_WORKTREE:-0}"

# Where committed baselines live, and the label used in their filename.
BASELINE_DIR="${BASELINE_DIR:-test/performance/baselines}"

SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)

case "$MODE" in
compare | baseline) ;;
*) die "unknown mode '$MODE' (expected 'compare' or 'baseline')" ;;
esac

# ---------------------------------------------------------------------------
# Repo / toolchain discovery.
# ---------------------------------------------------------------------------

REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)" ||
	die "not inside a git repository"
cd "$REPO_ROOT"

command -v go >/dev/null 2>&1 || die "go toolchain not found in PATH"
GO_VERSION="$(go version)"

# Validate the fixed-iteration invariant before doing any expensive work.
[[ "$BENCH_ITERS" == *x ]] ||
	die "BENCH_ITERS must be a fixed iteration count ending in 'x' (got '$BENCH_ITERS').
       A duration lets the two sides run different iteration counts, which makes
       the samples incomparable. Use e.g. BENCH_ITERS=100000x."

[[ "$BENCH_ROUNDS" =~ ^[0-9]+$ && "$BENCH_ROUNDS" -ge 1 ]] ||
	die "BENCH_ROUNDS must be a positive integer (got '$BENCH_ROUNDS')"

if [[ "$BENCH_ROUNDS" -lt 6 ]]; then
	warn "BENCH_ROUNDS=$BENCH_ROUNDS: benchstat needs >= 6 samples for a confidence"
	warn "interval and >= 4 for a p-value. Results will be reported without statistics."
fi

# Locate benchstat: PATH first, then the usual GOPATH/bin.
BENCHSTAT=""
if command -v benchstat >/dev/null 2>&1; then
	BENCHSTAT="$(command -v benchstat)"
elif [[ -x "$(go env GOPATH)/bin/benchstat" ]]; then
	BENCHSTAT="$(go env GOPATH)/bin/benchstat"
fi

# ---------------------------------------------------------------------------
# Output directory.
# ---------------------------------------------------------------------------

RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -z "$BENCH_OUT_DIR" ]]; then
	BENCH_OUT_DIR="$REPO_ROOT/build/bench-compare/$RUN_STAMP"
fi
mkdir -p "$BENCH_OUT_DIR"

CUR_RESULTS="$BENCH_OUT_DIR/current.txt"
BASE_RESULTS="$BENCH_OUT_DIR/baseline.txt"
COMPARE_OUT="$BENCH_OUT_DIR/compare.txt"
META_OUT="$BENCH_OUT_DIR/metadata.txt"
: >"$CUR_RESULTS"
[[ "$MODE" == compare ]] && : >"$BASE_RESULTS"

# ---------------------------------------------------------------------------
# Cleanup. Runs on every exit path so a failed build never leaves a stale
# worktree or a pile of test binaries on the remote host.
# ---------------------------------------------------------------------------

LOCAL_TMP=""
REMOTE_TMP=""
WORKTREE_DIR=""

cleanup() {
	local rc=$?
	set +e
	if [[ -n "$REMOTE_TMP" && -n "$PERF_HOST" ]]; then
		# Belt and braces: only ever remove a path we created under /tmp.
		case "$REMOTE_TMP" in
		/tmp/*) ssh "${SSH_OPTS[@]}" "$PERF_HOST" "rm -rf -- '$REMOTE_TMP'" >/dev/null 2>&1 ;;
		esac
	fi
	if [[ -n "$LOCAL_TMP" && -d "$LOCAL_TMP" ]]; then
		rm -rf -- "$LOCAL_TMP"
	fi
	if [[ -n "$WORKTREE_DIR" && "$BENCH_KEEP_WORKTREE" != "1" ]]; then
		git -C "$REPO_ROOT" worktree remove --force -- "$WORKTREE_DIR" >/dev/null 2>&1
		# `worktree remove` refuses in some states; make sure the metadata and
		# the directory are both gone so the next run starts clean.
		rm -rf -- "$WORKTREE_DIR"
		git -C "$REPO_ROOT" worktree prune >/dev/null 2>&1
		rmdir -- "$(dirname "$WORKTREE_DIR")" >/dev/null 2>&1
	fi
	exit "$rc"
}
trap cleanup EXIT

# Scratch space for the compiled test binaries. Deliberately under build/ (which
# is gitignored) rather than TMPDIR: a CI runner's /tmp is not guaranteed to be
# shared between steps or to have room for several hundred MB of test binaries,
# whereas the workspace always is and always does.
mkdir -p "$REPO_ROOT/build/bench-compare"
LOCAL_TMP="$(mktemp -d "$REPO_ROOT/build/bench-compare/tmp-XXXXXXXX")" ||
	die "cannot create a scratch directory under $REPO_ROOT/build/bench-compare"

# ---------------------------------------------------------------------------
# Resolve the baseline ref up front: it is instant, and a typo here should not
# cost the caller an ssh connect timeout first.
# ---------------------------------------------------------------------------

# compare mode always needs a baseline ref. baseline mode measures the working
# tree by default, but accepts BASELINE_REF to record a clean, committed
# revision instead - a stored baseline built from a dirty tree is not
# reproducible and should not be committed.
BASELINE_SHA=""
BASELINE_DESC=""
WORKTREE_REF=""
if [[ "$MODE" == compare ]]; then
	WORKTREE_REF="$BASELINE"
elif [[ -n "${BASELINE_REF:-}" ]]; then
	WORKTREE_REF="$BASELINE_REF"
fi
if [[ -n "$WORKTREE_REF" ]]; then
	if ! BASELINE_SHA="$(git -C "$REPO_ROOT" rev-parse --verify --quiet "${WORKTREE_REF}^{commit}")"; then
		# A shallow or single-branch clone (the CI default) simply does not have
		# the object, which git reports as an indistinguishable "unknown
		# revision". Say which of the two it is.
		if [[ "$(git -C "$REPO_ROOT" rev-parse --is-shallow-repository 2>/dev/null)" == "true" ]]; then
			die "ref '$WORKTREE_REF' is not present in this SHALLOW clone.
       A shallow clone has no history to compare against. In CI, check out with
       full history (actions/checkout: 'fetch-depth: 0'), or deepen the clone:
           git fetch --unshallow origin
       Then re-run. Alternatively pass a ref this clone does have:
           make bench-compare BASELINE=<sha|tag|branch>"
		fi
		die "ref '$WORKTREE_REF' does not resolve to a commit.
       If it is a remote-tracking ref, fetch it first:
           git fetch --no-tags origin '+refs/heads/*:refs/remotes/origin/*'
       (single-branch clones, common in CI, do not have other branches).
       Otherwise pass an explicit ref: BASELINE=<sha|tag|branch>."
	fi
	BASELINE_DESC="$WORKTREE_REF ($(git -C "$REPO_ROOT" rev-parse --short "$BASELINE_SHA"))"
fi

# ---------------------------------------------------------------------------
# Execution host. Local (PERF_HOST unset) and remote (PERF_HOST=user@host) are
# both first-class: the measurement method below is byte-for-byte identical, and
# only the machine that runs the binaries changes.
#
# What is NOT allowed is falling back from remote to local. If PERF_HOST is set,
# the caller asked for numbers from that machine; quietly substituting a
# different one would attach the wrong provenance to the result.
# ---------------------------------------------------------------------------

RUN_HOSTNAME=""
RUN_CPU_MODEL=""
RUN_CPU_COUNT=""
RUN_GOVERNOR=""
RUN_LOADAVG=""

probe_local() {
	RUN_HOSTNAME="$(hostname)"
	RUN_CPU_MODEL="$(awk -F': ' '/model name/ {print $2; exit}' /proc/cpuinfo 2>/dev/null || echo unknown)"
	RUN_CPU_COUNT="$(nproc 2>/dev/null || echo unknown)"
	RUN_GOVERNOR="$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
	RUN_LOADAVG="$(awk '{print $1, $2, $3}' /proc/loadavg 2>/dev/null || echo unknown)"
}

probe_remote() {
	local probe
	probe="$(ssh "${SSH_OPTS[@]}" "$PERF_HOST" '
		hostname
		awk -F": " "/model name/ {print \$2; exit}" /proc/cpuinfo 2>/dev/null || echo unknown
		nproc 2>/dev/null || echo unknown
		cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown
		awk "{print \$1, \$2, \$3}" /proc/loadavg 2>/dev/null || echo unknown
	')" || die "cannot reach PERF_HOST '$PERF_HOST' over ssh.
       ssh runs with BatchMode=yes: it never prompts, so key-based auth must
       already work for this destination (in CI, load the key before invoking).
       Fix ssh access, or unset PERF_HOST to run on this machine instead.
       This target refuses to silently fall back to the local machine: you asked
       for numbers from '$PERF_HOST' and would otherwise get numbers from here."
	RUN_HOSTNAME="$(sed -n 1p <<<"$probe")"
	RUN_CPU_MODEL="$(sed -n 2p <<<"$probe")"
	RUN_CPU_COUNT="$(sed -n 3p <<<"$probe")"
	RUN_GOVERNOR="$(sed -n 4p <<<"$probe")"
	RUN_LOADAVG="$(sed -n 5p <<<"$probe")"
	[[ -n "$RUN_HOSTNAME" ]] || die "PERF_HOST '$PERF_HOST' returned no hostname; refusing to trust it"
}

if [[ -n "$PERF_HOST" ]]; then
	info "probing PERF_HOST $PERF_HOST"
	probe_remote
	REMOTE_TMP="$(ssh "${SSH_OPTS[@]}" "$PERF_HOST" 'mktemp -d -t quicraft-bench-XXXXXXXX')" ||
		die "failed to create a temp dir on $PERF_HOST"
	case "$REMOTE_TMP" in
	/tmp/*) ;;
	*) die "unexpected remote temp dir '$REMOTE_TMP' (expected under /tmp)" ;;
	esac
	info "running on $PERF_HOST ($RUN_HOSTNAME), workdir $REMOTE_TMP"
else
	probe_local
	info "running locally on $RUN_HOSTNAME (${RUN_CPU_COUNT} cpu, loadavg $RUN_LOADAVG)"
fi

# One consolidated accuracy note, printed once. Both apply to whichever machine
# ends up running the benchmarks; neither is an error.
declare -a ACCURACY_NOTES=()
if [[ -z "$PERF_HOST" ]]; then
	ACCURACY_NOTES+=("competing load on a shared machine widens confidence intervals; a quiet, dedicated host (PERF_HOST=user@host) gives tighter ones")
fi
# "unknown" means the host exposes no cpufreq interface at all (common inside
# containers). Nothing to report in that case - only report a governor we
# actually read and that is not 'performance'.
if [[ "$RUN_GOVERNOR" != "performance" && "$RUN_GOVERNOR" != "unknown" ]]; then
	ACCURACY_NOTES+=("cpu governor on $RUN_HOSTNAME is '$RUN_GOVERNOR', not 'performance'; clocks ramp during a run, which widens intervals (changing it needs root)")
fi
if [[ "${#ACCURACY_NOTES[@]}" -gt 0 ]]; then
	note "affects confidence-interval width (not correctness):"
	for n in "${ACCURACY_NOTES[@]}"; do
		printf '      - %s\n' "$n" >&2
	done
	printf '      Raise BENCH_ROUNDS/BENCH_ITERS if the reported +/- is too wide to conclude.\n' >&2
fi

# ---------------------------------------------------------------------------
# Baseline worktree (compare mode only).
# ---------------------------------------------------------------------------

if [[ -n "$BASELINE_SHA" ]]; then
	# Under build/, which is gitignored and therefore invisible to `git status`,
	# and which `./...` will not descend into because the worktree carries its
	# own go.mod (module boundaries stop package-pattern expansion). Keeping it
	# in the workspace means CI needs no writable path outside the checkout.
	WORKTREE_ROOT="${BENCH_WORKTREE_ROOT:-$REPO_ROOT/build/bench-worktrees}"
	WORKTREE_DIR="$WORKTREE_ROOT/$(git rev-parse --short "$BASELINE_SHA")"
	if [[ -d "$WORKTREE_DIR" ]]; then
		info "reusing worktree $WORKTREE_DIR"
	else
		info "creating worktree for $BASELINE_DESC"
		mkdir -p "$(dirname "$WORKTREE_DIR")"
		git -C "$REPO_ROOT" worktree add --detach --quiet "$WORKTREE_DIR" "$BASELINE_SHA" ||
			die "failed to create git worktree for $BASELINE_DESC.
       If this clone is shallow or partial, the baseline's trees may not be
       present locally; fetch full history (actions/checkout fetch-depth: 0)."
	fi
fi

# What the "current" side is built from. In baseline mode with BASELINE_REF set
# this is the pinned worktree, so the stored baseline is reproducible.
if [[ "$MODE" == baseline && -n "$BASELINE_SHA" ]]; then
	CUR_SRC_DIR="$WORKTREE_DIR"
	CURRENT_SHA="$BASELINE_SHA"
	CURRENT_DIRTY="clean (pinned to $BASELINE_DESC)"
else
	CUR_SRC_DIR="$REPO_ROOT"
	CURRENT_SHA="$(git -C "$REPO_ROOT" rev-parse HEAD)"
	CURRENT_DIRTY="clean"
	git -C "$REPO_ROOT" diff --quiet HEAD -- || CURRENT_DIRTY="dirty (uncommitted changes)"
	if [[ "$MODE" == baseline && "$CURRENT_DIRTY" != clean ]]; then
		warn "recording a baseline from a DIRTY working tree; it will not be reproducible."
		warn "Pin a committed revision instead: BASELINE_REF=<sha|tag> make bench-baseline"
	fi
fi

# ---------------------------------------------------------------------------
# Build. Both sides use GOWORK=off so neither picks up the shared go.work; the
# binaries then differ only by source revision.
# ---------------------------------------------------------------------------

slug() {
	# ./pkg/internal/raft/ -> pkg_internal_raft
	local p="${1#./}"
	p="${p%/}"
	printf '%s' "${p//[\/.]/_}"
}

declare -a PKG_LIST=()
read -r -a PKG_LIST <<<"$BENCH_PKGS"
[[ "${#PKG_LIST[@]}" -gt 0 ]] || die "BENCH_PKGS is empty"

declare -a SLUGS=()
for pkg in "${PKG_LIST[@]}"; do
	s="$(slug "$pkg")"
	SLUGS+=("$s")

	info "building current  $pkg"
	(cd "$CUR_SRC_DIR" && GOWORK=off go test -c -o "$LOCAL_TMP/cur-$s.test" "$pkg") ||
		die "failed to build current test binary for $pkg"

	if [[ "$MODE" == compare ]]; then
		info "building baseline $pkg"
		(cd "$WORKTREE_DIR" && GOWORK=off go test -c -o "$LOCAL_TMP/base-$s.test" "$pkg") ||
			die "failed to build baseline test binary for $pkg at $BASELINE_DESC.
       Does that package exist at the baseline revision? If the package was
       added after the baseline, narrow BENCH_PKGS or pick a newer BASELINE."
	fi

	# `go test -c` writes nothing when a package has no test files at all.
	[[ -x "$LOCAL_TMP/cur-$s.test" ]] ||
		die "no test binary produced for $pkg (package has no tests?)"
done

# ---------------------------------------------------------------------------
# Ship binaries to PERF_HOST (remote mode only).
# ---------------------------------------------------------------------------

if [[ -n "$PERF_HOST" ]]; then
	info "shipping test binaries to $PERF_HOST:$REMOTE_TMP"
	scp -q "${SSH_OPTS[@]}" "$LOCAL_TMP"/*.test "$PERF_HOST:$REMOTE_TMP/" ||
		die "failed to copy test binaries to $PERF_HOST"
fi

# ---------------------------------------------------------------------------
# Execution. One invocation = one sample for every benchmark in the package.
# ---------------------------------------------------------------------------

BENCH_ARGS=(-test.run='^$' -test.bench="$BENCH_FILTER" -test.benchmem
	-test.benchtime="$BENCH_ITERS" -test.timeout="$BENCH_TIMEOUT")

run_sample() {
	local binary="$1" outfile="$2" label="$3"
	if [[ -n "$PERF_HOST" ]]; then
		# The test binary is self-contained; run it from the remote temp dir.
		ssh "${SSH_OPTS[@]}" "$PERF_HOST" \
			"cd '$REMOTE_TMP' && ./'$binary' $(printf '%q ' "${BENCH_ARGS[@]}")" >>"$outfile" ||
			die "$label sample failed on $PERF_HOST (see $outfile)"
	else
		"$LOCAL_TMP/$binary" "${BENCH_ARGS[@]}" >>"$outfile" ||
			die "$label sample failed locally (see $outfile)"
	fi
}

info "running $BENCH_ROUNDS interleaved round(s), $BENCH_ITERS iterations, filter '$BENCH_FILTER'"
for ((round = 1; round <= BENCH_ROUNDS; round++)); do
	for s in "${SLUGS[@]}"; do
		# Interleave: current then baseline, every round. Never two contiguous
		# blocks — that is what manufactures phantom regressions.
		info "round $round/$BENCH_ROUNDS  $s  current"
		run_sample "cur-$s.test" "$CUR_RESULTS" "current/$s round $round"

		# Fail after the FIRST sample rather than burning every round on a
		# filter that matches nothing. Note that `make` expands '$' in
		# variables: pass a regex containing '$|' via the Makefile and it
		# arrives mangled unless the recipe uses $(value ...).
		if [[ "$round" -eq 1 && "$s" == "${SLUGS[0]}" ]]; then
			grep -q '^Benchmark' "$CUR_RESULTS" ||
				die "BENCH_FILTER='$BENCH_FILTER' matched no benchmarks in ${PKG_LIST[0]}.
       Check the regex (note it is applied verbatim as -test.bench)."
		fi
		if [[ "$MODE" == compare ]]; then
			info "round $round/$BENCH_ROUNDS  $s  baseline"
			run_sample "base-$s.test" "$BASE_RESULTS" "baseline/$s round $round"
		fi
	done
done

# Fail loudly rather than emitting an empty comparison: an over-narrow
# BENCH_FILTER is otherwise indistinguishable from "no regression".
grep -q '^Benchmark' "$CUR_RESULTS" ||
	die "no benchmark results produced. Does BENCH_FILTER='$BENCH_FILTER' match anything in $BENCH_PKGS?"

if [[ "$MODE" == compare ]]; then
	grep -q '^Benchmark' "$BASE_RESULTS" ||
		die "no baseline benchmark results produced at $BASELINE_DESC (filter '$BENCH_FILTER')"
fi

# ---------------------------------------------------------------------------
# Provenance. A baseline without provenance is worse than no baseline: it
# invites comparison against numbers from unknown hardware.
# ---------------------------------------------------------------------------

if [[ "$MODE" == compare ]]; then
	INVOCATION="BASELINE='$BASELINE'"
else
	INVOCATION="BASELINE_REF='${BASELINE_REF:-}'"
fi
INVOCATION="$INVOCATION PERF_HOST='$PERF_HOST' BENCH_PKGS='$BENCH_PKGS'"
INVOCATION="$INVOCATION BENCH_FILTER='$BENCH_FILTER' BENCH_ROUNDS='$BENCH_ROUNDS'"
INVOCATION="$INVOCATION BENCH_ITERS='$BENCH_ITERS' make bench-$MODE"

{
	echo "# quicraft benchmark run"
	echo "# generated:     $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "# mode:          $MODE"
	echo "# run-host:      $RUN_HOSTNAME  ($([[ -z "$PERF_HOST" ]] && echo 'local' || echo "PERF_HOST=$PERF_HOST"))"
	echo "# cpu-model:     $RUN_CPU_MODEL"
	echo "# cpu-count:     $RUN_CPU_COUNT"
	echo "# governor:      $RUN_GOVERNOR"
	echo "# loadavg:       $RUN_LOADAVG  (at start of run)"
	echo "# build-host:    $(hostname)"
	echo "# go-version:    $GO_VERSION"
	echo "# current-ref:   $CURRENT_SHA  [$CURRENT_DIRTY]"
	[[ "$MODE" == compare ]] && echo "# baseline-ref:  $BASELINE_SHA  ($BASELINE)"
	echo "# packages:      $BENCH_PKGS"
	echo "# bench-filter:  $BENCH_FILTER"
	echo "# rounds:        $BENCH_ROUNDS (interleaved)"
	echo "# iterations:    $BENCH_ITERS (fixed)"
	echo "# command:       $INVOCATION"
} >"$META_OUT"

# ---------------------------------------------------------------------------
# Comparison.
# ---------------------------------------------------------------------------

# Fallback comparison used only when benchstat is unavailable. It reports the
# median ns/op per benchmark and the percentage delta. It is deliberately much
# weaker than benchstat: no confidence intervals, no significance test. A small
# delta here means nothing.
median_compare() {
	local base="$1" cur="$2"
	awk '
		function median(arr, n,   i, tmp, j, k, t) {
			for (i = 1; i <= n; i++) tmp[i] = arr[i]
			for (i = 2; i <= n; i++) { t = tmp[i]; for (j = i-1; j >= 1 && tmp[j] > t; j--) tmp[j+1] = tmp[j]; tmp[j+1] = t }
			if (n % 2) return tmp[(n+1)/2]
			return (tmp[n/2] + tmp[n/2+1]) / 2
		}
		/^Benchmark/ {
			name = $1
			sub(/-[0-9]+$/, "", name)
			for (i = 2; i <= NF; i++) {
				if ($i == "ns/op") {
					if (FILENAME == BASEFILE) { bn[name]++; b[name, bn[name]] = $(i-1) + 0 }
					else                      { cn[name]++; c[name, cn[name]] = $(i-1) + 0 }
				}
			}
		}
		END {
			printf "%-46s %14s %14s %10s %8s\n", "benchmark", "baseline ns/op", "current ns/op", "delta", "n"
			for (name in cn) {
				if (!(name in bn)) { printf "%-46s %14s %14s %10s %8d\n", name, "-", "(new)", "-", cn[name]; continue }
				for (i = 1; i <= bn[name]; i++) ba[i] = b[name, i]
				for (i = 1; i <= cn[name]; i++) ca[i] = c[name, i]
				mb = median(ba, bn[name]); mc = median(ca, cn[name])
				d = (mb > 0) ? (mc - mb) / mb * 100 : 0
				printf "%-46s %14.1f %14.1f %+9.2f%% %8d\n", name, mb, mc, d, cn[name]
			}
			for (name in bn) if (!(name in cn)) printf "%-46s %14s %14s %10s %8d\n", name, "(removed)", "-", "-", bn[name]
		}
	' BASEFILE="$base" "$base" "$cur" | { read -r hdr; printf '%s\n' "$hdr"; sort; }
}

if [[ "$MODE" == compare ]]; then
	if [[ -n "$BENCHSTAT" ]]; then
		info "comparing with benchstat"
		"$BENCHSTAT" "baseline=$BASE_RESULTS" "current=$CUR_RESULTS" >"$COMPARE_OUT" ||
			die "benchstat failed"
	else
		warn "benchstat not found; falling back to a median/delta table."
		warn "The fallback has NO confidence intervals and NO significance test."
		warn "Install it with: go install golang.org/x/perf/cmd/benchstat@latest"
		{
			echo "# benchstat unavailable - median/delta fallback (no statistics)"
			median_compare "$BASE_RESULTS" "$CUR_RESULTS"
		} >"$COMPARE_OUT"
	fi
else
	# baseline mode: summarise the single side so the stored file is readable.
	if [[ -n "$BENCHSTAT" ]]; then
		"$BENCHSTAT" "$CUR_RESULTS" >"$COMPARE_OUT" || die "benchstat failed"
	else
		cp "$CUR_RESULTS" "$COMPARE_OUT"
	fi
fi

# ---------------------------------------------------------------------------
# Store a committed baseline (baseline mode).
# ---------------------------------------------------------------------------

if [[ "$MODE" == baseline ]]; then
	label="${BASELINE_LABEL:-$RUN_HOSTNAME-$(git -C "$REPO_ROOT" rev-parse --short "$CURRENT_SHA")}"
	dest="$REPO_ROOT/$BASELINE_DIR/${label}.txt"
	mkdir -p "$(dirname "$dest")"
	{
		cat "$META_OUT"
		echo "#"
		echo "# Raw samples follow in Go benchmark format. Diff a future run against"
		echo "# this file with:"
		echo "#   benchstat $BASELINE_DIR/${label}.txt build/bench-compare/<run>/current.txt"
		echo "#"
		cat "$CUR_RESULTS"
	} >"$dest"
	info "stored baseline: ${dest#"$REPO_ROOT"/}"
fi

# ---------------------------------------------------------------------------
# Report.
# ---------------------------------------------------------------------------

echo
cat "$META_OUT"
echo
cat "$COMPARE_OUT"
echo
info "artifacts in ${BENCH_OUT_DIR#"$REPO_ROOT"/}"
