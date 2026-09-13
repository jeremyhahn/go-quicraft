#!/usr/bin/env bash
# run-remote.sh — measure a machine profile on the machine it names.
#
# One command drives the whole thing:
#
#   make perf-remote PERF_HOST=user@host PERF_PROFILE=omega
#
# which is this script's "all": stage the working tree onto the target, capture
# the target's specifications, run the benchmark suite there, fetch the results
# AND the capture back into profiles/<name>/, and regenerate that profile's
# documents locally.
#
# The profile name is an argument because it cannot be derived. lscpu describes
# hardware; it does not know which of our machines is the desktop-class
# reference box and which is the dual-socket server. Everything else about the
# machine IS derived, on the target, at run time — see capture-env.sh — so the
# specification table beside a number is never hand-maintained and can never go
# stale.
#
# The benchmark itself runs under nohup on the target and this script tails it.
# A dropped ssh session therefore costs you the tail, not the run; reconnect
# with './run-remote.sh status' or './run-remote.sh wait'.
#
# Individual phases, for when you want just one:
#
#   ./run-remote.sh stage      rsync the working tree to the target
#   ./run-remote.sh env        capture the target's specs into the profile
#   ./run-remote.sh run        start the suite there, in the background
#   ./run-remote.sh wait       stream the remote log until the run exits
#   ./run-remote.sh status     tail the remote log once
#   ./run-remote.sh fetch      copy results and env.json back
#   ./run-remote.sh generate   regenerate this profile's documents locally
set -euo pipefail

# ssh destination of the machine being measured. Same variable name as the
# bench-compare harness (scripts/bench-compare.sh) so one export covers both.
# There is no default: a benchmark whose machine is implicit is a benchmark
# whose numbers cannot be interpreted.
HOST="${PERF_HOST:?PERF_HOST is required (ssh destination of the bench machine, e.g. user@host)}"

# The machine profile these results belong to. Operator-supplied, always.
PROFILE="${PERF_PROFILE:?PERF_PROFILE is required (machine profile name, e.g. omega or r640)}"

# Staging directory on the bench host. Relative paths resolve against the remote
# login's home directory (both ssh and rsync do this); override with an absolute
# path if the staging tree belongs elsewhere.
REMOTE_DIR="${REMOTE_DIR:-bench/go-quicraft}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PERF_DIR="$REPO_ROOT/test/performance"
BENCHTIME="${BENCHTIME:-30s}"
PERF_TIMEOUT="${PERF_TIMEOUT:-7200s}"
# Systems to run, and the systems the generated documents must cover. Narrow it
# explicitly when a profile is genuinely missing an engine; generation refuses
# to run — rather than quietly write a partial set — if anything named here has
# no results.
PERF_SYSTEMS="${PERF_SYSTEMS:-all}"
# Scratch dir on the bench host for every engine's WAL (bind-mounted into each
# container as /bench-tmp), so fsync I/O lands on the storage tier you mean to
# characterize rather than on a container overlay.
#
# The default must be creatable by the login user on ANY bench host, which is
# why it is under $HOME rather than a mount point. It used to default to
# /mnt/disk1/bench-tmp, a path that exists on one particular server; everywhere
# else `mkdir -p` failed on the missing mount and the run never started.
# Override it to put the WAL on the storage tier you actually mean to
# characterize -- $HOME is correct for portability, not necessarily for I/O.
BENCH_TMPDIR="${BENCH_TMPDIR:-\$HOME/bench-tmp}"
# Target platform for the cross-built orchestrator. The bench host needs Docker
# but no Go toolchain.
PERF_GOOS="${PERF_GOOS:-linux}"
PERF_GOARCH="${PERF_GOARCH:-amd64}"

PROFILE_REL="profiles/$PROFILE"
LOCAL_PROFILE="$PERF_DIR/$PROFILE_REL"
REMOTE_PERF="$REMOTE_DIR/test/performance"
RUN_LOG="$REMOTE_PERF/bench-run.log"
RUN_PID="$REMOTE_PERF/bench-run.pid"
# The suite's EXIT STATUS, written by the wrapper below once perfcompare exits.
# Without it a failed run was indistinguishable from a successful one to this
# script: wait_for_run returns `tail`'s status, not the benchmark's (#181).
RUN_RC="$REMOTE_PERF/bench-run.rc"
# RUN_RC_ABS is RUN_RC as a path that means the same thing from ANY directory
# on the target. The status file is written from inside a subshell that has
# already cd'd into $REMOTE_PERF, and read back from the login home, so a
# relative RUN_RC needs anchoring at $HOME -- but only a relative one. The
# documented REMOTE_DIR override may be absolute (/mnt/bench), and prefixing
# that gives $HOME//mnt/bench/..., a directory staging never created, so every
# run would report "no exit status" and the gate would refuse everything.
case "$RUN_RC" in
  /*) RUN_RC_ABS="$RUN_RC" ;;
  *)  RUN_RC_ABS="\$HOME/$RUN_RC" ;;
esac

say() { echo ">> $*"; }

stage() {
  # Refuse to stage over an active run: rsync --delete below would clobber the
  # suite's working tree mid-flight.
  #
  # The bracket around the first character is load-bearing. `pgrep -f` matches
  # full command lines, and ssh runs this check via `bash -c '<the command>'`,
  # so a plain `pgrep -f perfcompare` MATCHES ITSELF -- the guard fired on every
  # invocation and staging could never proceed. `[p]erfcompare` is a regex that
  # matches the text "perfcompare" while the command line carrying it does not
  # contain that text, so the check sees real runs and not its own shell.
  if ssh "$HOST" "pgrep -f '[p]erfcompare' >/dev/null 2>&1"; then
    echo "ERROR: a perfcompare run is active on $HOST; refusing to stage" >&2
    exit 1
  fi

  say "cross-building perfcompare ($PERF_GOOS/$PERF_GOARCH)"
  ( cd "$PERF_DIR" && GOWORK=off GOOS="$PERF_GOOS" GOARCH="$PERF_GOARCH" \
      go build -o "$PERF_DIR/perfcompare.remote" ./cmd/perfcompare/ )

  say "rsyncing working tree -> $HOST:$REMOTE_DIR"
  ssh "$HOST" "mkdir -p '$REMOTE_DIR'"
  # .git is included: the orchestrator's findRepoRoot locates the Docker build
  # context by it. The profiles tree is excluded in both directions — on the
  # target it is output only, so --delete can never reach back and destroy a
  # stored result or a hand-written analysis.
  rsync -a --delete \
    --exclude 'test/performance/openraft/target/' \
    --exclude 'test/performance/profiles/' \
    --exclude 'test/performance/perfcompare' \
    --exclude 'test/performance/perfcompare.remote' \
    --exclude 'build/' \
    --exclude '.build-cache/' \
    --exclude 'test/performance/bench-run.log' \
    "$REPO_ROOT/" "$HOST:$REMOTE_DIR/"
  scp -q "$PERF_DIR/perfcompare.remote" "$HOST:$REMOTE_PERF/perfcompare"
  ssh "$HOST" "chmod +x '$REMOTE_PERF/perfcompare' '$REMOTE_PERF/capture-env.sh'"
  rm -f "$PERF_DIR/perfcompare.remote"
  say "stage complete"
}

env_capture() {
  say "capturing $PROFILE specifications on $HOST"
  ssh "$HOST" "mkdir -p '$REMOTE_PERF/$PROFILE_REL' && \
    cd '$REMOTE_PERF' && ./capture-env.sh '$PROFILE' > '$PROFILE_REL/env.json'"
  ssh "$HOST" "cat '$REMOTE_PERF/$PROFILE_REL/env.json'"
}

run() {
  say "launching suite on $HOST (profile=$PROFILE systems=$PERF_SYSTEMS benchtime=$BENCHTIME WAL=$BENCH_TMPDIR)"

  # Preflight the scratch directory in its OWN command, not as the head of a
  # backgrounded && chain. It used to be `mkdir -p ... && cd ... && nohup ... &`,
  # where the & backgrounds the WHOLE list: a mkdir that failed took the launch
  # with it silently, and the `echo $!` that followed still wrote a pidfile --
  # for the subshell, not for perfcompare. A failed launch was therefore
  # indistinguishable from a running one, and `wait` sat tailing a log that
  # would never be created against a pid that was already gone.
  if ! ssh "$HOST" "mkdir -p \"$BENCH_TMPDIR\"" 2>&1; then
    echo "ERROR: cannot create the scratch directory $BENCH_TMPDIR on $HOST." >&2
    echo "       Set BENCH_TMPDIR to a path the login user can create." >&2
    exit 1
  fi

  # The cd is confined to a SUBSHELL. $RUN_PID and $RUN_LOG are relative to the
  # login home, so a cd left in the foreground would make the pidfile write
  # land somewhere that does not exist and silently produce no pidfile at all.
  #
  # The subshell no longer `exec`s perfcompare: it runs it and then records the
  # exit status, so $! is a one-line wrapper's pid rather than perfcompare's.
  # That is deliberate (#181), and it preserves the property the exec was
  # protecting -- the pidfile must name a process whose death means the run is
  # over. The wrapper does nothing after perfcompare but write $RUN_RC, so it
  # dies microseconds later, and `tail --pid` in wait_for_run now returns AFTER
  # the status file exists rather than racing it.
  #
  # A stale $RUN_RC is removed first. Reading a previous run's status would be
  # worse than reading none, because it looks authoritative.
  # $RUN_RC is written with a path anchored at $HOME on the target, NOT the
  # bare relative one. Every other remote path here is relative to the login
  # home and is used from there, but this one is written from INSIDE the
  # subshell that has already cd'd into $REMOTE_PERF -- so a relative path
  # would land at $REMOTE_PERF/$REMOTE_PERF/... and require_run_success, which
  # reads from the home directory, would never find it. The failure mode is
  # every run reporting "recorded no exit status", i.e. the gate refusing
  # everything.
  ssh "$HOST" "rm -f \"$RUN_RC_ABS\""
  ssh "$HOST" "( cd '$REMOTE_PERF' && BENCH_TMPDIR=\"$BENCH_TMPDIR\" \
      nohup sh -c './perfcompare --benchtime=\"$BENCHTIME\" --timeout=\"$PERF_TIMEOUT\" \
        --profile=\"$PROFILE_REL\" $PERF_SYSTEMS; echo \$? > \"$RUN_RC_ABS\"' ) > '$RUN_LOG' 2>&1 & \
    echo \$! > '$RUN_PID'; echo \"PID \$(cat '$RUN_PID')\""

  # Confirm it is actually running. Launching and reporting a pid proves only
  # that a fork happened; a binary that dies on its first flag leaves the same
  # trace. Count the live process, and show the log if it is already gone.
  sleep 3
  if ! ssh "$HOST" "pid=\$(cat '$RUN_PID' 2>/dev/null); \
      [ -n \"\$pid\" ] && kill -0 \"\$pid\" 2>/dev/null"; then
    echo "ERROR: the suite exited immediately on $HOST. Its output:" >&2
    ssh "$HOST" "tail -40 '$RUN_LOG' 2>/dev/null || echo '(no $RUN_LOG was created)'" >&2
    exit 1
  fi
}

wait_for_run() {
  say "streaming $RUN_LOG until the run exits"
  # tail --pid returns when the benchmark process does, so this blocks for
  # exactly as long as the run and no longer.
  ssh "$HOST" "pid=\$(cat '$RUN_PID' 2>/dev/null); \
    if [ -z \"\$pid\" ]; then echo 'no recorded run'; exit 1; fi; \
    tail -n +1 -f --pid=\"\$pid\" '$RUN_LOG'"
}

status() {
  # The container consoles are listed beside the results because a failed run
  # produces the second and not the first, and they stay on the target: nothing
  # is fetched for a run that failed. Naming the path here is the difference
  # between "dragonboat exited 1" and being able to read why (#184).
  ssh "$HOST" "tail -40 '$RUN_LOG' 2>/dev/null; echo '--- results ---'; \
    ls -la '$REMOTE_PERF/$PROFILE_REL/results/' 2>/dev/null || echo 'none yet'; \
    echo '--- container consoles ---'; \
    ls -la '$REMOTE_PERF/$PROFILE_REL/logs/' 2>/dev/null || echo 'none yet'"
  echo ">> read one with: ssh $HOST \"cat $REMOTE_PERF/$PROFILE_REL/logs/<system>.log\""
}

fetch() {
  mkdir -p "$LOCAL_PROFILE/results"
  say "fetching results and env.json into $PROFILE_REL/"
  # No --delete: the local profile also holds analysis.md, which is the human's
  # and has never been on the target.
  rsync -a "$HOST:$REMOTE_PERF/$PROFILE_REL/results/" "$LOCAL_PROFILE/results/"
  rsync -a "$HOST:$REMOTE_PERF/$PROFILE_REL/env.json" "$LOCAL_PROFILE/env.json"
  ls -la "$LOCAL_PROFILE"
}

generate() {
  say "regenerating $PROFILE_REL documents"
  ( cd "$PERF_DIR" && GOWORK=off go build -o perfcompare ./cmd/perfcompare/ && \
    ./perfcompare generate --profile="$PROFILE_REL" --systems="$PERF_SYSTEMS" )
}

# require_run_success refuses to touch the local profile when the suite failed.
#
# This is the other half of #181, and the more insidious half. A failed run is
# not inert: fetch copies the target's results over the local ones and generate
# rewrites the profile's charts, per-system documents and env.json. On
# 2026-09-10 a run whose ONLY requested system produced no results still
# rewrote five charts, three results/*.json, env.json and two documents -- a
# "refreshed" profile built on no new measurement, which only a git status
# caught before it was committed.
#
# A profile is a published record. When the run that was meant to update it
# failed, the right outcome is that it stays exactly as it was.
require_run_success() {
  local rc
  rc="$(ssh "$HOST" "cat \"$RUN_RC_ABS\" 2>/dev/null" || true)"
  if [ -z "$rc" ]; then
    echo "ERROR: the suite recorded no exit status at $RUN_RC on $HOST." >&2
    echo "       Treating that as a failure: the local profile is UNCHANGED." >&2
    echo "       Inspect with '$0 status'." >&2
    exit 1
  fi
  if [ "$rc" -ne 0 ]; then
    echo "ERROR: the suite exited $rc on $HOST. The local profile is UNCHANGED:" >&2
    echo "       no results fetched, no documents regenerated." >&2
    echo "       The failing systems are named on the ERROR lines in the log;" >&2
    echo "       inspect with '$0 status', then re-run when they are addressed." >&2
    exit "$rc"
  fi
  say "suite exited 0"
}

all() {
  stage
  env_capture
  run
  wait_for_run
  require_run_success
  fetch
  generate
}

case "${1:-all}" in
  all)      all ;;
  stage)    stage ;;
  env)      env_capture ;;
  run)      run ;;
  wait)     wait_for_run ;;
  status)   status ;;
  fetch)    fetch ;;
  rc)       require_run_success ;;
  generate) generate ;;
  *) echo "usage: $0 {all|stage|env|run|wait|rc|status|fetch|generate}" >&2; exit 1 ;;
esac
