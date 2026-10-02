#!/usr/bin/env bash
# run-remote.sh — measure a machine profile on the machine it names.
#
# One command drives the whole thing:
#
#   make perf-remote PERF_HOST=user@host PERF_PROFILE=<profile>
#
# which is this script's "all": stage the working tree onto the target, capture
# the target's specifications, run the benchmark suite there, fetch the results
# AND the capture back into profiles/<name>/, and regenerate that profile's
# documents locally.
#
# The profile name is an argument because hardware description cannot tell
# which machine profile a host belongs to. Everything else about the machine is
# captured on the target at run time (see capture-env.sh), so the
# specification table beside a number is never hand-maintained.
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
PROFILE="${PERF_PROFILE:?PERF_PROFILE is required (machine profile name, e.g. bench-01)}"

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
# characterize rather than on a container overlay. The default under $HOME is
# chosen because the login user can create it on any host; override it to put
# the WAL on the storage you actually want to measure.
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
# The suite's exit status, written by the wrapper below once perfcompare exits.
# wait_for_run returns `tail`'s status, not the benchmark's.
RUN_RC="$REMOTE_PERF/bench-run.rc"
# RUN_RC_ABS is RUN_RC as a path that means the same from any directory on the
# target. The status file is written from a subshell that has already cd'd
# into $REMOTE_PERF, so a relative RUN_RC is anchored at $HOME. An absolute
# REMOTE_DIR is left as it is.
case "$RUN_RC" in
  /*) RUN_RC_ABS="$RUN_RC" ;;
  *)  RUN_RC_ABS="\$HOME/$RUN_RC" ;;
esac

say() { echo ">> $*"; }

# module_auth prints this environment's module credentials as lines a shell
# sources, with perfcompare's precedence: a composed GOMODAUTH payload verbatim
# (already sourceable K='V' lines), else GOPRIVATE and the GIT_CONFIG_* rewrite,
# only when the rewrite is set. It reports presence on stderr, never content.
module_auth() {
  if [ -n "${GOMODAUTH:-}" ]; then
    say "module credentials: forwarding the composed GOMODAUTH payload to $HOST over stdin" >&2
    printf '%s\n' "$GOMODAUTH"
    return 0
  fi
  if [ -z "${GIT_CONFIG_KEY_0:-}" ]; then
    say "module credentials: none in this environment; the bench host builds against the public proxy only" >&2
    return 0
  fi
  say "module credentials: forwarding the 4 variables to $HOST over stdin" >&2
  for k in GOPRIVATE GIT_CONFIG_COUNT GIT_CONFIG_KEY_0 GIT_CONFIG_VALUE_0; do
    printf '%s=%s\n' "$k" "${!k:-}"
  done
}

stage() {
  # Refuse to stage over an active run: rsync --delete below would clobber the
  # suite's working tree mid-flight.
  #
  # `[p]erfcompare` rather than `perfcompare`: pgrep -f matches full command
  # lines, and ssh runs this via `bash -c`, so the plain pattern would match
  # the check's own shell.
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
  # context by it. The profiles tree is excluded in both directions, so --delete
  # can never destroy a stored result or a hand-written analysis.
  # The credential file is excluded and any earlier copy removed by hand
  # (--delete never touches an excluded path). Credentials cross on stdin.
  rsync -a --delete \
    --exclude '.devcontainer/.env' \
    --exclude 'test/performance/openraft/target/' \
    --exclude 'test/performance/profiles/' \
    --exclude 'test/performance/perfcompare' \
    --exclude 'test/performance/perfcompare.remote' \
    --exclude 'build/' \
    --exclude '.build-cache/' \
    --exclude 'test/performance/bench-run.log' \
    "$REPO_ROOT/" "$HOST:$REMOTE_DIR/"
  ssh "$HOST" "rm -f '$REMOTE_DIR/.devcontainer/.env'"
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

  # Preflight the scratch directory in its own command. As the head of a
  # backgrounded && chain, a failed mkdir would take the launch with it while
  # the pidfile still got written.
  if ! ssh "$HOST" "mkdir -p \"$BENCH_TMPDIR\"" 2>&1; then
    echo "ERROR: cannot create the scratch directory $BENCH_TMPDIR on $HOST." >&2
    echo "       Set BENCH_TMPDIR to a path the login user can create." >&2
    exit 1
  fi

  # The cd is confined to a subshell, because $RUN_PID and $RUN_LOG are
  # relative to the login home.
  #
  # The subshell runs perfcompare and then records its exit status, so $! is
  # the wrapper's pid. The wrapper exits right after perfcompare, so
  # `tail --pid` in wait_for_run returns after the status file exists.
  #
  # A stale $RUN_RC is removed first: a previous run's status would look
  # authoritative. It is written via RUN_RC_ABS because the subshell has
  # already cd'd into $REMOTE_PERF.
  ssh "$HOST" "rm -f \"$RUN_RC_ABS\""
  # Module credentials cross to the bench host on ssh's stdin, never its
  # command line. The remote shell sources stdin under set -a, so the token
  # reaches perfcompare's environment and nowhere else: not `ps` on either
  # host, not a file, not the run log. With no credentials the pipe is empty
  # and the build resolves from the public proxy only.
  module_auth | ssh "$HOST" "set -a; . /dev/stdin; set +a; \
    ( cd '$REMOTE_PERF' && BENCH_TMPDIR=\"$BENCH_TMPDIR\" \
      nohup sh -c './perfcompare --benchtime=\"$BENCHTIME\" --timeout=\"$PERF_TIMEOUT\" \
        --profile=\"$PROFILE_REL\" $PERF_SYSTEMS; echo \$? > \"$RUN_RC_ABS\"' ) > '$RUN_LOG' 2>&1 < /dev/null & \
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
  # The container consoles stay on the target, and a failed run produces only
  # those, so list them beside the results.
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
# fetch overwrites the local results and generate rewrites the profile's charts
# and documents, so a failed run would otherwise produce a "refreshed" profile
# with no new measurement behind it. A profile is a published record; when the
# run meant to update it failed, it stays as it was.
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
