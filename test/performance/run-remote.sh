#!/usr/bin/env bash
# run-remote.sh — provision the fairness-normalized benchmark suite onto the
# dedicated bench server (ci-node-1) and run it in Docker.
#
# The bench server has Docker but no Go/Rust toolchain, so the perfcompare
# orchestrator is cross-compiled locally and shipped alongside the source. The
# dragonboat fork is pre-staged into .build-cache/dragonboat so the remote
# Docker build needs no extra source.
#
# Usage:
#   ./run-remote.sh stage     # rsync repo + dragonboat fork to the server
#   ./run-remote.sh run       # start the full 30s suite in the background
#   ./run-remote.sh status    # tail the remote run log
#   ./run-remote.sh fetch     # copy JSON results back into ./results
set -euo pipefail

# ssh destination of the machine that runs the suite. Same variable name as the
# bench-compare harness (scripts/bench-compare.sh) so one export covers both.
# Unlike bench-compare this suite has no local mode: it rsyncs a working tree and
# drives Docker on the target, so a host is always required.
HOST="${PERF_HOST:-ci-node-1}"
REMOTE_DIR="${REMOTE_DIR:-/home/jhahn/bench/go-quicraft}"
DRAGONBOAT_SRC="${DRAGONBOAT_SRC:-/home/jhahn/sources/dragonboat}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BENCHTIME="${BENCHTIME:-30s}"
PERF_TIMEOUT="${PERF_TIMEOUT:-7200s}"
# NVMe-backed scratch dir on the bench host. All engines write their WAL here
# (bind-mounted into each container as /bench-tmp) so fsync I/O lands on NVMe,
# matching the production storage tier and the prior R640 baseline.
BENCH_TMPDIR="${BENCH_TMPDIR:-/mnt/disk1/bench-tmp}"

stage() {
  # Refuse to stage over an active run: rsync --delete below would clobber
  # the suite's working tree and results mid-flight.
  if ssh "$HOST" "pgrep -f perfcompare >/dev/null 2>&1"; then
    echo "ERROR: a perfcompare run is active on $HOST; refusing to stage" >&2
    exit 1
  fi

  echo ">> staging dragonboat fork into .build-cache"
  mkdir -p "$REPO_ROOT/.build-cache"
  rsync -a --delete --exclude '.git' "$DRAGONBOAT_SRC/" "$REPO_ROOT/.build-cache/dragonboat/"

  echo ">> cross-building perfcompare (linux/amd64)"
  ( cd "$REPO_ROOT/test/performance" && GOWORK=off GOOS=linux GOARCH=amd64 \
      go build -o /tmp/perfcompare.linux ./cmd/perfcompare/ )

  echo ">> rsyncing repo -> $HOST:$REMOTE_DIR"
  ssh "$HOST" "mkdir -p '$REMOTE_DIR'"
  # Include .git (orchestrator's findRepoRoot needs it) and .build-cache; skip
  # heavy/irrelevant trees. The remote results dir and run log are excluded so
  # --delete never destroys data from previous runs when re-staging.
  rsync -a --delete \
    --exclude 'test/performance/openraft/target/' \
    --exclude 'build/' \
    --exclude 'test/performance/results/' \
    --exclude 'test/performance/bench-run.log' \
    "$REPO_ROOT/" "$HOST:$REMOTE_DIR/"
  scp /tmp/perfcompare.linux "$HOST:$REMOTE_DIR/test/performance/perfcompare"
  ssh "$HOST" "chmod +x '$REMOTE_DIR/test/performance/perfcompare'"
  echo ">> stage complete"
}

run() {
  echo ">> launching full suite on $HOST (benchtime=$BENCHTIME, WAL on $BENCH_TMPDIR)"
  ssh "$HOST" "mkdir -p '$BENCH_TMPDIR' && cd '$REMOTE_DIR/test/performance' && \
    BENCH_TMPDIR='$BENCH_TMPDIR' \
    nohup ./perfcompare --benchtime='$BENCHTIME' --timeout='$PERF_TIMEOUT' \
      --json-dir=results all > bench-run.log 2>&1 & echo \"PID \$!\""
  echo ">> started; use './run-remote.sh status' to watch"
}

status() {
  ssh "$HOST" "tail -40 '$REMOTE_DIR/test/performance/bench-run.log'; \
    echo '--- json ---'; ls -la '$REMOTE_DIR/test/performance/results/' 2>/dev/null || true"
}

fetch() {
  mkdir -p "$REPO_ROOT/test/performance/results"
  rsync -a "$HOST:$REMOTE_DIR/test/performance/results/" \
    "$REPO_ROOT/test/performance/results/"
  echo ">> fetched results into test/performance/results/"
}

case "${1:-}" in
  stage) stage ;;
  run) run ;;
  status) status ;;
  fetch) fetch ;;
  *) echo "usage: $0 {stage|run|status|fetch}" >&2; exit 1 ;;
esac
