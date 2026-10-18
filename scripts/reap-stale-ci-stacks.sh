#!/usr/bin/env bash
# Remove this repo's stopped CI leftovers on the runner host.
#
#   usage: scripts/reap-stale-ci-stacks.sh <prefix> [min-age-seconds]
#
# v1 (ci-template v1.18, dept-infra 2026-10-12); v2 below. An integration job's
# `if: always()` teardown runs only while the job process lives. When the host
# or its Docker daemon dies mid-job, the job is gone and so is its teardown: the
# compose stack stays behind, exited, with its networks and volumes (11 stacks
# on two runner hosts after the 2026-10-11/12 restarts). Nothing else
# removes them, so the first step of every integration job runs this.
#
# What it removes, on the Docker host the job talks to:
#   - a compose project (label com.docker.compose.project) whose name starts
#     with <prefix>, when EVERY container in it is stale; then the project's
#     networks and volumes (same label);
#   - a container outside compose whose name starts with <prefix>, when stale,
#     with its anonymous volumes.
# v2 (ci-template v1.23, Company Agent m674 item 3, 2026-10-18; the go-qrdb
# #814 pattern): images too. The integration image is named by its content,
# <prefix>it:<hash> (`make -s it-image`, ci-standard.md §9), and every job and
# run of that tree on this runner reuses it, so no job removes it. This
# removes a <prefix>it image created at least REAP_IMAGE_MIN_AGE seconds ago,
# default 7200 (2 h: the longest full-tier run plus headroom), so an image a
# run still needs is not old enough. `docker image rm` without force refuses
# an image a container (running or stopped) uses, so a live stack's image is
# never removed. Age alone does not protect a reused image older than that,
# so every job that needs the image holds a lease on it: a created, never
# started container named <prefix>lease-<run>-..., removed at its Tear down
# (ci.yml). A lease a dead job leaves is stale like any other container
# outside compose, and goes after <min-age-seconds>; its image goes on the
# reap after that.
#
# Stale: exited or dead and finished at least <min-age-seconds> ago, or
# created and never started at least that long ago. A project with any
# container running, paused, restarting or removing is never touched.
#
# <min-age-seconds> defaults to 11100: the runners' 3h job timeout plus 5 min,
# so no job that could still own the stack is alive. Pass the job's own
# timeout-minutes * 60 plus a margin only if you know every runner's timeout.
#
# The prefix is the repo's own (`qrdb-int`, `recountly-ci-`): the name the job
# gives its compose project, without the run id. Never a prefix another repo's
# projects could start with.
#
# A failed removal is reported and skipped: the reaper never fails the job.
# REAP_DRY_RUN=1 prints what would be removed and removes nothing.
set -u

prefix=${1:-}
min_age=${2:-11100}
case $prefix in
  '' | *[!A-Za-z0-9_.-]*) echo "usage: $0 <prefix [A-Za-z0-9_.-]> [min-age-seconds]" >&2; exit 2 ;;
esac
case $min_age in
  '' | *[!0-9]*) echo "min-age-seconds must be a number: $min_age" >&2; exit 2 ;;
esac
image_min_age=${REAP_IMAGE_MIN_AGE:-7200}
case $image_min_age in
  '' | *[!0-9]*) echo "REAP_IMAGE_MIN_AGE must be a number: $image_min_age" >&2; exit 2 ;;
esac

now=$(date +%s)

# run <command...>: the command, or only its echo under REAP_DRY_RUN=1.
run() {
  if [ "${REAP_DRY_RUN:-0}" = 1 ]; then echo "dry   $*"; else "$@" >/dev/null; fi
}

# stale <container>: 0 when the container is stale (as above), 1 otherwise.
stale() {
  local state finished created t
  read -r state finished created < <(docker inspect -f '{{.State.Status}} {{.State.FinishedAt}} {{.Created}}' "$1" 2>/dev/null) || return 1
  case $state in
    exited | dead) t=$finished ;;
    created) t=$created ;;
    *) return 1 ;;
  esac
  t=$(date -d "$t" +%s 2>/dev/null) || return 1
  [ $((now - t)) -ge "$min_age" ]
}

reaped=0

# Compose projects.
for project in $(docker ps -a --filter label=com.docker.compose.project \
    --format '{{.Label "com.docker.compose.project"}}' | sort -u); do
  case $project in "$prefix"*) ;; *) continue ;; esac
  ids=$(docker ps -aq --filter "label=com.docker.compose.project=$project")
  keep=0
  for id in $ids; do stale "$id" || { keep=1; break; }; done
  if [ "$keep" = 1 ]; then
    echo "keep  $project (a container is live or stopped within ${min_age}s)"
    continue
  fi
  # shellcheck disable=SC2086 # one id per word
  set -- $ids
  echo "reap  $project: $# container(s)"
  run docker rm -v "$@" || echo "warn  $project: container removal failed"
  for net in $(docker network ls -q --filter "label=com.docker.compose.project=$project"); do
    run docker network rm "$net" || echo "warn  $project: network $net not removed"
  done
  for vol in $(docker volume ls -q --filter "label=com.docker.compose.project=$project"); do
    run docker volume rm "$vol" || echo "warn  $project: volume $vol not removed"
  done
  reaped=$((reaped + 1))
done

# Containers outside compose.
docker ps -a --filter "name=^$prefix" \
    --format '{{.ID}} {{.Names}} {{.Label "com.docker.compose.project"}}' |
  while read -r id name project; do
    [ -z "$project" ] || continue
    case $name in "$prefix"*) ;; *) continue ;; esac
    if stale "$id"; then
      echo "reap  container $name"
      run docker rm -v "$id" || echo "warn  container $name not removed"
    fi
  done

# Content-addressed integration images (v2, above).
images=0
for ref in $(docker images --filter "reference=${prefix}it" --format '{{.Repository}}:{{.Tag}}'); do
  case $ref in "${prefix}it:"*) ;; *) continue ;; esac
  created=$(docker image inspect -f '{{.Created}}' "$ref" 2>/dev/null) || continue
  t=$(date -d "$created" +%s 2>/dev/null) || continue
  if [ $((now - t)) -lt "$image_min_age" ]; then
    echo "keep  image $ref (created within ${image_min_age}s)"
    continue
  fi
  echo "reap  image $ref"
  if run docker image rm "$ref"; then images=$((images + 1)); else echo "warn  image $ref not removed (in use?)"; fi
done

if [ "${REAP_DRY_RUN:-0}" = 1 ]; then done_as="would be removed"; else done_as=removed; fi
echo "reaper: $reaped compose project(s) and $images image(s) $done_as for prefix '$prefix'"
exit 0
