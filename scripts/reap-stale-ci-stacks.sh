#!/usr/bin/env bash
# Remove this repo's stale CI leftovers on the runner host (usage: scripts/reap-stale-ci-stacks.sh <prefix> [min-age-seconds]): fully stale compose projects named <prefix>* with their networks and volumes, stale <prefix>* containers, and <prefix>it images older than REAP_IMAGE_MIN_AGE (default 7200s, age from the later of Created and LastTagTime) that no container or lease uses. Stale means exited, dead or never started for at least min-age-seconds (default 11100). Failures are reported, never fatal; REAP_DRY_RUN=1 only prints.
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
  read -r created tagged < <(docker image inspect -f '{{.Created}} {{.Metadata.LastTagTime.Unix}}' "$ref" 2>/dev/null) || continue
  t=$(date -d "$created" +%s 2>/dev/null) || continue
  # A never-tagged image reports the zero time, a large negative number.
  case $tagged in ''|*[!0-9]*) ;; *) [ "$tagged" -le "$t" ] || t=$tagged ;; esac
  if [ $((now - t)) -lt "$image_min_age" ]; then
    echo "keep  image $ref (created or tagged within ${image_min_age}s)"
    continue
  fi
  echo "reap  image $ref"
  if run docker image rm "$ref"; then images=$((images + 1)); else echo "warn  image $ref not removed (in use?)"; fi
done

if [ "${REAP_DRY_RUN:-0}" = 1 ]; then done_as="would be removed"; else done_as=removed; fi
echo "reaper: $reaped compose project(s) and $images image(s) $done_as for prefix '$prefix'"
exit 0
