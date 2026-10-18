#!/usr/bin/env bash
# Bound the size of this repo's CI cache volumes on the runner host.
#
#   usage: scripts/trim-ci-caches.sh <prefix> <image> [max-gib]
#   e.g.   scripts/trim-ci-caches.sh go-quicraft-ci- "$TOOLCHAIN_IMAGE"
#
# v1 (ci-template v1.22, Company Agent m616 item 2). in-toolchain-image.sh v2
# keeps the build, golangci-lint and module caches in named volumes,
# <prefix>cache-<class>-{gobuild,golangci,gomod}, on the Docker host the job
# talks to (ci-standard.md §6). Go trims its build cache of entries unused for
# five days, but nothing trims the module cache, so the volumes would grow
# without bound. This measures them and, while their total is over <max-gib>,
# removes the largest one no container is using. A removed volume comes back
# empty at the next stage, which costs one cold build, never a wrong one.
#
# Sizes are measured by `du` in <image> (the toolchain image the job just
# built: it has du, and it is already on the host), run as IN_IMAGE_UID (the
# volumes' owner, default 10001) with no network, no capability and the volume
# read-only. A volume any container (running or
# stopped) mounts is never removed; `docker volume rm` refuses one in use in any
# case.
#
# <max-gib> defaults to CI_CACHE_MAX_GIB, else 32: the bound for this repo's
# volumes, both trust classes together, on one host.
#
# A failed measurement or removal is reported and skipped: this never fails
# the job. REAP_DRY_RUN=1 prints what would be removed and removes nothing.
set -u

prefix=${1:-}
image=${2:-}
max_gib=${3:-${CI_CACHE_MAX_GIB:-32}}
case $prefix in
  '' | *[!A-Za-z0-9_.-]*) echo "usage: $0 <prefix [A-Za-z0-9_.-]> <image> [max-gib]" >&2; exit 2 ;;
esac
if [ -z "$image" ]; then
  echo "usage: $0 <prefix> <image> [max-gib]" >&2
  exit 2
fi
case $max_gib in
  '' | *[!0-9]*) echo "max-gib must be a number: $max_gib" >&2; exit 2 ;;
esac
max_kib=$((max_gib * 1024 * 1024))
uid=${IN_IMAGE_UID:-10001}
case $uid in
  "" | 0 | *[!0-9]*) echo "IN_IMAGE_UID must be a non-root numeric uid: $uid" >&2; exit 2 ;;
esac

# size <volume>: the volume's size in KiB, or nothing when it cannot be read.
size() {
  docker run --rm --network none --cap-drop ALL --security-opt no-new-privileges:true \
    --user "$uid:$uid" -v "$1:/c:ro" --entrypoint du "$image" -sk /c 2>/dev/null | awk '{print $1}'
}

list=$(mktemp)
trap 'rm -f "$list"' EXIT
total=0
for vol in $(docker volume ls -q --filter "name=${prefix}cache-"); do
  case $vol in "${prefix}cache-"*) ;; *) continue ;; esac
  kib=$(size "$vol")
  case $kib in
    '' | *[!0-9]*) echo "warn  $vol: size not read"; continue ;;
  esac
  echo "$kib $vol" >> "$list"
  total=$((total + kib))
done
echo "caches: $((total / 1024 / 1024)) GiB of ${max_gib} GiB for prefix '$prefix'"

removed=0
while read -r kib vol; do
  [ "$total" -gt "$max_kib" ] || break
  if [ -n "$(docker ps -aq --filter "volume=$vol")" ]; then
    echo "keep  $vol ($((kib / 1024)) MiB, in use)"
    continue
  fi
  if [ "${REAP_DRY_RUN:-0}" = 1 ]; then
    echo "dry   docker volume rm $vol ($((kib / 1024)) MiB)"
  elif docker volume rm "$vol" >/dev/null 2>&1; then
    echo "trim  $vol ($((kib / 1024)) MiB)"
  else
    echo "warn  $vol not removed"
    continue
  fi
  total=$((total - kib))
  removed=$((removed + 1))
done < <(sort -rn "$list")

echo "trim: $removed volume(s), $((total / 1024 / 1024)) GiB left for prefix '$prefix'"
exit 0
