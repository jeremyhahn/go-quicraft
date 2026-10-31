#!/usr/bin/env bash
# ci-image-cache.sh v1 (ci-template v1.30, fix 1 of the 72-hour CI evaluation):
# the toolchain image's registry cache. It ships beside ci.yml as
# scripts/ci-image-cache.sh.
#
#   ci-image-cache.sh pull                 before `make docker-build`: when the
#                                          daemon lacks the toolchain tag, pull it
#                                          from the forge registry and tag it locally
#   ci-image-cache.sh publish [make args]  on the release runner, on a push to main
#                                          only: build and push the toolchain image
#                                          when the registry lacks its tag
#
# The toolchain tag is `make -s toolchain-image`, <repo>-toolchain:<hash>, the
# hash of the Dockerfile, the tool pin file and every devcontainer module's
# go.mod and go.sum (ci-standard.md §9). The published name is
# <registry>/<namespace>/<repo>-toolchain:<hash>-main, so the image is built
# once per change to those files, by the release runner, and every other job
# and runner pulls it instead of building it. The registry is the forge's own,
# host[:port] from GITHUB_SERVER_URL, as in go-qrdb's scripts/ci-registry.sh;
# no address is written here. The namespace is CI_IMAGE_NAMESPACE, by default
# agent-ci-images, public for anonymous pull (ruling 217(4)); only the release
# runner writes there (rulings 169 and 173). On github.com both commands do
# nothing.
#
# pull exits 0 when the daemon holds the tag after it, and 1 otherwise; the
# caller runs `scripts/ci-image-cache.sh pull || true`, so a miss, an
# unreachable registry or a failed pull falls back to the local build, never to
# a red job. A pulled image keeps its digest in the log.
#
# publish reads the release runner's credential contract (ruling 173):
# RELEASE_REGISTRY, which must be this forge's registry, RELEASE_REGISTRY_USER
# and RELEASE_REGISTRY_TOKEN. No credential value is printed or put on a
# command line: docker reads the token on stdin, into a private DOCKER_CONFIG
# that is removed on exit. A pull logs in with nothing, through an empty
# DOCKER_CONFIG.
set -euo pipefail

die() { echo "ci-image-cache: $*" >&2; exit 1; }

case ${GITHUB_SERVER_URL:-} in
  '' | https://github.com) echo "ci-image-cache: no forge registry on this host; nothing to do"; exit 0 ;;
esac
host=${GITHUB_SERVER_URL#*://}
host=${host%%/*}
ns=${CI_IMAGE_NAMESPACE:-agent-ci-images}

local_tag=$(make -s toolchain-image) || die "make -s toolchain-image failed"
case $local_tag in
  *:*) ;;
  *) die "make -s toolchain-image printed '$local_tag', not <name>:<tag>" ;;
esac
remote="$host/$ns/$local_tag-main"

cfg=$(mktemp -d)
cleanup() { rm -f "$cfg/config.json"; rmdir "$cfg" 2>/dev/null || true; }
trap cleanup EXIT

case ${1:-} in
  pull)
    if docker image inspect "$local_tag" >/dev/null 2>&1; then
      echo "ci-image-cache: the daemon holds $local_tag; no pull"
      exit 0
    fi
    if ! DOCKER_CONFIG=$cfg docker pull -q "$remote" >/dev/null 2>&1; then
      echo "ci-image-cache: $remote is not in the registry (or the pull failed); the job builds it"
      exit 1
    fi
    digest=$(docker image inspect --format '{{index .RepoDigests 0}}' "$remote" 2>/dev/null || echo unknown)
    docker tag "$remote" "$local_tag"
    docker image rm "$remote" >/dev/null 2>&1 || true   # untags only; $local_tag keeps the image
    echo "ci-image-cache: pulled $digest as $local_tag"
    ;;
  publish)
    shift
    [ "${GITHUB_EVENT_NAME:-}" = push ] && [ "${GITHUB_REF:-}" = refs/heads/main ] \
      || die "publish runs on a push to main only"
    [ -n "${RELEASE_REGISTRY_USER:-}" ] || die "RELEASE_REGISTRY_USER is not set (the release runner's env file exports it)"
    [ -n "${RELEASE_REGISTRY_TOKEN:-}" ] || die "RELEASE_REGISTRY_TOKEN is not set (the release runner's env file exports it)"
    [ "${RELEASE_REGISTRY:-$host}" = "$host" ] || die "RELEASE_REGISTRY is not this forge's registry; refusing the push"
    if DOCKER_CONFIG=$cfg docker manifest inspect "$remote" >/dev/null 2>&1; then
      echo "ci-image-cache: the registry holds $remote; nothing to publish"
      exit 0
    fi
    make docker-build-toolchain "$@"
    printf '%s' "$RELEASE_REGISTRY_TOKEN" | DOCKER_CONFIG=$cfg docker login "$host" -u "$RELEASE_REGISTRY_USER" --password-stdin >/dev/null
    docker tag "$local_tag" "$remote"
    DOCKER_CONFIG=$cfg docker push -q "$remote" >/dev/null
    digest=$(docker image inspect --format '{{index .RepoDigests 0}}' "$remote" 2>/dev/null || echo unknown)
    docker image rm "$remote" >/dev/null 2>&1 || true
    echo "ci-image-cache: published $local_tag as $digest"
    ;;
  *)
    die "usage: ci-image-cache.sh pull | publish [make args]"
    ;;
esac
