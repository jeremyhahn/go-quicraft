#!/usr/bin/env bash
# ci-image-cache.sh pull|publish [make args]: pull the toolchain image from the forge registry, or build and push it from the release runner; no-op on github.com.
set -euo pipefail

die() { echo "ci-image-cache: $*" >&2; exit 1; }

case ${GITHUB_SERVER_URL:-} in
  '' | https://github.com) echo "ci-image-cache: no forge registry on this host; nothing to do"; exit 0 ;;
esac
host=${GITHUB_SERVER_URL#*://}
host=${host%%/*}
ns=${CI_IMAGE_NAMESPACE:-ci-images}

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
