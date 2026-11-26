# go-quicraft devcontainer: one Dockerfile, the shipped stages built FROM base
# (handbook devcontainer.md §1).
#
#   tools        build-only: the pinned tools, built or downloaded into /out
#   devtools     build-only: the pinned gopls, for the dev stage only
#   modules      build-only: the module cache, fetched with the forge credentials
#   base         the Go toolchain by digest, the C compiler -race needs, a
#                non-root user and the verified module cache; GOPROXY=off
#   toolchain    base + every tool the Makefile calls. `make build`, `make test`,
#                `make security` and `make deps-check` run here, in CI too.
#   integration  base + the source only. `make integration-test` runs it under
#                the hardened compose service (compose.yaml beside this file).
#   dev          toolchain + gopls: the editor's image (.devcontainer). The one
#                stage that fetches modules at run time: the baked cache first,
#                offline, then the public proxy into a writable cache under
#                /home/dev. No credentials and no direct fetch. CI never runs it.
#
# Build with `make docker-build`: it passes GO_IMAGE and every tool version from
# scripts/tool-pins.sh, the one pin file the Makefile also reads, and the forge
# credentials as the gomodauth BuildKit secret. GO_IMAGE has no default here, so
# a build that bypasses the pin file fails instead of drifting. Requires
# BuildKit.
#
# The build context is the repository root, filtered by the root .dockerignore.
# A CI runner reaches its Docker daemon through a socket, so a bind mount would
# resolve on the daemon's host, where the checkout does not exist: integration
# bakes the source in, and CI copies the checkout into a toolchain container
# (scripts/in-toolchain-image.sh) instead of mounting it.

ARG GO_IMAGE
# Build-time module and tool downloads go through this proxy (devcontainer.md
# §4). The caller's environment supplies another; no tracked file does. No
# direct entry (ci-template v1.34.1): a module the proxy cannot serve fails
# the build instead of being fetched from its origin.
ARG GO_BUILD_PROXY=https://proxy.golang.org

# ---------------------------------------------------------------------------
FROM ${GO_IMAGE} AS tools
ARG GO_BUILD_PROXY
ARG TARGETARCH
ENV GOTOOLCHAIN=local CGO_ENABLED=0 GOPROXY=${GO_BUILD_PROXY} GOBIN=/out
RUN apk add --no-cache curl tar
COPY scripts/tool-pins.sh /tmp/tool-pins.sh
# gosec builds in a throwaway module so x/tools can be raised to XTOOLS_VERSION
# (see tool-pins.sh); the others build with the x/tools their go.mod names.
# retry runs a module fetch up to three times, so one transient proxy error
# does not fail the image build.
RUN set -eu; \
    retry() { \
      n=1; \
      while :; do \
        rc=0; "$@" || rc=$?; \
        [ "$rc" -eq 0 ] && return 0; \
        [ "$n" -ge 3 ] && return "$rc"; \
        if [ "$n" -eq 1 ]; then s=5; else s=15; fi; \
        n=$((n + 1)); \
        echo "==> retry $n/3: $*" >&2; \
        sleep "$s"; \
      done; \
    }; \
    . /tmp/tool-pins.sh; \
    retry go install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_VERSION}"; \
    retry go install "github.com/rhysd/actionlint/cmd/actionlint@${ACTIONLINT_VERSION}"; \
    ( d="$(mktemp -d)"; cd "$d"; \
      go mod init gosec-build; \
      retry go get "github.com/securego/gosec/v2/cmd/gosec@${GOSEC_VERSION}" "golang.org/x/tools@${XTOOLS_VERSION}"; \
      retry go install github.com/securego/gosec/v2/cmd/gosec; \
      cd /; rm -rf "$d" ); \
    retry go install "golang.org/x/vuln/cmd/govulncheck@${GOVULNCHECK_VERSION}"; \
    case "${TARGETARCH:-amd64}" in \
      amd64) arch=64bit; sum="${TRIVY_SHA256_LINUX_AMD64}"; asset="${TRIVY_ASSET_ID_LINUX_AMD64}" ;; \
      arm64) arch=ARM64; sum="${TRIVY_SHA256_LINUX_ARM64}"; asset="${TRIVY_ASSET_ID_LINUX_ARM64}" ;; \
      *) echo "no trivy pin for ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    v="${TRIVY_VERSION#v}"; \
    f="trivy_${v}_Linux-${arch}.tar.gz"; \
    curl -fsSL --retry 3 -o "/tmp/${f}" "https://github.com/aquasecurity/trivy/releases/download/${TRIVY_VERSION}/${f}" \
      || curl -fsSL --retry 3 -H 'Accept: application/octet-stream' -o "/tmp/${f}" \
           "https://api.github.com/repos/aquasecurity/trivy/releases/assets/${asset}"; \
    echo "${sum}  /tmp/${f}" > /tmp/trivy.sha256; \
    sha256sum -c /tmp/trivy.sha256; \
    tar -xzf "/tmp/${f}" -C /out trivy; \
    rm -f "/tmp/${f}"

# ---------------------------------------------------------------------------
# The editor's language server, for the dev stage only.
FROM ${GO_IMAGE} AS devtools
ARG GO_BUILD_PROXY
ENV GOTOOLCHAIN=local CGO_ENABLED=0 GOPROXY=${GO_BUILD_PROXY} GOBIN=/out
COPY scripts/tool-pins.sh /tmp/tool-pins.sh
RUN set -eu; \
    . /tmp/tool-pins.sh; \
    go install "golang.org/x/tools/gopls@${GOPLS_VERSION}"

# ---------------------------------------------------------------------------
# github.com/jeremyhahn/* modules resolve from the forge through git, so this
# stage has git and base does not.
FROM ${GO_IMAGE} AS modules
ARG GO_BUILD_PROXY
ENV GOTOOLCHAIN=local GOFLAGS=-mod=readonly GOMODCACHE=/gomodcache GOPROXY=${GO_BUILD_PROXY}
RUN apk add --no-cache git
WORKDIR /src

# Download the module cache in its own layer. Credentials for the project
# forge arrive as the optional gomodauth BuildKit secret (the K=V lines of
# .devcontainer/.env), mounted for this RUN only: sourced, never written to a
# layer or a command line. With no secret, modules resolve from the public
# proxy, so a module only the forge serves (go-enclave) fails the build here,
# loudly. This RUN is byte-identical in every image that downloads such a
# module (test/buildgates/module_download_test.go). Requires BuildKit.
COPY go.mod go.sum* ./
RUN --mount=type=secret,id=gomodauth \
    set -eu; \
    if [ -s /run/secrets/gomodauth ]; then \
      set -a; . /run/secrets/gomodauth; set +a; \
      echo "==> go mod download: Gitea module credentials mounted"; \
    else \
      echo "==> go mod download: no module credentials mounted; github.com/jeremyhahn/* resolves from the public proxy only"; \
    fi; \
    go mod download
RUN go mod verify

# ---------------------------------------------------------------------------
FROM ${GO_IMAGE} AS base
# GOTOOLCHAIN=local: a go.mod asking for a newer Go fails instead of
# downloading one. CGO for -race.
ENV GOTOOLCHAIN=local \
    CGO_ENABLED=1 \
    GOFLAGS=-mod=readonly \
    GOMODCACHE=/gomodcache \
    GOCACHE=/gocache
RUN set -eu; \
    apk upgrade --no-cache; \
    apk add --no-cache gcc musl-dev; \
    addgroup -g 10001 dev; \
    adduser -D -u 10001 -G dev -h /home/dev dev; \
    install -d -m 0755 -o 10001 -g 10001 /src
COPY --from=modules /gomodcache /gomodcache
# Pre-warm the race-instrumented standard library, then open the build cache to
# any uid (the toolchain image runs as the invoking user).
RUN set -eu; \
    chmod -R a+rX /gomodcache; \
    go build -race std; \
    chmod -R a+rwX /gocache
# Nothing fetches a module at run time (devcontainer.md §4).
ENV GOPROXY=off

# ---------------------------------------------------------------------------
FROM base AS toolchain
ENV HOME=/tmp
RUN apk add --no-cache bash make git curl jq shellcheck \
      coreutils grep sed gawk findutils diffutils tar xz openssh-keygen; \
    git config --system --add safe.directory '*'
COPY --from=tools /out/ /usr/local/bin/
WORKDIR /src
USER dev:dev

# ---------------------------------------------------------------------------
FROM base AS integration
# ci-template v1.29 (devcontainer.md §6): the Alpine package index checksum CI
# reads (scripts/apk-index-sum.sh). A new sum misses the cache from here on, so
# the upgrade below re-runs and takes the fixed package. Declared in this stage
# only; base and toolchain do not take it.
ARG APK_INDEX_SUM
RUN apk upgrade --no-cache
WORKDIR /src
# The source only, by name, owned by root so the suite cannot rewrite it.
COPY --chown=root:root go.mod go.sum ./
COPY --chown=root:root pkg ./pkg
COPY --chown=root:root test ./test
# Compile the suites here, so a broken build fails the image, and leave their
# packages in the cache for the run.
RUN set -eu; \
    go vet -tags=integration ./test/integration/...; \
    chmod -R a+rwX /gocache
ENV HOME=/home/dev
USER dev:dev

# ---------------------------------------------------------------------------
# The editor's image: the toolchain plus gopls. `make lint` walks the nested
# modules, whose dependencies the baked cache does not hold, so this stage
# serves the baked cache first, offline, and fetches anything else from the
# public proxy into a cache dev can write. No credentials, no direct entry.
FROM toolchain AS dev
COPY --from=devtools /out/gopls /usr/local/bin/
ENV HOME=/home/dev \
    GOMODCACHE=/home/dev/go/pkg/mod \
    GOPROXY=file:///gomodcache/cache/download,https://proxy.golang.org
USER dev:dev
