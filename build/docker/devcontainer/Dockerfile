# go-quicraft devcontainer stages: tools, modules, base, toolchain (make build/test/security/deps-check) and integration; build with make docker-build (pins from scripts/tool-pins.sh).

ARG GO_IMAGE
# Build-time downloads go through this proxy only; a module it cannot serve fails the build.
ARG GO_BUILD_PROXY=https://proxy.golang.org

# ---------------------------------------------------------------------------
FROM ${GO_IMAGE} AS tools
ARG GO_BUILD_PROXY
ARG TARGETARCH
ENV GOTOOLCHAIN=local CGO_ENABLED=0 GOPROXY=${GO_BUILD_PROXY} GOBIN=/out
RUN apk add --no-cache curl tar
COPY scripts/tool-pins.sh /tmp/tool-pins.sh
# gosec builds in a throwaway module so x/tools can be raised to XTOOLS_VERSION; retry runs a fetch up to three times.
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
# github.com/jeremyhahn/* modules resolve from the forge through git, so this
# stage has git and base does not.
FROM ${GO_IMAGE} AS modules
ARG GO_BUILD_PROXY
ENV GOTOOLCHAIN=local GOFLAGS=-mod=readonly GOMODCACHE=/gomodcache GOPROXY=${GO_BUILD_PROXY}
RUN apk add --no-cache git
WORKDIR /src

# Module cache layer; forge credentials come from the optional gomodauth BuildKit secret and are never written to a layer. Kept byte-identical across images (test/buildgates).
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
      coreutils grep sed gawk findutils diffutils tar xz; \
    git config --system --add safe.directory '*'
COPY --from=tools /out/ /usr/local/bin/
WORKDIR /src
USER dev:dev

# ---------------------------------------------------------------------------
FROM base AS integration
# Alpine package index checksum from scripts/apk-index-sum.sh; a new sum busts the cache so the upgrade below re-runs.
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
