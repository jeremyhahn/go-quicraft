FROM golang:1.27.3 AS builder
WORKDIR /src
COPY . .
WORKDIR /src/test/performance/quicraft
# go mod download (not tidy) so the committed go.mod/go.sum are used
# verbatim and builds stay reproducible.
#
# Optional module credentials for a private module host arrive as a BuildKit
# secret (K=V lines) mounted for this RUN only. They are sourced into the
# shell, never written to a layer or a command line; set -a exports them for
# the git that go invokes. With no secret, modules resolve from the public
# proxy. Requires BuildKit (docker-buildx); the legacy builder rejects --mount.
# This RUN is byte-identical in every image that downloads a module only the
# forge serves (test/buildgates/module_download_test.go).
RUN --mount=type=secret,id=gomodauth \
    set -eu; \
    if [ -s /run/secrets/gomodauth ]; then \
      set -a; . /run/secrets/gomodauth; set +a; \
      echo "==> go mod download: Gitea module credentials mounted"; \
    else \
      echo "==> go mod download: no module credentials mounted; github.com/jeremyhahn/* resolves from the public proxy only"; \
    fi; \
    go mod download

# The binary is stamped with the project's VERSION file, so a stored result
# names the release it measured.
RUN go test -c -tags=benchmark \
      -ldflags "-X github.com/jeremyhahn/go-quicraft/test/performance/benchutil.Version=$(cat /src/VERSION)" \
      -o /bench-quicraft .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /bench-quicraft /usr/local/bin/bench-quicraft

# Host socket buffer limits are raised once for every engine by perfcompare
# (prepareHostNetwork), so this container needs no privileges of its own.
ENTRYPOINT ["/usr/local/bin/bench-quicraft"]
CMD ["-test.bench=.", "-test.run=^$", "-test.benchtime=5s", "-test.timeout=600s", "-test.v"]
