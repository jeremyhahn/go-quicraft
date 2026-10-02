FROM golang:1.27.0 AS builder
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
#
# The binary is stamped with the project's VERSION file, so a stored result
# names the release it measured.
RUN --mount=type=secret,id=gomodauth \
    set -eu; \
    if [ -s /run/secrets/gomodauth ]; then \
      set -a; . /run/secrets/gomodauth; set +a; \
      echo "==> go mod download: Gitea module credentials mounted"; \
    else \
      echo "==> go mod download: no module credentials mounted; github.com/jeremyhahn/* resolves from the public proxy only"; \
    fi; \
    go mod download && \
    go test -c -tags=benchmark \
      -ldflags "-X github.com/jeremyhahn/go-quicraft/test/performance/benchutil.Version=$(cat /src/VERSION)" \
      -o /bench-quicraft .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates procps && rm -rf /var/lib/apt/lists/*
COPY --from=builder /bench-quicraft /usr/local/bin/bench-quicraft

# Entrypoint script raises kernel UDP buffer limits (requires --privileged)
# and then execs the benchmark binary. Without these limits, multi-node QUIC
# benchmarks suffer packet drops under burst traffic.
COPY test/performance/quicraft/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["-test.bench=.", "-test.run=^$", "-test.benchtime=5s", "-test.timeout=600s", "-test.v"]
