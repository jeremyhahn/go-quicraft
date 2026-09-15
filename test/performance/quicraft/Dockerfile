FROM golang:1.27.0 AS builder
WORKDIR /src
COPY . .
WORKDIR /src/test/performance/quicraft
# go mod download (not tidy) so the committed go.mod/go.sum are used
# verbatim and builds stay reproducible.
#
# go-enclave is an internal Gitea module (#54), and nothing in this container
# can reach it unless the build is told how: with no credentials, Go goes to
# github.com, where the module is not, and dies at "could not read Username"
# (#184). The credentials arrive as a BuildKit secret mounted for this one RUN
# -- dependency-resolution.md section 6 -- sourced into the shell and never
# exported into the layer, so the token reaches neither the image, its
# metadata, nor a command line. The payload is the four variables of that
# section as K=V lines; set -a exports them for the git that go invokes.
#
# The binary is stamped with the project's VERSION file, copied in with the
# context, so a stored result names the release it measured -- the same
# value `make build` stamps into every other binary (#189).
#
# The mount is optional at both ends. With no secret the build resolves
# github.com/jeremyhahn/* from the public proxy only, which is the correct
# shape for a build outside the Gitea network. Requires BuildKit (the
# docker-buildx plugin); the legacy builder rejects --mount outright.
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
