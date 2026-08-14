# Copyright 2026 go-quicraft Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
#
# Multi-stage build for QuicRaft vs etcd/raft performance benchmarks.
#
# Usage:
#   docker build -t quicraft-bench-etcd -f test/performance/etcd/Dockerfile .
#   docker run --rm quicraft-bench-etcd
#
# The build context must be the repository root so the replace directive
# in go.mod (replace => ../../../) resolves correctly.

# ---------------------------------------------------------------------------
# Stage 1: Build the benchmark test binary
# ---------------------------------------------------------------------------
FROM golang:1.26.6 AS builder

WORKDIR /src

# Copy the full repository since the benchmark module depends on the
# root module via a replace directive.
COPY . .

WORKDIR /src/test/performance/etcd

# Download dependencies and build the test binary with the benchmark
# build tag. The -c flag compiles the test binary without running it.
# go mod download (not tidy) so the committed go.mod/go.sum are used
# verbatim and builds stay reproducible.
RUN go mod download && \
    go test -c -tags=benchmark -o /bench-etcd .

# ---------------------------------------------------------------------------
# Stage 2: Minimal runtime image
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /bench-etcd /usr/local/bin/bench-etcd

# ENTRYPOINT is the binary only; CMD holds the default flags so the
# orchestrator can override benchtime/timeout via `docker run <image> <flags>`.
ENTRYPOINT ["/usr/local/bin/bench-etcd"]
CMD ["-test.bench=.", "-test.run=^$", "-test.benchtime=5s", "-test.timeout=600s", "-test.v"]
