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
FROM golang:1.26.1 AS builder

WORKDIR /src

# Copy the full repository since the benchmark module depends on the
# root module via a replace directive.
COPY . .

WORKDIR /src/test/performance/etcd

# Download dependencies and build the test binary with the benchmark
# build tag. The -c flag compiles the test binary without running it.
RUN go mod tidy && \
    go test -c -tags=benchmark -o /bench-etcd .

# ---------------------------------------------------------------------------
# Stage 2: Minimal runtime image
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /bench-etcd /usr/local/bin/bench-etcd

# Run all benchmarks with 5s duration per benchmark, memory stats,
# and a single iteration count baseline.
ENTRYPOINT ["/usr/local/bin/bench-etcd", \
    "-test.bench=.", \
    "-test.benchtime=5s", \
    "-test.benchmem", \
    "-test.count=1", \
    "-test.v"]
