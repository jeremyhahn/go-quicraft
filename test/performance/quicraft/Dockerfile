FROM golang:1.26.1 AS builder
WORKDIR /src
COPY . .
WORKDIR /src/test/performance/quicraft
RUN go mod tidy && go test -c -tags=benchmark -o /bench-quicraft .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates procps && rm -rf /var/lib/apt/lists/*
COPY --from=builder /bench-quicraft /usr/local/bin/bench-quicraft

# Entrypoint script raises kernel UDP buffer limits (requires --privileged)
# and then execs the benchmark binary. Without these limits, multi-node QUIC
# benchmarks suffer packet drops under burst traffic.
COPY test/performance/quicraft/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["-test.bench=.", "-test.benchtime=5s", "-test.benchmem", "-test.count=1", "-test.v"]
