#!/bin/sh
# Raise kernel UDP buffer limits for QUIC transport.
# Multi-node benchmarks with 5+ QUIC connections on localhost exhaust
# the default ~200KB buffers, causing packet drops and retransmissions.
# Requires --privileged when running the container.
sysctl -w net.core.rmem_max=26214400 >/dev/null 2>&1 || true
sysctl -w net.core.wmem_max=26214400 >/dev/null 2>&1 || true
sysctl -w net.core.rmem_default=26214400 >/dev/null 2>&1 || true
sysctl -w net.core.wmem_default=26214400 >/dev/null 2>&1 || true

exec /usr/local/bin/bench-quicraft "$@"
