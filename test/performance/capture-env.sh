#!/usr/bin/env bash
# capture-env.sh — describe the machine this script is running on, as env.json.
#
# This runs on the benchmark target, not on the workstation that drives it, so
# the specification table beside a benchmark number describes the machine that
# produced it, at the time it produced it.
#
# The profile name is the one thing not derived; the operator chooses it.
#
# Anything this script cannot obtain is emitted as an empty string, never a
# guess or a zero. Consumers render an empty field as "not captured", so an
# uncaptured core count is distinct from a real zero.
#
# Usage:
#   ./capture-env.sh <profile-name> > env.json
set -uo pipefail

if [ $# -lt 1 ] || [ -z "${1:-}" ]; then
  echo "usage: $0 <profile-name>   (e.g. $0 bench-01)" >&2
  exit 1
fi
PROFILE="$1"

# try runs a probe and prints its (trimmed, single-line) output. A probe that
# fails, is absent, or prints nothing yields the empty string rather than a
# partial or defaulted value.
try() {
  local out
  out="$("$@" 2>/dev/null | head -1)" || return 0
  printf '%s' "${out}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

# lscpu_field extracts one labelled field from lscpu. Leading indentation is
# stripped first, because util-linux 2.38 and later nest the fields under
# headings; the anchor after that is exact, so "CPU(s):" does not also match
# "NUMA node0 CPU(s):".
lscpu_field() {
  command -v lscpu >/dev/null 2>&1 || return 0
  LC_ALL=C lscpu 2>/dev/null |
    sed 's/^[[:space:]]*//' |
    awk -F: -v key="$1" 'index($0, key ":")==1 {sub(/^[ \t]+/, "", $2); print $2; exit}'
}

# json_escape makes a captured value safe to embed in a JSON string.
json_escape() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

emit() { printf '  "%s": "%s"' "$1" "$(json_escape "$2")"; }

cpu_model="$(lscpu_field 'Model name')"
sockets="$(lscpu_field 'Socket(s)')"
cores_per_socket="$(lscpu_field 'Core(s) per socket')"
threads="$(lscpu_field 'CPU(s)')"
numa_nodes="$(lscpu_field 'NUMA node(s)')"

# Physical cores is a product, so it is captured only when both factors were.
physical_cores=""
if [ -n "$sockets" ] && [ -n "$cores_per_socket" ]; then
  physical_cores="$(awk -v s="$sockets" -v c="$cores_per_socket" 'BEGIN{print s*c}')"
fi

memory_total=""
if [ -r /proc/meminfo ]; then
  memory_total="$(awk '/^MemTotal:/ {printf "%.1f GiB", $2/1048576; exit}' /proc/meminfo)"
fi

os_name=""
os_version=""
if [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  os_name="${NAME:-}"
  os_version="${VERSION:-${VERSION_ID:-}}"
fi

kernel="$(try uname -r)"
hostname="$(try uname -n)"

docker_version="$(try docker version --format '{{.Server.Version}}')"
if [ -z "$docker_version" ]; then
  # The daemon may be unreachable while the client is present; the client
  # version still describes the toolchain, so record it and say which it is.
  client="$(try docker version --format '{{.Client.Version}}')"
  [ -n "$client" ] && docker_version="$client (client only; daemon unreachable)"
fi

go_version=""
if command -v go >/dev/null 2>&1; then
  go_version="$(go version 2>/dev/null | awk '{print $3, $4}')"
fi

captured_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

{
  echo '{'
  emit profile        "$PROFILE";        echo ','
  emit captured_at    "$captured_at";    echo ','
  emit hostname       "$hostname";       echo ','
  emit cpu_model      "$cpu_model";      echo ','
  emit sockets        "$sockets";        echo ','
  emit physical_cores "$physical_cores"; echo ','
  emit threads        "$threads";        echo ','
  emit numa_nodes     "$numa_nodes";     echo ','
  emit memory_total   "$memory_total";   echo ','
  emit os_name        "$os_name";        echo ','
  emit os_version     "$os_version";     echo ','
  emit kernel         "$kernel";         echo ','
  emit docker_version "$docker_version"; echo ','
  emit go_version     "$go_version";     echo
  echo '}'
}
