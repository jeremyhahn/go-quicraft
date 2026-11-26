#!/usr/bin/env bash
# scripts/ci-mask-lan.sh: mask the LAN addresses a pool runner's environment
# names (internal addresses stay out of public
# repositories, and their CI logs count). The pool sets the DEVCACHE_*
# package caches, GOPROXY's LAN module proxy and the LAN CI registry; on a
# forge, the release registry (RELEASE_REGISTRY) and the forge's own host
# (GITHUB_SERVER_URL), which the image publishing and resolving name. A
# BuildKit build echoes its build arguments and `go env` prints GOPROXY.
# Run before any step that builds: for each entry of those variables whose
# host is a private IPv4 address, it prints ::add-mask:: for the entry, its
# host:port and its host, so the runner masks the address in every later
# line. Public hosts and keywords (proxy.golang.org, direct, off) are left.
# An entry's userinfo (scheme://user:password@host) is masked on any host,
# and the host is read after it.
#
#   usage: scripts/ci-mask-lan.sh
set -euo pipefail

private() {
	case $1 in
	10.* | 192.168.* | 172.1[6-9].* | 172.2[0-9].* | 172.3[01].*) return 0 ;;
	esac
	return 1
}

for v in $(compgen -e); do
	case $v in
	DEVCACHE_* | GOPROXY | CI_REGISTRY | CI_IMAGE_REGISTRY | RELEASE_REGISTRY | GITHUB_SERVER_URL) ;;
	*) continue ;;
	esac
	IFS='|, ' read -ra entries <<<"${!v}"
	for entry in "${entries[@]}"; do
		authority=${entry#*://}
		authority=${authority%%/*}
		hostport=${authority##*@}
		host=${hostport%%:*}
		if [ "$hostport" != "$authority" ]; then
			# userinfo: its password, or a user long enough to be a
			# token, is masked whatever the host. A short user name
			# such as git is left, or every later "git" would be masked.
			userinfo=${authority%@*}
			password=${userinfo#*:}
			if [ "$password" != "$userinfo" ]; then
				[ -z "$password" ] || echo "::add-mask::$password"
				echo "::add-mask::$userinfo"
			elif [ "${#userinfo}" -gt 8 ]; then
				echo "::add-mask::$userinfo"
			fi
		fi
		private "$host" || continue
		echo "::add-mask::$entry"
		[ "$authority" = "$entry" ] || echo "::add-mask::$authority"
		[ "$hostport" = "$authority" ] || echo "::add-mask::$hostport"
		[ "$host" = "$hostport" ] || echo "::add-mask::$host"
	done
done
