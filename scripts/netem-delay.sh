#!/usr/bin/env bash
# Delay TCP traffic to and from one port (default: the coordinator's 8080) with
# netem, to see how signalling latency shows up in the dial stage timings.
# Media is UDP and unaffected. Needs root (sudo) and the sch_netem module.
#
#   scripts/netem-delay.sh on [delay_ms=250] [port=8080] [iface=lo]
#   scripts/netem-delay.sh off [iface=lo]
#   scripts/netem-delay.sh show [iface=lo]
#
# On `lo` both directions pass the qdisc, so a dial sees the delay twice per
# round trip. Always run `off` afterwards: the qdisc outlives this script.
set -euo pipefail

tc() { sudo tc "$@"; }

case "${1:-}" in
on)
    delay="${2:-250}" port="${3:-8080}" dev="${4:-lo}"
    # Bands 0-2 keep the default priomap; only filtered traffic reaches band 4.
    tc qdisc add dev "$dev" root handle 1: prio bands 4
    tc qdisc add dev "$dev" parent 1:4 handle 40: netem delay "${delay}ms"
    for dir in dport sport; do
        tc filter add dev "$dev" parent 1:0 protocol ip prio 1 u32 \
            match ip protocol 6 0xff match ip "$dir" "$port" 0xffff flowid 1:4
        tc filter add dev "$dev" parent 1:0 protocol ipv6 prio 2 u32 \
            match ip6 protocol 6 0xff match ip6 "$dir" "$port" 0xffff flowid 1:4
    done
    echo "netem: +${delay}ms on TCP port $port via $dev (undo: $0 off $dev)"
    ;;
off)
    tc qdisc del dev "${2:-lo}" root
    ;;
show)
    tc qdisc show dev "${2:-lo}"
    tc filter show dev "${2:-lo}"
    ;;
*)
    sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
