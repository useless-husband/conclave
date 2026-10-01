#!/bin/sh
# Measure a 3-server cluster on this machine: each configuration starts a
# fresh cluster (scripts/cluster.sh), runs `conclave bench` against all
# three API addresses, and stops it.
#
#   scripts/bench.sh                 # the README table
#   DURATION=30s scripts/bench.sh    # longer runs
set -eu

dir=${BENCH_DIR:-$(mktemp -d)}
duration=${DURATION:-10s}
trap 'sh scripts/cluster.sh stop "$dir/c" >/dev/null 2>&1 || true' EXIT

echo "# $(uname -sm), $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -p), $(go version | cut -d' ' -f3)"
echo "# each run: $duration after 1 s warm-up, 3 servers on 127.0.0.1, clients in one process"
for fsync in full none; do
	rm -rf "$dir/c"
	FSYNC=$fsync sh scripts/cluster.sh start "$dir/c" >/dev/null
	. "$dir/c/env"
	for mix in put get mixed; do
		for clients in 1 16 64; do
			echo "## fsync=$fsync mix=$mix clients=$clients"
			"$dir/c/conclave" bench -mix "$mix" -clients "$clients" -duration "$duration" | tail -3
		done
	done
	sh scripts/cluster.sh stop "$dir/c" >/dev/null
done
