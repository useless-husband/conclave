#!/bin/sh
# Run a 3-server conclave cluster on 127.0.0.1 with ephemeral ports.
#
#   scripts/cluster.sh start [DIR]     build, start n1..n3, join them (or restart an existing
#                                      cluster in DIR); prints the client address list
#   scripts/cluster.sh stop [DIR]      stop every server started from DIR (by PID)
#   scripts/cluster.sh kill N [DIR]    SIGKILL server N
#   scripts/cluster.sh restart N [DIR] start server N again on its old data and ports
#   scripts/cluster.sh status [DIR]    show each server's view
#
# DIR defaults to .cluster. After start, `. DIR/env` sets CONCLAVE_ADDR so
# that `DIR/conclave get KEY` etc. find the cluster. Servers log to DIR/nN.log.
# FSYNC=none|fsync|full (default full) chooses the durability mode.
set -eu

cmd=${1:-start}
case $cmd in
kill | restart) n=${2:?server number}; dir=${3:-.cluster} ;;
*) dir=${2:-.cluster} ;;
esac
bin=$dir/conclave
fsync=${FSYNC:-full}

field() { # field NAME FILE: read "NAME":"value" or "NAME":number from JSON
	sed -n "s/.*\"$1\":\"*\([^\",}]*\).*/\1/p" "$2"
}

wait_addr() {
	i=0
	while [ ! -s "$dir/n$1/addresses.json" ] || [ "$(field pid "$dir/n$1/addresses.json")" != "$(cat "$dir/n$1.pid")" ]; do
		i=$((i + 1))
		if [ $i -gt 200 ]; then
			echo "n$1 did not start; see $dir/n$1.log" >&2
			exit 1
		fi
		if ! kill -0 "$(cat "$dir/n$1.pid")" 2>/dev/null; then
			echo "n$1 exited; see $dir/n$1.log" >&2
			tail -5 "$dir/n$1.log" >&2
			exit 1
		fi
		sleep 0.05
	done
}

launch() { # launch N [extra flags]
	id=$1
	shift
	mkdir -p "$dir/n$id"
	"$bin" serve -id "$id" -data "$dir/n$id" -fsync "$fsync" "$@" >>"$dir/n$id.log" 2>&1 &
	echo $! >"$dir/n$id.pid"
	wait_addr "$id"
}

api() { field api "$dir/n$1/addresses.json"; }
raftaddr() { field raft "$dir/n$1/addresses.json"; }

case $cmd in
start)
	if [ -f "$dir/n1.pid" ] && kill -0 "$(cat "$dir/n1.pid")" 2>/dev/null; then
		echo "a cluster is already running from $dir; stop it first" >&2
		exit 1
	fi
	mkdir -p "$dir"
	go build -o "$bin" ./cmd/conclave
	if [ -d "$dir/n1/wal" ]; then
		# An existing cluster: bring its servers back on their data.
		launch 1
		launch 2
		launch 3
	else
		launch 1 -bootstrap
		launch 2
		launch 3
		for id in 2 3; do
			"$bin" members add -addr "$(api 1)" -id "$id" -raft "$(raftaddr $id)" -api "$(api $id)" >/dev/null
		done
	fi
	addrs="$(api 1),$(api 2),$(api 3)"
	echo "export CONCLAVE_ADDR=$addrs" >"$dir/env"
	echo "cluster running: n1 pid $(cat "$dir/n1.pid"), n2 pid $(cat "$dir/n2.pid"), n3 pid $(cat "$dir/n3.pid")"
	echo "client addresses: $addrs"
	echo "use it with:  . $dir/env && $bin put greeting hello && $bin get greeting"
	;;
stop)
	for f in "$dir"/n*.pid; do
		[ -f "$f" ] || continue
		pid=$(cat "$f")
		if kill -0 "$pid" 2>/dev/null; then
			kill "$pid"
			while kill -0 "$pid" 2>/dev/null; do sleep 0.05; done
		fi
		rm -f "$f"
	done
	echo "stopped"
	;;
kill)
	kill -9 "$(cat "$dir/n$n.pid")"
	echo "killed n$n (pid $(cat "$dir/n$n.pid"))"
	;;
restart)
	launch "$n"
	echo "n$n running again, pid $(cat "$dir/n$n.pid")"
	;;
status)
	. "$dir/env"
	"$bin" status
	;;
*)
	echo "usage: $0 start|stop|status [DIR] | kill|restart N [DIR]" >&2
	exit 2
	;;
esac
