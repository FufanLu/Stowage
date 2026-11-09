#!/usr/bin/env bash
# Chapter 11: the same YCSB workload against Cartwheel (AP: leaderless N=3,
# R=W=2, sloppy quorum + hinted handoff) and Stowage (CP: Raft, 3 replicas,
# serializable transactions), three nodes each, on one machine, through the
# same faults.
#
#   CARTWHEEL=~/Cartwheel bench/ap-vs-cp.sh [workload A|B|C] [seconds]
#
# Scenarios per system: healthy; node 2 SIGKILLed at 1/3 of the run and
# restarted at 2/3; node 2 SIGSTOPped (frozen: to its peers this looks like a
# partition) for the middle third. Output goes to bench/out/<system>-<scenario>.txt.
set -euo pipefail
cd "$(dirname "$0")/.."
CW="${CARTWHEEL:?set CARTWHEEL to the Cartwheel checkout}"
WL="${1:-A}"
DUR="${2:-30}"
KEYS=50000
THREADS=32
OUT=bench/out
TMP="$(mktemp -d)"
mkdir -p "$OUT"
cargo build --profile dist -p stowd -p stow-bench >/dev/null
SB=target/dist/stow-bench
SD=target/dist/stowd

# ---- Cartwheel: three wheel-node processes on 7301-7303 (metrics 93<run><node>)
cw_start() {
  local seeds="wheel-1=127.0.0.1:7301,wheel-2=127.0.0.1:7302,wheel-3=127.0.0.1:7303"
  for i in "$@"; do
    mkdir -p "$TMP/cw/wheel-$i"
    CARTWHEEL_NODE_ID=wheel-$i CARTWHEEL_NODE_ADVERTISE=127.0.0.1:730$i \
    CARTWHEEL_SERVER_LISTEN=127.0.0.1:730$i CARTWHEEL_METRICS_LISTEN=127.0.0.1:93$RUN$i \
    CARTWHEEL_STORAGE_DIR="$TMP/cw/wheel-$i" CARTWHEEL_CLUSTER_ENABLED=true \
    CARTWHEEL_CLUSTER_SEEDS="$seeds" CARTWHEEL_SERVER_DRAIN_GRACE_MS=200 \
      "$CW/build/release/src/kv/wheel-node" >>"$TMP/cw-$i.log" 2>&1 &
    echo $! >"$TMP/cw-$i.pid"
  done
  sleep 3
  for i in "$@"; do kill -0 "$(cat "$TMP/cw-$i.pid")" || { echo "wheel-$i died:"; tail -5 "$TMP/cw-$i.log"; exit 1; }; done
}
cw_bench() {
  "$CW/build/release/src/tools/wheel-bench" --targets 127.0.0.1:7301,127.0.0.1:7302,127.0.0.1:7303 \
    --workload "$WL" --keys $KEYS --threads $THREADS "$@"
}

# ---- Stowage: three stowd processes on 7401-7403
sw_start() {
  local peers="1=127.0.0.1:7401,2=127.0.0.1:7402,3=127.0.0.1:7403"
  for i in "$@"; do
    "$SD" --node-id "$i" --listen 127.0.0.1:740$i --peers "$peers" --data-dir "$TMP/sw/n$i" \
      --pg-listen "" --ddb-listen "" --http-listen 127.0.0.1:949$((i + 6)) ${SW_FLAGS:-} >>"$TMP/sw-$i.log" 2>&1 &
    echo $! >"$TMP/sw-$i.pid"
  done
  sleep 3
  for i in "$@"; do kill -0 "$(cat "$TMP/sw-$i.pid")" || { echo "stowd $i died:"; tail -5 "$TMP/sw-$i.log"; exit 1; }; done
}
sw_bench() {
  "$SB" ycsb --peers 1=127.0.0.1:7401,2=127.0.0.1:7402,3=127.0.0.1:7403 \
    --workload "$WL" --keys $KEYS --threads $THREADS "$@"
}

# Stowage without fsync: the durability Cartwheel runs with by default
# (replication only), to separate the cost of fsync from the cost of consensus.
swn_start() { SW_FLAGS=--unsafe-skip-fsync sw_start "$@"; }
swn_bench() { sw_bench "$@"; }

stop_all() {
  for f in "$TMP"/*.pid; do kill -CONT "$(cat "$f")" 2>/dev/null || true; kill "$(cat "$f")" 2>/dev/null || true; done
  sleep 1
  rm -f "$TMP"/*.pid
}
trap 'stop_all; rm -rf "$TMP"' EXIT

fault() { # system scenario
  local pid; pid=$(cat "$TMP/${1%n}-2.pid")
  case "$2" in
    kill)
      sleep $((DUR / 3)); kill -9 "$pid"; echo "## t=$((DUR / 3))s: SIGKILL node 2" >&2
      sleep $((DUR / 3)); "$1_start" 2; echo "## t=$((2 * DUR / 3))s: node 2 restarted" >&2 ;;
    pause)
      sleep $((DUR / 3)); kill -STOP "$pid"; echo "## t=$((DUR / 3))s: SIGSTOP node 2" >&2
      sleep $((DUR / 3)); kill -CONT "$pid"; echo "## t=$((2 * DUR / 3))s: SIGCONT node 2" >&2 ;;
  esac
}

# Cartwheel's metrics listener does not set SO_REUSEADDR, so every scenario
# gets its own metrics ports (93<run><node>) instead of waiting out TIME_WAIT.
RUN=0
run() { # system scenario name [bench flags...]
  local sys=$1 scenario=$2 name=$3; shift 3
  RUN=$((RUN + 1))
  rm -rf "$TMP/cw" "$TMP/sw"
  "${sys}_start" 1 2 3
  if [ "$sys" = cw ]; then cw_bench --load $KEYS --duration 0 >/dev/null; else "${sys}_bench" --load $KEYS --presplit 3 --duration 0 >/dev/null; fi
  local f="$OUT/$name-$WL-$scenario.txt" fpid=""
  echo "== $f"
  if [ "$scenario" != healthy ]; then fault "$sys" "$scenario" 2>>"$f.events" & fpid=$!; fi
  "${sys}_bench" --duration "$DUR" "$@" | tee "$f" | tail -1
  # Only the fault injector: a bare `wait` would also wait on the servers.
  [ -n "$fpid" ] && wait "$fpid"
  [ -f "$f.events" ] && cat "$f.events" >>"$f" && rm "$f.events"
  stop_all
}

for scenario in healthy kill pause; do
  run cw "$scenario" cartwheel
  run sw "$scenario" stowage
  run swn "$scenario" stowage-nofsync
done
run sw healthy stowage-blind --update blind
