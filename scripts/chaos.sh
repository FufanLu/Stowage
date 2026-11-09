#!/usr/bin/env bash
# Chaos with a checker: serializable bank transfers against a three-node
# cluster while a nemesis SIGKILLs and SIGSTOPs random nodes.
#
#   scripts/chaos.sh [seconds] [seed]
#
# The workload is `stow-bench bank`: transfers between 1000 accounts plus
# read-only audits, all serializable. Two checks run at the end:
#   1. conservation: one snapshot read of every account sums to the funded total;
#   2. serial replay: replaying the acknowledged transfers in commit-timestamp
#      order, each one must have read exactly what its predecessors left.
# A transfer whose acknowledgement was lost to a fault (counted as "failed") may
# still have committed. Check 2 can then report it, because the replay never saw
# the write. Check 1 has no such ambiguity, and the script fails on it.
#
# The nemesis stops five seconds before the workload so the final audit runs
# on a healthy cluster. Process faults only. A SIGSTOPped node looks
# partitioned to its peers, but packet loss and delay (`tc netem`) need Linux
# network namespaces and are covered only by the deterministic simulator.
set -euo pipefail
cd "$(dirname "$0")/.."
DUR="${1:-60}"
SEED="${2:-$RANDOM}"
OUT=bench/out
TMP="$(mktemp -d)"
mkdir -p "$OUT"
cargo build --profile dist -p stowd -p stow-bench >/dev/null
SD=target/dist/stowd
PEERS="1=127.0.0.1:7501,2=127.0.0.1:7502,3=127.0.0.1:7503"

start() {
  "$SD" --node-id "$1" --listen 127.0.0.1:750$1 --peers "$PEERS" --data-dir "$TMP/n$1" \
    --pg-listen "" --ddb-listen "" --http-listen 127.0.0.1:951$1 >>"$TMP/n$1.log" 2>&1 &
  echo $! >"$TMP/n$1.pid"
}
stop_all() {
  for f in "$TMP"/*.pid; do
    [ -f "$f" ] || continue
    kill -CONT "$(cat "$f")" 2>/dev/null || true
    kill "$(cat "$f")" 2>/dev/null || true
  done
}
trap 'stop_all; sleep 1; rm -rf "$TMP"' EXIT

for i in 1 2 3; do start "$i"; done
sleep 3

f="$OUT/chaos-bank-seed$SEED.txt"
echo "seed $SEED, $DUR s" | tee "$f"
target/dist/stow-bench bank --peers "$PEERS" --duration "$DUR" --concurrency 16 >"$TMP/bench.out" 2>&1 &
bench=$!

# Wait for the accounts to be funded before injecting faults.
until grep -q funded "$TMP/bench.out" 2>/dev/null; do
  kill -0 $bench 2>/dev/null || { cat "$TMP/bench.out"; exit 1; }
  sleep 0.5
done
t0=$(date +%s)

RANDOM=$SEED
end=$((t0 + DUR - 5))
while [ "$(date +%s)" -lt $((end - 6)) ]; do
  sleep $((2 + RANDOM % 3))
  n=$((1 + RANDOM % 3))
  pid=$(cat "$TMP/n$n.pid")
  hold=$((2 + RANDOM % 4))
  if [ $((RANDOM % 2)) = 0 ]; then
    echo "t=$(($(date +%s) - t0))s SIGKILL node $n, restart after ${hold}s" | tee -a "$f"
    kill -9 "$pid"
    sleep "$hold"
    start "$n"
  else
    echo "t=$(($(date +%s) - t0))s SIGSTOP node $n for ${hold}s" | tee -a "$f"
    kill -STOP "$pid"
    sleep "$hold"
    kill -CONT "$pid"
  fi
done
echo "t=$(($(date +%s) - t0))s nemesis stops" | tee -a "$f"

status=0
wait $bench || status=$?
cat "$TMP/bench.out" | tee -a "$f"
exit $status
