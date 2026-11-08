#!/usr/bin/env bash
# Rolling upgrade (or restart) of a compose cluster, one node at a time.
#
#   scripts/rolling-upgrade.sh [compose file]
#
# For each node: restart it on the current image, wait until its /health
# answers and every range has a leader again (from that node's /status), then
# move on. A three-replica cluster keeps a quorum throughout, so clients see at
# most a re-election per range (chapter 03) and retry through it.
set -euo pipefail
compose="${1:-deploy/compose/docker-compose.yml}"
for i in 1 2 3; do
  port=$((9480 + i))
  echo "== n$i: restarting"
  docker compose -f "$compose" up -d --no-deps --force-recreate "n$i"
  for _ in $(seq 1 120); do
    if curl -fsS "http://localhost:$port/health" >/dev/null 2>&1; then
      leaderless=$(curl -fsS "http://localhost:$port/status" | python3 -c 'import json,sys; print(sum(1 for r in json.load(sys.stdin)["ranges"] if r["leader"] is None))' 2>/dev/null || echo 1)
      if [ "$leaderless" = "0" ]; then
        echo "   n$i healthy; every range has a leader"
        break
      fi
    fi
    sleep 1
  done
  sleep 2 # let the restarted node catch up before taking the next one away
done
echo "== done"
