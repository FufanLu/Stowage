#!/usr/bin/env bash
# Install the Helm chart on a kind cluster and check it end to end:
# every range gets a leader, SQL and the DynamoDB API answer, and data survives
# a rolling restart of every pod. CI runs this; locally it needs Docker, kind,
# helm and kubectl.
#
#   scripts/kind-smoke.sh            # builds stowage/stowd:dev first
#   SKIP_BUILD=1 scripts/kind-smoke.sh
set -euo pipefail
cluster="${KIND_CLUSTER:-stowage}"
rel=t
svc="$rel-stowage"

if [ -z "${SKIP_BUILD:-}" ]; then
  docker build -f deploy/docker/stowd.Dockerfile -t stowage/stowd:dev .
fi
kind get clusters | grep -qx "$cluster" || kind create cluster --name "$cluster" --wait 120s
kind load docker-image stowage/stowd:dev --name "$cluster"

helm upgrade --install "$rel" deploy/helm/stowage --wait --timeout 5m
kubectl rollout status statefulset/"$svc" --timeout 5m

kubectl port-forward svc/"$svc" 19480:9480 18000:8000 >/tmp/pf.log 2>&1 &
pf=$!
trap 'kill $pf 2>/dev/null || true' EXIT
sleep 3

sql() {
  curl -fsS -X POST localhost:19480/api/sql -H 'Content-Type: application/json' \
    -d "$(python3 -c 'import json,sys; print(json.dumps({"sql": sys.argv[1]}))' "$1")"
}
wait_leaders() {
  for _ in $(seq 1 60); do
    n=$(curl -fsS localhost:19480/status 2>/dev/null | python3 -c 'import json,sys; r=json.load(sys.stdin)["ranges"]; print(sum(1 for x in r if x["leader"] is None))' 2>/dev/null || echo x)
    [ "$n" = "0" ] && return 0
    sleep 2
  done
  echo "ranges without a leader"; curl -s localhost:19480/status; return 1
}

echo "== every range has a leader"
wait_leaders
curl -fsS localhost:19480/api/nodes | python3 -c 'import json,sys; n=json.load(sys.stdin)["nodes"]; print([(x["id"], x["up"]) for x in n]); assert all(x["up"] for x in n)'

echo "== SQL"
sql "CREATE TABLE smoke (id INT PRIMARY KEY, v TEXT)"
sql "INSERT INTO smoke VALUES (1, 'before restart'), (2, 'b')"
sql "SELECT v FROM smoke WHERE id = 1" | tee /dev/stderr | grep -q "before restart"

echo "== DynamoDB API"
curl -fsS localhost:18000 -H 'X-Amz-Target: DynamoDB_20120810.ListTables' \
  -H 'Content-Type: application/x-amz-json-1.0' -d '{}'
echo

echo "== console"
curl -fsS localhost:19480/ | grep -q "<title>Stowage console</title>"

echo "== rolling restart"
kubectl rollout restart statefulset/"$svc"
kubectl rollout status statefulset/"$svc" --timeout 5m
kill $pf 2>/dev/null || true
kubectl port-forward svc/"$svc" 19480:9480 >/tmp/pf.log 2>&1 &
pf=$!
sleep 3
wait_leaders
sql "SELECT v FROM smoke WHERE id = 1" | tee /dev/stderr | grep -q "before restart"
echo
echo "== ok"
