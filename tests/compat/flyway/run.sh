#!/usr/bin/env bash
# Flyway 13 + pgjdbc against a running Stowage node: apply Conveyor's and
# Milkrun's real migrations, each into its own fresh database state, and check
# that the constraints they declare are enforced.
#
#   tests/compat/flyway/run.sh [pg port, default 5481]
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
port="${1:-5481}"
jars="${FLYWAY_JARS:-$here/.jars}"
mkdir -p "$jars"
fetch() { # group artifact version
  local f="$jars/$2-$3.jar"
  [ -s "$f" ] || curl -fsSL -o "$f" "https://repo1.maven.org/maven2/${1//.//}/$2/$3/$2-$3.jar"
}
fetch org.flywaydb flyway-core 13.12.1
fetch org.flywaydb flyway-database-postgresql 13.12.1
fetch org.postgresql postgresql 42.7.14
fetch com.fasterxml.jackson.core jackson-databind 2.22.4
fetch com.fasterxml.jackson.core jackson-core 2.22.4
fetch com.fasterxml.jackson.core jackson-annotations 2.22
fetch com.google.code.gson gson 2.14.0
cp="$(ls "$jars"/*.jar | tr '\n' ':')"
build="$(mktemp -d)"
javac -d "$build" -cp "$cp" "$here/Migrate.java"

url="jdbc:postgresql://127.0.0.1:$port/stowage"
psql_() { psql -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$port" -U stowage -d stowage "$@"; }
reset() { # drop everything the previous project created
  for t in $(psql_ -At -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"); do
    psql_ -c "DROP TABLE IF EXISTS $t CASCADE" >/dev/null 2>&1 || true
  done
}
for d in "$here"/migrations/*/; do
  reset
  echo "== $(basename "$d")"
  java -cp "$cp$build" Migrate "$url" "$d" 2>&1 | grep -v -E "^(INFO|WARNING|[A-Z][a-z]{2} [0-9]{2}, )" 
done
echo "== constraints declared by the migrations are enforced"
expect_error() { # sqlstate, statement
  out=$(psql_ -c "$2" 2>&1 || true)
  echo "$out" | grep -q "ERROR" || { echo "expected $1 from: $2"; exit 1; }
  echo "  $1  $(echo "$out" | sed -n 's/^ERROR: *//p' | head -1)"
}
psql_ -c "INSERT INTO route (id, version) VALUES ('r1', 1)"
expect_error 23514 "INSERT INTO route (id, version) VALUES ('r2', 0)"
expect_error 23503 "INSERT INTO stop (route_id, id, version, sequence, status, snapshot) VALUES ('nope', 's1', 1, 1, 'pending', '{}')"
psql_ -c "INSERT INTO stop (route_id, id, version, sequence, status, snapshot) VALUES ('r1', 's1', 1, 1, 'pending', '{}')"
expect_error 23503 "DELETE FROM route WHERE id = 'r1'"
expect_error 23514 "INSERT INTO mutation_log (route_id, client_id, seq, stop_id, hlc, type, payload, outcome) VALUES ('r1', 'c', 1, 's1', 'h', 'deliver', '{}', 'maybe')"
echo "== ok"
