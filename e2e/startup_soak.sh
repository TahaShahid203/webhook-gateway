#!/usr/bin/env bash
# Soak test for the parallel-stack startup race: start three full stacks at once while the CPUs are saturated,
# repeat N times, and fail if any broker does not come up healthy. Before the broker-init fix this failed
# about one run in three under load (RabbitMQ: "Error when reading /var/lib/rabbitmq/.erlang.cookie: eacces").
set -u
cd "$(dirname "$0")/.."
N=${1:-6}
SERVICES="db broker migrate ingest edge worker dispatcher fake-consumer"
for i in $(seq 1 "$N"); do
  for s in gw-a gw-b gw-c; do docker compose -p $s down -v --remove-orphans >/dev/null 2>&1; done
  docker run -d --rm --cpus 10 alpine sh -c 'for n in 1 2 3 4 5 6 7 8 9 10; do (while :; do :; done) & done; sleep 35' >/dev/null 2>&1
  for p in "gw-a 8101 9101" "gw-b 8102 9102" "gw-c 8103 9103"; do set -- $p
    EDGE_PORT=$2 CONSUMER_PORT=$3 docker compose -p $1 up -d --build $SERVICES >/dev/null 2>&1 &
  done
  wait; sleep 8
  bad=""
  for s in gw-a gw-b gw-c; do
    st=$(docker compose -p $s ps -a --format '{{.Status}}' broker)
    case "$st" in *healthy*) ;; *) bad="$bad $s($st)";; esac
  done
  echo "soak $i/$N: ${bad:-all three brokers healthy}"
  [ -n "$bad" ] && { for s in gw-a gw-b gw-c; do docker compose -p $s down -v --remove-orphans >/dev/null 2>&1; done; exit 1; }
  sleep 30   # let the CPU burner finish
done
for s in gw-a gw-b gw-c; do docker compose -p $s down -v --remove-orphans >/dev/null 2>&1; done
echo "SOAK PASSED ($N rounds)"
