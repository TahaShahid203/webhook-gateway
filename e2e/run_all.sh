#!/usr/bin/env bash
# Runs every end-to-end shard in parallel, each against its own isolated compose stack:
#   gw-a  non-destructive delivery/admin scenarios (run as concurrent threads inside the shard)
#   gw-b  worker kill + graceful deploy (they stop the worker, so they get a stack of their own)
#   gw-c  Tier 1 crash tests on RabbitMQ (stop db / ingest / broker, kill -9 the broker)
#   gw-d  the same crash tests on Redis with appendfsync=everysec (Redis's usual setting)
#   gw-e  the same crash tests on Redis with appendfsync=always (fsync every write)
#   gw-f  the same crash tests on Redis with persistence OFF (the contrast case: it can lose acknowledged events)
# Time is scaled down so hours of backoff run in seconds. Production uses the defaults (scale 1).
set -u
cd "$(dirname "$0")/.."
export DELIVERY_TIME_SCALE=0.02 DELIVERY_MAX_ATTEMPTS=5 DELIVERY_STUCK_GRACE=2
# alerts go to the fake consumer's /alerts; any dead delivery trips the threshold; checked every second
export ALERT_WEBHOOK_URL=http://fake-consumer:9000/alerts ALERT_DEAD_THRESHOLD=0 ALERT_CHECK_EVERY_SECONDS=1
SERVICES="db broker redis migrate ingest ingest-redis edge worker dispatcher web fake-consumer"
OUT=$(mktemp -d)

stack_up() { # project edge-port consumer-port web-port redis-fsync redis-appendonly
  EDGE_PORT=$2 CONSUMER_PORT=$3 WEB_PORT=$4 REDIS_APPENDFSYNC=$5 REDIS_APPENDONLY=$6 docker compose -p "$1" up -d --build $SERVICES >"$OUT/$1.up" 2>&1
}
stack_down() { docker compose -p "$1" down -v --remove-orphans >/dev/null 2>&1; }
ready() { # edge-port consumer-port web-port: the whole stack answers
  for _ in $(seq 1 120); do
    curl -fs "http://localhost:$1/healthz" >/dev/null 2>&1 \
      && curl -fs -X POST "http://localhost:$2/_reset" >/dev/null 2>&1 \
      && curl -fs "http://localhost:$3/stats.json" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}
STACKS=("gw-a 8101 9101 8201 everysec yes" "gw-b 8102 9102 8202 everysec yes" "gw-c 8103 9103 8203 everysec yes"
        "gw-d 8104 9104 8204 everysec yes" "gw-e 8105 9105 8205 always yes" "gw-f 8106 9106 8206 no no")

for spec in "${STACKS[@]}"; do set -- $spec; stack_down "$1"; done
for spec in "${STACKS[@]}"; do set -- $spec; stack_up "$@" & done; wait

for spec in "${STACKS[@]}"; do
  set -- $spec
  if ! ready "$2" "$3" "$4"; then
    echo "STACK $1 NEVER BECAME READY. startup log:"; tail -20 "$OUT/$1.up"
    docker compose -p "$1" ps -a
    for s in "${STACKS[@]}"; do set -- $s; stack_down "$1"; done
    exit 1
  fi
done

COMPOSE_PROJECT=gw-a EDGE=http://localhost:8101 CONSUMER=http://localhost:9101 WEB=http://localhost:8201 python3 e2e/delivery_tests.py main  >"$OUT/a.txt" 2>&1 & PA=$!
COMPOSE_PROJECT=gw-b EDGE=http://localhost:8102 CONSUMER=http://localhost:9102 WEB=http://localhost:8202 python3 e2e/delivery_tests.py crash >"$OUT/b.txt" 2>&1 & PB=$!
COMPOSE_PROJECT=gw-c EDGE=http://localhost:8103 BROKER_KIND=rabbitmq python3 e2e/crash_tests.py >"$OUT/c.txt" 2>&1 & PC=$!
COMPOSE_PROJECT=gw-d EDGE=http://localhost:8104 BROKER_KIND=redis REDIS_APPENDFSYNC=everysec python3 e2e/crash_tests.py >"$OUT/d.txt" 2>&1 & PD=$!
COMPOSE_PROJECT=gw-e EDGE=http://localhost:8105 BROKER_KIND=redis REDIS_APPENDFSYNC=always python3 e2e/crash_tests.py >"$OUT/e.txt" 2>&1 & PE=$!
COMPOSE_PROJECT=gw-f EDGE=http://localhost:8106 BROKER_KIND=redis REDIS_APPENDFSYNC=no REDIS_APPENDONLY=no python3 e2e/crash_tests.py >"$OUT/f.txt" 2>&1 & PF=$!
wait $PA; RA=$?; wait $PB; RB=$?; wait $PC; RC=$?; wait $PD; RD=$?; wait $PE; RE=$?; wait $PF; RF=$?

cat "$OUT/a.txt" "$OUT/b.txt" "$OUT/c.txt" "$OUT/d.txt" "$OUT/e.txt" "$OUT/f.txt"
for s in "${STACKS[@]}"; do set -- $s; stack_down "$1"; done
[ $RA -eq 0 ] && [ $RB -eq 0 ] && [ $RC -eq 0 ] && [ $RD -eq 0 ] && [ $RE -eq 0 ] && [ $RF -eq 0 ] \
  && echo "ALL SHARDS PASSED" || { echo "SOME SHARDS FAILED"; exit 1; }
