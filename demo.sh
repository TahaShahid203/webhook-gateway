#!/usr/bin/env bash
# Start the whole gateway plus the "Webhook Range" playground, with retry timing slowed to a watchable pace
# (backoff 3 s instead of 30 s, breaker cooldown 6 s, 3 attempts so a dead letter takes seconds, not 7 hours). Production values are the defaults (scale 1).
set -e
cd "$(dirname "$0")"
export DELIVERY_TIME_SCALE=${DELIVERY_TIME_SCALE:-0.1} DELIVERY_MAX_ATTEMPTS=${DELIVERY_MAX_ATTEMPTS:-3} DELIVERY_STUCK_GRACE=${DELIVERY_STUCK_GRACE:-10}
export ALERT_CHECK_EVERY_SECONDS=${ALERT_CHECK_EVERY_SECONDS:-5} ALERT_DEAD_THRESHOLD=${ALERT_DEAD_THRESHOLD:-3}
docker compose -f docker-compose.yml -f docker-compose.chaos.yml up -d --build db broker redis redis-volatile migrate ingest ingest-redis ingest-redis-volatile edge worker dispatcher web fake-consumer
echo
echo "Playground : http://localhost:8080/play/"
echo "Admin      : http://localhost:8080/admin/   (admin / admin, dev only)"
echo "Metrics    : http://localhost:8080/metrics"
echo "Edge       : http://localhost:8000/in/<source>"
echo "Stop       : docker compose -f docker-compose.yml -f docker-compose.chaos.yml down -v"
