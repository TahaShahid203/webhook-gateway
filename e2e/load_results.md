# Load test results

Run at 2026-11-24 16:31: 50 events/s for 300s through the real edge, RabbitMQ transport,
consumer failing 30% of requests at random, 1 ingest worker(s), retries time-scaled by DELIVERY_TIME_SCALE=0.05.

| What | Result |
|---|---|
| Requests sent | 16127 (14357 unique valid, 1127 duplicates, 643 bad signatures) |
| Sustained send rate | 53.8 requests/s over 300s |
| Edge answers | 200: 15484, 401: 643 |
| Edge response time (client side) | p50 3 ms, p95 7 ms, p99 16 ms, max 146 ms |
| Events stored | 14357 (distinct ids: 14357); expected 14357 |
| Duplicates absorbed | 1127 of 1127 sent |
| Same id, different body (conflicts) | 0 (expected 0: the test sends identical bytes) |
| Bad signatures stored | 0 (rejected with 401: 643 of 643) |
| Received to stored (queue + ingest worker) | p50 7 ms, p95 311 ms, p99 1755 ms |
| Deliveries | delivered: 14357 |
| Delivery attempts | 20594 total, 14357 answered 200, 6237 answered 5xx (the injected failures) |
| Received to delivered (incl. retries) | p50 0.5s, p95 14.7s, p99 27.2s, max 884.4s |
| Peak backlog of due deliveries | 1062 (oldest waited up to 24.3s) |
| Circuit breaker trips | 46 |
| Time until fully drained | 988s from the start |
