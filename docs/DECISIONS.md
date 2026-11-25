# Design decisions (owner: Taha)

Dates are 2026. Spec: startup-research/course/project2-webhook-gateway-spec.md.

## Promise
No accepted event is lost, even if the core (Django, Postgres) is down for hours. At-least-once delivery with a stable event id; never "exactly once".

## Flow
provider -> FastAPI edge (verify signature on raw bytes, stamp received_at, publish to RabbitMQ with publisher confirm, 200)
-> Celery ingest worker (dedup, store event + delivery rows in one transaction, ack)
-> Postgres -> delivery workers -> consumers. Django core owns models, admin, replay.

Broker down: edge returns 503, never a false 200.

## Signature schemes
- Stripe-style: `Stripe-Signature: t=<unix>,v1=<hex>`; HMAC-SHA256 over `"<t>.<raw body>"`.
- Shopify-style: `X-Shopify-Hmac-Sha256: <base64>`; HMAC-SHA256 over raw body. No timestamp.
Constant-time compare. Bad signature -> 401, nothing stored.

## Replay window (Nov 17, Nov 23)
5 minutes, only for timestamped schemes, signed timestamp compared with `received_at` (never "now").
Valid signature but outside the window -> stored as `quarantined`, flagged to a human, never delivered.

## Dedup (Nov 23)
- Key = (source, provider event id) when the provider sends one; else (source, sha256(raw body)).
- Id keys match forever. Hash keys are kept forever but only match if the earlier copy arrived within 24 hours.
- Same id + same body hash: drop, keep duplicate count + last-seen.
- Same id + different body hash: store in `event_conflicts`, alert.
- Dedup keys are never deleted.

## Rejected (bad signature) attempts (Nov 23)
Logged in full until a size cap is reached. At the cap: raise one alert, then keep a counter and log a sample only.
When space is freed (logs deleted) and size is under the cap, go back to full logging.

## Tier 2: delivery (Nov 24)
- Retry: 14 attempts, backoff 30 s doubling to a 1 h cap, plus 0-25% jitter that never shortens a wait
  (about 7 h of outage covered). After the last attempt: status `dead`, kept forever, replayable.
- Retry only: timeout, connection error, 5xx, 408, 429. Other 4xx = failed immediately, no retry. `Retry-After` honoured (capped at 1 h).
- Isolation: a cap on how many deliveries one consumer may have in flight at once (default 4), so a dead or slow
  consumer cannot take all the workers. Built as the cap instead of a queue per consumer; same effect.
- Circuit breaker: 5 consecutive failures pause a consumer. Events already in flight are stopped and retried later
  (not counted as an attempt). A single probe request after a cooldown decides whether to resume.
- Ordering: opt-in per subscription, off by default. Order is per key (e.g. Shopify order id) by arrival at the edge:
  partitioned queues (hash of key), one consumer per partition, one delivery in flight per key.
  A `dead` event in an ordered key blocks later events for that key until a human replays or skips it (Nov 24, decided).
  Built as a database gate (no earlier delivery of the same key may be pending, in flight, dead or failed), not as
  partitioned queues: same behaviour, one delivery in flight per key, parallel across keys.

## Backlog (decided Nov 24)
- Redis broker as an alternative to RabbitMQ (needs a broker abstraction in the edge and the workers), then the
  kill -9 comparison from spec scenario 15. The animated front end will let you switch between the two.


## Operator model (Nov 24)
One superuser does everything. No permission model, no roles, no multi-tenancy (spec Tier 4 D1 is intentionally not built).
Every manual action is still written to the audit log with the username. Dev login admin/admin is for local use only.

## Retention (Nov 24, confirmed)
Raw bodies are deleted after 30 days; ids, hashes, statuses and the attempt log are kept. The raw body is the only place
personal data (names, emails, addresses) lives. Never purged while a delivery is pending, in flight, dead or failed.

## Backpressure (Nov 24, my default, change if you disagree)
The edge keeps accepting and storing when the queue is deep (that is the promise: nothing accepted is lost). It never
returns 503 for depth. Protection comes from a per-source rate limit (429 + Retry-After), a queue-age alert and a
dead-count alert. 503 is only for "the broker did not confirm".

## Secrets and sources
The database is the source of truth for sources. Every change rewrites the file the edge reads (atomic rename), so adding
a source, rotating a secret or changing a rate limit reaches the edge in about a second. During a rotation a source holds
two secrets and a signature made with either one is accepted.

## Raw layer and corrections
The stored raw body is never edited. A human fix is an `event_corrections` row (a JSON object merged over the top-level
keys); delivery sends raw + corrections, re-serialised, with `X-Gateway-Corrected: true`. With no correction the
original bytes go out untouched.

## Silent sources
A source that goes quiet for longer than its normal gap raises an alert. Normal gap = 5 x the 90th-percentile gap
between its recent events at the same hour of the day (falls back to all hours with too little history), unless
`stale_after_seconds` is set on the source.

## Transports
RabbitMQ (default) and Redis are both supported at the edge and in the ingest worker. A request may choose its transport
with `X-Gateway-Broker` only when ALLOW_BROKER_OVERRIDE=1 (dev / the playground). Redis uses the reliable-queue pattern
(BLMOVE to a processing list, remove after commit) so ack-after-commit holds there too.

## Not built, on purpose
- Shadow mode (spec F4): needs the old scripts' real event record to compare against; nothing to compare with here.
- Multi-tenancy and roles (D1): single operator.
- Redis loss on a power cut or OS crash with `appendfsync everysec`: cannot be simulated with `kill -9` (see README).

## Ingest scaling (Nov 24, from the load test)
One ingest worker stores ~75 events/s; three store ~138/s (Postgres-bound). Dedup is safe across workers. Ordered
subscriptions need exactly one ingest worker because order is arrival order at ingest.
