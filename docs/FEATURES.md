# Features

Back to the [README](../README.md). Decisions and reasons: [DECISIONS.md](DECISIONS.md).

| Area | What it does |
|---|---|
| Edge (FastAPI) | Verifies Stripe-style (timestamped HMAC) and Shopify-style (base64 HMAC) signatures on the **raw** body with a constant-time compare. 5-minute replay window checked against the time the edge *received* the request. Size and content-type limits. Per-source rate limit (429 + Retry-After). Two secrets valid at once during a rotation. Bad-signature log with a disk cap. |
| Dedup | Event id when the provider sends one, otherwise a hash of the body (only matching within 24 h). Same id + same body: dropped and counted. Same id + different body: kept in a conflicts table and an alert is raised. Dedup keys are never deleted. |
| Quarantine | Valid signature but a stale timestamp: stored, flagged, alerted, **never delivered**. |
| Delivery | Transactional outbox (delivery rows commit with the event). Retries: 14 attempts, 30 s doubling to a 1 h cap, +0-25% jitter (covers about 7 hours of outage), `Retry-After` honoured, other 4xx fail at once. Dead letters are kept forever and replayable. |
| Ordering | Opt-in per subscription, per key (e.g. one Shopify order id). One delivery in flight per key, in arrival order, parallel across keys. A dead event holds its key until a human replays or skips it. Needs a **single ingest worker** (see Scaling). |
| Admin | Event list with filters, raw body exactly as received, every delivery attempt, replay and skip actions, bulk replay with a dry-run confirmation page, CSV export, event corrections, audit log. |
| Corrections | The raw body is immutable. A fix is a separate row merged in at delivery time (`X-Gateway-Corrected: true`). |
| Operations | `/metrics` (Prometheus) and `/stats.json`. Alerts to a webhook: dead count, queue age, quarantined events, conflicts, paused consumers, **silent sources** (a source quiet for longer than its learned normal gap at that hour). Retention: raw bodies deleted after 30 days, metadata kept. |
| Transports | RabbitMQ or Redis, same promises, chosen per request in dev. |

