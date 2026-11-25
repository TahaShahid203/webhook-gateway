"""FastAPI edge: verify on the raw body, stamp received_at, queue durably, then 200.

No database call here. If the broker is down the answer is 503 so the provider retries.
"""
import asyncio
import collections
import os
import time

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from .publisher import ALERT_QUEUE, PublishError, Publisher, RabbitPublisher, RedisPublisher
from .rejection_log import RejectionLog
from .signatures import SCHEMES
from .sources import SourceStore, TokenBucket

MAX_BODY_BYTES = int(os.getenv("MAX_BODY_BYTES", 1_048_576))
# headers worth keeping with the event (the core also needs the provider's event id headers)
KEEP_HEADERS = ("content-type", "stripe-signature", "x-shopify-hmac-sha256",
                "x-shopify-webhook-id", "x-shopify-topic", "x-shopify-shop-domain")


def create_app(sources: SourceStore, publisher: Publisher, rejections: RejectionLog,
               now=time.time, publishers: dict | None = None, default_broker: str = "rabbitmq",
               allow_override: bool = False) -> FastAPI:
    """`publishers` maps a broker name to a Publisher. With allow_override (dev/playground only) a request may choose
    its transport with the X-Gateway-Broker header; otherwise every request uses `default_broker`."""
    app = FastAPI(title="webhook-gateway edge")
    publishers = publishers or {default_broker: publisher}
    buckets = TokenBucket(now)
    seq = {"n": 0}
    recent = collections.deque(maxlen=500)      # what the front end animates: the most recent requests
    counters = collections.Counter()

    def note(slug, status, outcome, broker=None, tag=None):
        seq["n"] += 1
        counters[outcome] += 1
        recent.append({"seq": seq["n"], "ts": time.time(), "source": slug, "status": status,
                       "outcome": outcome, "broker": broker, "tag": tag})

    def _alert(info: dict) -> None:
        # best effort: the broker may be the thing that is down
        async def send():
            try:
                await publisher.publish(str(info).encode(), {"kind": info["kind"]}, ALERT_QUEUE)
            except PublishError:
                pass
        asyncio.get_event_loop().create_task(send())

    rejections.alert = _alert

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    @app.get("/_recent")
    async def recent_requests(since: int = 0):
        return {"seq": seq["n"], "counters": dict(counters),
                "items": [r for r in recent if r["seq"] > since]}

    @app.post("/in/{slug}")
    async def receive(slug: str, request: Request):
        tag = request.headers.get("x-play-tag")        # dev only: lets the playground recognise its own shots
        t_received = now()
        received_at = int(t_received)          # whole seconds: what the replay-window check compares

        source = sources.get(slug)
        if source is None:
            note(slug, 404, "unknown_source", tag=tag)
            return JSONResponse({"error": "unknown source"}, status_code=404)
        if not buckets.allow(source):
            note(slug, 429, "rate_limited", tag=tag)
            return JSONResponse({"error": "rate limit exceeded"}, status_code=429, headers={"Retry-After": "1"})
        broker = default_broker
        if allow_override and request.headers.get("x-gateway-broker") in publishers:
            broker = request.headers["x-gateway-broker"]

        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > MAX_BODY_BYTES:
            return JSONResponse({"error": "body too large"}, status_code=413)
        if not request.headers.get("content-type", "").lower().startswith("application/json"):
            return JSONResponse({"error": "content-type must be application/json"}, status_code=415)

        body = await request.body()   # raw bytes, nothing parsed before verification
        if len(body) > MAX_BODY_BYTES:
            return JSONResponse({"error": "body too large"}, status_code=413)

        header_name, verify = SCHEMES[source.scheme]
        # during a secret rotation two secrets are valid at once, so no event signed with the old one is lost
        verdict = next((v for v in (verify(sec, request.headers.get(header_name), body, received_at)
                                    for sec in source.secrets) if v.valid), None)
        if verdict is None:
            rejections.record(slug, "bad_signature", request.client.host if request.client else None)
            note(slug, 401, "rejected", broker, tag)
            return JSONResponse({"error": "invalid signature"}, status_code=401)

        headers = {k: request.headers[k] for k in KEEP_HEADERS if k in request.headers}
        headers.update({"source": slug, "received_at": received_at, "received_ms": int(t_received * 1000),
                        "quarantined": bool(verdict.stale)})
        try:
            await publishers[broker].publish(body, headers)
        except PublishError:
            note(slug, 503, "unavailable", broker, tag)
            return JSONResponse({"error": "temporarily unavailable"}, status_code=503)

        # a stale-but-valid event is accepted (so the provider stops retrying) and flagged
        note(slug, 200, "quarantined" if verdict.stale else "accepted", broker, tag)
        return Response(status_code=200)

    return app


def build_default_app() -> FastAPI:
    sources = SourceStore(os.environ["SOURCES_FILE"])
    publishers = {"rabbitmq": RabbitPublisher(os.environ["BROKER_URL"])}
    if os.getenv("REDIS_URL"):
        publishers["redis"] = RedisPublisher(os.environ["REDIS_URL"])
    if os.getenv("REDIS_VOLATILE_URL"):                     # Redis with persistence off: the demo's contrast case
        publishers["redis_volatile"] = RedisPublisher(os.environ["REDIS_VOLATILE_URL"])
    default_broker = os.getenv("DEFAULT_BROKER", "rabbitmq")
    rejections = RejectionLog(os.getenv("REJECTION_LOG", "/var/log/edge/rejections.log"),
                              cap_bytes=int(os.getenv("REJECTION_LOG_CAP_BYTES", 50_000_000)),
                              sample_every=int(os.getenv("REJECTION_SAMPLE_EVERY", 100)))
    return create_app(sources, publishers[default_broker], rejections, publishers=publishers,
                      default_broker=default_broker, allow_override=os.getenv("ALLOW_BROKER_OVERRIDE") == "1")
