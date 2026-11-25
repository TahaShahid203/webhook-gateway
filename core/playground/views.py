"""The playground: a front end that drives and watches the REAL gateway. Nothing here is mocked.

fire    signs a real webhook (or a forged one) and POSTs it to the real edge
poll    reports what the real system did since the last poll: edge outcomes, rows in Postgres, delivery attempts,
        queue depth read from RabbitMQ / Redis themselves
control flips real switches: how the (fake) consumer behaves, and whether the ingest worker consumes
DEV ONLY: it signs with the dev secrets stored in the Source table. PLAYGROUND=0 turns it off."""
import base64
import hashlib
import hmac
import json
import random
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

import redis
from django.conf import settings
from django.http import Http404, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from events import controls, metrics
from events.audit import audit
from events.models import Delivery, DeliveryAttempt, Event, Source
from events.replay import ReplayRefused, replay_delivery

CONSUMER_MODES = {"ok": "ok", "flaky": "flaky:2", "down": "status:503", "slow": "slow:4"}
CONSUMER_PATHS = ("/demo", "/shop")
CONSUMER_FOR = {"stripe-a": "c-demo", "shop-a": "c-shop"}


def _guard():
    if not settings.PLAYGROUND:
        raise Http404


def _json(request) -> dict:
    return json.loads(request.body or b"{}")


def _stripe_headers(secret: str, body: bytes) -> dict:
    ts = int(time.time())
    sig = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return {"stripe-signature": f"t={ts},v1={sig}"}


def _shopify_headers(secret: str, body: bytes, wid: str) -> dict:
    mac = base64.b64encode(hmac.new(secret.encode(), body, hashlib.sha256).digest()).decode()
    return {"x-shopify-hmac-sha256": mac, "x-shopify-webhook-id": wid, "x-shopify-topic": "orders/create"}


def _make_shot(gun: str) -> tuple[str, bytes, dict, str, str]:
    """-> (source slug, body, headers, label, ref). `ref` is the id the system will know this event by."""
    stripe, shop = Source.objects.get(slug="stripe-a"), Source.objects.get(slug="shop-a")
    if gun == "stripe":
        ref = f"evt_{uuid.uuid4().hex[:12]}"
        body = json.dumps({"id": ref, "type": "payment_intent.succeeded",
                           "amount": random.choice([990, 2500, 4900, 12000])}).encode()
        return "stripe-a", body, _stripe_headers(stripe.secret, body), body.decode(), ref
    if gun == "shopify":
        wid = str(uuid.uuid4())
        body = json.dumps({"order": random.randint(1000, 9999), "total": random.choice([19, 42, 88, 120])}).encode()
        return "shop-a", body, _shopify_headers(shop.secret, body, wid), body.decode(), wid
    # malicious: three ways to fake a webhook
    slug, src, make = random.choice([("stripe-a", stripe, "stripe"), ("shop-a", shop, "shopify")])
    body = json.dumps({"id": f"evt_{uuid.uuid4().hex[:12]}", "type": "payment_intent.succeeded", "amount": 1}).encode()
    how = random.choice(["forged", "tampered", "unsigned"])
    if how == "unsigned":
        return slug, body, {}, how, ""
    secret = "not-the-secret" if how == "forged" else src.secret
    headers = (_stripe_headers(secret, body) if make == "stripe"
               else _shopify_headers(secret, body, str(uuid.uuid4())))
    if how == "tampered":
        body = body.replace(b'"amount": 1', b'"amount": 999999')       # signed one body, sent another
    return slug, body, headers, how, ""


def _post_edge(slug: str, body: bytes, headers: dict, broker: str, tag: str = "") -> tuple[int, float]:
    req = urllib.request.Request(f"{settings.EDGE_URL}/in/{slug}", data=body, method="POST", headers={
        "content-type": "application/json", "x-gateway-broker": broker, "x-play-tag": tag, **headers})
    t0 = time.perf_counter()
    try:
        status = urllib.request.urlopen(req, timeout=5).status
    except urllib.error.HTTPError as e:
        status = e.code
    except Exception:
        status = 0
    return status, (time.perf_counter() - t0) * 1000


@csrf_exempt
def fire(request):
    _guard()
    p = _json(request)
    gun = p.get("gun", "stripe")
    broker = p.get("broker", "rabbitmq") if p.get("broker") in ("rabbitmq", "redis", "redis_volatile") else "rabbitmq"
    count = max(1, min(int(p.get("count", 1)), 30))
    shots = []
    for _ in range(count):
        slug, body, headers, label, ref = _make_shot(gun)
        repeats = 2 if (p.get("duplicate") and gun != "malicious") else 1
        for i in range(repeats):
            status, ms = _post_edge(slug, body, headers, broker, str(p.get("tag", ""))[:40])
            shots.append({"gun": gun, "source": slug, "status": status, "ms": round(ms, 1), "broker": broker,
                          "label": label[:60], "duplicate": i == 1, "ref": ref})
    return JsonResponse({"shots": shots})


_rabbit = {"conn": None}


def _rabbit_depth():
    """Live message count of the ingest queue. A passive declare returns it straight from the broker; the
    management API only refreshes its statistics every 5 seconds, which made a filling queue look empty."""
    import pika
    for _ in range(2):                                   # once more with a fresh connection if the cached one died
        try:
            conn = _rabbit["conn"]
            if conn is None or conn.is_closed:
                conn = _rabbit["conn"] = pika.BlockingConnection(pika.URLParameters(settings.CELERY_BROKER_URL))
            ch = conn.channel()
            n = ch.queue_declare(queue="ingest", passive=True).method.message_count
            ch.close()
            return n
        except Exception:
            _rabbit["conn"] = None
    return None


def _queue_depths() -> dict:
    out = {"rabbitmq": _rabbit_depth(), "redis": None, "redis_volatile": None}
    for key, url in (("redis", settings.REDIS_URL), ("redis_volatile", settings.REDIS_VOLATILE_URL)):
        try:
            r = redis.Redis.from_url(url, socket_timeout=1)
            out[key] = r.llen("ingest") + r.llen("ingest:processing")
        except Exception:
            pass
    return out


def poll(request):
    _guard()
    q = request.GET
    now = datetime.now(timezone.utc)
    # edge: what it just did with each request
    # the first poll (no cursor yet) must not replay history: ask for "everything after the end" to learn the cursor
    since = int(q["edge"]) if "edge" in q else 10**12
    try:
        edge = json.load(urllib.request.urlopen(f"{settings.EDGE_URL}/_recent?since={since}", timeout=1.5))
    except Exception:
        edge = {"seq": int(q.get("edge", 0)), "items": [], "counters": {}}

    first = "attempt" not in q          # first poll only establishes cursors: nothing is replayed from history
    event_cur = datetime.fromisoformat(q["event_ts"]) if q.get("event_ts") else now
    dup_cur = datetime.fromisoformat(q["dup_ts"]) if q.get("dup_ts") else now
    # A cursor of 0 is a real cursor (a fresh stack has no attempts yet); only a MISSING one means "start from now".
    attempt_cur = int(q["attempt"]) if "attempt" in q else (DeliveryAttempt.objects.order_by("-id").values_list("id", flat=True).first() or 0)

    events = [] if first else list(Event.objects.filter(created_at__gt=event_cur).select_related("source")
                                   .order_by("created_at")[:60])
    dups = [] if first else list(Event.objects.filter(last_seen_at__gt=dup_cur).order_by("last_seen_at")[:60])
    attempts = []
    if not first:
        for a in (DeliveryAttempt.objects.filter(id__gt=attempt_cur)
                  .select_related("delivery__event", "delivery__consumer").order_by("id")[:100]):
            # An attempt row exists from the moment a worker CLAIMS the delivery; its result arrives milliseconds later.
            # Never move the cursor past one that is still in flight, or its result would never be reported (this
            # lost deliveries from the animation during bursts). One that has been open for 30 s is a dead worker's.
            if a.finished_at is None and (now - a.started_at).total_seconds() < 30:
                break
            attempts.append(a)

    snap = metrics.snapshot(now)
    return JsonResponse({
        "cursors": {
            "edge": edge["seq"],
            "attempt": max([a.id for a in attempts] + [attempt_cur]),
            "event_ts": (events[-1].created_at if events else event_cur).isoformat(),
            "dup_ts": (dups[-1].last_seen_at if dups else dup_cur).isoformat(),
        },
        "edge": edge,
        "events": [{"id": str(e.id), "short": str(e.id)[:8], "provider_id": e.provider_event_id, "source": e.source.slug,
                    "type": e.event_type, "status": e.status} for e in events],
        "duplicates": [{"id": str(e.id), "provider_id": e.provider_event_id, "count": e.duplicate_count} for e in dups],
        "attempts": [{"id": a.id, "event": str(a.delivery.event_id), "provider_id": a.delivery.event.provider_event_id,
                      "consumer": a.delivery.consumer.name, "number": a.number, "round": a.replay_round,
                      "status": a.status_code, "error": a.error[:60], "done": a.finished_at is not None}
                     for a in attempts],
        "queue": _queue_depths(),
        "snapshot": {k: snap[k] for k in ("events_total", "events_last_minute", "queue_depth", "in_flight",
                                          "delivery_p95_latency_seconds_15m", "dead_by_consumer", "breaker",
                                          "open_alerts")},
        "controls": {"ingest_paused": controls.ingest_paused(), "chaos": settings.PLAYGROUND_CHAOS,
                     "consumer_mode": controls.get("play_consumer_mode", "ok")},
    })


def _set_consumer_mode(mode: str) -> None:
    for path in CONSUMER_PATHS:
        req = urllib.request.Request(f"{settings.FAKE_CONSUMER_URL}/_control", method="POST",
                                     data=json.dumps({"path": path, "mode": CONSUMER_MODES[mode]}).encode())
        urllib.request.urlopen(req, timeout=3).read()
    controls.put("play_consumer_mode", mode)


@csrf_exempt
def control(request):
    _guard()
    p = _json(request)
    if p.get("consumer_mode") in CONSUMER_MODES:
        _set_consumer_mode(p["consumer_mode"])
        audit("playground", "consumer_mode", "consumer", "", "", p["consumer_mode"])
    if "ingest_paused" in p:
        controls.put("ingest_paused", "1" if p["ingest_paused"] else "0")
        audit("playground", "ingest_paused", "control", "ingest_paused", "", p["ingest_paused"])
    return JsonResponse({"ok": True})


@csrf_exempt
def replay_dead(request):
    _guard()
    n = 0
    for d in Delivery.objects.filter(status=Delivery.Status.DEAD).order_by("id")[:50]:
        try:
            replay_delivery(d.id, "playground", reason="replay button")
            n += 1
        except ReplayRefused:
            continue
    return JsonResponse({"replayed": n})


def index(request):
    _guard()
    return render(request, "playground/index.html", {})


# ---- chaos: a real kill -9 of a broker container, then a restart. Needs the Docker socket (docker-compose.chaos.yml). ----
import http.client
import socket
import threading
import urllib.parse

CHAOS_SERVICE = {"rabbitmq": "broker", "redis": "redis", "redis_volatile": "redis-volatile"}
_chaos_lock = threading.Lock()


class _DockerConn(http.client.HTTPConnection):
    def __init__(self):
        super().__init__("localhost")

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect("/var/run/docker.sock")


def _docker(method: str, path: str):
    c = _DockerConn()
    c.request(method, path)
    r = c.getresponse()
    return r.status, r.read()


def _find_container(service: str) -> str:
    _, me = _docker("GET", f"/containers/{socket.gethostname()}/json")
    project = json.loads(me)["Config"]["Labels"]["com.docker.compose.project"]
    flt = urllib.parse.quote(json.dumps({"label": [f"com.docker.compose.project={project}", f"com.docker.compose.service={service}"]}))
    _, body = _docker("GET", f"/containers/json?all=1&filters={flt}")
    found = json.loads(body)
    if not found:
        raise RuntimeError(f"no container for service {service}")
    return found[0]["Id"]


def _kill_then_restart(cid: str, down_seconds: float = 4.0):
    try:
        _docker("POST", f"/containers/{cid}/kill?signal=KILL")
        time.sleep(down_seconds)
        _docker("POST", f"/containers/{cid}/start")
    finally:
        _chaos_lock.release()


@csrf_exempt
def chaos(request):
    _guard()
    if not settings.PLAYGROUND_CHAOS:
        return JsonResponse({"ok": False, "error": "crash demo is off (start the demo with demo.sh to enable it)"}, status=403)
    target = _json(request).get("target")
    if target not in CHAOS_SERVICE:
        return JsonResponse({"ok": False, "error": "unknown transport"}, status=400)
    if not _chaos_lock.acquire(blocking=False):
        return JsonResponse({"ok": False, "error": "a crash is already in progress"}, status=409)
    try:
        cid = _find_container(CHAOS_SERVICE[target])
    except Exception as e:
        _chaos_lock.release()
        return JsonResponse({"ok": False, "error": f"cannot reach Docker: {e}"}, status=500)
    audit("playground", "kill_broker", "broker", target, "", "kill -9 then restart")
    threading.Thread(target=_kill_then_restart, args=(cid,), daemon=True).start()
    return JsonResponse({"ok": True, "target": target})
