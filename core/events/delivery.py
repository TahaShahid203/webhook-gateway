"""Delivery engine: claim -> send -> finish. All state lives in Postgres; Celery only executes.

claim():  under row locks, check ordering gate, per-consumer in-flight cap and circuit breaker, then mark
          the delivery in_flight and log the attempt (so even a crash leaves a trace).
send():   POST the ORIGINAL body bytes with our signature, the stable event id and the attempt number.
finish(): record the outcome, update the breaker, schedule a retry or mark delivered / failed / dead.
"""
import hashlib
import hmac
import time
import urllib.error
import urllib.request
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from django.db import connection, transaction

from . import delivery_policy as policy
from .corrections import effective_body
from .models import Alert, Consumer, Delivery, DeliveryAttempt

OPEN_STATES = (Delivery.Status.PENDING, Delivery.Status.IN_FLIGHT, Delivery.Status.DEAD, Delivery.Status.FAILED)


def now() -> datetime:
    return datetime.now(timezone.utc)


def _breaker(c: Consumer) -> policy.CircuitBreaker:
    return policy.CircuitBreaker(state=c.breaker_state, consecutive_failures=c.breaker_failures,
                                 opened_at=c.breaker_opened_at, cooldown=c.breaker_cooldown)


def _save_breaker(c: Consumer, b: policy.CircuitBreaker) -> None:
    c.breaker_state, c.breaker_failures = b.state, b.consecutive_failures
    c.breaker_opened_at, c.breaker_cooldown = b.opened_at, b.cooldown
    c.save(update_fields=["breaker_state", "breaker_failures", "breaker_opened_at", "breaker_cooldown"])


def _advisory_lock(consumer_id: int, key: str) -> None:
    with connection.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", [zlib.crc32(f"order:{consumer_id}:{key}".encode())])


@dataclass
class Claimed:
    delivery_id: int
    attempt_id: int
    attempt_no: int
    url: str
    secret: str
    timeout: int
    body: bytes
    event_id: str
    source: str
    event_type: str
    content_type: str
    replayed: bool
    corrected: bool = False


def claim(delivery_id: int) -> Claimed | None:
    t = now()
    with transaction.atomic():
        d = (Delivery.objects.select_for_update(skip_locked=True, of=("self",))
             .select_related("event", "event__source")
             .filter(pk=delivery_id, status=Delivery.Status.PENDING, next_attempt_at__lte=t).first())
        if d is None:
            return None
        c = Consumer.objects.select_for_update().get(pk=d.consumer_id)
        if not c.active:
            return None

        if d.ordering_key:
            # one delivery in flight per key, strictly in arrival order; dead/failed ones block until a human acts
            _advisory_lock(c.id, d.ordering_key)
            if Delivery.objects.filter(consumer=c, ordering_key=d.ordering_key, id__lt=d.id,
                                       status__in=OPEN_STATES).exists():
                return None

        if Delivery.objects.filter(consumer=c, status=Delivery.Status.IN_FLIGHT).count() >= c.max_in_flight:
            return None                      # isolation: this consumer already uses its share of the workers

        b = _breaker(c)
        if not b.allow(t.timestamp()):       # last check: allow() spends the single half-open probe
            return None
        _save_breaker(c, b)

        d.status, d.in_flight_since = Delivery.Status.IN_FLIGHT, t
        d.attempts += 1
        d.save(update_fields=["status", "in_flight_since", "attempts"])
        attempt = DeliveryAttempt.objects.create(delivery=d, number=d.attempts, started_at=t,
                                                 replay_round=d.replay_count)
        e = d.event
        body, corrected = effective_body(e)
        return Claimed(d.id, attempt.id, d.attempts, c.url, c.secret, c.timeout_seconds, body,
                       str(e.id), e.source.slug, e.event_type, e.headers.get("content-type", "application/json"),
                       d.replayed, corrected)


def send(cl: Claimed) -> tuple[int | None, str, str, str | None]:
    """-> (status or None, error, response body (truncated), Retry-After header)"""
    ts = int(time.time())
    sig = hmac.new(cl.secret.encode(), f"{ts}.".encode() + cl.body, hashlib.sha256).hexdigest()
    req = urllib.request.Request(cl.url, data=cl.body, method="POST", headers={
        "Content-Type": cl.content_type,
        "X-Gateway-Event-Id": cl.event_id,
        "X-Gateway-Attempt": str(cl.attempt_no),
        "X-Gateway-Source": cl.source,
        "X-Gateway-Event-Type": cl.event_type,
        "X-Gateway-Replayed": "true" if cl.replayed else "false",
        "X-Gateway-Corrected": "true" if cl.corrected else "false",
        "X-Gateway-Signature": f"t={ts},v1={sig}",
    })
    try:
        with urllib.request.urlopen(req, timeout=cl.timeout) as r:
            return r.status, "", r.read(500).decode(errors="replace"), None
    except urllib.error.HTTPError as e:
        return e.code, "", e.read(500).decode(errors="replace"), e.headers.get("Retry-After")
    except Exception as e:                    # timeout, connection refused, DNS ...
        return None, f"{type(e).__name__}: {e}"[:300], "", None


def finish(cl: Claimed, status: int | None, error: str, body: str, retry_after: str | None) -> str:
    """Apply the result. Returns the delivery outcome."""
    t = now()
    with transaction.atomic():
        d = Delivery.objects.select_for_update().get(pk=cl.delivery_id)
        c = Consumer.objects.select_for_update().get(pk=d.consumer_id)
        b = _breaker(c)
        was_open = b.state == "open"
        before = b.state

        DeliveryAttempt.objects.filter(pk=cl.attempt_id).update(
            finished_at=t, status_code=status, error=error, response_body=body)
        outcome = policy.outcome_after(d.attempts, status)
        d.in_flight_since = None

        if outcome == "delivered":
            d.status, d.delivered_at = Delivery.Status.DELIVERED, t
            b.record(True, t.timestamp())
        elif outcome == "failed":
            d.status = Delivery.Status.FAILED
            Alert.objects.create(kind="delivery_failed", detail={
                "delivery": d.id, "consumer": c.name, "status": status})
        elif was_open:
            # the breaker opened while this one was in flight: stop it, don't burn an attempt, retry later
            d.attempts -= 1
            d.status = Delivery.Status.PENDING
            d.next_attempt_at = datetime.fromtimestamp(b.opened_at + b.cooldown, tz=timezone.utc)
            DeliveryAttempt.objects.filter(pk=cl.attempt_id).update(error="stopped: circuit breaker open")
            outcome = "stopped"
        else:
            b.record(False, t.timestamp())
            if outcome == "retry":
                d.status = Delivery.Status.PENDING
                d.next_attempt_at = t + timedelta(seconds=policy.next_delay(d.attempts, retry_after))
            else:
                d.status = Delivery.Status.DEAD
                Alert.objects.create(kind="delivery_dead", detail={
                    "delivery": d.id, "consumer": c.name, "event": str(d.event_id), "attempts": d.attempts})
        d.save()
        if b.state == "open" and before != "open":
            Alert.objects.create(kind="consumer_paused", detail={"consumer": c.name,
                                                                  "failures": b.consecutive_failures})
        _save_breaker(c, b)
    return outcome


def next_in_key(d_id: int) -> int | None:
    """After a delivery succeeds, the next event of the same ordering key can go right away."""
    d = Delivery.objects.get(pk=d_id)
    if not d.ordering_key:
        return None
    nxt = (Delivery.objects.filter(consumer_id=d.consumer_id, ordering_key=d.ordering_key,
                                   id__gt=d.id, status=Delivery.Status.PENDING).order_by("id").first())
    return nxt.id if nxt else None
