"""Alerts: threshold checks and delivery of Alert rows to a webhook (Slack-style JSON POST).

Open an alert when a threshold is crossed, resolve it when the value drops back, and send each alert once."""
import json
import urllib.request
from datetime import datetime, timezone

from django.conf import settings

from .models import Alert, Consumer, Delivery, Event, Source

DEAD_KIND, QUEUE_KIND = "dead_count_high", "queue_age_high"


def _open(kind: str, key: str):
    return Alert.objects.filter(kind=kind, resolved=False, detail__key=key).first()


def check_thresholds(now: datetime | None = None, dead_threshold: int | None = None,
                     queue_age_seconds: float | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    dead_threshold = settings.ALERT_DEAD_THRESHOLD if dead_threshold is None else dead_threshold
    queue_age_seconds = settings.ALERT_QUEUE_AGE_SECONDS if queue_age_seconds is None else queue_age_seconds

    for c in Consumer.objects.all():
        dead = Delivery.objects.filter(consumer=c, status=Delivery.Status.DEAD).count()
        existing = _open(DEAD_KIND, c.name)
        if dead > dead_threshold and not existing:
            Alert.objects.create(kind=DEAD_KIND, detail={"key": c.name, "consumer": c.name, "dead": dead,
                                                         "threshold": dead_threshold})
        elif dead <= dead_threshold and existing:
            Alert.objects.filter(pk=existing.pk).update(resolved=True)

    oldest = (Delivery.objects.filter(status=Delivery.Status.PENDING, next_attempt_at__lte=now)
              .order_by("next_attempt_at").values_list("next_attempt_at", flat=True).first())
    age = (now - oldest).total_seconds() if oldest else 0.0
    check_silent_sources(now)
    existing = _open(QUEUE_KIND, "queue")
    if age > queue_age_seconds and not existing:
        Alert.objects.create(kind=QUEUE_KIND, detail={"key": "queue", "age_seconds": round(age, 1),
                                                      "threshold": queue_age_seconds})
    elif age <= queue_age_seconds and existing:
        Alert.objects.filter(pk=existing.pk).update(resolved=True)


def post_json(url: str, payload: dict, timeout: float = 5.0) -> bool:
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return 200 <= r.status < 300
    except Exception:
        return False


def notify_unsent(url: str | None = None, sender=post_json) -> int:
    """Send every alert not yet sent. A failure leaves it unsent, so the next pass retries."""
    url = url if url is not None else settings.ALERT_WEBHOOK_URL
    if not url:
        return 0
    sent = 0
    for a in Alert.objects.filter(notified_at__isnull=True).order_by("id"):
        payload = {"text": f"[gateway] {a.kind}: {json.dumps(a.detail)}", "kind": a.kind, "detail": a.detail,
                   "created_at": a.created_at.isoformat()}
        if sender(url, payload):
            Alert.objects.filter(pk=a.pk).update(notified_at=datetime.now(timezone.utc))
            sent += 1
        else:
            break
    return sent


SILENT_KIND = "source_silent"
MIN_EVENTS_TO_LEARN = 10


def learned_gap_threshold(source: Source, now: datetime) -> float | None:
    """How long a source may stay quiet before that is a failure. Learned from the gaps between its recent events
    at THIS hour of the day (a store with 3 events at night and 300 by day has different normals); falls back to
    all hours when that hour has too little history. threshold = 5 x the 90th-percentile gap. None = not enough data."""
    if source.stale_after_seconds:
        return float(source.stale_after_seconds)
    times = list(Event.objects.filter(source=source).order_by("-received_at").values_list("received_at", flat=True)[:300])
    if len(times) < MIN_EVENTS_TO_LEARN:
        return None
    gaps = [(a - b).total_seconds() for a, b in zip(times, times[1:])]
    same_hour = [g for g, ts in zip(gaps, times) if ts.hour == now.hour]
    pool = sorted(same_hour if len(same_hour) >= MIN_EVENTS_TO_LEARN else gaps)
    p90 = pool[min(len(pool) - 1, int(len(pool) * 0.9))]
    return max(5 * p90, 1.0)


def check_silent_sources(now: datetime | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    for src in Source.objects.filter(active=True):
        threshold = learned_gap_threshold(src, now)
        last = Event.objects.filter(source=src).order_by("-received_at").values_list("received_at", flat=True).first()
        if threshold is None or last is None:
            continue
        quiet = (now - last).total_seconds()
        existing = _open(SILENT_KIND, src.slug)
        if quiet > threshold and not existing:
            Alert.objects.create(kind=SILENT_KIND, detail={"key": src.slug, "source": src.slug,
                                                           "quiet_seconds": round(quiet), "normal_gap_limit": round(threshold)})
        elif quiet <= threshold and existing:
            Alert.objects.filter(pk=existing.pk).update(resolved=True)
