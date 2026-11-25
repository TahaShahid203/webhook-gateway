"""Metrics from the database, as a dict (for the stats page / front end) and as Prometheus text."""
from datetime import datetime, timedelta, timezone

from django.db import connection
from django.db.models import Count

from .models import Alert, Consumer, Delivery, Event, EventConflict


def snapshot(now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    pending = Delivery.objects.filter(status=Delivery.Status.PENDING)
    due = pending.filter(next_attempt_at__lte=now)
    oldest = due.order_by("next_attempt_at").values_list("next_attempt_at", flat=True).first()

    by_consumer: dict = {}
    for row in Delivery.objects.values("consumer__name", "status").annotate(n=Count("id")):
        by_consumer.setdefault(row["consumer__name"], {})[row["status"]] = row["n"]

    hour_ago = now - timedelta(hours=1)
    success_rate = {}
    for name in by_consumer:
        recent = Delivery.objects.filter(consumer__name=name, created_at__gte=hour_ago)
        done = recent.filter(status__in=["delivered", "dead", "failed"]).count()
        ok = recent.filter(status="delivered").count()
        success_rate[name] = round(ok / done, 4) if done else None

    with connection.cursor() as cur:
        cur.execute(
            "SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (delivered_at - created_at))) "
            "FROM events_delivery WHERE status = 'delivered' AND delivered_at >= %s",
            [now - timedelta(minutes=15)])
        p95 = cur.fetchone()[0]

    return {
        "events_total": Event.objects.count(),
        "events_last_minute": Event.objects.filter(created_at__gte=now - timedelta(minutes=1)).count(),
        "events_quarantined": Event.objects.filter(status=Event.Status.QUARANTINED).count(),
        "event_conflicts": EventConflict.objects.count(),
        "queue_depth": due.count(),
        "in_flight": Delivery.objects.filter(status=Delivery.Status.IN_FLIGHT).count(),
        "oldest_queued_age_seconds": round((now - oldest).total_seconds(), 3) if oldest else 0.0,
        "delivery_p95_latency_seconds_15m": round(float(p95), 3) if p95 is not None else None,
        "deliveries_by_consumer": by_consumer,
        "dead_by_consumer": {n: c.get("dead", 0) for n, c in by_consumer.items()},
        "success_rate_1h": success_rate,
        "breaker": {c.name: c.breaker_state for c in Consumer.objects.all()},
        "open_alerts": Alert.objects.filter(resolved=False).count(),
    }


def prometheus_text(snap: dict) -> str:
    out = []

    def g(name, help_, value, labels=""):
        if value is None:
            return
        out.append(f"# HELP {name} {help_}\n# TYPE {name} gauge") if f"# TYPE {name} " not in "\n".join(out) else None
        out.append(f"{name}{labels} {value}")

    g("gateway_events_total", "Events stored", snap["events_total"])
    g("gateway_events_last_minute", "Events stored in the last minute", snap["events_last_minute"])
    g("gateway_events_quarantined", "Quarantined events", snap["events_quarantined"])
    g("gateway_event_conflicts", "Same id, different body", snap["event_conflicts"])
    g("gateway_queue_depth", "Deliveries due now and waiting", snap["queue_depth"])
    g("gateway_in_flight", "Deliveries being sent", snap["in_flight"])
    g("gateway_oldest_queued_age_seconds", "Age of the oldest due delivery", snap["oldest_queued_age_seconds"])
    g("gateway_delivery_p95_latency_seconds", "p95 receive-to-delivered latency, last 15 min",
      snap["delivery_p95_latency_seconds_15m"])
    g("gateway_open_alerts", "Unresolved alerts", snap["open_alerts"])
    for consumer, counts in snap["deliveries_by_consumer"].items():
        for status, n in counts.items():
            g("gateway_deliveries", "Deliveries by consumer and status", n,
              f'{{consumer="{consumer}",status="{status}"}}')
    for consumer, n in snap["dead_by_consumer"].items():
        g("gateway_dead_deliveries", "Dead deliveries per consumer", n, f'{{consumer="{consumer}"}}')
    for consumer, r in snap["success_rate_1h"].items():
        g("gateway_delivery_success_rate", "delivered / finished, last hour", r, f'{{consumer="{consumer}"}}')
    for consumer, state in snap["breaker"].items():
        g("gateway_breaker_open", "1 while the circuit breaker is open", 0 if state == "closed" else 1,
          f'{{consumer="{consumer}"}}')
    return "\n".join(out) + "\n"
