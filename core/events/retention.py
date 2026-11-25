"""Retention: raw bodies are deleted after N days; metadata (ids, hashes, statuses, attempt log) is kept.

Never purged: events with a delivery still pending or in flight, and events with a dead or failed delivery
(a human still has to decide: replay needs the body)."""
from datetime import datetime, timedelta, timezone

from django.db.models import Exists, OuterRef

from .audit import audit
from .models import Delivery, Event

NEEDS_THE_BODY = (Delivery.Status.PENDING, Delivery.Status.IN_FLIGHT, Delivery.Status.DEAD, Delivery.Status.FAILED)


def purgeable(days: int, now: datetime | None = None):
    now = now or datetime.now(timezone.utc)
    needs = Delivery.objects.filter(event=OuterRef("pk"), status__in=NEEDS_THE_BODY)
    return (Event.objects.filter(created_at__lt=now - timedelta(days=days), raw_purged_at__isnull=True)
            .exclude(Exists(needs)))


def purge_raw_bodies(days: int, actor: str = "system", now: datetime | None = None, dry_run: bool = False) -> int:
    now = now or datetime.now(timezone.utc)
    qs = purgeable(days, now)
    n = qs.count()
    if dry_run or n == 0:
        return n
    qs.update(raw_body=b"", raw_purged_at=now)
    audit(actor, "purge_raw_bodies", "event", "", "", n, days=days)
    return n
