"""Replay and skip: the manual actions. Every one writes an AuditLog row (who, when, old -> new).

A replay re-delivers the SAME event: same event id, original raw bytes, header X-Gateway-Replayed: true.
For an ordered key, replaying an earlier dead event holds back later events of that key until it is done
(that is the point), and `skip` releases the key without delivering the dead event."""
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from django.db import transaction

from .audit import audit
from .models import Delivery

REPLAYABLE = (Delivery.Status.DELIVERED, Delivery.Status.DEAD, Delivery.Status.FAILED, Delivery.Status.SKIPPED)
SKIPPABLE = (Delivery.Status.DEAD, Delivery.Status.FAILED)
DEFAULT_RATE_PER_SECOND = 5.0


class ReplayRefused(Exception):
    pass


def replay_delivery(delivery_id: int, actor: str, at: datetime | None = None, reason: str = "") -> Delivery:
    """Re-deliver one event to one consumer. `at` lets a bulk replay spread deliveries out (rate limit)."""
    with transaction.atomic():
        d = Delivery.objects.select_for_update().select_related("event").get(pk=delivery_id)
        if d.status not in REPLAYABLE:
            raise ReplayRefused(f"delivery {d.pk} is {d.status}: only finished deliveries can be replayed")
        if d.event.raw_purged_at is not None:
            raise ReplayRefused(f"event {d.event_id} raw body was purged by retention on {d.event.raw_purged_at:%Y-%m-%d}")
        old = d.status
        d.status, d.attempts, d.replayed = Delivery.Status.PENDING, 0, True
        d.replay_count += 1
        d.next_attempt_at = at or datetime.now(timezone.utc)
        d.enqueued_at, d.in_flight_since, d.delivered_at = None, None, None
        d.save()
        audit(actor, "replay", "delivery", d.pk, old, d.status, event=str(d.event_id),
              consumer=d.consumer_id, replay_count=d.replay_count, reason=reason)
    return d


def skip_delivery(delivery_id: int, actor: str, reason: str = "") -> Delivery:
    """Decide this dead/failed delivery will never be sent. Releases an ordered key held behind it."""
    with transaction.atomic():
        d = Delivery.objects.select_for_update().get(pk=delivery_id)
        if d.status not in SKIPPABLE:
            raise ReplayRefused(f"delivery {d.pk} is {d.status}: only dead or failed deliveries can be skipped")
        old = d.status
        d.status = Delivery.Status.SKIPPED
        d.save(update_fields=["status"])
        audit(actor, "skip", "delivery", d.pk, old, d.status, event=str(d.event_id), reason=reason)
    return d


@dataclass
class BulkPlan:
    count: int
    ids: list
    rate_per_second: float

    @property
    def duration_seconds(self) -> float:
        return self.count / self.rate_per_second if self.rate_per_second else 0.0


def bulk_filter(consumer: str | None = None, status: str | None = None, source: str | None = None,
                since: datetime | None = None, until: datetime | None = None):
    qs = Delivery.objects.filter(status__in=REPLAYABLE, event__raw_purged_at__isnull=True)
    if consumer:
        qs = qs.filter(consumer__name=consumer)
    if status:
        qs = qs.filter(status=status)
    if source:
        qs = qs.filter(event__source__slug=source)
    if since:
        qs = qs.filter(created_at__gte=since)
    if until:
        qs = qs.filter(created_at__lt=until)
    return qs.order_by("id")


def plan_bulk_replay(rate_per_second: float = DEFAULT_RATE_PER_SECOND, **filters) -> BulkPlan:
    """The dry run: how many would be replayed and how long the rate limit stretches it. Changes nothing."""
    ids = list(bulk_filter(**filters).values_list("id", flat=True))
    return BulkPlan(len(ids), ids, rate_per_second)


def execute_bulk_replay(plan: BulkPlan, actor: str, filters_text: str = "") -> int:
    """Replay every delivery in the plan, spread out at the plan's rate so a recovering consumer is not flooded.
    Deliveries are replayed in id (arrival) order, which keeps ordered keys in sequence."""
    start = datetime.now(timezone.utc)
    done = 0
    for i, delivery_id in enumerate(plan.ids):
        try:
            replay_delivery(delivery_id, actor, at=start + timedelta(seconds=i / plan.rate_per_second),
                            reason="bulk")
            done += 1
        except ReplayRefused:
            continue            # changed state since the dry run; skip it, never fail the whole batch
    audit(actor, "bulk_replay", "delivery", "", "", done, filters=filters_text, planned=plan.count,
          rate_per_second=plan.rate_per_second)
    return done
