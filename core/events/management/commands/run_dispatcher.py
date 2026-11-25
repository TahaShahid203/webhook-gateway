"""Dispatcher: hands due deliveries to Celery and recovers deliveries orphaned by a dead worker.

The Delivery table is the truth. This loop is stateless, so it can crash and restart freely."""
import logging
import time
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Q

from events import alerts
from events.delivery import now
from events.models import Delivery, DeliveryAttempt
from events.tasks import deliver

log = logging.getLogger("dispatcher")


class Command(BaseCommand):
    help = "Run the delivery dispatcher"

    def handle(self, *args, **opts):
        logging.basicConfig(level=logging.INFO)
        log.info("dispatcher ready")
        last_alert_pass = 0.0
        while True:
            try:
                self.recover_orphans()
                self.enqueue_due()
                if time.monotonic() - last_alert_pass >= settings.ALERT_CHECK_EVERY_SECONDS:
                    last_alert_pass = time.monotonic()
                    alerts.check_thresholds()
                    alerts.notify_unsent()
            except Exception:
                log.exception("dispatcher pass failed; continuing")
            time.sleep(0.5)

    def recover_orphans(self):
        t = now()
        for d in Delivery.objects.select_related("consumer").filter(status=Delivery.Status.IN_FLIGHT):
            limit = d.consumer.timeout_seconds + settings.STUCK_GRACE_SECONDS
            if d.in_flight_since and (t - d.in_flight_since).total_seconds() > limit:
                # worker died mid-delivery: the attempt stays counted and visible; deliver again (at-least-once)
                DeliveryAttempt.objects.filter(delivery=d, finished_at__isnull=True).update(
                    finished_at=t, error="worker lost mid-delivery")
                Delivery.objects.filter(pk=d.pk, status=Delivery.Status.IN_FLIGHT).update(
                    status=Delivery.Status.PENDING, in_flight_since=None, next_attempt_at=t, enqueued_at=None)
                log.warning("recovered orphaned delivery %s", d.pk)

    def enqueue_due(self):
        t = now()
        cutoff = t - timedelta(seconds=settings.ENQUEUE_TTL_SECONDS)
        ids = list(Delivery.objects.filter(status=Delivery.Status.PENDING, next_attempt_at__lte=t)
                   .filter(Q(enqueued_at__isnull=True) | Q(enqueued_at__lt=cutoff))
                   .order_by("id").values_list("id", flat=True)[:200])
        if not ids:
            return
        Delivery.objects.filter(id__in=ids).update(enqueued_at=t)
        for i in ids:
            deliver.delay(i)
